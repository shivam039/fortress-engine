# engine/main.py
from fastapi import APIRouter, BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import pandas as pd
import uvicorn
import sys
import os

# Ensure engine directory is in path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock_scanner.logic import (
    check_institutional_fortress,
    apply_advanced_scoring,
    get_stock_data,
    DEFAULT_SCORING_CONFIG
)
from stock_scanner.ui import generate_action_link
from mf_lab.logic import run_full_mf_scan
from mf_lab.jobs import run_mf_background_job
from commodities.logic import build_commodities_frame
from fortress_config import TICKER_GROUPS

import traceback
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fortress-api")

# API key auth — set FORTRESS_API_KEY env var to enable. Unset = local dev (no auth).
_FORTRESS_API_KEY = os.environ.get("FORTRESS_API_KEY", "")
if not _FORTRESS_API_KEY:
    logger.warning("FORTRESS_API_KEY is not set — FastAPI endpoints are unauthenticated. Set this env var in production.")

app = FastAPI(title="Fortress API", version="2.0")
mf_router = APIRouter(prefix="/mf", tags=["mutual-funds"])


@app.middleware("http")
async def api_key_auth_middleware(request, call_next):
    """Require X-API-Key header when FORTRESS_API_KEY env var is configured."""
    if _FORTRESS_API_KEY:
        # Health check and CORS preflight always pass
        if request.url.path not in ("/api/health",) and request.method != "OPTIONS":
            provided_key = request.headers.get("X-API-Key", "")
            if provided_key != _FORTRESS_API_KEY:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Unauthorized. Provide a valid X-API-Key header."},
                )
    return await call_next(request)


@app.middleware("http")
async def catch_exceptions_middleware(request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        # Full traceback is logged server-side — never exposed to the client
        logger.error(f"Unhandled exception on {request.method} {request.url.path}: {exc}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={
                "error": "An internal server error occurred. Please try again or contact support.",
                "path": str(request.url.path),
                # Error ID helps correlate with server logs without leaking internals
                "error_id": f"{hash(str(exc)) & 0xFFFFFF:06X}",
            },
        )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScanRequest(BaseModel):
    universe: str
    portfolio_val: float = 1000000
    risk_pct: float = 0.01
    weights: Optional[Dict[str, float]] = None
    enable_regime: bool = True
    liquidity_cr_min: float = 8.0
    market_cap_cr_min: float = 1500.0
    price_min: float = 80.0
    broker: str = "Zerodha"


class MFJobRequest(BaseModel):
    """Request body for async MF background jobs."""
    job_type: str = Field(
        ...,
        examples=["refresh_nav", "update_metrics", "full_refresh", "recalculate_rankings"],
    )
    force_refresh: bool = False
    scheme_codes: Optional[List[str]] = None

@app.get("/api/health")
def health_check():
    return {"status": "healthy", "version": "2.0"}

@app.get("/api/universes")
def get_universes():
    return list(TICKER_GROUPS.keys())

@app.post("/api/scan")
async def run_scan(req: ScanRequest):
    from stock_scanner.pulse import get_current_regime

    tickers = TICKER_GROUPS.get(req.universe)
    if not tickers:
        raise HTTPException(status_code=404, detail="Universe not found")

    # ── Fetch live market regime ONCE for the entire scan ──────────────────────
    try:
        regime_data = get_current_regime()
        logger.info(f"Scan regime: {regime_data['Market_Regime']} (x{regime_data['Regime_Multiplier']})")
    except Exception as e:
        logger.warning(f"Regime fetch failed, defaulting to Range: {e}")
        regime_data = {"Market_Regime": "Range", "Regime_Multiplier": 1.0, "VIX": 20.0}

    results = []
    batch_data = get_stock_data(tickers, period="1y", interval="1d", group_by="ticker")

    for ticker in tickers:
        try:
            hist = batch_data[ticker].dropna() if len(tickers) > 1 else batch_data.dropna()
            if not hist.empty and len(hist) >= 210:
                res = check_institutional_fortress(
                    ticker,
                    hist,
                    None,
                    req.portfolio_val,
                    req.risk_pct,
                    selected_universe=req.universe,
                    regime_data=regime_data,          # ← live regime passed
                )
                if res:
                    results.append(res)
        except Exception as e:
            logger.warning(f"Error scanning {ticker}: {e}")

    if not results:
        return {"results": [], "summary": "No tickers met criteria"}

    df = pd.DataFrame(results)
    scoring_config = DEFAULT_SCORING_CONFIG.copy()
    scoring_config.update({
        "enable_regime": req.enable_regime,
        "liquidity_cr_min": req.liquidity_cr_min,
        "market_cap_cr_min": req.market_cap_cr_min,
        "price_min": req.price_min,
        "regime": regime_data,                        # ← live regime for apply_advanced_scoring
    })
    if req.weights:
        scoring_config["weights"] = req.weights

    df = apply_advanced_scoring(df, scoring_config)

    # Generate action links
    df["Actions"] = df.apply(lambda row: generate_action_link(row, req.broker), axis=1)

    return df.to_dict(orient="records")


@app.get("/api/sector-pulse")
async def get_sector_pulse(universe: str = "Nifty 50"):
    # This logic replicates the "Sector Intelligence" from legacy ui.py
    tickers = TICKER_GROUPS.get(universe, [])
    if not tickers:
        raise HTTPException(status_code=404, detail="Universe not found")
    
    batch_data = get_stock_data(tickers, period="1y", interval="1d", group_by="ticker")
    results = []
    
    for ticker in tickers:
        try:
            hist = batch_data[ticker].dropna() if len(tickers) > 1 else batch_data.dropna()
            if not hist.empty and len(hist) >= 210:
                res = check_institutional_fortress(ticker, hist, None, 1000000, 0.01, selected_universe=universe)
                if res: results.append(res)
        except Exception: 
            continue
            
    if not results: return []
    
    df = pd.DataFrame(results)
    df = apply_advanced_scoring(df)
    
    if "Sector" not in df.columns or "Velocity" not in df.columns:
        return []

    sector_stats = df.groupby("Sector").agg({
        "Velocity": "mean",
        "Above_EMA200": "mean",
        "Score": "mean"
    }).reset_index()

    sector_stats["Breadth"] = (sector_stats["Above_EMA200"] * 100).round(1)
    sector_stats["Avg_Score"] = sector_stats["Score"].round(1)
    sector_stats["Velocity"] = sector_stats["Velocity"].round(2)
    
    # Thesis Generation
    def get_thesis(row):
        if row["Score"] > 75 and row["Velocity"] > 0:
            return "🐂 Bullish Accumulation"
        elif row["Score"] < 35 and row["Breadth"] < 40:
            return "❄️ Structural Weakness"
        elif row["Velocity"] > 2:
            return "🚀 High Momentum"
        else:
            return "⚖️ Neutral / Rotation"

    sector_stats["Thesis"] = sector_stats.apply(get_thesis, axis=1)

    # Classification
    def check_rise(row):
        if row['Velocity'] > 0 and row['Breadth'] > 70: return "🔥 YES"
        return ""

    def check_fall(row):
        if row['Velocity'] < 0 or row['Breadth'] < 40: return "❄️ YES"
        return ""

    sector_stats['On_the_Rise'] = sector_stats.apply(check_rise, axis=1)
    sector_stats['On_the_Fall'] = sector_stats.apply(check_fall, axis=1)
    
    return sector_stats.to_dict(orient="records")

@app.get("/api/mf-analysis")
async def get_mf_analysis(limit: Optional[int] = Query(None)):
    df = run_full_mf_scan(limit=limit)
    return df.to_dict(orient="records")


@mf_router.post("/trigger-job", status_code=202)
async def trigger_mf_job(req: MFJobRequest, background_tasks: BackgroundTasks):
    """
    Accepts a Mutual Fund processing job and immediately schedules it as a
    background task (HTTP 202 Accepted). The caller (Streamlit) is never blocked.

    Supported job types:
      - 'refresh_nav'
      - 'update_metrics'
      - 'full_refresh'
      - 'recalculate_rankings'
    """
    VALID_JOBS = {"refresh_nav", "full_refresh", "update_metrics", "recalculate_rankings"}
    if req.job_type not in VALID_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job_type '{req.job_type}'. Valid options: {sorted(VALID_JOBS)}"
        )

    background_tasks.add_task(
        run_mf_background_job,
        job_type=req.job_type,
        force_refresh=req.force_refresh,
        scheme_codes=req.scheme_codes,
    )

    logger.info(f"MF background job queued: {req.job_type} (force={req.force_refresh})")
    return {
        "status": "accepted",
        "job_type": req.job_type,
        "force_refresh": req.force_refresh,
        "scheme_codes": req.scheme_codes or [],
        "message": f"Job '{req.job_type}' is running on the server. Streamlit stays responsive."
    }

@app.get("/api/commodities")
async def get_commodities():
    df = build_commodities_frame()
    return df.to_dict(orient="records")

app.include_router(mf_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
