# engine/main.py
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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
from mf_lab.logic import run_full_mf_scan, fetch_nav_history
from commodities.logic import build_commodities_frame
from fortress_config import TICKER_GROUPS

import traceback
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fortress-api")

app = FastAPI(title="Fortress API", version="2.0")

@app.middleware("http")
async def catch_exceptions_middleware(request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        logger.error(f"Unhandled exception: {exc}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"detail": str(exc), "traceback": traceback.format_exc().splitlines()[-3:]}
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

@app.get("/api/health")
def health_check():
    return {"status": "healthy", "version": "2.0"}

@app.get("/api/universes")
def get_universes():
    return list(TICKER_GROUPS.keys())

@app.post("/api/scan")
async def run_scan(req: ScanRequest):
    tickers = TICKER_GROUPS.get(req.universe)
    if not tickers:
        raise HTTPException(status_code=404, detail="Universe not found")
    
    # Simple synchronous scan for now (as in Streamlit version)
    results = []
    
    # Batch download to speed up
    batch_data = get_stock_data(tickers, period="1y", interval="1d", group_by="ticker")
    
    for ticker in tickers:
        try:
            hist = batch_data[ticker].dropna() if len(tickers) > 1 else batch_data.dropna()
            if not hist.empty and len(hist) >= 210:
                res = check_institutional_fortress(
                    ticker,
                    hist,
                    None, # tkr_obj not strictly used for core logic if hist is provided
                    req.portfolio_val,
                    req.risk_pct,
                    selected_universe=req.universe
                )
                if res:
                    results.append(res)
        except Exception as e:
            print(f"Error scanning {ticker}: {e}")
            
    if not results:
        return {"results": [], "summary": "No tickers met criteria"}
    
    df = pd.DataFrame(results)
    scoring_config = DEFAULT_SCORING_CONFIG.copy()
    scoring_config.update({
        "enable_regime": req.enable_regime,
        "liquidity_cr_min": req.liquidity_cr_min,
        "market_cap_cr_min": req.market_cap_cr_min,
        "price_min": req.price_min,
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

@app.get("/api/commodities")
async def get_commodities():
    df = build_commodities_frame()
    return df.to_dict(orient="records")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
