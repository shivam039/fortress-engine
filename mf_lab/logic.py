"""
mf_lab/logic.py — Fortress MF Consistency Engine
Speed improvements:
  1. NAV history cached in Neon (mf_nav_cache, 20h TTL) — no repeat HTTP hits
  2. Benchmark OHLCV cached in Neon (ohlcv_cache, 20h TTL)
  3. 30-worker ThreadPoolExecutor — true parallelism
  4. mfapi.in discovery response cached in-process for the session
  5. Per-fund timeout guard (10 s) prevents straggler funds blocking the pool
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf

from fortress_config import INDEX_BENCHMARKS, MF_SCHEMES

try:
    from mftool import Mftool
except Exception:
    Mftool = None

logger = logging.getLogger(__name__)

# ── In-process caches ────────────────────────────────────────────────────────
_BENCH_CACHE: Dict[str, pd.Series] = {}
_DISCOVERY_CACHE: Optional[List[str]] = None   # refreshed once per process


# ────────────────────────────────────────────────────────────────────────────
#  Utilities
# ────────────────────────────────────────────────────────────────────────────

def _retry(op, name: str, retries: int = 2, base_delay: float = 0.5):
    last = None
    for attempt in range(retries):
        try:
            return op()
        except Exception as e:
            last = e
            logger.debug("%s attempt %d: %s", name, attempt + 1, e)
            time.sleep(base_delay * (2 ** attempt))
    raise RuntimeError(f"{name} failed") from last


def classify_category(name: str) -> str:
    nm = (name or "").lower()
    if any(k in nm for k in ["liquid", "bond", "gilt", "debt", "duration"]):
        return "Debt"
    if any(k in nm for k in ["hybrid", "balanced", "asset allocation"]):
        return "Hybrid"
    return "Equity"


# ────────────────────────────────────────────────────────────────────────────
#  Benchmark helpers
# ────────────────────────────────────────────────────────────────────────────

def _get_benchmark_series(ticker: str) -> pd.Series:
    if ticker in _BENCH_CACHE:
        return _BENCH_CACHE[ticker]
    # 1. Neon OHLCV cache
    try:
        from utils.db import fetch_ohlcv_cache, upsert_ohlcv_cache
        cached = fetch_ohlcv_cache(ticker, period="5y", max_age_hours=20)
        if cached is not None and not cached.empty and "Close" in cached.columns:
            s = cached["Close"].pct_change().dropna()
            _BENCH_CACHE[ticker] = s
            return s
    except Exception:
        pass
    # 2. Live yfinance
    def _dl():
        d = yf.download(ticker, period="5y", interval="1d", progress=False, auto_adjust=True)
        if d.empty:
            return pd.Series(dtype=float)
        if isinstance(d.columns, pd.MultiIndex):
            d.columns = d.columns.get_level_values(0)
        try:
            from utils.db import upsert_ohlcv_cache
            upsert_ohlcv_cache(ticker, "5y", d)
        except Exception:
            pass
        close_series = d.get("Close")
        if close_series is None or close_series.empty:
            close_series = d.iloc[:, 0]
        return close_series.pct_change().dropna()

    s = _retry(_dl, f"bench_{ticker}")
    _BENCH_CACHE[ticker] = s
    return s


@st.cache_data(ttl=1800)
def fetch_benchmark_returns(ticker: str = INDEX_BENCHMARKS.get("Nifty 50", "^NSEI")) -> pd.Series:
    return _get_benchmark_series(ticker)


# ────────────────────────────────────────────────────────────────────────────
#  NAV History — Neon-cached with graceful fallback
# ────────────────────────────────────────────────────────────────────────────

def fetch_nav_history(scheme_code: str, max_age_hours: int = 20) -> pd.DataFrame:
    """
    Priority: Neon mf_nav_cache → mfapi.in live + UPSERT back to Neon.
    """
    code = str(scheme_code)

    # 1. Neon cache
    try:
        from utils.db import fetch_mf_nav_cache, upsert_mf_nav_cache
        cached = fetch_mf_nav_cache(code, max_age_hours=max_age_hours)
        if cached is not None and not cached.empty:
            return cached
    except Exception:
        pass

    # 2. Live mfapi.in
    def _load():
        resp = requests.get(f"https://api.mfapi.in/mf/{code}", timeout=10)
        resp.raise_for_status()
        rows = resp.json().get("data", [])
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")
        df["nav"]  = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["date", "nav"]).sort_values("date").set_index("date")
        df = df.asfreq("B").ffill()
        df["ret"] = df["nav"].pct_change()
        return df.dropna()

    try:
        df = _retry(_load, f"nav_{code}", retries=2, base_delay=0.3)
        # Back-fill Neon cache
        try:
            from utils.db import upsert_mf_nav_cache
            upsert_mf_nav_cache(code, df)
        except Exception:
            pass
        return df
    except Exception as e:
        logger.debug("fetch_nav_history %s: %s", code, e)
        return pd.DataFrame()


def backtest_vs_benchmark(scheme_code: str) -> pd.DataFrame:
    fund = fetch_nav_history(scheme_code)
    bench = fetch_benchmark_returns()
    if fund.empty or bench.empty:
        return pd.DataFrame()
    merged = pd.DataFrame({"fund": fund["ret"], "bench": bench}).dropna()
    if merged.empty:
        return pd.DataFrame()
    out = (1 + merged).cumprod()
    out.columns = ["Fund", "Nifty 50"]
    return out


# ────────────────────────────────────────────────────────────────────────────
#  Per-fund scorer (runs inside thread)
# ────────────────────────────────────────────────────────────────────────────

def _score_fund(code: str, bench_default: pd.Series) -> Optional[Dict[str, Any]]:
    try:
        history = fetch_nav_history(str(code))
        if history.empty:
            return None

        nav          = float(history["nav"].iloc[-1])
        scheme_name  = f"Scheme {code}"

        bench_ticker = INDEX_BENCHMARKS.get("Nifty 50", "^NSEI")
        nm = scheme_name.lower()
        if "small cap" in nm or "smallcap" in nm:
            bench_ticker = INDEX_BENCHMARKS.get("Nifty Smallcap 250", "^CNXSC")
        elif "mid cap" in nm or "midcap" in nm:
            bench_ticker = INDEX_BENCHMARKS.get("Nifty Midcap 150", "^NSMIDCP")

        bench = bench_default if bench_ticker == INDEX_BENCHMARKS.get("Nifty 50") \
                else _get_benchmark_series(bench_ticker)
        ret = history["ret"].dropna()

        alpha = beta = np.nan
        if not bench.empty and not ret.empty:
            combined = pd.concat([ret, bench], axis=1, join="inner").dropna()
            if len(combined) > 60:
                cov = np.cov(combined.iloc[:, 0], combined.iloc[:, 1])
                var = np.var(combined.iloc[:, 1])
                beta  = cov[0, 1] / var if var else 1.0
                rp    = combined.iloc[:, 0].mean() * 252
                rm    = combined.iloc[:, 1].mean() * 252
                alpha = ((rp - 0.06) - beta * (rm - 0.06)) * 100

        n = len(history)
        ret_1y = history["nav"].pct_change(252).iloc[-1] * 100 if n > 252 else np.nan
        ret_3y = ((history["nav"].iloc[-1] / history["nav"].iloc[-min(756, n)])  ** (252 / min(756, n))  - 1) * 100 if n > 252 else np.nan
        ret_5y = ((history["nav"].iloc[-1] / history["nav"].iloc[-min(1260, n)]) ** (252 / min(1260, n)) - 1) * 100 if n > 252 else np.nan

        vol       = ret.std() * np.sqrt(252) * 100
        downside  = ret[ret < 0].std() * np.sqrt(252) * 100
        sharpe    = ((ret.mean() * 252) - 0.06) / (ret.std() * np.sqrt(252) + 1e-9)
        sortino   = ((ret.mean() * 252) - 0.06) / ((ret[ret < 0].std() * np.sqrt(252)) + 1e-9)
        roll_std  = ret.rolling(21).std().mean() * np.sqrt(252) * 100

        return {
            "Scheme Code": code,
            "Scheme":      scheme_name,
            "NAV":         nav,
            "1Y Return":   ret_1y,
            "3Y Return":   ret_3y,
            "5Y Return":   ret_5y,
            "Volatility":  vol,
            "Downside Deviation": downside,
            "Sharpe":      sharpe,
            "Sortino":     sortino,
            "Rolling Std": roll_std,
            "Alpha":       alpha,
            "Beta":        beta,
            "Benchmark":   bench_ticker,
        }
    except Exception as e:
        logger.debug("_score_fund %s: %s", code, e)
        return None


# ────────────────────────────────────────────────────────────────────────────
#  Fund Discovery
# ────────────────────────────────────────────────────────────────────────────

def discover_all_funds(limit: Optional[int] = None) -> List[str]:
    global _DISCOVERY_CACHE
    if _DISCOVERY_CACHE is not None:
        return _DISCOVERY_CACHE[:limit] if limit else _DISCOVERY_CACHE

    try:
        resp = requests.get("https://api.mfapi.in/mf", timeout=20)
        resp.raise_for_status()
        schemes = resp.json()

        equity_kw = ["flexi", "multi", "large", "mid", "small", "focused", "value", "contra", "elss"]
        debt_kw   = ["liquid", "gilt", "bond", "duration", "overnight", "corporate"]
        all_kw    = equity_kw + debt_kw

        codes = []
        for s in schemes:
            name = s["schemeName"].lower()
            if not all(r in name for r in ["direct", "growth"]):  continue
            if not any(k in name for k in all_kw):               continue
            if any(e in name for e in ["regular", "idcw"]):      continue
            if "etf" in name and not any(k in name for k in debt_kw): continue
            codes.append(str(s["schemeCode"]))

        _DISCOVERY_CACHE = codes
        logger.info("discover_all_funds: found %d schemes", len(codes))
        return codes[:limit] if limit else codes

    except Exception as e:
        logger.error("discover_all_funds failed: %s", e)
        return [str(c) for c in MF_SCHEMES]


# ────────────────────────────────────────────────────────────────────────────
#  Pre-seed NAV cache from Neon  (warm up before parallel scoring)
# ────────────────────────────────────────────────────────────────────────────

def _bulk_preseed_nav_cache(codes: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Single SQL query to pull ALL non-stale NAV rows from Neon in one shot.
    Avoids N×SELECT inside threads and cuts cold-start latency dramatically.
    """
    if not codes:
        return {}
    try:
        from utils.db import _can_use_neon, get_db_engine
        from sqlalchemy import text as sa_text
        if not _can_use_neon():
            return {}

        engine = get_db_engine()
        placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
        params = {f"c{i}": c for i, c in enumerate(codes)}

        with engine.connect() as conn:
            rows = conn.execute(
                sa_text(f"""
                    SELECT scheme_code, nav_json
                    FROM mf_nav_cache
                    WHERE scheme_code IN ({placeholders})
                      AND updated_at >= NOW() - INTERVAL '20 hours'
                """),
                params
            ).fetchall()

        cache: Dict[str, pd.DataFrame] = {}
        import json as _json
        for r in rows:
            try:
                df = pd.read_json(_json.dumps(r[1]), orient="split")
                df.index = pd.to_datetime(df.index)
                cache[r[0]] = df
            except Exception:
                pass
        logger.info("_bulk_preseed_nav_cache: preloaded %d/%d funds from Neon", len(cache), len(codes))
        return cache
    except Exception as e:
        logger.error("_bulk_preseed_nav_cache error: %s", e)
        return {}


# ────────────────────────────────────────────────────────────────────────────
#  Full Parallel MF Scan
# ────────────────────────────────────────────────────────────────────────────

# Thread-local NAV cache (populated once per session key)
_NAV_MEM_CACHE: Dict[str, pd.DataFrame] = {}


def _score_fund_fast(code: str, bench_default: pd.Series) -> Optional[Dict[str, Any]]:
    """Same as _score_fund but reads nav from the in-memory cache first."""
    global _NAV_MEM_CACHE
    try:
        # Use pre-seeded memory cache if available
        if code in _NAV_MEM_CACHE:
            history = _NAV_MEM_CACHE[code]
        else:
            history = fetch_nav_history(code)          # Neon → mfapi
            _NAV_MEM_CACHE[code] = history

        if history is None or history.empty:
            return None

        nav         = float(history["nav"].iloc[-1])
        scheme_name = f"Scheme {code}"
        bench_ticker = INDEX_BENCHMARKS.get("Nifty 50", "^NSEI")
        nm = scheme_name.lower()
        if "small cap" in nm or "smallcap" in nm:
            bench_ticker = INDEX_BENCHMARKS.get("Nifty Smallcap 250", "^CNXSC")
        elif "mid cap" in nm or "midcap" in nm:
            bench_ticker = INDEX_BENCHMARKS.get("Nifty Midcap 150", "^NSMIDCP")

        bench = bench_default if bench_ticker == INDEX_BENCHMARKS.get("Nifty 50") \
                else _get_benchmark_series(bench_ticker)
        ret = history["ret"].dropna()

        alpha = beta = np.nan
        if not bench.empty and not ret.empty:
            combined = pd.concat([ret, bench], axis=1, join="inner").dropna()
            if len(combined) > 60:
                cov  = np.cov(combined.iloc[:, 0], combined.iloc[:, 1])
                var  = np.var(combined.iloc[:, 1])
                beta = cov[0, 1] / var if var else 1.0
                rp   = combined.iloc[:, 0].mean() * 252
                rm   = combined.iloc[:, 1].mean() * 252
                alpha = ((rp - 0.06) - beta * (rm - 0.06)) * 100

        n      = len(history)
        nav_s  = history["nav"]
        ret_1y = nav_s.pct_change(252).iloc[-1] * 100         if n > 252 else np.nan
        ret_3y = ((nav_s.iloc[-1]/nav_s.iloc[-min(756, n)])  **(252/min(756, n))  -1)*100 if n > 252 else np.nan
        ret_5y = ((nav_s.iloc[-1]/nav_s.iloc[-min(1260,n)]) **(252/min(1260,n)) -1)*100 if n > 252 else np.nan

        vol      = ret.std() * np.sqrt(252) * 100
        downside = ret[ret < 0].std() * np.sqrt(252) * 100
        sharpe   = ((ret.mean()*252) - 0.06) / (ret.std()*np.sqrt(252) + 1e-9)
        sortino  = ((ret.mean()*252) - 0.06) / (ret[ret<0].std()*np.sqrt(252) + 1e-9)
        roll_std = ret.rolling(21).std().mean() * np.sqrt(252) * 100

        return {
            "Scheme Code": code, "Scheme": scheme_name, "NAV": nav,
            "1Y Return": ret_1y, "3Y Return": ret_3y, "5Y Return": ret_5y,
            "Volatility": vol, "Downside Deviation": downside,
            "Sharpe": sharpe, "Sortino": sortino, "Rolling Std": roll_std,
            "Alpha": alpha, "Beta": beta, "Benchmark": bench_ticker,
        }
    except Exception as e:
        logger.debug("_score_fund_fast %s: %s", code, e)
        return None


def run_full_mf_scan(
    progress_callback=None,
    max_workers: int = 30,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Full parallel MF scan with:
      - Bulk NAV pre-seed from Neon (single SELECT for all codes)
      - 30-worker ThreadPoolExecutor
      - Per-fund 10-second timeout guard
      - Automatic UPSERT to Neon at completion
    """
    global _NAV_MEM_CACHE
    codes  = discover_all_funds(limit=limit)
    total  = len(codes)
    logger.info("run_full_mf_scan: %d funds, %d workers", total, max_workers)

    # 1. Warm benchmarks first (blocking, but tiny — only 1-3 tickers)
    bench_default = _get_benchmark_series(INDEX_BENCHMARKS.get("Nifty 50", "^NSEI"))

    # 2. Bulk pre-seed NAV cache from Neon (ONE query for all funds)
    seeded = _bulk_preseed_nav_cache(codes)
    _NAV_MEM_CACHE.update(seeded)
    logger.info("Pre-seeded %d navs from Neon", len(seeded))

    rows: List[Dict[str, Any]] = []
    done = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_score_fund_fast, c, bench_default): c for c in codes}
        for future in as_completed(futures):
            done += 1
            try:
                result = future.result(timeout=10)
                if result:
                    rows.append(result)
            except Exception:
                pass
            if progress_callback:
                progress_callback(done, total)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).fillna(0)
    df["Category"] = df["Scheme"].apply(classify_category)
    vol_penalty = (df["Volatility"] + df["Downside Deviation"] + df["Rolling Std"]).clip(lower=0)
    raw = (df["Sharpe"] + df["Sortino"] - vol_penalty / 100).fillna(0)
    mn, mx = raw.min(), raw.max()
    df["Consistency Score"] = 50.0 if mx == mn else ((raw - mn) / (mx - mn) * 100).clip(0, 100)

    # ── Conviction Enrichment (Decision Quality) ─────────────────────
    try:
        from utils.conviction_engine import enrich_mf_dataframe
        df = enrich_mf_dataframe(df)
        logger.info("run_full_mf_scan: enriched with conviction scores")
    except Exception as e:
        logger.error(f"Conviction enrichment failed: {e}")

    df = df.sort_values("Conviction Score", ascending=False).reset_index(drop=True)

    try:
        from utils.db import upsert_mf_scan_results
        upsert_mf_scan_results(df)
        logger.info("run_full_mf_scan: persisted %d funds", len(df))
    except Exception as e:
        logger.error("Neon persist failed: %s", e)

    return df


# ── Legacy aliases ────────────────────────────────────────────────────────────

def fetch_mf_snapshot(scheme_codes: List[str]) -> pd.DataFrame:
    bench = _get_benchmark_series(INDEX_BENCHMARKS.get("Nifty 50", "^NSEI"))
    rows  = [r for c in scheme_codes if (r := _score_fund(str(c), bench)) is not None]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).fillna(0)
    vol_p = (df["Volatility"] + df["Downside Deviation"] + df["Rolling Std"]).clip(lower=0)
    raw   = (df["Sharpe"] + df["Sortino"] - vol_p / 100).fillna(0)
    mn, mx = raw.min(), raw.max()
    df["Consistency Score"] = 50.0 if mx == mn else ((raw - mn) / (mx - mn) * 100).clip(0, 100)
    return df.sort_values("Consistency Score", ascending=False).reset_index(drop=True)


_score_fund = _score_fund_fast  # alias
DEFAULT_SCHEMES = MF_SCHEMES
