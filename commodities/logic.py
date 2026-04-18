import logging
import time

import numpy as np
import pandas as pd
import yfinance as yf

from fortress_config import COMMODITIES_TICKERS

logger = logging.getLogger(__name__)

COMMODITY_MAP = {
    "Gold": {"global": "GC=F", "local": "GOLDBEES.NS", "unit_adj": 1.0},
    "Silver": {"global": "SI=F", "local": "SILVERBEES.NS", "unit_adj": 1.0},
    "Crude": {"global": "CL=F", "local": "OIL.NS", "unit_adj": 1.0},
    "Copper": {"global": "HG=F", "local": "HINDCOPPER.NS", "unit_adj": 1.0},
}


def _retry(operation, module_name: str, retries: int = 3, base_delay: float = 1.0):
    last_error = None
    for attempt in range(retries):
        try:
            return operation()
        except Exception as e:
            last_error = e
            logger.error(f"{module_name} error: {e}")
            time.sleep(base_delay * (2 ** attempt))
    raise RuntimeError(f"{module_name} failed after retries") from last_error


def fetch_price_series(symbol: str, period: str = "6mo") -> pd.DataFrame:
    """Fetch OHLCV for a commodity symbol. Neon cache → yfinance fallback + UPSERT."""
    try:
        from utils.db import fetch_ohlcv_cache, upsert_ohlcv_cache
        cached = fetch_ohlcv_cache(symbol, period=period, max_age_hours=12)
        if cached is not None and not cached.empty:
            if isinstance(cached.columns, pd.MultiIndex):
                cached.columns = cached.columns.get_level_values(0)
            cols = {c.lower(): c for c in cached.columns}
            close_col = cols.get("close", list(cached.columns)[0])
            high_col  = cols.get("high",  close_col)
            low_col   = cols.get("low",   close_col)
            out = cached[[close_col, high_col, low_col]].copy()
            out.columns = ["close", "high", "low"]
            return out.dropna()
    except Exception:
        pass

    def _download():
        data = yf.download(symbol, period=period, progress=False, auto_adjust=True)
        if data.empty:
            return pd.DataFrame()
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        try:
            from utils.db import upsert_ohlcv_cache
            upsert_ohlcv_cache(symbol, period, data)
        except Exception:
            pass
        out = data[["Close", "High", "Low"]].copy()
        out.columns = ["close", "high", "low"]
        return out.dropna()

    return _retry(_download, f"commodities_{symbol}")


def compute_atr(df: pd.DataFrame, window: int = 14) -> float:
    if df.empty:
        return 0.0
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return float(tr.rolling(window).mean().iloc[-1]) if len(tr) > window else float(tr.mean())


from typing import Optional

def build_commodities_frame(selection: Optional[str] = None) -> pd.DataFrame:
    from utils.conviction_engine import score_commodity

    usdinr = fetch_price_series("INR=X", period="1mo")
    fx = float(usdinr["close"].iloc[-1]) if not usdinr.empty else 84.0

    rows = []
    for name, cfg in COMMODITY_MAP.items():
        if selection and name != selection:
            continue
        global_df = fetch_price_series(cfg["global"], period="6mo")
        local_df  = fetch_price_series(cfg["local"],  period="6mo")
        if global_df.empty or local_df.empty:
            continue

        g_price    = float(global_df["close"].iloc[-1])
        l_price    = float(local_df["close"].iloc[-1])
        parity     = g_price * fx * cfg["unit_adj"]
        spread_pct = ((l_price - parity) / (parity + 1e-9)) * 100
        atr        = compute_atr(local_df)
        vol        = local_df["close"].pct_change().std() * np.sqrt(252) * 100

        # Rich conviction scoring
        conviction = score_commodity(name, local_df, global_df, spread_pct, fx)

        rows.append({
            "Commodity":        name,
            "Price (₹)":       round(l_price, 2),
            "ATR":              round(atr, 2),
            "Volatility (Ann%)":round(vol, 1),
            "Spread %":         round(spread_pct, 2),
            "USDINR":           round(fx, 2),
            "1M Return %":      conviction["1M Return %"],
            "3M Return %":      conviction["3M Return %"],
            "6M Return %":      conviction["6M Return %"],
            "Trend":            conviction["Trend"],
            "ATR Regime":       conviction["ATR Regime"],
            "Conviction Score": conviction["Conviction Score"],
            "Conviction Label": conviction["Conviction Label"],
            "Conviction Emoji": conviction["Conviction Emoji"],
            "Decision":         conviction["Decision"],
            # Keep legacy field for DB compatibility
            "Global Symbol":    cfg["global"],
            "Local Symbol":     cfg["local"],
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.fillna(0).sort_values("Conviction Score", ascending=False)
