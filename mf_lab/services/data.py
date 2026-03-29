import requests
import time
import pandas as pd
import yfinance as yf
from mf_lab.services.config import logger, API_TIMEOUT, MAX_RETRIES, RETRY_DELAY
from utils.db import get_cached_benchmark, save_benchmark_data

# --- BENCHMARK SERVICES ---

def fetch_benchmark_data(ticker):
    """
    Fetches benchmark data (Close & Returns).
    Priority: 1) Neon OHLCV cache  2) SQLite cache  3) yfinance live fetch + UPSERT.
    """
    try:
        from utils.db import fetch_ohlcv_cache, upsert_ohlcv_cache
        neon_df = fetch_ohlcv_cache(ticker, period="5y", max_age_hours=20)
        if neon_df is not None and not neon_df.empty and "Close" in neon_df.columns:
            if "ret" not in neon_df.columns:
                neon_df["ret"] = neon_df["Close"].pct_change()
            return neon_df[["Close", "ret"]].dropna()
    except Exception:
        pass

    try:
        # 1. Try SQLite cache
        cached_df = get_cached_benchmark(ticker)
        if not cached_df.empty:
            last_date = cached_df.index.max()
            today = pd.Timestamp.now().normalize()
            if last_date >= today - pd.Timedelta(days=1):
                return cached_df

        # 2. Fetch from yfinance
        nifty = yf.download(ticker, period="5y", interval="1d", progress=False)
        if nifty.empty:
            return cached_df

        if isinstance(nifty.columns, pd.MultiIndex):
            nifty.columns = nifty.columns.get_level_values(0)

        nifty["ret"] = nifty["Close"].pct_change()
        data_to_save = nifty[["Close", "ret"]].dropna()

        # 3. Update SQLite cache
        save_benchmark_data(ticker, data_to_save)

        # 4. Upsert into Neon for next time
        try:
            from utils.db import upsert_ohlcv_cache
            upsert_ohlcv_cache(ticker, "5y", nifty)
        except Exception:
            pass

        return data_to_save

    except Exception as e:
        logger.error(f"Error fetching benchmark {ticker}: {e}")
        return pd.DataFrame()

# --- FUND DISCOVERY & NAV ---

def safe_api_get(url, params=None):
    """Robust API GET with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=API_TIMEOUT)
            if resp.status_code == 200:
                return resp
            # If 429, wait longer
            if resp.status_code == 429:
                time.sleep(RETRY_DELAY * (attempt + 2))
            else:
                time.sleep(RETRY_DELAY)
        except requests.RequestException as e:
            logger.warning(f"API Request failed ({attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_DELAY)

    return None

def discover_funds(limit=None):
    """
    Auto-discovers Direct Growth Funds from mfapi.in.
    Filters: Direct, Growth.
    Includes Debt Keywords and ETFs for Debt.
    """
    try:
        url = "https://api.mfapi.in/mf"
        resp = safe_api_get(url)
        if not resp:
            logger.error("Failed to discover funds (API Down)")
            return []

        schemes = resp.json()

        # Keywords
        equity_keywords = ["flexi", "multi", "large", "mid", "small", "focused", "value", "contra", "elss"]
        debt_keywords = ["liquid", "gilt", "bond", "duration", "overnight", "corporate"]
        all_keywords = equity_keywords + debt_keywords

        required = ["direct", "growth"]
        base_exclusions = ["regular", "idcw"]

        candidates = []
        for s in schemes:
            name = s['schemeName'].lower()

            if not all(req in name for req in required): continue
            if not any(k in name for k in all_keywords): continue
            if any(ex in name for ex in base_exclusions): continue

            # ETF Logic
            is_etf = "etf" in name
            is_debt = any(k in name for k in debt_keywords)

            if is_etf and not is_debt:
                continue

            candidates.append(s)

        if limit:
            return candidates[:limit]
        return candidates
    except Exception as e:
        logger.error(f"Discovery logic error: {e}")
        return []

def fetch_fund_nav(scheme_code):
    """
    Fetches NAV history for a single fund.
    Returns cleaned DataFrame with 'date', 'nav', 'ret'.
    """
    try:
        url = f"https://api.mfapi.in/mf/{scheme_code}"
        resp = safe_api_get(url)
        if not resp: return pd.DataFrame()

        data = resp.json()
        if not data.get('data'): return pd.DataFrame()

        df = pd.DataFrame(data['data'])
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
        df['nav'] = df['nav'].astype(float)
        df = df.sort_values('date')

        # Date Alignment & Filling (User Req 4)
        # Create continuous business day series
        df = df.set_index('date')
        df = df.asfreq('B') # Business Days
        df['nav'] = df['nav'].ffill() # Fill holidays

        # Calculate Returns
        df['ret'] = df['nav'].pct_change()

        return df[['nav', 'ret']].dropna()

    except Exception as e:
        logger.error(f"Error fetching NAV for {scheme_code}: {e}")
        return pd.DataFrame()
