import numpy as np
import pandas as pd
import pandas_ta as ta
import datetime
import streamlit as st
import yfinance as yf
from fortress_config import (
    SECTOR_MAP,
    NIFTY_SYMBOL,
    SECTOR_ROTATION_BONUS_POINTS,
    TOP_SECTOR_COUNT,
    SMALLCAP_LIQUIDITY_MIN_CR,
    TICKER_GROUPS,
)
from datetime import datetime

_BENCHMARK_CACHE = {}

DEFAULT_SCORING_CONFIG = {
    "weights": {"technical": 0.50, "fundamental": 0.25, "sentiment": 0.15, "context": 0.10},
    "liquidity_cr_min": 8.0,
    "market_cap_cr_min": 1500.0,
    "price_min": 80.0,
    "max_debt_to_equity": 2.0,
    "min_interest_coverage": 3.0,
    "enable_regime": True,
}

REGIME_LABELS = {
    "Bull": "🟢 Bull",
    "Range": "🟡 Range",
    "Bear": "🔴 Bear",
}


def _safe_float(value, default=0.0):
    try:
        val = float(value)
        return default if pd.isna(val) else val
    except:
        return default


@st.cache_data(ttl="10m")
def get_stock_data(symbol, period="1y", interval="1d", group_by="column"):
    """Cached market data fetch to reduce repeated Yahoo calls across reruns."""
    data = yf.download(symbol, period=period, interval=interval, group_by=group_by, progress=False, auto_adjust=False)
    if isinstance(data.columns, pd.MultiIndex) and group_by == "column":
        data.columns = data.columns.get_level_values(0)
    return data


@st.cache_data(ttl="10m")
def _download_close_series(symbol, period="1y", interval="1d"):
    bench = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=False)
    if isinstance(bench.columns, pd.MultiIndex):
        bench.columns = bench.columns.get_level_values(0)
    return bench.get("Close", pd.Series(dtype=float)).dropna()


@st.cache_data(ttl="10m")
def _get_ticker_info(symbol):
    try:
        return yf.Ticker(symbol).info
    except:
        return {}


@st.cache_data(ttl="10m")
def _get_ticker_news(symbol):
    try:
        return yf.Ticker(symbol).news
    except:
        return []


@st.cache_data(ttl="10m")
def _get_ticker_calendar(symbol):
    try:
        return yf.Ticker(symbol).calendar
    except:
        return None


@st.cache_data(ttl="10m")
def _get_ticker_earnings_dates(symbol):
    try:
        return yf.Ticker(symbol).earnings_dates
    except:
        return None


def _get_benchmark_series(symbol):
    cached = _BENCHMARK_CACHE.get(symbol)
    if cached is not None and len(cached) > 0:
        return cached

    try:
        close = _download_close_series(symbol)
        _BENCHMARK_CACHE[symbol] = close
        return close
    except:
        return pd.Series(dtype=float)


def _return_ratio(series, periods):
    if len(series) <= periods:
        return 1.0
    base = _safe_float(series.iloc[-(periods + 1)], default=0.0)
    now = _safe_float(series.iloc[-1], default=0.0)
    if base <= 0:
        return 1.0
    return now / base


def _safe_info_float(info, key, default=0.0):
    if not isinstance(info, dict):
        return default
    return _safe_float(info.get(key, default), default=default)


def _extract_sector(symbol):
    return SECTOR_MAP.get(symbol, "General")


def _compute_sector_rotation_bonus(df):
    if df is None or df.empty or "Sector" not in df.columns:
        return pd.Series(0.0, index=df.index if df is not None else pd.Index([]))

    sector_ret = pd.to_numeric(df.get("Ret_90D", np.nan), errors="coerce")
    sector_perf = (
        pd.DataFrame({"Sector": df["Sector"], "Ret_90D": sector_ret})
        .dropna(subset=["Sector"])
        .groupby("Sector", as_index=False)["Ret_90D"]
        .mean()
        .sort_values("Ret_90D", ascending=False)
    )
    top_sectors = set(sector_perf.head(TOP_SECTOR_COUNT)["Sector"].tolist())
    return df["Sector"].isin(top_sectors).astype(float) * float(SECTOR_ROTATION_BONUS_POINTS)


def _normalize_series(series):
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(50.0, index=series.index)
    q1 = numeric.quantile(0.25)
    q3 = numeric.quantile(0.75)
    iqr = q3 - q1
    if iqr > 0:
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        clipped = numeric.clip(lower=low, upper=high)
    else:
        clipped = numeric
    min_v = clipped.min()
    max_v = clipped.max()
    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        return pd.Series(50.0, index=series.index)
    return ((clipped - min_v) / (max_v - min_v) * 100).fillna(50.0)


def _normalize_weight_map(weight_map):
    merged = {
        "technical": _safe_float(weight_map.get("technical", DEFAULT_SCORING_CONFIG["weights"]["technical"]), 0.0),
        "fundamental": _safe_float(weight_map.get("fundamental", DEFAULT_SCORING_CONFIG["weights"]["fundamental"]), 0.0),
        "sentiment": _safe_float(weight_map.get("sentiment", DEFAULT_SCORING_CONFIG["weights"]["sentiment"]), 0.0),
        "context": _safe_float(weight_map.get("context", DEFAULT_SCORING_CONFIG["weights"]["context"]), 0.0),
    }
    total = sum(max(v, 0.0) for v in merged.values())
    if total <= 0:
        return DEFAULT_SCORING_CONFIG["weights"].copy()
    return {k: max(v, 0.0) / total for k, v in merged.items()}


def _get_default_regime():
    return {"Market_Regime": "Range", "Regime_Multiplier": 1.00, "VIX": 20.0}


def _apply_quality_gates(df, cfg):
    market_cap_col = "Market_Cap_Cr" if "Market_Cap_Cr" in df.columns else None
    debt_col = "Debt_To_Equity" if "Debt_To_Equity" in df.columns else None
    gate_conditions = {
        f"Liquidity<{cfg['liquidity_cr_min']}Cr": pd.to_numeric(df.get("Avg_Value_20D_Cr", np.nan), errors="coerce") <= cfg["liquidity_cr_min"],
        f"Price<{cfg['price_min']}": pd.to_numeric(df.get("Price", np.nan), errors="coerce") <= cfg["price_min"],
    }
    if market_cap_col:
        gate_conditions[f"MCap<{cfg['market_cap_cr_min']}Cr"] = pd.to_numeric(df.get(market_cap_col), errors="coerce") <= cfg["market_cap_cr_min"]
    if debt_col:
        gate_conditions[f"Debt/Equity>{cfg['max_debt_to_equity']}"] = pd.to_numeric(df.get(debt_col), errors="coerce") >= cfg["max_debt_to_equity"]

    # Fix: Ensure Liquidity_Flag is treated as Series and handle missing values safely
    if "Liquidity_Flag" in df.columns:
        gate_conditions["LowLiquidityFlag"] = df["Liquidity_Flag"].fillna("").astype(str).eq("Low Liquidity - Avoid")
    else:
        gate_conditions["LowLiquidityFlag"] = pd.Series(False, index=df.index)

    gate_frame = pd.DataFrame({k: v.fillna(False) for k, v in gate_conditions.items()}, index=df.index)
    df["Quality_Gate_Pass"] = ~gate_frame.any(axis=1)
    df["Quality_Gate_Failures"] = gate_frame.apply(lambda row: "|".join(row.index[row.values]), axis=1)
    return df


def apply_advanced_scoring(df, scoring_config=None):
    if df is None or df.empty:
        return df

    cfg = DEFAULT_SCORING_CONFIG.copy()
    if scoring_config:
        cfg.update({k: v for k, v in scoring_config.items() if k != "weights"})
        if "weights" in scoring_config:
            cfg["weights"] = _normalize_weight_map(scoring_config["weights"])
    else:
        cfg["weights"] = _normalize_weight_map(cfg["weights"])

    # Optimized: Avoid unnecessary copy if safe
    # df = df.copy()

    sector_rotation_bonus = _compute_sector_rotation_bonus(df)
    df["Sector_Rotation_Bonus"] = sector_rotation_bonus.round(2)
    df["Context_Raw"] = pd.to_numeric(df.get("Context_Raw", 0), errors="coerce").fillna(0) + df["Sector_Rotation_Bonus"]

    # Sector-relative scoring: normalize RSI and conviction by sector before blending into Context_Raw.
    if "Sector" in df.columns:
        rsi_raw = pd.to_numeric(df.get("RSI", np.nan), errors="coerce")
        conviction_raw = pd.to_numeric(df.get("Score", np.nan), errors="coerce")

        def _sector_zscore(series):
            std = series.std(ddof=0)
            if std is None or std == 0 or pd.isna(std):
                return pd.Series(0.0, index=series.index)
            return (series - series.mean()) / std

        rsi_z = rsi_raw.groupby(df["Sector"]).transform(_sector_zscore).fillna(0.0)
        conviction_z = conviction_raw.groupby(df["Sector"]).transform(_sector_zscore).fillna(0.0)
        df["Sector_RSI_Z"] = rsi_z.round(3)
        df["Sector_Conviction_Z"] = conviction_z.round(3)
        df["Context_Raw"] += ((rsi_z + conviction_z) * 5.0)

    # Sector rotation bonus (moved from UI to Core Logic)
    # Calculate top sectors based on 3M perf proxy (Ret_90D) directly from the current dataframe
    if "Sector" in df.columns and "Ret_90D" in df.columns:
        sector_perf = df.groupby("Sector")["Ret_90D"].mean().sort_values(ascending=False)
        top_sectors = set(sector_perf.head(3).index.tolist())
        df["Sector_Rotation_Bonus"] = df["Sector"].isin(top_sectors).astype(int) * 10
        # Add to Context_Raw before normalization
        df["Context_Raw"] = pd.to_numeric(df.get("Context_Raw", 0), errors="coerce").fillna(0) + df["Sector_Rotation_Bonus"]

    # Normalize category sub-scores within scan universe
    df["Technical_Score"] = _normalize_series(df.get("Technical_Raw", 50)).round(2)
    df["Fundamental_Score"] = _normalize_series(df.get("Fundamental_Raw", 50)).round(2)
    df["Sentiment_Score"] = _normalize_series(df.get("Sentiment_Raw", 50)).round(2)
    df["Context_Score"] = _normalize_series(df.get("Context_Raw", 50)).round(2)

    # Why: RSI tiers reduce false positives by rewarding healthy momentum instead of exhaustion.
    rsi = pd.to_numeric(df.get("RSI", np.nan), errors="coerce")
    rsi_bonus = np.select(
        [rsi.between(45, 65, inclusive="both"), rsi.between(40, 72, inclusive="both")],
        [15, 8],
        default=0,
    )
    df["Technical_Score"] = (df["Technical_Score"] + rsi_bonus).clip(lower=0, upper=100)

    # RS ranking and top quartile bonus
    rs_base = pd.to_numeric(df.get("RS_6M", df.get("RS_Composite", np.nan)), errors="coerce")
    df["RS_Rank"] = (rs_base.rank(method="average", pct=True) * 100).fillna(50)
    rs_gate = (pd.to_numeric(df.get("RS_Composite", 0), errors="coerce") > 1.0) | (df["RS_Rank"] >= 75)
    df.loc[rs_gate.fillna(False), "Context_Score"] = (df.loc[rs_gate.fillna(False), "Context_Score"] + 20).clip(upper=100)

    sentiment = pd.to_numeric(df.get("sentiment_score", df.get("Sentiment_Raw", np.nan)), errors="coerce")
    if "news_date" in df.columns:
        news_ts = pd.to_datetime(df["news_date"], errors="coerce")
        days_old = (pd.Timestamp.now().normalize() - news_ts).dt.days.clip(lower=0)
        decay = np.exp(-(days_old.fillna(0) / 5.0))
        sentiment = sentiment.fillna(50) * decay
    if "Sector" in df.columns:
        sector_avg = sentiment.groupby(df["Sector"]).transform("mean")
        sentiment = sentiment - sector_avg.fillna(0)
    df["Sentiment_Score"] = _normalize_series(sentiment.fillna(50)).round(2)

    # Regime handling
    regime = cfg.get("regime", _get_default_regime()) if cfg.get("enable_regime", True) else _get_default_regime()
    df["Market_Regime"] = regime["Market_Regime"]
    df["Regime"] = regime["Market_Regime"]
    df["Regime_Multiplier"] = regime["Regime_Multiplier"]
    df["Regime_Tag"] = REGIME_LABELS.get(regime["Market_Regime"], f"🟡 {regime['Market_Regime']}")
    df["India_VIX"] = round(regime["VIX"], 2)

    w = cfg["weights"]
    df["Weight_Technical"] = round(w["technical"] * 100, 2)
    df["Weight_Fundamental"] = round(w["fundamental"] * 100, 2)
    df["Weight_Sentiment"] = round(w["sentiment"] * 100, 2)
    df["Weight_Context"] = round(w["context"] * 100, 2)
    df["Score_Pre_Regime"] = (
        df["Technical_Score"] * w["technical"]
        + df["Fundamental_Score"] * w["fundamental"]
        + df["Sentiment_Score"] * w["sentiment"]
        + df["Context_Score"] * w["context"]
    )
    df["sub_scores"] = df.apply(
        lambda row: {
            "technical": round(_safe_float(row.get("Technical_Score")), 2),
            "fundamental": round(_safe_float(row.get("Fundamental_Score")), 2),
            "sentiment": round(_safe_float(row.get("Sentiment_Score")), 2),
            "context": round(_safe_float(row.get("Context_Score")), 2),
        },
        axis=1,
    )
    df["Score"] = (df["Score_Pre_Regime"] * df["Regime_Multiplier"]).clip(lower=0, upper=100).round(2)

    df = _apply_quality_gates(df, cfg)
    fail_mask = ~df["Quality_Gate_Pass"]
    df.loc[fail_mask, "Score"] = (df.loc[fail_mask, "Score"] - 1000).clip(lower=0)

    avoid_mask = (df.get("Black_Swan_Flag", 0).astype(float) > 0) | (df.get("News") == "🚨 BLACK SWAN")
    df.loc[avoid_mask.fillna(False), "Score"] = (df.loc[avoid_mask.fillna(False), "Score"] - 50).clip(lower=0)
    df["Avoid_Flag"] = avoid_mask.fillna(False)

    # Keep verdict semantics backward-compatible
    df["Verdict"] = df["Score"].apply(lambda x: "🔥 HIGH" if x >= 85 else "🚀 PASS" if x >= 60 else "🟡 WATCH")
    df.loc[fail_mask, "Verdict"] = "❌ FAIL"
    df.loc[df["Avoid_Flag"], "Verdict"] = "🚨 AVOID"

    # Backward-compatible aliases + transparency columns requested by desk users.
    df["Tech_Score"] = df["Technical_Score"]
    df["Fund_Score"] = df["Fundamental_Score"]
    df["Sent_Score"] = df["Sentiment_Score"]
    df["Context_Score"] = df["Context_Score"]
    return df

def check_institutional_fortress(ticker, data, ticker_obj, portfolio_value, risk_per_trade, selected_universe=None, regime_data=None):
    try:
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        if len(data)<210: return None

        close, high, low, open_price = data["Close"], data["High"], data["Low"], data["Open"]
        volume = data.get("Volume", pd.Series(0, index=data.index, dtype=float)).fillna(0)

        # Immediate Liquidity Guard
        price = _safe_float(close.iloc[-1])
        avg_volume_20 = _safe_float(volume.tail(20).mean())
        avg_value_20d_cr = (avg_volume_20 * price) / 1e7 if price > 0 else 0.0
        if selected_universe == "Nifty Smallcap 250" and avg_value_20d_cr < SMALLCAP_LIQUIDITY_MIN_CR:
            return None

        ema200 = _safe_float(ta.ema(close,200).iloc[-1])
        ema50 = _safe_float(ta.ema(close,50).iloc[-1])
        rsi = _safe_float(ta.rsi(close,14).iloc[-1])
        atr = _safe_float(ta.atr(high,low,close,14).iloc[-1])
        atr100 = _safe_float(ta.atr(high, low, close, 100).iloc[-1])
        st_df = ta.supertrend(high,low,close,10,3)
        trend_col = [c for c in st_df.columns if c.startswith("SUPERTd")][0]
        trend_dir = int(_safe_float(st_df[trend_col].iloc[-1]))
        # price already defined above
        prev_close = _safe_float(close.iloc[-2])
        curr_open = _safe_float(open_price.iloc[-1])
        curr_low = _safe_float(low.iloc[-1])
        current_volume = _safe_float(volume.iloc[-1])
        # avg_volume_20 already defined above
        # avg_value_20d_cr already defined above

        vol_surge_ratio = (current_volume / avg_volume_20) if avg_volume_20 > 0 else 0.0
        vol_surge = vol_surge_ratio > 1.5

        weekly_close = close.resample("W-FRI").last().dropna() if isinstance(close.index, pd.DatetimeIndex) else pd.Series(dtype=float)
        weekly_ema30 = _safe_float(ta.ema(weekly_close, 30).iloc[-1]) if len(weekly_close) >= 30 else 0.0

        tech_base = price>ema200 and trend_dir==1
        mtf_aligned = price > weekly_ema30 if weekly_ema30 > 0 else False

        regime = regime_data if regime_data else _get_default_regime()
        regime_multiplier = float(regime.get("Regime_Multiplier", 1.0))
        # Adaptive: tighter stops in Bull, wider in Bear to avoid shakeouts
        adaptive_mult = np.clip(regime_multiplier, 0.7, 1.3)
        sl_distance = atr * (1.5 / adaptive_mult)
        sl_price = round(price-sl_distance,2)
        target_10d = round(price + atr*1.8,2)
        risk_amount = portfolio_value*risk_per_trade
        pos_size = int(risk_amount / sl_distance) if sl_distance>0 else 0

        conviction = 0
        score_mod = 0
        news_sentiment = "Neutral"
        event_status = "✅ Safe"
        black_swan_flag = 0

        # --- Resilience & Gap Logic ---
        # War/News Resilience
        drop = prev_close - curr_low
        resilience_label = "✅ Safe"
        if drop > (2.0 * atr):
            if price > ema200:
                resilience_label = "🛡️ HOLD (Shakeout)"
            else:
                resilience_label = "💀 FAIL (Breakdown)"
                score_mod -= 40 # Automatic penalty

        # Gap Integrity
        gap_integrity = "N/A"
        if curr_open < prev_close:
            gap_size = prev_close - curr_open
            # "Integral" if Open > EMA200 AND gap < 1.5 ATR
            if curr_open > ema200 and gap_size < (1.5 * atr):
                gap_integrity = "✅ Integral"
            else:
                gap_integrity = "⚠️ Gap Risk"

        try:
            # Optimized: Use cached news fetch
            news = _get_ticker_news(ticker) or []
            titles = " ".join(n.get("title","").lower() for n in news[:5])
            if any(k in titles for k in ["fraud","investigation","default","bankruptcy","scam","legal"]):
                news_sentiment = "🚨 BLACK SWAN"
                score_mod -= 40
                black_swan_flag = 1
        except: pass
        try:
            # Optimized: Use cached calendar fetch
            cal = _get_ticker_calendar(ticker)
            if isinstance(cal,pd.DataFrame) and not cal.empty:
                next_date = pd.to_datetime(cal.iloc[0,0]).date()
                days_to = (next_date - datetime.now().date()).days
                if 0<=days_to<=7:
                    event_status = f"🚨 EARNINGS ({next_date.strftime('%d-%b')})"
                    score_mod -= 20
        except: pass

        analyst_count = target_high = target_low = target_median = target_mean = 0
        market_cap_cr = debt_to_equity = interest_coverage = 0.0
        earnings_ts = None
        earnings_surprise = 0.0
        negative_earnings_surprise = False
        try:
            # Optimized: Use cached info fetch
            info = _get_ticker_info(ticker) or {}
            analyst_count = info.get("numberOfAnalystOpinions",0)
            target_high = info.get("targetHighPrice",0)
            target_low = info.get("targetLowPrice",0)
            target_median = info.get("targetMedianPrice",0)
            target_mean = info.get("targetMeanPrice",0)
            market_cap_cr = _safe_info_float(info, "marketCap", 0.0) / 1e7
            debt_to_equity = _safe_info_float(info, "debtToEquity", 0.0)
            interest_coverage = _safe_info_float(info, "interestCoverage", 0.0)
        except: pass

        try:
            # Optimized: Use cached earnings fetch
            earnings = _get_ticker_earnings_dates(ticker)
            if isinstance(earnings, pd.DataFrame) and not earnings.empty:
                latest = earnings.sort_index(ascending=False).iloc[0]
                earnings_ts = earnings.sort_index(ascending=False).index[0]
                earnings_surprise = _safe_float(latest.get("Surprise(%)", 0.0), default=0.0)
                negative_earnings_surprise = earnings_surprise < 0
        except:
            pass

        if tech_base:
            conviction += 60
            if 48<=rsi<=62: conviction+=20
            elif 40<=rsi<=72: conviction+=10
            conviction += score_mod

        # Relative Strength vs Nifty 50
        benchmark_close = _get_benchmark_series(NIFTY_SYMBOL)
        rs_score = 0.0
        try:
            stock_ret_30d = ((price / _safe_float(close.iloc[-31], default=price)) - 1) * 100 if len(close) > 30 else 0.0
            nifty_ret_30d = 0.0
            if len(benchmark_close) > 30:
                bench_now = _safe_float(benchmark_close.iloc[-1])
                bench_30 = _safe_float(benchmark_close.iloc[-31], default=bench_now)
                nifty_ret_30d = ((bench_now / bench_30) - 1) * 100 if bench_30 > 0 else 0.0
            rs_score = stock_ret_30d - nifty_ret_30d
        except:
            rs_score = 0.0

        if rs_score > 0:
            conviction += 15

        # Multi-horizon RS
        benchmark_close = _get_benchmark_series(NIFTY_SYMBOL)
        rs_3m = _return_ratio(close, 63) / max(_return_ratio(benchmark_close, 63), 1e-6)
        rs_6m = _return_ratio(close, 126) / max(_return_ratio(benchmark_close, 126), 1e-6)
        rs_12m = _return_ratio(close, 252) / max(_return_ratio(benchmark_close, 252), 1e-6)
        rs_composite = (rs_3m * 0.5) + (rs_6m * 0.3) + (rs_12m * 0.2)

        # Volume confirmation
        breakout = False
        if len(close) > 20:
            breakout_level = _safe_float(high.iloc[-21:-1].max(), default=price)
            breakout = price > breakout_level
        if vol_surge:
            conviction += 10
        if breakout and current_volume < avg_volume_20:
            conviction -= 10

        # Volatility contraction (VCP-like)
        is_coiling = atr > 0 and atr100 > 0 and atr < (atr100 * 0.8)
        if is_coiling:
            conviction += 10

        # Mean reversion / over-extension guard
        extension_pct = ((price - ema50) / ema50) * 100 if ema50 > 0 else 0.0
        extension_ema200_pct = ((price - ema200) / ema200) * 100 if ema200 > 0 else 0.0
        overextended = extension_pct > 15
        if overextended:
            conviction -= 20
        if extension_ema200_pct > 40:
            conviction -= 20

        dispersion_pct = ((target_high-target_low)/price)*100 if price>0 else 0
        dispersion_alert = "⚠️ High Dispersion" if dispersion_pct>30 else "✅"
        if dispersion_pct>30: conviction -= 10

        # Sub-score raw components
        technical_raw = 0.0
        technical_raw += 35 if tech_base else 0
        if 45 <= rsi <= 65:
            technical_raw += 15
        elif (40 <= rsi < 45) or (65 < rsi <= 72):
            technical_raw += 8
        technical_raw += 10 if vol_surge_ratio > 1.8 else 0
        technical_raw += 8 if is_coiling else 0
        technical_raw -= 20 if extension_ema200_pct > 40 else 0
        technical_raw = max(0, technical_raw)

        fundamental_raw = 30.0
        if analyst_count > 0:
            upside_pct = ((_safe_float(target_mean, price) - price) / price) * 100 if price > 0 else 0
            fundamental_raw += min(max(upside_pct, -20), 25)
        fundamental_raw += 10 if market_cap_cr > 1500 else 0
        if dispersion_pct > 25:
            fundamental_raw *= 0.7

        sentiment_raw = 50.0
        if news_sentiment == "🚨 BLACK SWAN":
            sentiment_raw -= 50
        half_life_days = 5.0
        decay = 1.0
        if earnings_ts is not None:
            days_ago = max((datetime.now().date() - earnings_ts.date()).days, 0)
            decay = 0.5 ** (days_ago / half_life_days)
        sentiment_raw += 10 * decay
        sentiment_raw -= 15 if negative_earnings_surprise else 0

        context_raw = 30.0
        context_raw += 20 if mtf_aligned else 0
        context_raw += 20 if rs_composite > 1.0 else 0
        ret_6m = (_return_ratio(close, 126) - 1) * 100
        vol_adj_mom = ret_6m / atr if atr > 0 else 0
        context_raw += min(max(vol_adj_mom, -10), 20)

        conviction = max(0,min(100,conviction))
        verdict = "🔥 HIGH" if conviction>=85 and mtf_aligned else "🚀 PASS" if conviction>=60 else "🟡 WATCH" if tech_base else "❌ FAIL"
        if overextended:
            verdict = "⚠️ OVEREXTENDED"

        # Backtest returns (7, 30, 60, 90 days)
        current_date = close.index[-1]
        returns = {}
        for days in [7, 30, 60, 90]:
            try:
                target_date = current_date - pd.Timedelta(days=days)
                # Find nearest index
                idx = close.index.get_indexer([target_date], method='nearest')[0]
                past_price = float(close.iloc[idx])
                pct_change = ((price - past_price) / past_price) * 100
                returns[f"Ret_{days}D"] = pct_change
            except:
                returns[f"Ret_{days}D"] = 0.0

        # --- Velocity & Strategy ---
        ret_7d = returns.get("Ret_7D", 0.0)
        ret_30d = returns.get("Ret_30D", 0.0)
        velocity = ret_7d - ret_30d

        strategy = "Neutral"
        if price > ema50 and 55 <= rsi <= 70:
            strategy = "Momentum Pick"
        elif price > ema200 and dispersion_pct <= 30:
            strategy = "Long-Term Pick"

        buy_zone_high = price + (0.5 * atr)
        buy_zone = f"₹{price:.2f} - ₹{buy_zone_high:.2f}"

        steam_left = target_10d - price
        rsi_vel_factor = rsi / 50.0
        days_to_target = 0
        if rsi_vel_factor > 0 and atr > 0:
             days_to_target = steam_left / (atr * rsi_vel_factor)

        return {
            "Symbol": ticker,
            "Verdict": verdict,
            "Score": conviction,
            "Price": round(price,2),
            "RSI": round(rsi,1),
            "News": news_sentiment,
            "Events": event_status,
            "Sector": SECTOR_MAP.get(ticker,"General"),
            "Position_Qty": pos_size,
            "Stop_Loss": sl_price,
            "Target_10D": target_10d,
            "Analysts": analyst_count,
            "Tgt_High": target_high,
            "Tgt_Median": target_median,
            "Tgt_Low": target_low,
            "Tgt_Mean": target_mean,
            "Dispersion_Alert": dispersion_alert,
            "Ret_30D": returns.get("Ret_30D"),
            "Ret_60D": returns.get("Ret_60D"),
            "Ret_90D": returns.get("Ret_90D"),
            # New Metrics
            "Ret_7D": ret_7d,
            "Velocity": velocity,
            "Strategy": strategy,
            "Buy_Zone": buy_zone,
            "Steam_Left": steam_left,
            "Days_To_Target": days_to_target,
            "Resilience": resilience_label,
            "Gap_Integrity": gap_integrity,
            "Above_EMA200": price > ema200,
            "RS_Score": round(rs_score, 2),
            "Vol_Surge_Ratio": round(vol_surge_ratio, 2),
            "Extension_Pct": round(extension_pct, 2),
            "Is_Coiling": is_coiling,
            "Avg_Volume_20D": round(avg_volume_20, 0),
            "Avg_Value_20D_Cr": round(avg_value_20d_cr, 2),
            "Market_Cap_Cr": round(market_cap_cr, 2),
            "Debt_To_Equity": round(debt_to_equity, 2),
            "Interest_Coverage": round(interest_coverage, 2),
            "Negative_Earnings_Surprise": bool(negative_earnings_surprise),
            "Black_Swan_Flag": black_swan_flag,
            "RS_3M": round(rs_3m, 3),
            "RS_6M": round(rs_6m, 3),
            "RS_12M": round(rs_12m, 3),
            "RS_Composite": round(rs_composite, 3),
            "Vol_Adj_Mom": round(vol_adj_mom, 2),
            "EMA200_Extension_Pct": round(extension_ema200_pct, 2),
            "Technical_Raw": round(technical_raw, 2),
            "Fundamental_Raw": round(fundamental_raw, 2),
            "Sentiment_Raw": round(sentiment_raw, 2),
            "Context_Raw": round(context_raw, 2),
            "Regime_Multiplier": round(regime_multiplier, 2)
        }
    except: return None


def backtest_top_picks(scan_timestamp):
    """Backtest top picks from a scan timestamp against Nifty benchmark forward returns."""
    try:
        from utils.db import _read_df

        query = """
            SELECT d.symbol AS Symbol, d.price AS Entry_Price
            FROM scan_history_details d
            INNER JOIN scans s ON s.scan_id = d.scan_id
            WHERE s.timestamp = :timestamp
              AND s.scan_type = 'STOCK'
              AND COALESCE(d.score, 0) >= 60
        """
        picks = _read_df(query, params={"timestamp": scan_timestamp})

        if picks.empty:
            return pd.DataFrame()

        benchmark = _download_close_series(NIFTY_SYMBOL, period="2y")
        if benchmark.empty:
            return pd.DataFrame()

        horizon_days = [7, 30, 60]
        out_rows = []
        for _, row in picks.iterrows():
            symbol = row["Symbol"]
            data = _download_close_series(symbol, period="2y")
            if data.empty:
                continue
            latest = float(data.iloc[-1])
            stock_returns = {}
            nifty_returns = {}
            for days in horizon_days:
                if len(data) > days and len(benchmark) > days:
                    stock_returns[f"Stock_{days}D_%"] = ((latest / float(data.iloc[-(days + 1)])) - 1) * 100
                    nifty_returns[f"Nifty_{days}D_%"] = ((float(benchmark.iloc[-1]) / float(benchmark.iloc[-(days + 1)])) - 1) * 100
                else:
                    stock_returns[f"Stock_{days}D_%"] = np.nan
                    nifty_returns[f"Nifty_{days}D_%"] = np.nan
            out_rows.append({"Symbol": symbol, **stock_returns, **nifty_returns})

        detail_df = pd.DataFrame(out_rows)
        if detail_df.empty:
            return detail_df

        avg_row = {"Symbol": "AVERAGE"}
        for days in horizon_days:
            avg_row[f"Stock_{days}D_%"] = detail_df[f"Stock_{days}D_%"].mean()
            avg_row[f"Nifty_{days}D_%"] = detail_df[f"Nifty_{days}D_%"].mean()
        avg_df = pd.concat([detail_df, pd.DataFrame([avg_row])], ignore_index=True)
        return avg_df
    except Exception:
        return pd.DataFrame()
