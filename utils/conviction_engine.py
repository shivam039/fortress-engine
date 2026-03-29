"""
utils/conviction_engine.py
Unified Conviction Score framework for all Fortress modules.

Each scorer returns:
    {
        "Conviction Score": 0-100,
        "Conviction Label": "STRONG BUY" | "BUY" | "HOLD" | "UNDERPERFORMER" | "AVOID",
        "Conviction Emoji": "🔥" | "✅" | "🟡" | "⚠️" | "❌",
        "Sub-Scores": { dimension: score, ... }   # transparent breakdown
        "Decision": str   # plain-English user-facing recommendation
    }
"""

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────
#  Shared helpers
# ─────────────────────────────────────────────

def _label(score: float) -> tuple[str, str]:
    """Return (label, emoji) for a 0-100 score."""
    if score >= 80:
        return "STRONG BUY", "🔥"
    if score >= 65:
        return "BUY", "✅"
    if score >= 50:
        return "HOLD", "🟡"
    if score >= 35:
        return "UNDERPERFORMER", "⚠️"
    return "AVOID", "❌"


def _clamp(val, lo=0, hi=100):
    return float(np.clip(val, lo, hi))


# ─────────────────────────────────────────────
#  MF Conviction Score
# ─────────────────────────────────────────────

# Category-specific Sharpe benchmarks (what we expect from a good fund in each category)
_CATEGORY_SHARPE_BENCH = {"Equity": 0.8, "Hybrid": 0.6, "Debt": 0.5}
_CATEGORY_RET_BENCH    = {"Equity": 12.0, "Hybrid": 9.0, "Debt": 6.0}   # 1Y % baseline

def score_mf_fund(row: dict, category: str = "Equity") -> dict:
    """
    Compute a conviction score for a single MF fund row.

    Sub-scores (each 0-25):
    ─────────────────────────────
    1. Alpha Quality        — positive alpha vs benchmark, stability
    2. Risk-Adjusted Return — Sharpe + Sortino, category-adjusted baseline
    3. Multi-Horizon Return — 1Y/3Y/5Y weighted, penalise front-loading
    4. Downside Protection  — low downside deviation, low rolling vol, good Sortino
    """
    def _safe(key, default=0.0):
        v = row.get(key, default)
        try:
            f = float(v)
            return f if not np.isnan(f) else default
        except Exception:
            return default

    alpha    = _safe("Alpha")
    sharpe   = _safe("Sharpe")
    sortino  = _safe("Sortino")
    ret_1y   = _safe("1Y Return")
    ret_3y   = _safe("3Y Return")
    ret_5y   = _safe("5Y Return")
    vol      = _safe("Volatility")
    downside = _safe("Downside Deviation")
    roll_std = _safe("Rolling Std")
    beta     = _safe("Beta", 1.0)

    # ── 1. Alpha Quality (0-25) ──────────────────────────────────────────
    # Alpha > 5% great, 0-5% decent, negative bad
    alpha_score = _clamp(25 + min(alpha * 2.0, 15) if alpha >= 0 else 25 + max(alpha * 3.0, -25), 0, 25)

    # ── 2. Risk-Adjusted Return (0-25) ──────────────────────────────────
    bench_sharpe = _CATEGORY_SHARPE_BENCH.get(category, 0.7)
    sharpe_pts   = _clamp((sharpe / max(bench_sharpe, 0.1)) * 15, 0, 15)
    sortino_pts  = _clamp((sortino / max(bench_sharpe * 1.3, 0.1)) * 10, 0, 10)
    risk_adj     = sharpe_pts + sortino_pts

    # ── 3. Multi-Horizon Return (0-25) ───────────────────────────────────
    bench_ret = _CATEGORY_RET_BENCH.get(category, 10.0)
    r1 = _clamp((ret_1y / max(bench_ret, 1)) * 10, 0, 10)
    r3 = _clamp((ret_3y / max(bench_ret, 1)) * 8,  0, 8)
    r5 = _clamp((ret_5y / max(bench_ret, 1)) * 7,  0, 7)
    # Penalise: if 1Y >> 3Y/5Y it's a hot fund, not a consistent one
    consistency_penalty = 0
    if ret_1y > 0 and ret_3y > 0:
        ratio = ret_1y / max(ret_3y, 1)
        if ratio > 2.5:         # suspiciously front-loaded
            consistency_penalty = 5
    momentum_score = _clamp(r1 + r3 + r5 - consistency_penalty, 0, 25)

    # ── 4. Downside Protection (0-25) ────────────────────────────────────
    # Lower downside deviation and rolling vol = better
    # Normalise: assume 20% vol = ok baseline for equity
    vol_baseline = {"Equity": 20.0, "Hybrid": 14.0, "Debt": 6.0}.get(category, 20.0)
    vol_pts   = _clamp(15 - max(0, (vol - vol_baseline) * 0.4), 0, 15)
    down_pts  = _clamp(10 - max(0, (downside - vol_baseline * 0.6) * 0.5), 0, 10)
    downside_score = vol_pts + down_pts

    # ── Final score ───────────────────────────────────────────────────────
    total = _clamp(alpha_score + risk_adj + momentum_score + downside_score, 0, 100)
    label, emoji = _label(total)

    # ── Decision string ───────────────────────────────────────────────────
    decision_parts = []
    if alpha > 3:
        decision_parts.append(f"generating {alpha:.1f}% alpha")
    elif alpha < 0:
        decision_parts.append(f"underperforming benchmark by {abs(alpha):.1f}%")
    if sharpe >= bench_sharpe * 1.2:
        decision_parts.append("strong risk-adjusted returns")
    if ret_1y >= bench_ret * 1.3:
        decision_parts.append(f"excellent {ret_1y:.1f}% 1Y growth")
    if downside > vol_baseline * 0.8:
        decision_parts.append("high downside risk — size carefully")

    if decision_parts:
        decision = f"{emoji} {label}: Fund is " + ", ".join(decision_parts) + "."
    else:
        decision = f"{emoji} {label}: Balanced fund with no standout signal."

    return {
        "Conviction Score": round(total, 1),
        "Conviction Label": label,
        "Conviction Emoji": emoji,
        "Sub-Scores": {
            "Alpha Quality":       round(alpha_score, 1),
            "Risk-Adjusted":       round(risk_adj, 1),
            "Multi-Horizon Return":round(momentum_score, 1),
            "Downside Protection": round(downside_score, 1),
        },
        "Decision": decision,
    }


def enrich_mf_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised: add Conviction Score, Label, Emoji, Decision to a full MF scan DataFrame."""
    if df.empty:
        return df

    results = []
    for _, row in df.iterrows():
        cat = row.get("Category", "Equity") or "Equity"
        res = score_mf_fund(row.to_dict(), category=str(cat))
        results.append(res)

    score_df = pd.DataFrame(results)
    df = df.copy().reset_index(drop=True)
    df["Conviction Score"] = score_df["Conviction Score"]
    df["Conviction Label"] = score_df["Conviction Label"]
    df["Conviction Emoji"] = score_df["Conviction Emoji"]
    df["Decision"]         = score_df["Decision"]

    return df.sort_values("Conviction Score", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────
#  Commodity Conviction Score
# ─────────────────────────────────────────────

def score_commodity(
    name: str,
    local_df: pd.DataFrame,
    global_df: pd.DataFrame,
    spread_pct: float,
    fx: float,
) -> dict:
    """
    Compute a rich conviction score for a commodity.

    Sub-scores (each 0-25):
    ─────────────────────────────
    1. Trend Strength       — price vs SMA20 / SMA50 / SMA100
    2. Momentum             — 1-month, 3-month returns vs historical avg
    3. Volatility Regime    — ATR compression (coiling) = upcoming explosive move
    4. Arbitrage Edge       — global vs local price divergence (spread signal)
    """
    close = local_df["close"]
    n = len(close)

    # ── 1. Trend Strength (0-25) ─────────────────────────────────────────
    price = float(close.iloc[-1])
    sma20  = float(close.tail(20).mean())  if n >= 20  else price
    sma50  = float(close.tail(50).mean())  if n >= 50  else price
    sma100 = float(close.tail(100).mean()) if n >= 100 else price

    trend_pts = 0
    if price > sma20:  trend_pts += 8
    if price > sma50:  trend_pts += 10
    if price > sma100: trend_pts += 7
    # Bonus: all MAs aligned (price > 20 > 50 > 100)
    if sma20 > sma50 > sma100 and price > sma20:
        trend_pts = min(trend_pts + 5, 25)
    trend_score = _clamp(trend_pts, 0, 25)

    # ── 2. Momentum (0-25) ───────────────────────────────────────────────
    ret_1m = float(((close.iloc[-1] / close.iloc[-min(21, n)]) - 1) * 100)  if n >= 21  else 0.0
    ret_3m = float(((close.iloc[-1] / close.iloc[-min(63, n)]) - 1) * 100)  if n >= 63  else 0.0
    ret_6m = float(((close.iloc[-1] / close.iloc[-min(126, n)]) - 1) * 100) if n >= 126 else 0.0

    # Historical average monthly return
    hist_avg_1m = float(close.pct_change(21).dropna().mean() * 100) if n >= 42 else 0.5
    hist_avg_3m = float(close.pct_change(63).dropna().mean() * 100) if n >= 90 else 1.5

    mom1 = _clamp(10 + (ret_1m - hist_avg_1m) * 0.8, 0, 10)
    mom3 = _clamp(10 + (ret_3m - hist_avg_3m) * 0.5, 0, 10)
    mom6_pts = _clamp(5  + ret_6m * 0.2, 0, 5)
    momentum_score = _clamp(mom1 + mom3 + mom6_pts, 0, 25)

    # ── 3. Volatility Regime (0-25) ─────────────────────────────────────
    # Low current ATR vs historical ATR = coiling = potential breakout
    tr = pd.concat([
        (local_df["high"] - local_df["low"]).abs(),
        (local_df["high"] - local_df["close"].shift(1)).abs(),
        (local_df["low"]  - local_df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)

    atr14  = float(tr.rolling(14).mean().iloc[-1])  if n >= 14  else float(tr.mean())
    atr100 = float(tr.rolling(100).mean().iloc[-1]) if n >= 100 else atr14

    if atr100 > 0:
        atr_ratio = atr14 / atr100
        # Coiling (ratio < 0.7): explosive setup
        if atr_ratio < 0.6:
            vol_score = 25
        elif atr_ratio < 0.75:
            vol_score = 18
        elif atr_ratio < 0.90:
            vol_score = 12
        elif atr_ratio < 1.10:
            vol_score = 8      # Normal regime
        else:
            vol_score = 3      # Expanded/chaotic regime
    else:
        vol_score = 10
    vol_score = _clamp(vol_score, 0, 25)

    # ── 4. Arbitrage Edge (0-25) ─────────────────────────────────────────
    # Negative spread (local cheaper than global parity) = strong buy
    # Positive spread (local premium) = caution
    if spread_pct < -5:
        arb_score = 25     # Big discount to global = great entry
    elif spread_pct < -2:
        arb_score = 20
    elif spread_pct < 0:
        arb_score = 15     # Slight discount = fair
    elif spread_pct < 2:
        arb_score = 10     # At parity
    elif spread_pct < 5:
        arb_score = 5      # Slight premium = caution
    else:
        arb_score = 0      # Expensive vs global = avoid
    arb_score = _clamp(arb_score, 0, 25)

    # ── Global momentum bonus (checks if global commodity is also bullish) ─
    if not global_df.empty:
        g_close = global_df["close"]
        g_1m = float(((g_close.iloc[-1] / g_close.iloc[-min(21, len(g_close))]) - 1) * 100) if len(g_close) >= 21 else 0
        global_bonus = _clamp(g_1m * 0.3, -5, 5)   # small confirmation boost
    else:
        global_bonus = 0

    # ── Final score ───────────────────────────────────────────────────────
    total = _clamp(trend_score + momentum_score + vol_score + arb_score + global_bonus, 0, 100)
    label, emoji = _label(total)

    # ── Plain-English decision ─────────────────────────────────────────────
    parts = []
    if price > sma50:
        parts.append("above 50-day trend")
    else:
        parts.append("below 50-day trend ⚠️")

    if spread_pct < -2:
        parts.append(f"local underpriced vs global by {abs(spread_pct):.1f}% (buy edge)")
    elif spread_pct > 3:
        parts.append(f"local overpriced vs global by {spread_pct:.1f}% (reduce exposure)")

    if vol_score >= 18:
        parts.append("volatility coiling — breakout setup")
    elif vol_score <= 5:
        parts.append("volatile regime — wait for calm")

    if ret_1m > hist_avg_1m * 1.5:
        parts.append(f"strong momentum ({ret_1m:.1f}% last month)")
    elif ret_1m < 0:
        parts.append(f"negative momentum ({ret_1m:.1f}% last month)")

    decision = f"{emoji} {label}: {name} is " + ", ".join(parts) + "."

    return {
        "Conviction Score": round(total, 1),
        "Conviction Label": label,
        "Conviction Emoji": emoji,
        "1M Return %":      round(ret_1m, 2),
        "3M Return %":      round(ret_3m, 2),
        "6M Return %":      round(ret_6m, 2),
        "Trend":            "↑ Bull" if price > sma50 else "↓ Bear",
        "SMA20":            round(sma20, 2),
        "SMA50":            round(sma50, 2),
        "ATR Regime":       "Coiling 🔥" if vol_score >= 18 else ("Normal" if vol_score >= 8 else "Expanded ⚠️"),
        "Sub-Scores": {
            "Trend Strength":   round(trend_score, 1),
            "Momentum":         round(momentum_score, 1),
            "Volatility Regime":round(vol_score, 1),
            "Arbitrage Edge":   round(arb_score, 1),
        },
        "Decision": decision,
    }
