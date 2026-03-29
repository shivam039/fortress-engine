import pandas as pd
import numpy as np
from mf_lab.services.config import logger

# --- METRICS CALCULATION ---

def calculate_metrics(fund_df, benchmark_df):
    """
    Calculates detailed performance metrics for a fund against a benchmark.
    Returns a dictionary of metrics.
    """
    try:
        # Align Data
        # Inner join on date index
        combined = pd.merge(fund_df[['ret']], benchmark_df[['ret']],
                             left_index=True, right_index=True, suffixes=('_f', '_b'))

        # Minimum history check (e.g., ~1 year)
        if len(combined) < 200:
            return None

        f_ret = combined['ret_f']
        b_ret = combined['ret_b']

        # 1. Beta & Tracking Error
        # Covariance / Variance
        covariance = np.cov(f_ret, b_ret)[0][1]
        variance = np.var(b_ret)
        beta = covariance / variance if variance != 0 else 1.0

        # Tracking Error (Annualized Standard Deviation of differences)
        tracking_error = (f_ret - b_ret).std() * np.sqrt(252) * 100

        # 2. Alpha (True Alpha - Smoothed)
        # Annualized rolling 60-day Alpha mean
        alpha_series = (f_ret - b_ret).rolling(60).mean()
        alpha = alpha_series.mean() * 252 * 100

        # 3. Sortino Ratio
        # RFR = 6%
        RFR_annual = 0.06
        days = len(f_ret)
        years = days / 252.0

        # CAGR
        total_ret_f = (1 + f_ret).prod() - 1
        cagr_f = (1 + total_ret_f) ** (1/years) - 1 if years > 0 else 0

        excess_ret = cagr_f - RFR_annual

        # Downside Deviation (only negative returns)
        neg_ret = f_ret[f_ret < 0]
        downside_dev = neg_ret.std() * np.sqrt(252)

        sortino = excess_ret / downside_dev if downside_dev > 0 else 0

        # 4. Max Drawdown
        cum = (1 + f_ret).cumprod()
        running_max = cum.cummax()
        drawdown = (cum - running_max) / running_max
        max_dd = drawdown.min() * 100 # In percentage (negative)

        # 5. Win Rate (Rolling 1y outperformance)
        rolling_f = (1 + f_ret).rolling(252).apply(np.prod, raw=True) - 1
        rolling_b = (1 + b_ret).rolling(252).apply(np.prod, raw=True) - 1

        wins = (rolling_f > rolling_b).dropna()
        win_rate = wins.mean() * 100 if not wins.empty else 0.0

        # 6. Capture Ratios
        up_mkt = b_ret > 0
        down_mkt = b_ret < 0

        upside_cap = (f_ret[up_mkt].mean() / b_ret[up_mkt].mean()) * 100 if b_ret[up_mkt].mean() != 0 else 100
        downside_cap = (f_ret[down_mkt].mean() / b_ret[down_mkt].mean()) * 100 if b_ret[down_mkt].mean() != 0 else 100

        return {
            "alpha": alpha,
            "beta": beta,
            "te": tracking_error,
            "sortino": sortino,
            "max_dd": max_dd,
            "win_rate": win_rate,
            "upside": upside_cap,
            "downside": downside_cap,
            "cagr": cagr_f * 100
        }

    except Exception as e:
        logger.error(f"Metric calc error: {e}")
        return None
