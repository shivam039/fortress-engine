import logging
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from fortress_config import OPTIONS_UNDERLYINGS
from options_algo.logic import fetch_option_chain, get_available_expiries, payoff_curve, scan_strategies
from utils.broker_mappings import generate_dhan_url, generate_zerodha_url
from utils.db import log_audit, register_scan, save_scan_results

logger = logging.getLogger(__name__)


def render(broker_choice="Zerodha"):
    st.header("🤖 Options Algo Terminal")
    debug_mode = st.sidebar.toggle("Options Debug Mode", value=False)

    underlying = st.sidebar.selectbox("Underlying", OPTIONS_UNDERLYINGS)
    expiries = get_available_expiries(underlying)
    expiry = st.sidebar.selectbox("Expiry", expiries) if expiries else None
    risk_pct = st.sidebar.slider("Risk %", 0.5, 5.0, 1.0, 0.5)
    oi_threshold = st.sidebar.number_input("OI threshold", min_value=100, value=10000)

    if not expiry:
        st.warning("Data load failed - retry or check logs")
        return

    try:
        with st.spinner("Loading data..."):
            chain_df, spot, _ = fetch_option_chain(underlying, expiry)
    except Exception as e:
        logger.error(f"options_algo error: {e}")
        st.warning("Data load failed - retry or check logs")
        return

    chain_df = chain_df.fillna(0)
    st.dataframe(chain_df[["Strike", "Type", "IV", "Delta", "OI", "Premium"]], use_container_width=True)
    st.download_button("⬇️ Export Options CSV", chain_df.to_csv(index=False).encode("utf-8"), "options_chain.csv", "text/csv")

    lots = max(1, int((risk_pct / 100) * 100))
    chain_df["Exec"] = chain_df.apply(
        lambda r: generate_zerodha_url(underlying, lots, "BUY") if broker_choice == "Zerodha" else generate_dhan_url(underlying, lots, transaction_type="BUY"),
        axis=1,
    )

    strat_df = scan_strategies(chain_df, oi_threshold=oi_threshold)
    if not strat_df.empty:
        st.subheader("Strategy Scanner")
        st.dataframe(strat_df, use_container_width=True)
        picked = st.selectbox("Payoff strategy", strat_df["Strategy"].tolist())
        p = float(strat_df[strat_df["Strategy"] == picked]["Premium"].iloc[0])
        grid = np.linspace(spot * 0.9, spot * 1.1, 120)
        pnl = payoff_curve(grid, picked, p, spot)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=grid, y=pnl, mode="lines", name=picked))
        fig.update_layout(title="Strategy Payoff", xaxis_title="Underlying", yaxis_title="P&L")
        st.plotly_chart(fig, use_container_width=True)

    try:
        scan_id = register_scan(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), universe=underlying, scan_type="OPTIONS", status="Completed")
        save_scan_results(scan_id, chain_df.head(200))
        log_audit("Options chain scan", "Options", f"{underlying} {expiry}")
    except Exception as e:
        logger.error(f"options_algo error: {e}")

    if debug_mode:
        st.write({"underlying": underlying, "expiry": expiry, "spot": spot})
        st.dataframe(chain_df, use_container_width=True)
