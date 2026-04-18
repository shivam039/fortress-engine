import logging
from datetime import datetime

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from commodities.logic import build_commodities_frame
from utils.db import log_audit, register_scan, save_scan_results

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Colour mapping for conviction labels
# ─────────────────────────────────────────────
LABEL_COLOR = {
    "STRONG BUY":    "#00c853",
    "BUY":           "#64dd17",
    "HOLD":          "#ffd600",
    "UNDERPERFORMER":"#ff6d00",
    "AVOID":         "#d50000",
}

DISPLAY_COLS = [
    "Conviction Emoji", "Commodity", "Conviction Score", "Conviction Label",
    "Price (₹)", "Trend", "ATR Regime", "1M Return %", "3M Return %",
    "Spread %", "USDINR",
]


def _scorecard(row: dict):
    """Render a single commodity conviction card."""
    label  = row.get("Conviction Label", "HOLD")
    emoji  = row.get("Conviction Emoji", "🟡")
    score  = row.get("Conviction Score", 50)
    color  = LABEL_COLOR.get(label, "#ffd600")
    decision = row.get("Decision", "")

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-left: 5px solid {color};
            border-radius: 12px;
            padding: 16px 20px;
            margin-bottom: 10px;
        ">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <h3 style="margin:0; color:#fff; font-size:1.15rem;">
                    {emoji} {row.get('Commodity','—')}
                </h3>
                <span style="
                    background:{color}; color:#000; font-weight:700;
                    padding:4px 14px; border-radius:20px; font-size:0.9rem;
                ">
                    {label} • {score}/100
                </span>
            </div>
            <hr style="border-color:#333; margin: 8px 0;">
            <p style="margin:2px 0; color:#ccc; font-size:0.88rem;">{decision}</p>
            <div style="margin-top:10px; display:flex; gap:24px; flex-wrap:wrap;">
                <div><span style="color:#8f8f8f">Price</span><br>
                     <b style="color:#fff">₹ {row.get("Price (₹)", 0):,.2f}</b></div>
                <div><span style="color:#8f8f8f">Trend</span><br>
                     <b style="color:#fff">{row.get("Trend","—")}</b></div>
                <div><span style="color:#8f8f8f">1M Return</span><br>
                     <b style="color:#fff">{row.get("1M Return %",0):+.2f}%</b></div>
                <div><span style="color:#8f8f8f">ATR Regime</span><br>
                     <b style="color:#fff">{row.get("ATR Regime","—")}</b></div>
                <div><span style="color:#8f8f8f">Spread</span><br>
                     <b style="color:#fff">{row.get("Spread %",0):+.2f}%</b></div>
                <div><span style="color:#8f8f8f">USDINR</span><br>
                     <b style="color:#fff">{row.get("USDINR",84.0):.2f}</b></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render(broker_choice="Zerodha"):
    st.header("🌍 Commodities Intelligence Terminal")

    commodity  = st.sidebar.selectbox("Commodity", ["All", "Gold", "Silver", "Crude", "Copper"])
    debug_mode = st.sidebar.toggle("Commodities Debug Mode", value=False)

    # ── Load data ────────────────────────────────────────────────────────
    try:
        with st.spinner("Loading commodities data…"):
            df = build_commodities_frame(None if commodity == "All" else commodity)
    except Exception as e:
        logger.error(f"commodities error: {e}")
        st.warning("Data load failed — retry or check logs")
        return

    if df.empty:
        st.warning("No commodity data available right now.")
        return

    # ── Conviction summary bar ────────────────────────────────────────────
    st.subheader("📊 Conviction Summary")
    if "Conviction Score" in df.columns:
        summary_cols = st.columns(len(df))
        for i, (_, row) in enumerate(df.iterrows()):
            with summary_cols[i]:
                score = row.get("Conviction Score", 50)
                label = row.get("Conviction Label", "HOLD")
                emoji = row.get("Conviction Emoji", "🟡")
                color = LABEL_COLOR.get(label, "#ffd600")
                st.markdown(
                    f"""<div style="text-align:center; background:#1a1a2e;
                        border-top: 4px solid {color}; border-radius:10px; padding:12px;">
                        <div style="font-size:1.3rem">{emoji}</div>
                        <div style="font-weight:700; color:#fff; font-size:0.95rem">
                            {row.get('Commodity','—')}</div>
                        <div style="font-size:1.6rem; font-weight:900; color:{color}">{score}</div>
                        <div style="color:#aaa; font-size:0.8rem">{label}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # ── Conviction cards ─────────────────────────────────────────────────
    st.subheader("🎯 Decision Cards")
    for _, row in df.iterrows():
        _scorecard(row.to_dict())

    st.markdown("---")

    # ── Data table ───────────────────────────────────────────────────────
    with st.expander("📋 Full Data Table", expanded=False):
        show_cols = [c for c in DISPLAY_COLS if c in df.columns]
        st.dataframe(df[show_cols], width='stretch', hide_index=True)

    # ── Heatmap ────────────────────────────────────────────────────────
    with st.expander("🔥 Conviction & Spread Heatmap", expanded=False):
        heat_data = df.set_index("Commodity")[["Conviction Score", "Spread %", "1M Return %", "3M Return %"]]
        heat = px.imshow(
            heat_data.T,
            aspect="auto",
            color_continuous_scale="RdYlGn",
            title="Commodity Intelligence Heatmap",
            labels={"color": "Value"},
        )
        heat.update_xaxes(tickvals=list(range(len(df))), ticktext=df["Commodity"].tolist())
        st.plotly_chart(heat, width='stretch')

    # ── Price Momentum Chart ──────────────────────────────────────────────
    with st.expander("📈 Return Comparison", expanded=False):
        ret_cols = [c for c in ["1M Return %", "3M Return %", "6M Return %"] if c in df.columns]
        if ret_cols:
            fig = go.Figure()
            for col in ret_cols:
                fig.add_trace(go.Bar(name=col, x=df["Commodity"], y=df[col]))
            fig.update_layout(
                barmode="group",
                title="Return Comparison across Time Horizons",
                template="plotly_dark",
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig, width='stretch')

    # ── Export ────────────────────────────────────────────────────────────
    st.download_button(
        "⬇️ Export Commodities CSV",
        df.to_csv(index=False).encode("utf-8"),
        f"commodities_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv",
    )

    # ── Save to DB ────────────────────────────────────────────────────────
    try:
        scan_id = register_scan(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            universe="Commodities", scan_type="COMMODITY", status="Completed",
        )
        save_scan_results(scan_id, df)
        log_audit("Commodity scan", "Commodities", f"Rows logged: {len(df)}")
    except Exception as e:
        logger.error(f"commodities DB error: {e}")

    if debug_mode:
        st.markdown("### 🔍 Debug — Raw DataFrame")
        st.dataframe(df, width='stretch')
