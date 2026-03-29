import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from mf_lab.logic import (
    DEFAULT_SCHEMES,
    backtest_vs_benchmark,
    classify_category,
    run_full_mf_scan,
)
from utils.db import fetch_mf_cached_results, log_audit, log_scan_results, upsert_mf_scan_results

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

DISPLAY_COLS = [
    "Conviction Emoji", "Scheme", "Conviction Score", "Conviction Label",
    "NAV", "1Y Return", "3Y Return", "5Y Return",
    "Sharpe", "Sortino", "Alpha", "Category", "Scheme Code",
]

LABEL_COLOR = {
    "STRONG BUY":    "#00c853",
    "BUY":           "#64dd17",
    "HOLD":          "#ffd600",
    "UNDERPERFORMER":"#ff6d00",
    "AVOID":         "#d50000",
}


def _post_process(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["Category"] = df.get("Scheme", pd.Series(dtype=str)).apply(classify_category)
    numeric_cols = ["NAV", "1Y Return", "3Y Return", "5Y Return", "Sharpe",
                    "Sortino", "Consistency Score", "Volatility", "Alpha", "Beta"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    # Apply conviction scoring
    try:
        from utils.conviction_engine import enrich_mf_dataframe
        df = enrich_mf_dataframe(df)
    except Exception as e:
        logger.warning(f"Conviction scoring skipped: {e}")
    return df


def _apply_filters(df: pd.DataFrame, categories, min_sharpe) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df["Category"].isin(categories) & (df["Sharpe"] >= min_sharpe)
    return df[mask].copy()


def _scorecard(row: dict):
    """Render a single fund conviction card."""
    label = row.get("Conviction Label", "HOLD")
    emoji = row.get("Conviction Emoji", "🟡")
    score = row.get("Conviction Score", 50)
    color = LABEL_COLOR.get(label, "#ffd600")
    decision = row.get("Decision", "")

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-left: 5px solid {color};
            border-radius: 12px;
            padding: 16px 20px;
            margin-bottom: 15px;
        ">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <h3 style="margin:0; color:#fff; font-size:1.1rem;">
                    {emoji} {row.get('Scheme','—')}
                </h3>
                <span style="
                    background:{color}; color:#000; font-weight:700;
                    padding:4px 14px; border-radius:20px; font-size:0.85rem;
                ">
                    {label} • {score}/100
                </span>
            </div>
            <hr style="border-color:#333; margin: 10px 0;">
            <p style="margin:5px 0; color:#ccc; font-size:0.9rem; line-height:1.4;">{decision}</p>
            <div style="margin-top:12px; display:flex; gap:25px; flex-wrap:wrap;">
                <div><span style="color:#8f8f8f; font-size:0.75rem;">1Y Return</span><br>
                     <b style="color:#fff; font-size:0.95rem;">{row.get("1Y Return",0):+.2f}%</b></div>
                <div><span style="color:#8f8f8f; font-size:0.75rem;">Sharpe</span><br>
                     <b style="color:#fff; font-size:0.95rem;">{row.get("Sharpe",0):.2f}</b></div>
                <div><span style="color:#8f8f8f; font-size:0.75rem;">Alpha</span><br>
                     <b style="color:#fff; font-size:0.95rem;">{row.get("Alpha",0):+.2f}%</b></div>
                <div><span style="color:#8f8f8f; font-size:0.75rem;">Category</span><br>
                     <b style="color:#fff; font-size:0.95rem;">{row.get("Category","—")}</b></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────
#  Main render
# ─────────────────────────────────────────────

def render():
    st.subheader("🛡️ Fortress MF Pro: Consistency Lab")

    # ── Sidebar filters ──────────────────────────────────────────────
    debug_mode = st.sidebar.toggle("MF Debug Mode", value=False)
    categories = st.sidebar.multiselect(
        "Category", ["Equity", "Debt", "Hybrid"],
        default=["Equity", "Debt", "Hybrid"]
    )
    min_sharpe = st.sidebar.number_input("Min Sharpe", min_value=0.0, value=0.5, step=0.1)

    # ── Load from Neon cache (instant) ───────────────────────────────
    cached_df = pd.DataFrame()
    cache_info_text = "No cached results yet."

    try:
        cached_df = fetch_mf_cached_results(max_age_days=31)
        if not cached_df.empty:
            cached_df = _post_process(cached_df)
            # Show scan freshness
            if "scan_date" in cached_df.columns:
                latest = cached_df["scan_date"].max()
                cache_info_text = f"✅ Showing results from last scan: **{latest}** ({len(cached_df)} funds)"
            else:
                cache_info_text = f"✅ Showing {len(cached_df)} cached funds."
    except Exception as e:
        logger.error(f"Neon cache load failed: {e}")

    # ── Run monthly scan button ───────────────────────────────────────
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(cache_info_text)
    with col2:
        run_scan = st.button(
            "🚀 Run Full MF Scan",
            type="primary",
            use_container_width=True,
            help="Discovers ALL Direct-Growth funds from AMFI and scores them. Takes 5-15 mins. Results saved to DB for 30 days."
        )

    if run_scan:
        st.info("⏳ Discovering all Direct-Growth funds from AMFI and scoring in parallel (20 workers)… This may take 5–15 minutes. You can switch tabs — results will be saved automatically.")

        progress_bar = st.progress(0.0)
        status_text  = st.empty()
        results_holder = st.empty()

        def _update_progress(done, total):
            pct = done / total if total > 0 else 0
            progress_bar.progress(min(pct, 1.0))
            status_text.text(f"Scored {done}/{total} funds…")

        try:
            df = run_full_mf_scan(progress_callback=_update_progress, max_workers=20)
            if not df.empty:
                progress_bar.progress(1.0)
                status_text.success(f"✅ Scan complete! {len(df)} funds scored and saved to Neon.")
                cached_df = _post_process(df)
                log_audit("MF Full Scan", "Mutual Funds", f"{len(df)} funds scored and saved")
            else:
                st.warning("Scan returned no results. Check AMFI API availability.")
        except Exception as e:
            st.error(f"Scan failed: {e}")
            logger.error(f"MF full scan error: {e}")

    # ── Display results ───────────────────────────────────────────────
    if cached_df.empty:
        st.info("No data yet. Click **🚀 Run Full MF Scan** to populate results (runs once a month).")
        return

    filtered = _apply_filters(cached_df, categories, min_sharpe)

    if filtered.empty:
        st.warning("No funds match the current filters. Adjust Sharpe or Category in the sidebar.")
    else:
        # ── Top Conviction Picks ────────────────────────────────────
        st.subheader("🎯 Top Conviction Picks")
        top_picks = filtered[filtered["Conviction Label"].isin(["STRONG BUY", "BUY"])].head(5)
        if not top_picks.empty:
            for _, row in top_picks.iterrows():
                _scorecard(row.to_dict())
        else:
            st.info("No funds currently meet the 'Strong Buy' or 'Buy' threshold. Showing all results below.")

        st.markdown("---")
        st.subheader("📋 Full Result Universe")
        display_cols = [c for c in DISPLAY_COLS if c in filtered.columns]
        st.dataframe(
            filtered[display_cols].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

        csv_bytes = filtered.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Export MF CSV",
            data=csv_bytes,
            file_name=f"mf_consistency_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

        # ── Backtest ─────────────────────────────────────────────────
        if not filtered.empty and "Scheme Code" in filtered.columns:
            scheme_choice = st.selectbox(
                "📊 Backtest a fund vs Nifty 50",
                filtered["Scheme Code"].tolist()
            )
            if scheme_choice:
                with st.spinner("Loading backtest…"):
                    bt = backtest_vs_benchmark(str(scheme_choice))
                if not bt.empty:
                    st.line_chart(bt)
                else:
                    st.info("Not enough NAV history for backtest (need > 1 year).")

    if debug_mode:
        st.markdown("### 🔍 Debug — Full Raw Cache")
        st.dataframe(cached_df, use_container_width=True)
