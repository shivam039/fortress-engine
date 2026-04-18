import json
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from utils.db import DB_NAME, _read_df
from stock_scanner.logic import backtest_top_picks

KEY_COLUMNS = [
    "symbol",
    "price",
    "target_price",
    "conviction_score",
    "rsi",
    "ema200",
    "analyst_target_mean",
    "regime",
    "pick_type",
    "scan_timestamp",
]


@st.cache_data(ttl=300)
def get_full_scan_history(limit=1000):
    query = f"""
    SELECT *
    FROM scan_history_details
    ORDER BY scan_timestamp DESC
    LIMIT {limit}
    """
    try:
        return _read_df(query)
    except Exception:
        try:
            with sqlite3.connect(DB_NAME, timeout=15.0) as sqlite_conn:
                return pd.read_sql_query(query, sqlite_conn)
        except Exception:
            return pd.DataFrame()

@st.cache_data(ttl=1800)
def get_unique_scan_timestamps():
    query = """
    SELECT DISTINCT scan_timestamp
    FROM scan_history_details
    WHERE scan_timestamp IS NOT NULL
    ORDER BY scan_timestamp DESC
    LIMIT 50
    """
    try:
        df = _read_df(query)
        if df.empty:
            raise ValueError("empty")
    except Exception:
        # Fallback: derive timestamps from scans table
        try:
            df = _read_df(
                "SELECT timestamp AS scan_timestamp FROM scans "
                "WHERE status='Completed' ORDER BY timestamp DESC LIMIT 50"
            )
        except Exception:
            pass

    if df.empty:
        return []

    ts_col = pd.to_datetime(df["scan_timestamp"], errors="coerce", utc=True).dropna()
    return ts_col.tolist()


@st.cache_data(ttl=1800)
def get_scan_data_for_timestamp(selected_timestamp):
    try:
        raw_df = _read_df(
            """
            SELECT * FROM scan_history_details
            WHERE scan_timestamp = :ts
            ORDER BY conviction_score DESC
            """,
            params={"ts": selected_timestamp},
        )
    except Exception:
        try:
            with sqlite3.connect(DB_NAME, timeout=15.0) as sqlite_conn:
                raw_df = pd.read_sql_query(
                    """
                    SELECT d.*, s.timestamp AS scan_timestamp_fallback
                    FROM scan_history_details d
                    LEFT JOIN scans s ON d.scan_id = s.scan_id
                    WHERE COALESCE(d.scan_timestamp, s.timestamp) = ?
                    ORDER BY d.conviction_score DESC
                    """,
                    sqlite_conn,
                    params=(selected_timestamp,),
                )
        except Exception:
            return pd.DataFrame()

    if raw_df.empty:
        return pd.DataFrame()

    if "raw_data" in raw_df.columns:
        def parse_raw(value):
            if isinstance(value, dict):
                return value
            if isinstance(value, str) and value.strip():
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return {}
            return {}

        normalized = pd.json_normalize(raw_df["raw_data"].apply(parse_raw))
        merged = pd.concat([normalized, raw_df], axis=1)
        merged = merged.loc[:, ~merged.columns.duplicated()]
        return merged

    return raw_df


def classify_long_term(df):
    if df.empty:
        return df

    scores = pd.to_numeric(df.get("conviction_score"), errors="coerce")
    regime = df.get("regime", pd.Series("", index=df.index)).astype(str).str.lower()
    price = pd.to_numeric(df.get("price"), errors="coerce")
    analyst_target = pd.to_numeric(df.get("analyst_target_mean"), errors="coerce")
    upside = analyst_target / price

    return df[(scores >= 80) | ((regime == "bull") & (upside >= 1.25))].copy()


def classify_momentum(df):
    if df.empty:
        return df
    scores = pd.to_numeric(df.get("conviction_score"), errors="coerce")
    return df[(scores >= 65) & (scores < 80)].copy()


def classify_strategic(df):
    if df.empty:
        return df

    long_idx = classify_long_term(df).index
    momentum_idx = classify_momentum(df).index
    return df[~df.index.isin(long_idx.union(momentum_idx))].copy()


def _pick_type_mask(df: pd.DataFrame, patterns: list[str]) -> pd.Series:
    if "pick_type" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    lower = df["pick_type"].astype(str).str.lower()
    mask = pd.Series([False] * len(df), index=df.index)
    for pattern in patterns:
        mask = mask | lower.str.contains(pattern, na=False)
    return mask


def _format_ts_for_display(ts):
    parsed = pd.to_datetime(ts, errors="coerce", utc=True)
    if pd.isna(parsed):
        return str(ts)
    return parsed.tz_convert(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST")


def _apply_symbol_filter(df: pd.DataFrame, search_term: str) -> pd.DataFrame:
    if df.empty or not search_term:
        return df
    if "symbol" not in df.columns:
        return df.iloc[0:0]
    return df[df["symbol"].astype(str).str.contains(search_term, case=False, na=False)].copy()


def _display_pick_table(title: str, df: pd.DataFrame, selected_label: str, search_term: str):
    with st.expander(title, expanded=True):
        if df.empty:
            st.info("No matches" if search_term else "No data available for this scan")
            return

        display_cols = [c for c in KEY_COLUMNS if c in df.columns]
        if not display_cols:
            display_cols = [c for c in df.columns if c not in {"id", "scan_id", "raw_data"}]

        table_df = df[display_cols]
        st.dataframe(table_df, width='stretch', hide_index=True)
        st.download_button(
            f"📥 Export {title} CSV",
            table_df.to_csv(index=False).encode("utf-8"),
            f"{title.lower().replace(' ', '_')}_{selected_label.replace(':', '-')}.csv",
            "text/csv",
        )


def render():
    st.header("📜 Master Scan History")

    timestamps = get_unique_scan_timestamps()
    if not timestamps:
        st.info("No data available for this scan")
        return

    options = [None] + timestamps
    selected_timestamp = st.selectbox(
        "Select Scan Timestamp",
        options=options,
        format_func=lambda ts: "Select a scan timestamp" if ts is None else _format_ts_for_display(ts),
    )
    symbol_search = st.text_input("Search symbol across all tables", placeholder="e.g. RELIANCE")

    if selected_timestamp is None:
        st.info("Select a scan timestamp to view historical results")
        return

    selected_timestamp_str = pd.to_datetime(selected_timestamp, utc=True).isoformat()
    selected_label = _format_ts_for_display(selected_timestamp)

    with st.spinner(f"Loading scan snapshot for {selected_label}..."):
        df = get_scan_data_for_timestamp(selected_timestamp_str)

    if df.empty:
        st.info("No data available for this scan")
        return

    if "pick_type" in df.columns:
        long_term = df[_pick_type_mask(df, ["long-term", "long term", "longterm"])].copy()
        momentum = df[_pick_type_mask(df, ["momentum"])].copy()
        strategic = df[_pick_type_mask(df, ["strategic", "actionable"])].copy()

        # Keep unclassified rows visible in Strategic table
        if strategic.empty:
            used_idx = long_term.index.union(momentum.index)
            strategic = df[~df.index.isin(used_idx)].copy()
    else:
        long_term = classify_long_term(df)
        momentum = classify_momentum(df)
        strategic = classify_strategic(df)

    long_term = _apply_symbol_filter(long_term, symbol_search)
    momentum = _apply_symbol_filter(momentum, symbol_search)
    strategic = _apply_symbol_filter(strategic, symbol_search)

    _display_pick_table("Long-Term Picks", long_term, selected_label, symbol_search)
    _display_pick_table("Momentum Picks", momentum, selected_label, symbol_search)
    _display_pick_table("Strategic Picks", strategic, selected_label, symbol_search)

    combined = pd.concat(
        [
            long_term.assign(_table="Long-Term Picks"),
            momentum.assign(_table="Momentum Picks"),
            strategic.assign(_table="Strategic Picks"),
        ],
        ignore_index=True,
    )
    if not combined.empty:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.download_button(
                "📥 Export Combined CSV",
                combined.to_csv(index=False).encode("utf-8"),
                f"scan_history_combined_{selected_label.replace(':', '-')}.csv",
                "text/csv",
                width='stretch'
            )
        with col2:
            pass

    st.markdown("### 🧪 Backtest Analysis")
    st.caption(f"Simulate returns for picks from **{selected_label}** vs Nifty 50.")

    if st.button("Run Backtest for This Scan", key="hist_backtest_btn", type="primary"):
        with st.spinner("Running backtest against Nifty..."):
            bt_results = backtest_top_picks(selected_timestamp_str)
            if not bt_results.empty:
                st.success("Backtest complete!")
                st.dataframe(bt_results, width='stretch')
                st.download_button(
                    "📥 Download Backtest Results",
                    bt_results.to_csv(index=False).encode("utf-8"),
                    f"backtest_results_{selected_label.replace(':', '-')}.csv",
                    "text/csv"
                )
            else:
                st.warning("No backtest data available (needs >7 days history) or no valid picks found in this scan.")

    st.markdown("---")
    st.subheader("📥 Bulk Data Export")
    with st.expander("Export Full Scan History (Raw Data)", expanded=False):
        st.caption("Download the last 1000 raw scan records for offline analysis.")
        if st.button("Prepare Full History CSV"):
            with st.spinner("Fetching data..."):
                full_df = get_full_scan_history(limit=1000)
                if not full_df.empty:
                    st.dataframe(full_df.head(5), width='stretch')
                    st.download_button(
                        "📥 Download Full History (Last 1000)",
                        full_df.to_csv(index=False).encode("utf-8"),
                        f"full_scan_history_last1000_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv",
                        width='content'
                    )
                else:
                    st.warning("No history data found.")

    # Utility for DB Migration (Optional Display)
    # st.code(
    #     "ALTER TABLE scan_history_details ADD COLUMN IF NOT EXISTS pick_type TEXT;",
    #     language="sql",
    # )
