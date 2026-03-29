import streamlit as st
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from datetime import datetime
import pytz
from fortress_config import INDEX_BENCHMARKS, NIFTY_SYMBOL

@st.cache_data(ttl="60s")
def fetch_market_pulse_data():
    """
    Fetches market pulse data on-demand.
    Returns a dict with:
      - 'snapshot': dict of {name: {close, change_pct, status, rsi, ...}} for tiles
      - 'regime': dict of {Market_Regime, Regime_Multiplier, VIX} for scanner
      - 'timestamp': str formatted as "HH:MM IST"
    """
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist).strftime("%H:%M IST")

    out = {
        "snapshot": {},
        "regime": {"Market_Regime": "Range", "Regime_Multiplier": 1.0, "VIX": 20.0},
        "timestamp": now_ist
    }

    # 1. Define symbols to fetch
    symbols_map = INDEX_BENCHMARKS.copy()
    if "Nifty 50" not in symbols_map:
        symbols_map["Nifty 50"] = NIFTY_SYMBOL

    vix_symbol = "^INDIAVIX"
    fetch_list = list(set(list(symbols_map.values()) + [vix_symbol]))

    try:
        # Bulk download
        all_data = yf.download(fetch_list, period="1y", interval="1d", group_by='ticker', threads=True, progress=False, auto_adjust=False)
    except Exception as e:
        return out

    is_multi = isinstance(all_data.columns, pd.MultiIndex)

    # 2. Process Benchmarks for Snapshot
    for name, symbol in symbols_map.items():
        try:
            if is_multi:
                if symbol in all_data.columns.levels[0]:
                    idx_data = all_data[symbol].copy()
                else:
                    continue
            else:
                # If single ticker and it matches (or we assume it matches if only 1 requested)
                # But fetch_list usually > 1. If single, check if symbol matches.
                # yf.download for single symbol returns flat DF.
                # We can only support this if fetch_list had 1 item.
                idx_data = all_data.copy()

            idx_data = idx_data.dropna(subset=["Close"])
            if idx_data.empty:
                continue

            # Calculate Metrics
            close = idx_data["Close"]
            current_price = close.iloc[-1]
            prev_close = close.iloc[-2] if len(close) > 1 else current_price
            change_pct = ((current_price - prev_close) / prev_close) * 100

            # EMA 200 Status
            status = "⚪ N/A"
            ema200 = None
            if len(close) >= 200:
                ema_series = ta.ema(close, length=200)
                if ema_series is not None and not ema_series.empty:
                    ema200 = ema_series.iloc[-1]
                    status = "🟢 BULL" if current_price > ema200 else "🔴 BEAR"
                else:
                    status = "🟡 ND"
            else:
                status = "🟡 ND"

            # RSI
            rsi = 50.0
            if len(close) >= 14:
                rsi_series = ta.rsi(close, length=14)
                if rsi_series is not None and not rsi_series.empty:
                    rsi = rsi_series.iloc[-1]

            out["snapshot"][name] = {
                "close": current_price,
                "change_pct": change_pct,
                "status": status,
                "rsi": rsi,
                "ema200": ema200
            }
        except Exception:
            continue

    # 3. Process Regime (Nifty + VIX)
    try:
        nifty_now = 0.0
        nifty_ema200 = 0.0
        vix_val = 20.0

        # Nifty Data
        if is_multi:
            if NIFTY_SYMBOL in all_data.columns.levels[0]:
                nifty_df = all_data[NIFTY_SYMBOL].dropna(subset=["Close"])
            else:
                nifty_df = pd.DataFrame()
        else:
            nifty_df = all_data.copy() # Assume flat is Nifty if that's what we got? Unlikely with VIX in list.

        if not nifty_df.empty:
            nifty_close = nifty_df["Close"]
            nifty_now = nifty_close.iloc[-1]
            if len(nifty_close) >= 200:
                nifty_ema200 = ta.ema(nifty_close, 200).iloc[-1]

        # VIX Data
        if is_multi:
            if vix_symbol in all_data.columns.levels[0]:
                vix_df = all_data[vix_symbol].dropna(subset=["Close"])
                if not vix_df.empty:
                    vix_val = vix_df["Close"].iloc[-1]
        elif not is_multi and len(fetch_list) == 1:
             # If we only fetched VIX (unlikely)
             vix_val = all_data["Close"].iloc[-1]

        # Regime Logic
        regime = "Range"
        multiplier = 1.0

        if nifty_now > nifty_ema200 and vix_val < 18:
            regime = "Bull"
            multiplier = 1.15
        elif nifty_now < nifty_ema200 or vix_val > 25:
            regime = "Bear"
            multiplier = 0.85

        out["regime"] = {
            "Market_Regime": regime,
            "Regime_Multiplier": multiplier,
            "VIX": vix_val
        }
    except Exception:
        pass

    return out

def render_market_pulse(pulse_data):
    """Renders the Market Pulse UI component."""
    if not pulse_data:
        st.warning("No pulse data available.")
        return

    snapshot = pulse_data.get("snapshot", {})
    regime = pulse_data.get("regime", {})

    # 1. Regime Header
    r_label = regime.get("Market_Regime", "Range")
    r_mult = regime.get("Regime_Multiplier", 1.0)
    r_vix = regime.get("VIX", 20.0)

    color = "green" if r_label == "Bull" else "red" if r_label == "Bear" else "orange"
    st.markdown(
        f"""
        <div style="padding: 10px; border-radius: 5px; background-color: rgba(128,128,128,0.1); border-left: 5px solid {color}; margin-bottom: 15px;">
            <strong>Market Regime: {r_label}</strong> &nbsp;|&nbsp;
            Stop-Loss Multiplier: <strong>{r_mult}x</strong> &nbsp;|&nbsp;
            India VIX: <strong>{r_vix:.2f}</strong>
        </div>
        """,
        unsafe_allow_html=True
    )

    # 2. Index Tiles
    if snapshot:
        # Use columns for metrics
        cols = st.columns(len(snapshot))
        keys = list(snapshot.keys())
        for i, col in enumerate(cols):
            if i < len(keys):
                name = keys[i]
                metrics = snapshot[name]
                col.metric(
                    label=name,
                    value=f"{metrics['close']:,.0f}",
                    delta=f"{metrics['change_pct']:.2f}%"
                )
                col.caption(f"{metrics['status']} | RSI: {metrics['rsi']:.1f}")
    else:
        st.info("Benchmarks data unavailable.")
