import yfinance as yf
import pandas as pd
from fortress_config import INDEX_BENCHMARKS
import sys

def check_benchmark(name, symbol):
    print(f"Checking {name} ({symbol})...", end=" ")
    try:
        data = yf.download(symbol, period="1mo", interval="1d", progress=False, auto_adjust=False)

        # Handle MultiIndex columns if present (new yfinance behavior)
        if isinstance(data.columns, pd.MultiIndex):
            # Try to cross-section by symbol if it exists in levels
            try:
                data = data.xs(symbol, axis=1, level=1)
            except KeyError:
                try:
                    data = data.xs(symbol, axis=1, level=0)
                except KeyError:
                    pass # Maybe it's flat but MultiIndex?

        if not data.empty and len(data) > 0:
            if 'Close' in data.columns:
                val = data['Close'].iloc[-1]
                # Ensure scalar
                if isinstance(val, pd.Series):
                    val = val.iloc[0]

                print(f"✅ OK (Last Close: {float(val):.2f})")
                return True
            else:
                 print(f"❌ FAIL (Missing 'Close' column. Columns: {data.columns})")
                 return False
        else:
            print(f"❌ FAIL (Empty Data)")
            return False
    except Exception as e:
        print(f"❌ FAIL (Error: {e})")
        return False

def main():
    print("\n--- 🛡️ Fortress System Health Report ---")

    # Check Nifty Smallcap 250 specifically as requested
    smallcap_symbol = INDEX_BENCHMARKS.get("Nifty Smallcap 250", "^CNXSC")
    print(f"Target Benchmark for Smallcap: {smallcap_symbol}")

    success = check_benchmark("Nifty Smallcap 250", smallcap_symbol)

    print("-" * 40)

    # Check all benchmarks
    print("Verifying all configured benchmarks:")
    all_passed = True
    for name, symbol in INDEX_BENCHMARKS.items():
        if name == "Nifty Smallcap 250": continue # Already checked
        if not check_benchmark(name, symbol):
            all_passed = False

    if success and all_passed:
        print("\n✅ SYSTEM HEALTH: GREEN. All benchmarks accessible.")
        sys.exit(0)
    else:
        print("\n⚠️ SYSTEM HEALTH: YELLOW/RED. Some benchmarks failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
