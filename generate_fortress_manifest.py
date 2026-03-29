import json
import os
import sys

# Import dynamic configuration
try:
    from fortress_config import TICKER_GROUPS, SECTOR_MAP
except ImportError:
    print("Warning: Could not import fortress_config. Using empty defaults.")
    TICKER_GROUPS = {}
    SECTOR_MAP = {}

def generate_manifest():
    manifest = {
        "architecture": {
            "files": {
                "logic.py": "Core calculation engine for integrity metrics (Alpha, Beta, Sortino), drift detection, and data normalization. Located in mf_lab/.",
                "ui.py": "Streamlit UI components for rendering charts, metrics, and report dashboards. Located in mf_lab/.",
                "cron_mf_audit.py": "Background scheduled task for fetching data, running audits, and persisting results to DB. Runs independently.",
                "db.py": "Database abstraction layer for SQLite interactions (scan results, audit logs). Located in utils/.",
                "fortress_config.py": "Central configuration for ticker groups, sector maps, and benchmarks.",
                "streamlit_app.py": "Application entry point and navigation controller."
            },
            "dependencies": {
                "yfinance": "Market data fetching (Nifty indices, stock prices).",
                "pandas_ta": "Technical analysis library.",
                "streamlit": "Web application framework.",
                "mftool": "Mutual fund utility (supplementary to direct API calls).",
                "requests": "Direct HTTP requests to mfapi.in for NAV data.",
                "pandas": "Data manipulation and analysis.",
                "sqlite3": "Local database storage."
            },
            "dependency_graph": "streamlit_app -> [mf_lab/ui, mf_lab/logic, utils/db]; cron_mf_audit -> [mf_lab/logic, utils/db]"
        },
        "mathematical_core": {
            "formulas": {
                "scoring": "(Alpha * 0.4) + (Sortino * 0.3) + ((100 - DownsideCapture) * 0.3)",
                "normalization": "Category-Isolated Min-Max Scaling: ((Score - Cat_Min) / (Cat_Max - Cat_Min)) * 100. Defaults to 50.0 if variance is 0, 100.0 if single fund.",
                "alpha": "Annualized excess return over benchmark (60-day rolling average).",
                "sortino": "(CAGR - RiskFreeRate_6%) / DownsideDeviation",
                "win_rate": "Percentage of rolling 252-day periods where Fund_CAGR > Benchmark_CAGR",
                "beta": "Covariance(Fund, Benchmark) / Variance(Benchmark)",
                "tracking_error": "Stdev(Fund_Return - Benchmark_Return) * Sqrt(252) * 100",
                "capture_ratio_upside": "(Mean_Fund_Return_UpMkt / Mean_Benchmark_Return_UpMkt) * 100",
                "capture_ratio_downside": "(Mean_Fund_Return_DownMkt / Mean_Benchmark_Return_DownMkt) * 100"
            },
            "backtest_logic": {
                "methodology": "On-demand fetching of daily NAV history via mfapi.in for selected funds.",
                "crisis_windows": {
                    "Global Recession Scare (2020)": "Jan 1, 2020 – April 30, 2020 (Pandemic Crash)",
                    "Rate Hike Pivot (2022)": "Jan 1, 2022 – July 31, 2022 (Inflation & Rates)",
                    "Small-Cap Shakeout (2024)": "Feb 1, 2024 – March 31, 2024 (SEBI Warnings)"
                },
                "metrics": {
                    "max_drawdown": "Deepest peak-to-trough decline within the specified window.",
                    "recovery_time": "Time taken to reclaim the previous peak (or simplified recovery metric)."
                }
            },
            "overlap_algorithm": {
                "definition": "Statistical Return Correlation of daily returns over the common history.",
                "threshold": "Correlation > 0.85 is flagged as 'High Overlap / Low Diversification'.",
                "visualization": "Risk Factor Radar (Heatmap) showing pairwise correlations."
            },
            "integrity_thresholds": {
                "debt_breach": "CAGR < 5.75% (Repo Rate 6.5% - 75bps tolerance).",
                "tracking_error_limits": {
                    "Large Cap": 6.0,
                    "Others": 9.0
                },
                "beta_limits": {
                    "Large Cap": 1.15,
                    "Flexi/Multi Cap": 1.20,
                    "Mid Cap": 1.25,
                    "Small Cap": 1.45
                },
                "technical_debt_note": "Resolved: Small Cap Beta threshold unified to 1.45 as Source of Truth."
            }
        },
        "taxonomy": {
            "categories": {
                "equity_sub_categories": ["Large Cap", "Mid Cap", "Small Cap", "Flexi/Multi Cap", "Focused", "Value/Contra", "ELSS"],
                "debt_sub_categories": ["Liquid/Overnight", "Ultra Short/Low Duration", "Corporate Bond", "Gilt/Dynamic Bond"],
                "priority_order": ["Liquid/Overnight", "Ultra Short", "Corporate Bond", "Gilt", "Focused", "Value/Contra", "ELSS", "Flexi/Multi Cap", "Small Cap", "Mid Cap", "Large Cap"],
                "exclusions": ["regular", "idcw", "etf (Equity only)"],
                "inclusions": ["direct", "growth", "etf (Debt only)"]
            },
            "benchmarks": {
                "Large Cap": "^NSEI",
                "Mid Cap": "^NSEMDCP50",
                "Small Cap": "^CNXSC",
                "Flexi/Multi Cap": "^CNX500",
                "Focused": "^CNX500",
                "Value/Contra": "^CNX500",
                "ELSS": "^CNX500",
                "Debt (All Categories)": "LIQUIDBEES.NS (Nippon India ETF Liquid BeES)"
            }
        },
        "audit_workflows": {
            "background_scan": {
                "frequency": "Weekly, Sunday at 9:00 PM IST",
                "rate_limiting": "time.sleep(0.4) between API calls to respect AMFI limits.",
                "safety_checks": "Sleep enforced in main loop of cron_mf_audit.py.",
                "survival_filter": "Funds with < 750 days of history are skipped."
            },
            "health_check": {
                "sector_rotation": "Calculates 1-month returns for Banking, IT, Auto, FMCG, Infra. Identifies rotation flow.",
                "hidden_gem": "Identifies fund with largest positive Delta in Fortress Score compared to previous audit.",
                "market_breadth": "Nifty 50 Advance/Decline Ratio (2-day)."
            }
        },
        "data_schema": {
            "scan_mf_table": {
                "Symbol": "Name of the Mutual Fund Scheme.",
                "Scheme Code": "Unique identifier from AMFI.",
                "Category": "Derived category (e.g., Small Cap, Focused).",
                "Score": "Raw calculated Fortress Score before normalization.",
                "Fortress Score": "Normalized 0-100 score relative to category peers (calculated during reporting/viewing).",
                "Alpha (True)": "Rolling 60-day annualized Alpha.",
                "Sortino": "Risk-adjusted return ratio.",
                "Upside Cap": "Upside Capture Ratio.",
                "Downside Cap": "Downside Capture Ratio.",
                "Max Drawdown": "Maximum observed loss from peak to trough.",
                "Win Rate": "Consistency metric (% of rolling years beating benchmark).",
                "Verdict": "Integrity/Drift Status (e.g., 'Stable', 'Critical').",
                "Price": "Latest NAV.",
                "Beta": "Volatility relative to benchmark.",
                "Tracking Error": "Deviation from benchmark returns.",
                "cagr": "Compound Annual Growth Rate (Raw) - Used for Debt Integrity Checks."
            },
            "dynamic_config": {
                "ticker_groups": TICKER_GROUPS,
                "sector_map": SECTOR_MAP
            }
        }
    }

    # Write to file
    with open("fortress_manifest.json", "w") as f:
        json.dump(manifest, f, indent=4)

    # Validation Summary
    print("✅ Manifest Generated Successfully")
    print(f"📂 Files Indexed: {len(manifest['architecture']['files'])}")
    print(f"🧮 Formulas Documented: {len(manifest['mathematical_core']['formulas'])}")
    print(f"📊 Ticker Groups Imported: {len(manifest['data_schema']['dynamic_config']['ticker_groups'])}")

if __name__ == "__main__":
    generate_manifest()
