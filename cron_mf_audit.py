import pandas as pd
import sys
import os
import time
from datetime import datetime

# Add current directory to path
sys.path.append(os.getcwd())

# Services
from mf_lab.services.config import logger, LOCK_FILE
from mf_lab.services.data import discover_funds, fetch_fund_nav, fetch_benchmark_data
from mf_lab.services.metrics import calculate_metrics
from mf_lab.services.scoring import calculate_composite_score, normalize_batch_scores
from mf_lab.services.alerts import check_integrity_rules, generate_smart_alerts, send_telegram_alert
from mf_lab.logic import get_category
from utils.db import init_db, register_scan, update_scan_status, bulk_insert_results, log_audit, get_db_backend

# --- CORE LOGIC ---

def run_audit(limit=None):

    # 1. Concurrency Lock
    if os.path.exists(LOCK_FILE):
        logger.warning("Audit already running (Lock file exists). Exiting.")
        return

    try:
        open(LOCK_FILE, "w").close()

        start_time = datetime.now()
        timestamp = start_time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting Fortress Discovery Audit at {timestamp}...")

        # Init DB (ensure tables exist)
        init_db()
        logger.info(f"DB backend: {get_db_backend()}")

        # Register Scan
        scan_id = register_scan(timestamp, universe="Mutual Funds", status="In Progress")

        # 2. Fetch Benchmarks
        logger.info("Fetching Benchmarks (Cached)...")
        # Note: ^CNX500 is often unreliable on Yahoo. Using ^NSEI (Nifty 50) as robust proxy for broad market if 500 fails.
        # Or we could try ^CRSLDX (Nifty 500) if valid, but Nifty 50 is safest fallback.
        nifty50 = fetch_benchmark_data("^NSEI")

        benchmarks_map = {
            'Large Cap': nifty50,
            'Mid Cap': fetch_benchmark_data("^NSEMDCP50"),
            'Small Cap': fetch_benchmark_data("^CNXSC"),
            'Flexi/Multi Cap': nifty50, # Fallback from ^CNX500
            'Focused': nifty50,
            'Value/Contra': nifty50,
            'ELSS': nifty50,
            # Debt
            'Liquid/Overnight': fetch_benchmark_data("LIQUIDBEES.NS"),
            'Ultra Short/Low Duration': fetch_benchmark_data("LIQUIDBEES.NS"),
            'Corporate Bond': fetch_benchmark_data("LIQUIDBEES.NS"),
            'Gilt/Dynamic Bond': fetch_benchmark_data("LIQUIDBEES.NS")
        }

        # Fallback benchmark
        default_bench = benchmarks_map['Flexi/Multi Cap']
        debt_bench = benchmarks_map['Liquid/Overnight']

        # 3. Discover Funds
        logger.info("Discovering Funds...")
        candidates = discover_funds(limit=limit)
        logger.info(f"Found {len(candidates)} candidates.")

        results_data = []
        metrics_data = []
        alerts_data = []

        # 4. Audit Loop
        for i, c in enumerate(candidates):
            try:
                scheme_code = c['schemeCode']
                scheme_name = c['schemeName']

                # Fetch NAV
                fund_df = fetch_fund_nav(scheme_code)

                if len(fund_df) < 750: continue # Min history
                if fund_df['nav'].iloc[-1] < 10: continue # Low quality/penny fund filter

                cat = get_category(scheme_name)
                bench = benchmarks_map.get(cat)

                # Fallback Logic
                if bench is None or bench.empty:
                    if cat in ["Liquid/Overnight", "Ultra Short/Low Duration", "Corporate Bond", "Gilt/Dynamic Bond"]:
                        bench = debt_bench
                    else:
                        bench = default_bench

                if bench.empty: continue

                # Calculate Metrics
                metrics = calculate_metrics(fund_df, bench)

                if metrics:
                    # Calculate Score Components (Raw)
                    raw_score = calculate_composite_score(metrics)

                    # Drift Check
                    integrity, drift_status, drift_msg = check_integrity_rules(metrics, cat)

                    # Store Result
                    res_row = {
                        "scan_id": scan_id,
                        "symbol": scheme_name, # Full name as ID for now
                        "scheme_code": scheme_code, # Added for Blender Support
                        "category": cat,
                        "Score": raw_score, # Raw first, normalized later
                        "price": fund_df['nav'].iloc[-1],
                        "integrity_label": integrity,
                        "drift_status": drift_status,
                        "drift_message": drift_msg
                    }
                    results_data.append(res_row)

                    # Store Metrics
                    met_row = metrics.copy()
                    met_row['scan_id'] = scan_id
                    met_row['symbol'] = scheme_name
                    metrics_data.append(met_row)

                    # Generate Alerts
                    fund_alerts = generate_smart_alerts(res_row, metrics)
                    for alert in fund_alerts:
                        alert['scan_id'] = scan_id
                        alert['timestamp'] = timestamp
                        alerts_data.append(alert)
                        # Send Notification for Critical
                        if alert['severity'] == 'High':
                            send_telegram_alert(f"{alert['type']}: {alert['symbol']} - {alert['message']}")

                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(candidates)} funds...")

            except Exception as e:
                logger.error(f"Failed to process {c.get('schemeName')}: {e}")
                continue

        # 5. Normalization & Persistence
        if results_data:
            # Create DFs
            res_df = pd.DataFrame(results_data)
            met_df = pd.DataFrame(metrics_data)
            alt_df = pd.DataFrame(alerts_data) if alerts_data else pd.DataFrame()

            # Normalize Scores (0-100) per Universe
            res_df = normalize_batch_scores(res_df)

            # Ensure Schema Alignment for Insert
            # 'Fortress Score' is added by normalize, but 'Score' is updated.
            # We can drop 'Fortress Score' as it's redundant if 'Score' is the final value.
            if 'Fortress Score' in res_df.columns:
                res_df = res_df.drop(columns=['Fortress Score'])

            # Bulk Insert
            logger.info(f"Saving {len(res_df)} results to DB...")
            bulk_insert_results(res_df, met_df, alt_df)

            top10 = res_df.sort_values("Score", ascending=False).head(10).copy()
            top10["timestamp"] = timestamp
            top10.to_csv("mf_top10_consistent.csv", index=False)
            log_audit("MF Top10", "Mutual Funds", f"Top consistent schemes: {', '.join(top10['symbol'].astype(str).tolist())}")

            update_scan_status(scan_id, "Completed")
            log_audit("Scan Completed", "Mutual Funds", f"Scanned {len(res_df)} funds.")
            logger.info("Audit Finished Successfully.")

        else:
            logger.warning("No results found.")
            update_scan_status(scan_id, "Failed")

    except Exception as e:
        logger.critical(f"Audit Crash: {e}")
    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

if __name__ == "__main__":
    limit = None
    if len(sys.argv) > 1:
        try:
            arg = sys.argv[1]
            if arg.startswith("--limit="):
                limit = int(arg.split("=")[1])
        except: pass

    run_audit(limit)
