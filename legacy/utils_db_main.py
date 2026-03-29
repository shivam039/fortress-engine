import sqlite3
import pandas as pd
import streamlit as st
import json
from datetime import datetime
import logging

# Configure basic logging for DB operations (will be overridden by main app config)
logger = logging.getLogger(__name__)

DB_NAME = 'fortress_history.db'

# ---------------- HELPER FUNCTIONS ----------------

def get_connection():
    return sqlite3.connect(DB_NAME)

def get_table_name_from_universe(u):
    # Legacy support
    if "Mutual Funds" == u: return "scan_mf"
    if "Commodities" == u: return "scan_commodities"
    return "scan_entries"

# ---------------- DB INITIALIZATION ----------------
def init_db():
    """Initializes the database with the new Enterprise Schema."""
    try:
        with get_connection() as conn:
            c = conn.cursor()

            # 1. Scans Metadata Table
            # Added scan_type column for unified tracking
            c.execute('''CREATE TABLE IF NOT EXISTS scans (
                            scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            timestamp TEXT NOT NULL,
                            universe TEXT,
                            scan_type TEXT,
                            status TEXT
                        )''')

            # Ensure scan_type column exists (Schema Evolution for existing DBs)
            try:
                c.execute("SELECT scan_type FROM scans LIMIT 1")
            except sqlite3.OperationalError:
                c.execute("ALTER TABLE scans ADD COLUMN scan_type TEXT")

            # 2. Scan Results (Fact Table) - RENAMED TO scan_entries TO AVOID COLLISION
            # scan_id links to scans.id
            c.execute('''CREATE TABLE IF NOT EXISTS scan_entries (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            scan_id INTEGER,
                            symbol TEXT,
                            scheme_code TEXT,
                            category TEXT,
                            score REAL,
                            price REAL,
                            integrity_label TEXT,
                            drift_status TEXT,
                            drift_message TEXT,
                            FOREIGN KEY(scan_id) REFERENCES scans(scan_id)
                        )''')

            # 3. Fund Metrics (Details)
            c.execute('''CREATE TABLE IF NOT EXISTS fund_metrics (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            scan_id INTEGER,
                            symbol TEXT,
                            alpha REAL,
                            beta REAL,
                            te REAL,
                            sortino REAL,
                            max_dd REAL,
                            win_rate REAL,
                            upside REAL,
                            downside REAL,
                            cagr REAL,
                            FOREIGN KEY(scan_id) REFERENCES scans(scan_id)
                        )''')

            # 4. Alerts
            c.execute('''CREATE TABLE IF NOT EXISTS alerts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            scan_id INTEGER,
                            symbol TEXT,
                            alert_type TEXT,
                            severity TEXT,
                            message TEXT,
                            timestamp TEXT,
                            FOREIGN KEY(scan_id) REFERENCES scans(scan_id)
                        )''')

            # 5. Benchmark History (Caching)
            # Composite PK: ticker + date
            c.execute('''CREATE TABLE IF NOT EXISTS benchmark_history (
                            ticker TEXT,
                            date TEXT,
                            close REAL,
                            ret REAL,
                            PRIMARY KEY (ticker, date)
                        )''')

            # 6. Commodity Scans (Auto-created if missing)
            c.execute('''CREATE TABLE IF NOT EXISTS scan_commodities (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            scan_id INTEGER,
                            symbol TEXT,
                            global_price REAL,
                            local_price REAL,
                            usd_inr REAL,
                            parity_price REAL,
                            spread REAL,
                            arb_yield REAL,
                            action_label TEXT,
                            FOREIGN KEY(scan_id) REFERENCES scans(scan_id)
                        )''')

            # 7. Algo Trade Log (Auto-created if missing)
            c.execute('''CREATE TABLE IF NOT EXISTS algo_trade_log (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            timestamp TEXT NOT NULL,
                            strategy_name TEXT,
                            symbol TEXT,
                            action TEXT,
                            details TEXT,
                            status TEXT
                        )''')

            # 8. Audit Logs
            c.execute('''CREATE TABLE IF NOT EXISTS audit_logs (
                            timestamp TEXT,
                            action TEXT,
                            universe TEXT,
                            details TEXT
                        )''')

            # 9. Unified Scan History Details
            # Stores heterogenous results (Stocks, Options, Commodities) using automated schema evolution
            c.execute('''CREATE TABLE IF NOT EXISTS scan_history_details (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            scan_id INTEGER,
                            symbol TEXT,
                            FOREIGN KEY(scan_id) REFERENCES scans(scan_id)
                        )''')

            # Legacy Tables Support (Optional: keep them if needed or let them be)
            # c.execute('''CREATE TABLE IF NOT EXISTS scan_results ...''') # Old flat table

            conn.commit()

            # Create Indexes for Performance
            c.execute("CREATE INDEX IF NOT EXISTS idx_scans_ts ON scans(timestamp)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_scan_history_scan_id ON scan_history_details(scan_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_entries_scan_sym ON scan_entries(scan_id, symbol)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_metrics_scan_sym ON fund_metrics(scan_id, symbol)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(timestamp)")

            # Commit handled by context manager on exit (if no error)? No, sqlite3 context manager commits on success.
            # But explicit commit is safer if doing DDL sometimes.
            # The pattern `with conn:` automatically commits.

    except Exception as e:
        print(f"Database initialization error: {e}")

def _infer_sql_type(series):
    dtype = series.dtype
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    if str(series.name).lower() == "sub_scores":
        return "JSONB"
    return "TEXT"


def log_scan_results(df, table_name="scan_results"):
    """
    Logs scan results with automated schema evolution.
    Uses bulk schema inspection + ALTER before appending rows.
    """
    if df.empty:
        return

    df = df.copy()
    # Persist detailed score components as JSON payload for richer audit/backtest analysis.
    if "sub_scores" in df.columns:
        df["sub_scores"] = df["sub_scores"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else x
        )

    try:
        with get_connection() as conn:
            with conn:
                c = conn.cursor()
                is_sqlite = isinstance(conn, sqlite3.Connection)

                if is_sqlite:
                    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    table_exists = c.fetchone()
                    if not table_exists:
                        df.to_sql(table_name, conn, if_exists="replace", index=False)
                        return

                    c.execute(f"PRAGMA table_info({table_name})")
                    existing_cols = {row[1] for row in c.fetchall()}
                else:
                    c.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s
                        """,
                        (table_name,),
                    )
                    existing_cols = {row[0] for row in c.fetchall()}

                missing_cols = [col for col in df.columns if col not in existing_cols]

                if missing_cols:
                    add_clauses = [
                        f'ADD COLUMN "{col}" {_infer_sql_type(df[col])}'
                        for col in missing_cols
                    ]
                    alter_sql = f'ALTER TABLE "{table_name}" ' + ", ".join(add_clauses)
                    c.execute(alter_sql)

                df.to_sql(table_name, conn, if_exists="append", index=False)
    except Exception as e:
        print(f"Error logging scan results: {e}")


# --- NEW INSERTION LOGIC ---

def register_scan(timestamp, universe="Mutual Funds", scan_type="MF", status="In Progress"):
    """Creates a new scan record and returns the scan_id."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO scans (timestamp, universe, scan_type, status) VALUES (?, ?, ?, ?)",
                  (timestamp, universe, scan_type, status))
        scan_id = c.lastrowid
        # Transaction committed on exit
    return scan_id

def save_scan_results(scan_id, df):
    """
    Unified function to save any scan result dataframe to scan_history_details.
    Adds scan_id to the dataframe and uses log_scan_results for schema evolution.
    """
    if df.empty: return

    # Ensure scan_id is in the dataframe
    # Make a copy to avoid modifying original DF reference if used elsewhere
    df_to_save = df.copy()
    df_to_save['scan_id'] = scan_id

    # Use existing schema evolution logic
    log_scan_results(df_to_save, table_name="scan_history_details")

def update_scan_status(scan_id, status):
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE scans SET status = ? WHERE scan_id = ?", (status, scan_id))

def bulk_insert_results(results_df, metrics_df, alerts_df=None):
    """
    Inserts data into scan_entries, fund_metrics, and alerts tables.
    Expects DFs to have 'scan_id' column.
    """
    try:
        with get_connection() as conn:
            if not results_df.empty:
                results_df.to_sql('scan_entries', conn, if_exists='append', index=False)

            if not metrics_df.empty:
                metrics_df.to_sql('fund_metrics', conn, if_exists='append', index=False)

            if alerts_df is not None and not alerts_df.empty:
                alerts_df.to_sql('alerts', conn, if_exists='append', index=False)
            # Commit on exit
    except Exception as e:
        print(f"Bulk insert error: {e}")

# --- BENCHMARK CACHING ---

def get_cached_benchmark(ticker, start_date=None):
    """Retrieves benchmark data from SQLite."""
    with get_connection() as conn:
        query = "SELECT date, close, ret FROM benchmark_history WHERE ticker = ?"
        params = [ticker]

        if start_date:
            query += " AND date >= ?"
            params.append(start_date)

        query += " ORDER BY date"

        try:
            df = pd.read_sql(query, conn, params=params)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
            return df
        except Exception:
            return pd.DataFrame()

def save_benchmark_data(ticker, df):
    """Saves benchmark data to SQLite (Upsert)."""
    if df.empty: return

    # Prepare data
    data_to_insert = []
    for date, row in df.iterrows():
        # Handle cases where ret might be NaN (start of series)
        ret = row['ret'] if pd.notna(row['ret']) else 0.0
        # If 'Close' is missing, skip or use 0?
        close = row['Close'] if 'Close' in row else 0.0

        date_str = date.strftime('%Y-%m-%d')
        data_to_insert.append((ticker, date_str, close, ret))

    try:
        with get_connection() as conn:
            c = conn.cursor()
            c.executemany("INSERT OR REPLACE INTO benchmark_history (ticker, date, close, ret) VALUES (?, ?, ?, ?)", data_to_insert)
    except Exception as e:
        print(f"Error saving benchmark {ticker}: {e}")

# --- DATA FETCHING (UI Support) ---

# Legacy Wrapper for UI compatibility with new Schema
@st.cache_data(ttl=60)
def fetch_timestamps(table_name="scan_mf", scan_type=None):
    """
    Fetches available scan timestamps.
    Now reads from 'scans' table (filtered by scan_type if provided) but falls back to 'scan_mf' for legacy.
    """
    timestamps = []

    # 1. Try New Schema
    try:
        with get_connection() as conn:
            query = "SELECT timestamp FROM scans WHERE status='Completed'"
            params = []
            if scan_type:
                query += " AND scan_type = ?"
                params.append(scan_type)

            query += " ORDER BY timestamp DESC"

            new_scans = pd.read_sql(query, conn, params=params)
            if not new_scans.empty:
                timestamps.extend(new_scans['timestamp'].tolist())
    except Exception as e:
        pass

    # 2. Try Old Schema (Legacy) - Only if scan_type matches legacy types or is None
    try:
        with get_connection() as conn:
            old_scans = pd.read_sql("SELECT DISTINCT timestamp FROM scan_mf ORDER BY timestamp DESC", conn)
            if not old_scans.empty:
                # Avoid duplicates
                existing = set(timestamps)
                legacy = [t for t in old_scans['timestamp'].tolist() if t not in existing]
                timestamps.extend(legacy)
    except Exception as e:
        logger.error(f"db error: {e}")

    # Sort Descending
    timestamps.sort(reverse=True)
    return timestamps

@st.cache_data(ttl=60)
def fetch_history_data(table_name, timestamp, scan_type=None):
    """
    Fetches scan data for a specific timestamp.
    Performs a JOIN between scan_entries and fund_metrics if data is in new schema.
    Falls back to legacy table if not found in new schema.
    """
    with get_connection() as conn:
        # 1. Check if this timestamp exists in 'scans' table
        scan_info = pd.read_sql("SELECT scan_id, scan_type FROM scans WHERE timestamp = ?", conn, params=(timestamp,))

        if not scan_info.empty:
            scan_id = scan_info.iloc[0]['scan_id']
            # If scan_type was passed, ensure it matches? Or just use what DB says.
            db_scan_type = scan_info.iloc[0].get('scan_type')

            # Logic for unified history
            if db_scan_type in ['STOCK', 'OPTIONS', 'COMMODITY']:
                 try:
                     # Fetch from unified scan_history_details
                     df = pd.read_sql("SELECT * FROM scan_history_details WHERE scan_id = ?", conn, params=(scan_id,))
                     return df
                 except Exception as e:
                     print(f"Error fetching unified history data: {e}")

            if table_name == "scan_commodities" and (db_scan_type is None or db_scan_type == 'COMMODITY'):
                # Backward compatibility or specific table usage
                try:
                    df = pd.read_sql("SELECT * FROM scan_commodities WHERE scan_id = ?", conn, params=(scan_id,))
                    return df
                except Exception as e:
                    print(f"Error fetching commodity data: {e}")

            # Default MF Logic (JOIN Query)
            query = """
            SELECT
                r.symbol as Symbol,
                r.scheme_code as 'Scheme Code',
                r.category as Category,
                r.score as Score,
                r.price as Price,
                r.integrity_label as Integrity,
                r.drift_status as 'Drift Status',
                r.drift_message as 'Drift Message',
                m.alpha as 'Alpha (True)',
                m.beta as Beta,
                m.te as 'Tracking Error',
                m.sortino as Sortino,
                m.max_dd as 'Max Drawdown',
                m.win_rate as 'Win Rate',
                m.upside as 'Upside Cap',
                m.downside as 'Downside Cap',
                m.cagr as cagr
            FROM scan_entries r
            LEFT JOIN fund_metrics m ON r.scan_id = m.scan_id AND r.symbol = m.symbol
            WHERE r.scan_id = ?
            """
            try:
                df = pd.read_sql(query, conn, params=(scan_id,))

                # Post-processing to match expected UI columns
                # The new 'Score' is the 'Fortress Score' (already normalized in engine)
                if not df.empty and 'Score' in df.columns:
                    df['Fortress Score'] = df['Score']

                return df
            except Exception as e:
                # Fallback to scan_history_details if MF logic fails (maybe it was stored there?)
                 try:
                     df = pd.read_sql("SELECT * FROM scan_history_details WHERE scan_id = ?", conn, params=(scan_id,))
                     return df
                 except:
                     print(f"Error fetching joined data: {e}")

        # 2. Fallback to Legacy 'scan_mf'
        try:
            df = pd.read_sql(f"SELECT * FROM scan_mf WHERE timestamp=?", conn, params=(timestamp,))
            return df
        except:
            return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_symbol_history(table_name, symbol):
    """
    Fetches history for a symbol across all scans.
    Unifies data from new schema and old schema.
    """
    with get_connection() as conn:
        # New Schema History
        query_new = """
        SELECT
            s.timestamp,
            r.score as Score,
            r.price as Price,
            m.alpha as 'Alpha (True)',
            m.beta as Beta,
            m.te as 'Tracking Error'
        FROM scan_entries r
        JOIN scans s ON r.scan_id = s.scan_id
        LEFT JOIN fund_metrics m ON r.scan_id = m.scan_id AND r.symbol = m.symbol
        WHERE r.symbol = ?
        ORDER BY s.timestamp
        """

        df_new = pd.DataFrame()
        try:
            df_new = pd.read_sql(query_new, conn, params=(symbol,))
        except Exception as e:
            logger.error(f"db error: {e}")

        # Old Schema History
        df_old = pd.DataFrame()
        try:
            # Check columns of scan_mf first? Assumes standard
            # We need to map old columns to new names if they differ
            # Old: Score, Price, Alpha (True) [if saved? logic saves 'Alpha (True)' key in json but maybe column name?]
            # logic.py saves: "Alpha (True)": metrics['alpha']
            df_old = pd.read_sql("SELECT timestamp, Score, Price, `Alpha (True)`, Beta, `Tracking Error` FROM scan_mf WHERE Symbol = ?", conn, params=(symbol,))
        except Exception as e:
            logger.error(f"db error: {e}")

    # Combine
    if not df_new.empty and not df_old.empty:
        # Filter old to remove duplicates (timestamps present in new)
        existing_ts = set(df_new['timestamp'])
        df_old = df_old[~df_old['timestamp'].isin(existing_ts)]
        return pd.concat([df_old, df_new]).sort_values('timestamp')
    elif not df_new.empty:
        return df_new
    elif not df_old.empty:
        return df_old

    return pd.DataFrame()

# ---------------- LOGGING ----------------
def log_audit(action, universe="Global", details=""):
    try:
        with get_connection() as conn:
            c = conn.cursor()
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("INSERT INTO audit_logs VALUES (?,?,?,?)", (ts, action, universe, details))
    except Exception as e:
        logger.error(f"db error: {e}")

def log_algo_trade(strategy, symbol, action, details, status="Active"):
    try:
        with get_connection() as conn:
            c = conn.cursor()
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("INSERT INTO algo_trade_log (timestamp, strategy_name, symbol, action, details, status) VALUES (?, ?, ?, ?, ?, ?)",
                      (ts, strategy, symbol, action, details, status))
    except Exception as e:
        print(f"Error logging trade: {e}")

def fetch_active_trades():
    try:
        with get_connection() as conn:
            return pd.read_sql("SELECT * FROM algo_trade_log WHERE status='Active'", conn)
    except:
        return pd.DataFrame()

def close_all_trades():
    """Marks all active trades as Closed."""
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("UPDATE algo_trade_log SET status='Closed' WHERE status='Active'")
