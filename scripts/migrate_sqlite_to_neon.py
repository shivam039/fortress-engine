"""One-time migration utility: copy SQLite tables into Neon Postgres."""

import sqlite3

import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

DB_NAME = "fortress_history.db"
TABLES = [
    "scans",
    "scan_entries",
    "fund_metrics",
    "alerts",
    "audit_logs",
    "scan_history_details",
    "scan_history",
]


def migrate():
    neon_url = st.secrets["connections"]["neon"]["url"]
    engine = create_engine(neon_url)
    with sqlite3.connect(DB_NAME) as sqlite_conn:
        for table in TABLES:
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)
            except Exception:
                continue
            if df.empty:
                continue
            df.to_sql(table, engine, if_exists="append", index=False)
            print(f"Migrated {len(df)} rows -> {table}")


if __name__ == "__main__":
    migrate()
