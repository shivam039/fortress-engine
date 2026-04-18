"""
Mutual Fund Scheme Discovery & Categorization Service
- Fetches ALL 4000+ schemes from mfapi.in
- Categorizes by type (Large Cap, Mid Cap, Small Cap, Debt, Liquid, etc.)
- Caches monthly for performance
- Supports parallel discovery with rate limiting
"""

import json
import logging
import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from mf_lab.services.config import logger, API_TIMEOUT, MAX_RETRIES, RETRY_DELAY


def _recent_cache_predicate(days: int = 30) -> str:
    from utils.db import _can_use_neon

    if _can_use_neon():
        return f"cached_date >= CURRENT_DATE - INTERVAL '{days} days'"
    return f"cached_date >= date('now', '-{days} days')"

# ────────────────────────────────────────────────────────────────────
#  SCHEME CATEGORIZATION ENGINE
# ────────────────────────────────────────────────────────────────────

SCHEME_CATEGORIES = {
    # EQUITY
    "Large Cap": {
        "keywords": ["large cap", "largecap", "large-cap"],
        "type": "Equity",
        "subcategory": "Large Cap",
    },
    "Mid Cap": {
        "keywords": ["mid cap", "midcap", "mid-cap"],
        "type": "Equity",
        "subcategory": "Mid Cap",
    },
    "Small Cap": {
        "keywords": ["small cap", "smallcap", "small-cap"],
        "type": "Equity",
        "subcategory": "Small Cap",
    },
    "Multi Cap": {
        "keywords": ["multi cap", "multicap", "multi-cap", "flexi cap"],
        "type": "Equity",
        "subcategory": "Multi/Flexi Cap",
    },
    "Focused": {
        "keywords": ["focused"],
        "type": "Equity",
        "subcategory": "Focused",
    },
    "Value": {
        "keywords": ["value"],
        "type": "Equity",
        "subcategory": "Value/Contra",
    },
    "Contra": {
        "keywords": ["contra", "contrarian"],
        "type": "Equity",
        "subcategory": "Value/Contra",
    },
    "Dividend": {
        "keywords": ["dividend"],
        "type": "Equity",
        "subcategory": "Dividend",
    },
    "ELSS": {
        "keywords": ["elss", "tax saver"],
        "type": "Equity",
        "subcategory": "ELSS",
    },
    "Thematic": {
        "keywords": ["thematic", "banking", "fintech", "pharma", "psu"],
        "type": "Equity",
        "subcategory": "Thematic/Sector",
    },
    "International": {
        "keywords": ["international", "overseas", "global", "usa", "us dollar", "emerging"],
        "type": "Equity",
        "subcategory": "International/Global",
    },
    # DEBT
    "Liquid": {
        "keywords": ["liquid", "liquid fund"],
        "type": "Debt",
        "subcategory": "Liquid/Overnight",
    },
    "Ultra Short Duration": {
        "keywords": ["ultra short", "low duration", "overnight", "floating rate"],
        "type": "Debt",
        "subcategory": "Ultra Short Duration",
    },
    "Short Duration": {
        "keywords": ["short duration", "short term"],
        "type": "Debt",
        "subcategory": "Short Duration",
    },
    "Corporate Bond": {
        "keywords": ["corporate bond", "credit"],
        "type": "Debt",
        "subcategory": "Corporate Bond",
    },
    "Gilt": {
        "keywords": ["gilt", "government securities", "gsec"],
        "type": "Debt",
        "subcategory": "Gilt/Government",
    },
    "Dynamic Bond": {
        "keywords": ["dynamic", "dynamic bond"],
        "type": "Debt",
        "subcategory": "Dynamic Bond",
    },
    # HYBRID / BALANCED
    "Balanced Advantage": {
        "keywords": ["balanced", "aggressive hybrid", "conservative hybrid", "dynamic asset"],
        "type": "Hybrid",
        "subcategory": "Balanced/Hybrid",
    },
    # FUND OF FUNDS / OTHERS
    "Fund Of Funds": {
        "keywords": ["fund of funds", "fof"],
        "type": "Other",
        "subcategory": "Fund of Funds",
    },
}

def classify_scheme_category(scheme_name: str) -> Dict[str, str]:
    """
    Classify a scheme into category based on name keywords.
    Returns dict with 'category', 'type', 'subcategory'.
    """
    name_lower = scheme_name.lower()

    for cat_key, cat_info in SCHEME_CATEGORIES.items():
        if any(kw in name_lower for kw in cat_info["keywords"]):
            return {
                "category": cat_key,
                "type": cat_info["type"],
                "subcategory": cat_info["subcategory"],
            }

    # Default fallback
    if "equity" in name_lower or "stock" in name_lower:
        return {"category": "Multi Cap", "type": "Equity", "subcategory": "Equity"}
    elif "debt" in name_lower or "bond" in name_lower:
        return {"category": "Corporate Bond", "type": "Debt", "subcategory": "Debt"}
    else:
        return {"category": "Fund Of Funds", "type": "Other", "subcategory": "Other"}


def _parse_scheme_list(raw_schemes: List[Dict], direct_growth_only: bool = True) -> List[Dict[str, str]]:
    """
    Parse raw scheme list from mfapi.in and enrich with categorization.
    Filters for Direct-Growth schemes by default (recommended for performance).
    Returns list of enriched scheme dicts.
    """
    enriched = []

    # Keywords for categorization
    equity_keywords = ["flexi", "multi", "large", "mid", "small", "focused", "value", "contra", "elss"]
    debt_keywords = ["liquid", "gilt", "bond", "duration", "overnight", "corporate"]
    all_keywords = equity_keywords + debt_keywords

    for scheme in raw_schemes:
        scheme_code = str(scheme.get("schemeCode", "")).strip()
        scheme_name = scheme.get("schemeName", "").strip()

        if not scheme_code or not scheme_name:
            continue

        # ✓ FILTER: Only Direct-Growth schemes for performance
        if direct_growth_only:
            name_lower = scheme_name.lower()
            
            # Must have both "direct" and "growth"
            if not ("direct" in name_lower and "growth" in name_lower):
                continue
            
            # Must have at least one category keyword
            if not any(kw in name_lower for kw in all_keywords):
                continue
            
            # Exclude Regular and IDCW variants
            if any(ex in name_lower for ex in ["regular", "idcw"]):
                continue

        # Categorize
        cat_info = classify_scheme_category(scheme_name)

        enriched.append({
            "scheme_code": scheme_code,
            "scheme_name": scheme_name,
            "category": cat_info.get("category", "Other"),
            "type": cat_info.get("type", "Other"),
            "subcategory": cat_info.get("subcategory", "Other"),
            "amc_code": scheme.get("amcCode", ""),
            "amc_name": scheme.get("amcName", ""),
            "isin_div_payout": scheme.get("isinDivPayout", ""),
            "isin_div_reinvest": scheme.get("isinDivReinvest", ""),
            "isin_growth": scheme.get("isinGrowth", ""),
        })

    return enriched


# ────────────────────────────────────────────────────────────────────
#  SCHEME DISCOVERY & CACHING (MONTHLY)
# ────────────────────────────────────────────────────────────────────

def fetch_all_schemes_from_api(max_retries: int = 3) -> List[Dict]:
    """
    Fetch Direct-Growth schemes from mfapi.in (~4000 schemes).
    Filters automatically for Direct + Growth variants only.
    Includes rate-limiting and retry logic.
    """
    url = "https://api.mfapi.in/mf"

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching scheme catalog from mfapi.in (attempt {attempt + 1}/{max_retries})...")
            resp = requests.get(url, timeout=API_TIMEOUT)
            resp.raise_for_status()

            raw_schemes = resp.json()
            logger.info(f"✓ Downloaded {len(raw_schemes)} total schemes from API")

            # Parse and enrich (with Direct-Growth filtering)
            enriched = _parse_scheme_list(raw_schemes, direct_growth_only=True)
            logger.info(f"✓ Filtered to {len(enriched)} Direct-Growth schemes and categorized")

            return enriched

        except requests.RequestException as e:
            logger.warning(f"API fetch failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.info(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch schemes after {max_retries} attempts")
                return []

    return []


def get_all_schemes_cached(force_refresh: bool = False) -> pd.DataFrame:
    """
    Get all schemes with smart caching:
    1. Check if cache exists (database)
    2. If cache is <30 days old, return cached
    3. If older or missing, fetch from API and cache

    Returns DataFrame with columns:
    - scheme_code, scheme_name, category, type, subcategory, amc_code, amc_name
    """
    try:
        from utils.db import _read_df, _exec
        from datetime import datetime, timedelta

        # 1. Check for existing cache in database
        today = datetime.now().date()
        cache_cutoff = today - timedelta(days=30)

        if not force_refresh:
            try:
                # Try to fetch from cache table
                query = """
                SELECT scheme_code, scheme_name, category, type, subcategory, 
                       amc_code, amc_name, cached_date
                FROM mf_scheme_catalog
                WHERE cached_date >= :cutoff_date
                ORDER BY category, scheme_name
                """
                df = _read_df(query, {"cutoff_date": cache_cutoff}, ttl="30d")

                if not df.empty:
                    logger.info(f"✓ Loaded {len(df)} schemes from monthly cache (cached {df['cached_date'].iloc[0]})")
                    return df[
                        ["scheme_code", "scheme_name", "category", "type", "subcategory", "amc_code", "amc_name"]
                    ]
            except Exception as cache_err:
                logger.warning(f"Cache fetch failed, will refetch: {cache_err}")

        # 2. Fetch fresh from API
        logger.info("Fetching fresh scheme list from API (cache expired or forced refresh)...")
        schemes = fetch_all_schemes_from_api()

        if not schemes:
            logger.error("Failed to fetch schemes from API and cache is empty")
            return pd.DataFrame()

        df = pd.DataFrame(schemes)

        # 3. Store in database cache
        try:
            logger.info(f"Caching {len(df)} schemes to database...")

            df["cached_date"] = today
            cols = [
                "scheme_code", "scheme_name", "category", "type", "subcategory",
                "amc_code", "amc_name", "isin_div_payout", "isin_div_reinvest", "isin_growth", "cached_date"
            ]

            # Ensure table exists
            _exec("""
                CREATE TABLE IF NOT EXISTS mf_scheme_catalog (
                    scheme_code TEXT PRIMARY KEY,
                    scheme_name TEXT NOT NULL,
                    category TEXT,
                    type TEXT,
                    subcategory TEXT,
                    amc_code TEXT,
                    amc_name TEXT,
                    isin_div_payout TEXT,
                    isin_div_reinvest TEXT,
                    isin_growth TEXT,
                    cached_date DATE DEFAULT CURRENT_DATE
                )
            """)

            # Insert with ON CONFLICT (upsert)
            for _, row in df.iterrows():
                try:
                    _exec("""
                        INSERT INTO mf_scheme_catalog 
                        (scheme_code, scheme_name, category, type, subcategory, amc_code, amc_name, 
                         isin_div_payout, isin_div_reinvest, isin_growth, cached_date)
                        VALUES (:sc, :sn, :cat, :typ, :subcat, :ac, :an, :idp, :idir, :ig, :cd)
                        ON CONFLICT(scheme_code) DO UPDATE SET
                            scheme_name = EXCLUDED.scheme_name,
                            category = EXCLUDED.category,
                            type = EXCLUDED.type,
                            subcategory = EXCLUDED.subcategory,
                            amc_name = EXCLUDED.amc_name,
                            cached_date = EXCLUDED.cached_date
                    """, {
                        "sc": row["scheme_code"],
                        "sn": row["scheme_name"],
                        "cat": row["category"],
                        "typ": row["type"],
                        "subcat": row["subcategory"],
                        "ac": row["amc_code"],
                        "an": row["amc_name"],
                        "idp": row["isin_div_payout"],
                        "idir": row["isin_div_reinvest"],
                        "ig": row["isin_growth"],
                        "cd": today
                    })
                except Exception as insert_err:
                    logger.debug(f"Insert error for {row['scheme_code']}: {insert_err}")

            logger.info(f"✓ Cached {len(df)} schemes to database")

        except Exception as db_err:
            logger.warning(f"Could not cache schemes to database: {db_err}")

        # ✓ OPTIMIZE: Pre-compute category/type batches for fast filtering
        logger.info("Pre-computing category/type batches...")
        _compute_category_batches()

        return df[
            ["scheme_code", "scheme_name", "category", "type", "subcategory", "amc_code", "amc_name"]
        ]

    except Exception as e:
        logger.error(f"Error in get_all_schemes_cached: {e}")
        return pd.DataFrame()


def _compute_category_batches():
    """
    Pre-compute batch statistics by (type, category) for instant filtering.
    Stores counts and metadata so UI doesn't need to filter all schemes at runtime.
    """
    try:
        from utils.db import _read_df, _exec

        # Create batch table if not exists
        _exec("""
            CREATE TABLE IF NOT EXISTS mf_scheme_batches (
                type TEXT NOT NULL,
                category TEXT NOT NULL,
                scheme_count INT DEFAULT 0,
                amc_count INT DEFAULT 0,
                cached_date DATE DEFAULT CURRENT_DATE,
                PRIMARY KEY (type, category, cached_date)
            )
        """)

        # Query all schemes and compute batches
        cache_filter = _recent_cache_predicate(30)
        df = _read_df(f"""
            SELECT type, category, COUNT(*) as scheme_count, COUNT(DISTINCT amc_name) as amc_count
            FROM mf_scheme_catalog
            WHERE {cache_filter}
            GROUP BY type, category
            ORDER BY type, category
        """, ttl="1m")

        if df.empty:
            logger.warning("No schemes to batch - skipping batch computation")
            return

        # Upsert batch records
        for _, row in df.iterrows():
            try:
                _exec("""
                    INSERT INTO mf_scheme_batches (type, category, scheme_count, amc_count, cached_date)
                    VALUES (:typ, :cat, :sc, :ac, CURRENT_DATE)
                    ON CONFLICT(type, category, cached_date) DO UPDATE SET
                        scheme_count = EXCLUDED.scheme_count,
                        amc_count = EXCLUDED.amc_count
                """, {
                    "typ": row["type"],
                    "cat": row["category"],
                    "sc": int(row["scheme_count"]),
                    "ac": int(row["amc_count"])
                })
            except Exception as batch_err:
                logger.debug(f"Batch compute error for {row['type']} / {row['category']}: {batch_err}")

        logger.info(f"✓ Pre-computed {len(df)} category/type batches for instant filtering")

    except Exception as e:
        logger.error(f"Error computing category batches: {e}")


def get_batch_stats() -> Dict[str, any]:
    """
    Get pre-computed batch statistics for UI display.
    **O(1)** - pulls from mf_scheme_batches table (no filtering needed).
    """
    try:
        from utils.db import _read_df
        cache_filter = _recent_cache_predicate(30)

        df = _read_df(f"""
            SELECT type, category, scheme_count, amc_count
            FROM mf_scheme_batches
            WHERE {cache_filter}
            ORDER BY type, category
        """, ttl="1h")

        if df.empty:
            return {}

        result = {}
        total_schemes = 0
        total_amcs = 0

        for _, row in df.iterrows():
            key = f"{row['type']} - {row['category']}"
            result[key] = {
                "scheme_count": int(row["scheme_count"]),
                "amc_count": int(row["amc_count"])
            }
            total_schemes += int(row["scheme_count"])
            total_amcs += int(row["amc_count"])

        result["_metadata"] = {
            "total_schemes": total_schemes,
            "total_batches": len(df)
        }

        return result

    except Exception as e:
        logger.error(f"Error fetching batch stats: {e}")
        return {}


def get_batch_filtered_schemes(scheme_type: str = None, category: str = None) -> pd.DataFrame:
    """
    Get schemes filtered by type and/or category using indexed batch queries.
    **O(index lookup + result fetch)** - much faster than in-memory filtering.
    
    Parameters:
    - scheme_type: e.g., "Equity", "Debt", "Hybrid"
    - category: e.g., "Large Cap", "Liquid", "Gilt"
    
    Returns filtered DataFrame.
    """
    try:
        from utils.db import _read_df

        where_clauses = [_recent_cache_predicate(30)]
        params = {}

        if scheme_type:
            where_clauses.append("type = :typ")
            params["typ"] = scheme_type

        if category:
            where_clauses.append("category = :cat")
            params["cat"] = category

        where_str = " AND ".join(where_clauses)

        query = f"""
            SELECT scheme_code, scheme_name, category, type, subcategory, amc_code, amc_name
            FROM mf_scheme_catalog
            WHERE {where_str}
            ORDER BY scheme_name
        """

        df = _read_df(query, params, ttl="5m")
        return df if not df.empty else pd.DataFrame()

    except Exception as e:
        logger.error(f"Error fetching batch-filtered schemes: {e}")
        return pd.DataFrame()


def get_schemes_by_category(category: str = "Large Cap") -> pd.DataFrame:
    """
    Get all schemes for a specific category.
    Pulls from monthly cache.
    """
    all_schemes = get_all_schemes_cached()

    if all_schemes.empty:
        return pd.DataFrame()

    df = all_schemes[all_schemes["category"].str.lower() == category.lower()]
    return df.sort_values("scheme_name").reset_index(drop=True)


def get_schemes_summary() -> Dict[str, int]:
    """
    Return a summary of scheme counts by category.
    """
    all_schemes = get_all_schemes_cached()

    if all_schemes.empty:
        return {}

    summary = (
        all_schemes.groupby(["type", "category"])
        .size()
        .reset_index(name="count")
        .sort_values(["type", "count"], ascending=[True, False])
    )

    result = {}
    for _, row in summary.iterrows():
        key = f"{row['type']} - {row['category']}"
        result[key] = int(row["count"])

    return result


def parallel_fetch_scheme_navs(
    scheme_codes: List[str],
    max_workers: int = 10,
    progress_callback=None,
) -> Dict[str, pd.DataFrame]:
    """
    Parallel fetching of NAV data for multiple schemes.
    Respects rate limiting and reports progress.
    """
    from mf_lab.services.data import fetch_fund_nav

    nav_data = {}
    total = len(scheme_codes)
    completed = 0

    # Use ThreadPoolExecutor with rate limiting
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_fund_nav, code): code
            for code in scheme_codes
        }

        for future in as_completed(futures):
            completed += 1
            code = futures[future]

            try:
                df = future.result()
                if df is not None and not df.empty:
                    nav_data[code] = df
            except Exception as e:
                logger.debug(f"Error fetching NAV for {code}: {e}")

            if progress_callback:
                progress_callback(completed, total)

    return nav_data


# ────────────────────────────────────────────────────────────────────
#  CONVENIENCE FUNCTIONS FOR UI
# ────────────────────────────────────────────────────────────────────

def get_category_stats() -> Dict:
    """Return detailed statistics for all scheme categories."""
    all_schemes = get_all_schemes_cached()

    if all_schemes.empty:
        return {}

    stats = {}

    # Overall stats
    stats["total_schemes"] = len(all_schemes)
    stats["total_amcs"] = all_schemes["amc_name"].nunique()

    # By type
    type_stats = all_schemes.groupby("type").size().to_dict()
    stats["by_type"] = type_stats

    # By category (type + category)
    cat_stats = (
        all_schemes.groupby(["type", "category"])
        .agg({"scheme_code": "count", "amc_name": "nunique"})
        .rename(columns={"scheme_code": "scheme_count", "amc_name": "amc_count"})
        .reset_index()
        .to_dict("records")
    )
    stats["by_category"] = cat_stats

    return stats


# ────────────────────────────────────────────────────────────────────
#  DROPDOWN OPTION PRE-COMPUTATION (INSTANT UI SWITCHING)
# ────────────────────────────────────────────────────────────────────

def get_distinct_fund_types() -> List[str]:
    """
    Get all distinct fund types from the pre-computed batch table.
    Falls back to loading from cached schemes if batch table unavailable.
    
    Returns: Alphabetically sorted list of types like ["Debt", "Equity", "Hybrid", "Other"]
    """
    try:
        from utils.db import _read_df
        cache_filter = _recent_cache_predicate(30)

        # Query the pre-computed batch table to get distinct types
        query = f"""
            SELECT DISTINCT type
            FROM mf_scheme_batches
            WHERE {cache_filter}
            ORDER BY type
        """

        df = _read_df(query, {}, ttl="1h")
        
        if not df.empty:
            types = sorted(df["type"].unique().tolist())
            return types

    except Exception as e:
        logger.debug(f"Batch table unavailable, falling back to cached schemes: {e}")

    # Fallback: Load from monthly cached schemes
    try:
        all_schemes = get_all_schemes_cached()
        if not all_schemes.empty:
            types = sorted(all_schemes["type"].unique().tolist())
            return types
    except Exception as e:
        logger.error(f"Error fetching scheme types from cache: {e}")
    
    return []


def get_distinct_categories_for_type(scheme_type: str) -> List[str]:
    """
    Get all distinct categories for a given fund type.
    Falls back to loading from cached schemes if batch table unavailable.
    
    Parameters:
    - scheme_type: e.g., "Equity", "Debt", "Hybrid"
    
    Returns: Alphabetically sorted list of categories for that type
    """
    try:
        from utils.db import _read_df
        cache_filter = _recent_cache_predicate(30)

        # Query the pre-computed batch table
        query = f"""
            SELECT DISTINCT category
            FROM mf_scheme_batches
            WHERE type = :typ
            AND {cache_filter}
            ORDER BY category
        """

        df = _read_df(query, {"typ": scheme_type}, ttl="1h")
        
        if not df.empty:
            categories = sorted(df["category"].unique().tolist())
            return categories

    except Exception as e:
        logger.debug(f"Batch table unavailable for type '{scheme_type}', falling back to cached schemes: {e}")

    # Fallback: Load from monthly cached schemes
    try:
        all_schemes = get_all_schemes_cached()
        if not all_schemes.empty:
            filtered = all_schemes[all_schemes["type"] == scheme_type]
            if not filtered.empty:
                categories = sorted(filtered["category"].unique().tolist())
                return categories
    except Exception as e:
        logger.error(f"Error fetching categories from cache: {e}")
        return []
