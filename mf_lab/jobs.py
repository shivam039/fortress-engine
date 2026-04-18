import asyncio
import logging
from typing import Dict, List, Optional

import pandas as pd

from mf_lab.logic import (
    discover_all_funds,
    fetch_mf_snapshot,
    fetch_nav_history,
    run_full_mf_scan,
)
from utils.db import fetch_mf_cached_results, log_audit, upsert_mf_scan_results

logger = logging.getLogger(__name__)

DEFAULT_NAV_MAX_AGE_HOURS = 20


def _normalize_scheme_codes(scheme_codes: Optional[List[str]]) -> List[str]:
    if not scheme_codes:
        return []
    return [str(code).strip() for code in scheme_codes if str(code).strip()]


def _resolve_scheme_codes(scheme_codes: Optional[List[str]]) -> List[str]:
    normalized = _normalize_scheme_codes(scheme_codes)
    if normalized:
        return normalized
    return [str(code) for code in discover_all_funds()]


def _refresh_nav_cache(scheme_codes: Optional[List[str]], force_refresh: bool) -> Dict[str, int]:
    codes = _resolve_scheme_codes(scheme_codes)
    max_age_hours = 0 if force_refresh else DEFAULT_NAV_MAX_AGE_HOURS
    refreshed = 0

    for code in codes:
        history = fetch_nav_history(code, max_age_hours=max_age_hours)
        if history is not None and not history.empty:
            refreshed += 1

    return {"requested": len(codes), "refreshed": refreshed}


def _persist_snapshot(scheme_codes: List[str]) -> pd.DataFrame:
    snapshot = fetch_mf_snapshot(scheme_codes)
    if not snapshot.empty:
        upsert_mf_scan_results(snapshot)
    return snapshot


def _run_job_sync(job_type: str, force_refresh: bool = False, scheme_codes: Optional[List[str]] = None) -> Dict[str, int]:
    normalized_codes = _normalize_scheme_codes(scheme_codes)
    logger.info(
        "Starting MF background job [%s] (force_refresh=%s, targeted_schemes=%d)",
        job_type,
        force_refresh,
        len(normalized_codes),
    )

    if job_type == "refresh_nav":
        result = _refresh_nav_cache(normalized_codes, force_refresh)
        log_audit(
            "MF Backend Job",
            "Mutual Funds",
            f"refresh_nav completed. Refreshed {result['refreshed']} of {result['requested']} schemes.",
        )
        return result

    if job_type == "full_refresh":
        if force_refresh:
            _refresh_nav_cache(normalized_codes, force_refresh=True)

        if normalized_codes:
            snapshot = _persist_snapshot(normalized_codes)
            result = {"requested": len(normalized_codes), "processed": len(snapshot)}
        else:
            df = run_full_mf_scan(max_workers=20)
            result = {"requested": 0, "processed": len(df)}

        log_audit(
            "MF Backend Job",
            "Mutual Funds",
            f"full_refresh completed. Processed {result['processed']} schemes.",
        )
        return result

    if job_type == "update_metrics":
        if force_refresh:
            _refresh_nav_cache(normalized_codes, force_refresh=True)

        if normalized_codes:
            snapshot = _persist_snapshot(normalized_codes)
            result = {"requested": len(normalized_codes), "processed": len(snapshot)}
        else:
            df = run_full_mf_scan(max_workers=20)
            result = {"requested": 0, "processed": len(df)}

        log_audit(
            "MF Backend Job",
            "Mutual Funds",
            f"update_metrics completed. Processed {result['processed']} schemes.",
        )
        return result

    if job_type == "recalculate_rankings":
        cached_df = fetch_mf_cached_results(max_age_days=365)
        if cached_df.empty and normalized_codes:
            cached_df = _persist_snapshot(normalized_codes)
        elif not cached_df.empty:
            upsert_mf_scan_results(cached_df)

        result = {"requested": len(normalized_codes), "processed": len(cached_df)}
        log_audit(
            "MF Backend Job",
            "Mutual Funds",
            f"recalculate_rankings completed. Re-saved {result['processed']} cached rows.",
        )
        return result

    raise ValueError(f"Unknown MF background job type: {job_type}")


async def run_mf_background_job(
    job_type: str,
    force_refresh: bool = False,
    scheme_codes: Optional[List[str]] = None,
) -> None:
    """Execute a Mutual Fund processing job off the request path."""
    try:
        result = await asyncio.to_thread(
            _run_job_sync,
            job_type,
            force_refresh,
            scheme_codes,
        )
        logger.info("MF background job [%s] finished: %s", job_type, result)
    except Exception as exc:
        logger.error("MF background job [%s] crashed: %s", job_type, exc, exc_info=True)
        log_audit("MF Backend Job Failed", "Mutual Funds", f"{job_type} failed: {exc}")
