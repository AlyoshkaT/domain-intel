"""
Batch processor - runs job in background with concurrency control
"""
import asyncio
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

from config.settings import BATCH_CONCURRENCY, DELAY_BETWEEN_DOMAINS
from processing.pipeline import process_domain
from core.bigquery import (
    create_job, update_job, get_job, save_result,
    prefetch_corp_cache, clear_prefetch_cache,
)

logger = logging.getLogger(__name__)

# Track whether a profiles sync is already running (avoid pile-up)
_sync_lock = threading.Lock()
_sync_running = False


def _trigger_profiles_sync(job_id: str):
    """Start domain_profiles sync in a background thread after job completes."""
    global _sync_running
    with _sync_lock:
        if _sync_running:
            logger.info(f"Profiles sync already running — skipping auto-sync for job {job_id}")
            return
        _sync_running = True

    def _do_sync():
        global _sync_running
        try:
            logger.info(f"Auto-sync profiles triggered by job {job_id}")
            from services.domain_profiles import sync_domain_profiles
            from api.explorer import invalidate_profiles_cache
            result = sync_domain_profiles()
            invalidate_profiles_cache()
            logger.info(f"Auto-sync profiles done: {result}")
        except Exception as e:
            logger.warning(f"Auto-sync profiles error: {e}")
        finally:
            with _sync_lock:
                _sync_running = False

    t = threading.Thread(target=_do_sync, daemon=True, name=f"profiles-sync-{job_id[:8]}")
    t.start()

# In-memory job registry for active jobs (worker status)
_active_jobs: dict[str, asyncio.Task] = {}


async def run_batch_job(job_id: str, domains: list[str], services: list[str], force_refresh: bool = False, username: str = ""):
    """
    Main batch processing coroutine. Runs in background.
    Updates job progress in BigQuery after each domain.
    """
    total = len(domains)
    processed = 0
    failed = 0

    logger.info(f"Job {job_id} started: {total} domains, services={services}")
    await _update_job_safe(job_id, status="running", total_domains=total)

    # Batch-prefetch all corp BQ cache in 3 queries (SW + BW + AI)
    # This avoids N×3 individual BQ queries (~2-5s each) during processing.
    if not force_refresh:
        try:
            from config.settings import BQ_SIMILARWEB_CACHE, BQ_BUILTWITH_CACHE
            tables = [BQ_SIMILARWEB_CACHE, BQ_BUILTWITH_CACHE]
            # AI cache: read from latest_categories_claude (deduplicated view)
            from services.claude_ai import CORP_AI_CACHE_KEY
            tables.append(CORP_AI_CACHE_KEY)
            prefetch_corp_cache(domains, tables)
        except Exception as e:
            logger.warning(f"Prefetch failed (will fall back to per-domain queries): {e}")

    semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

    async def process_one(domain: str):
        nonlocal processed, failed
        async with semaphore:
            result = await process_domain(domain, job_id, services, force_refresh=force_refresh, username=username)
            save_result(result)

            if result["status"] == "error":
                failed += 1
            else:
                processed += 1

            # Update progress every 10 domains or on last
            total_done = processed + failed
            if total_done % 10 == 0 or total_done == total:
                await _update_job_safe(
                    job_id,
                    processed_domains=processed,
                    failed_domains=failed,
                )

            if DELAY_BETWEEN_DOMAINS > 0:
                await asyncio.sleep(DELAY_BETWEEN_DOMAINS)

    tasks = [process_one(d) for d in domains]
    await asyncio.gather(*tasks, return_exceptions=True)

    final_status = "completed" if failed == 0 else "completed_with_errors"
    await _update_job_safe(
        job_id,
        status=final_status,
        processed_domains=processed,
        failed_domains=failed,
    )
    logger.info(f"Job {job_id} finished: {processed} ok, {failed} failed")
    clear_prefetch_cache()

    # Auto-export to admin Google Sheets folder
    try:
        from core.bigquery import get_results, get_job
        from services.sheets_export import export_job_to_sheets
        job = get_job(job_id)
        results = get_results(job_id)
        if results:
            url = export_job_to_sheets(job_id, job.get("filename", "results"), results)
            if url:
                from services.credits import _save_setting
                _save_setting(f"sheet_url_{job_id}", url)
                logger.info(f"Auto-exported job {job_id} to Sheets: {url}")
    except Exception as e:
        logger.warning(f"Auto Sheets export failed for job {job_id}: {e}")

    # Auto-sync domain_profiles so new domains appear in Explorer immediately
    _trigger_profiles_sync(job_id)


async def _update_job_safe(job_id: str, **kwargs):
    """Update job without raising on BQ errors."""
    try:
        update_job(job_id, **kwargs)
    except Exception as e:
        logger.error(f"Failed to update job {job_id}: {e}")


def start_job(domains: list[str], services: list[str], filename: str, force_refresh: bool = False, username: str = "") -> str:
    """
    Create a new job in BQ and launch background task.
    Returns job_id.
    """
    job_id = str(uuid.uuid4())
    create_job(job_id, len(domains), services, filename)

    loop = asyncio.get_event_loop()
    task = loop.create_task(run_batch_job(job_id, domains, services, force_refresh=force_refresh, username=username))
    _active_jobs[job_id] = task

    # Cleanup on completion
    def on_done(t):
        _active_jobs.pop(job_id, None)
        if t.exception():
            logger.error(f"Job {job_id} task raised: {t.exception()}")
            try:
                update_job(job_id, status="failed", error_message=str(t.exception()))
            except Exception:
                pass

    task.add_done_callback(on_done)
    return job_id


def cancel_job(job_id: str) -> bool:
    """Cancel a running job."""
    task = _active_jobs.get(job_id)
    if task and not task.done():
        task.cancel()
        update_job(job_id, status="cancelled")
        return True
    return False
