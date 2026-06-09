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
    prefetch_parsed, clear_parsed_cache,
    save_job_domains, get_job_domains, get_processed_domains_for_job,
)

logger = logging.getLogger(__name__)

# Track whether a profiles sync is already running (avoid pile-up)
_sync_lock = threading.Lock()
_sync_running = False


def _trigger_profiles_sync(job_id: str, domains: list[str]):
    """
    Fast incremental upsert — only updates profiles for the processed domains.
    Skips if a sync is already running to avoid pile-up.
    """
    global _sync_running
    with _sync_lock:
        if _sync_running:
            logger.info(f"Profiles sync already running — skipping auto-sync for job {job_id}")
            return
        _sync_running = True

    def _do_sync():
        global _sync_running
        try:
            logger.info(f"Incremental profiles sync triggered by job {job_id}: {len(domains)} domains")
            from services.domain_profiles import sync_domain_profiles_incremental
            from api.explorer import invalidate_profiles_cache
            result = sync_domain_profiles_incremental(domains)
            invalidate_profiles_cache()
            logger.info(f"Incremental profiles sync done: {result}")
        except Exception as e:
            logger.warning(f"Auto-sync profiles error: {e}")
        finally:
            with _sync_lock:
                _sync_running = False

    t = threading.Thread(target=_do_sync, daemon=True, name=f"profiles-sync-{job_id[:8]}")
    t.start()

# ── In-memory live progress ────────────────────────────────────────────────────
# Tracks running jobs so /api/jobs/{id} never needs a BQ query during processing.
# Format: { job_id: {status, total, processed, failed, started_at, services, ...} }
_active_jobs: dict[str, asyncio.Task] = {}
_job_progress: dict[str, dict] = {}


def get_live_progress(job_id: str) -> Optional[dict]:
    """Return live in-memory progress for a running job, or None if not active."""
    return _job_progress.get(job_id)


def _set_progress(job_id: str, **kwargs):
    """Update in-memory progress — zero BQ calls, pure arithmetic."""
    if job_id in _job_progress:
        _job_progress[job_id].update(kwargs)
    else:
        _job_progress[job_id] = kwargs


def _bq_checkpoint_interval(total: int) -> int:
    """
    How often to persist progress to BQ (for crash recovery only).
    UI always reads from memory — so this only affects what BQ knows on restart.
      <10   → never (start + end only)
      <100  → every 10
      <1000 → every 50
      1000+ → every 200
    """
    if total < 10:   return total      # effectively: only at end
    if total < 100:  return 10
    if total < 1000: return 50
    return 200


async def run_batch_job(
    job_id: str, domains: list[str], services: list[str],
    force_refresh: bool = False, username: str = "",
    processed_offset: int = 0, failed_offset: int = 0,
):
    """
    Main batch processing coroutine. Runs in background.
    processed_offset / failed_offset: used when resuming a partially-done job.

    Progress tracking strategy:
      - In-memory _job_progress updated after every domain (no BQ calls).
      - BQ written only at start (status=running) and end (status=completed/failed).
      - /api/jobs/{id} reads from memory while job is active, BQ after it finishes.
    """
    total = len(domains)
    processed = processed_offset
    failed    = failed_offset

    # Initialise in-memory snapshot
    _set_progress(job_id,
        status="running",
        total_domains=total + processed_offset + failed_offset,
        processed_domains=processed,
        failed_domains=failed,
        services=services,
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    # BQ write #1: mark job as running (start)
    if processed_offset == 0 and failed_offset == 0:
        logger.info(f"Job {job_id} started: {total} domains, services={services}")
        try:
            await asyncio.to_thread(update_job, job_id, status="running", total_domains=total)
        except Exception as e:
            logger.error(f"Job {job_id}: failed to mark running in BQ: {e}")
    else:
        logger.info(f"Job {job_id} resumed: {total} remaining, already done={processed_offset+failed_offset}")
        try:
            await asyncio.to_thread(update_job, job_id, status="running")
        except Exception as e:
            logger.error(f"Job {job_id}: failed to mark running in BQ: {e}")

    # Batch-prefetch: 2 privateBQ queries + 1 corpBQ query — total 3 BQ calls for whole job
    try:
        prefetch_parsed(domains)
    except Exception as e:
        logger.warning(f"prefetch_parsed failed (will fall back to per-domain queries): {e}")

    try:
        from services.claude_ai import CORP_AI_CACHE_KEY
        if not (force_refresh and "ai" in services):
            prefetch_corp_cache(domains, [CORP_AI_CACHE_KEY])
    except Exception as e:
        logger.warning(f"Prefetch AI failed (will fall back to per-domain queries): {e}")

    semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
    total_all  = total + processed_offset + failed_offset
    checkpoint = _bq_checkpoint_interval(total_all)
    logger.info(f"Job {job_id}: BQ checkpoint every {checkpoint} domains (total={total_all})")

    async def process_one(domain: str):
        nonlocal processed, failed
        async with semaphore:
            result = await process_domain(
                domain, job_id, services,
                force_refresh=force_refresh,
                username=username,
                skip_redirect=force_refresh,
            )
            await asyncio.to_thread(save_result, result)

            if result["status"] == "error":
                failed += 1
            else:
                processed += 1

            # UI: update in-memory counter after every domain — zero BQ calls
            _set_progress(job_id, processed_domains=processed, failed_domains=failed)

            # BQ: persist for crash recovery only — at computed interval
            total_done = processed + failed
            if total_done % checkpoint == 0:
                try:
                    await asyncio.to_thread(update_job, job_id,
                        processed_domains=processed, failed_domains=failed)
                except Exception as e:
                    logger.warning(f"Job {job_id}: checkpoint BQ write failed: {e}")

            if DELAY_BETWEEN_DOMAINS > 0:
                await asyncio.sleep(DELAY_BETWEEN_DOMAINS)

    tasks = [process_one(d) for d in domains]
    await asyncio.gather(*tasks, return_exceptions=True)

    final_status = "completed" if failed == 0 else "completed_with_errors"
    _set_progress(job_id, status=final_status, processed_domains=processed, failed_domains=failed)

    # BQ write #2: persist final state (end)
    try:
        await asyncio.to_thread(update_job, job_id,
            status=final_status,
            processed_domains=processed,
            failed_domains=failed,
        )
    except Exception as e:
        logger.error(f"Job {job_id}: failed to persist final state to BQ: {e}")

    logger.info(f"Job {job_id} finished: {processed} ok, {failed} failed")
    clear_prefetch_cache()
    clear_parsed_cache()

    # Remove from live progress after a short delay so last poll still gets the result
    async def _cleanup_progress():
        await asyncio.sleep(30)
        _job_progress.pop(job_id, None)
    asyncio.create_task(_cleanup_progress())

    # Persist BQ call counters
    try:
        from core.bigquery import flush_bq_call_stats
        await asyncio.to_thread(flush_bq_call_stats)
    except Exception as e:
        logger.warning(f"flush_bq_call_stats after job {job_id}: {e}")

    # Auto-export to Sheets
    try:
        from core.bigquery import get_results, get_job, get_users
        from services.sheets_export import export_job_to_sheets
        job = get_job(job_id)
        results = get_results(job_id)
        if results:
            creator = job.get("created_by", "") or username or ""
            folder_id = ""
            if creator:
                try:
                    users = {u["username"]: u for u in get_users()}
                    folder_id = users.get(creator, {}).get("google_folder") or ""
                except Exception:
                    pass
            url = export_job_to_sheets(job_id, job.get("filename", "results"), results,
                                       folder_id=folder_id)
            if url:
                from services.credits import _save_setting
                _save_setting(f"sheet_url_{job_id}", url)
                logger.info(f"Auto-exported job {job_id} to Sheets: {url}")
    except Exception as e:
        logger.warning(f"Auto Sheets export failed for job {job_id}: {e}")

    _trigger_profiles_sync(job_id, domains)


def resume_job(job_id: str, username: str = "") -> dict:
    """
    Resume a previously interrupted job using its stored domain list.
    Returns {"ok": True/False, "remaining": N, "already_done": N}
    """
    job = get_job(job_id)
    if not job:
        return {"ok": False, "error": "Job not found"}

    all_domains = get_job_domains(job_id)
    if not all_domains:
        return {"ok": False, "error": "No domain list saved for this job — cannot resume"}

    processed_set = get_processed_domains_for_job(job_id)
    remaining = [d for d in all_domains if d not in processed_set]
    already_done = len(processed_set)

    if not remaining:
        # Everything processed — just mark complete
        errors = sum(1 for r in [] if r)  # can't easily count errors here; use job record
        update_job(job_id, status="completed", error_message=None)
        return {"ok": True, "remaining": 0, "already_done": already_done}

    services = job.get("services") or []
    p_offset = job.get("processed_domains") or 0
    f_offset = job.get("failed_domains") or 0

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        run_batch_job(job_id, remaining, services, username=username,
                      processed_offset=p_offset, failed_offset=f_offset)
    )
    _active_jobs[job_id] = task

    def on_done(t):
        _active_jobs.pop(job_id, None)
        if t.exception():
            logger.error(f"Resumed job {job_id} raised: {t.exception()}")

    task.add_done_callback(on_done)
    logger.info(f"Resuming job {job_id}: {len(remaining)} remaining, {already_done} already done")
    return {"ok": True, "remaining": len(remaining), "already_done": already_done}


def start_job(domains: list[str], services: list[str], filename: str, force_refresh: bool = False, username: str = "") -> str:
    """
    Create a new job in BQ and launch background task.
    Returns job_id.
    """
    job_id = str(uuid.uuid4())
    create_job(job_id, len(domains), services, filename, created_by=username)

    # Persist domain list so the job can be resumed after a server restart
    try:
        save_job_domains(job_id, domains)
    except Exception as e:
        logger.warning(f"Could not save job domains for {job_id}: {e}")

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
