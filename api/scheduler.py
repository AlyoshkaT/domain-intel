"""
Scheduler — weekly auto-sync of domain_profiles.
Runs every Saturday at 02:00 UTC.
"""
import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _run_sync():
    """Run sync in background thread."""
    from services.domain_profiles import sync_domain_profiles
    logger.info("Scheduled sync started")
    result = sync_domain_profiles()
    logger.info(f"Scheduled sync done: {result}")

    # Invalidate Explorer cache after sync
    try:
        import api.explorer as explorer
        explorer._profiles_cache = None
    except Exception:
        pass


def _scheduler_loop():
    """Check every hour if it's time to sync (Saturday 02:00 UTC)."""
    import time
    while not _stop_event.is_set():
        now = datetime.utcnow()
        # Saturday = weekday 5, at 02:00
        if now.weekday() == 5 and now.hour == 2 and now.minute < 5:
            logger.info("Weekly sync triggered")
            thread = threading.Thread(target=_run_sync, daemon=True)
            thread.start()
            # Sleep 6 hours to avoid double-trigger
            _stop_event.wait(timeout=6 * 3600)
        else:
            # Check every 5 minutes
            _stop_event.wait(timeout=300)


def start_scheduler():
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _stop_event.clear()
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="sync-scheduler")
    _scheduler_thread.start()
    logger.info("Sync scheduler started (weekly, Saturday 02:00 UTC)")


def stop_scheduler():
    _stop_event.set()
    logger.info("Sync scheduler stopped")
