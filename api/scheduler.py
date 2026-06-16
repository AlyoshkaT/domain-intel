"""
Scheduler — daily auto-sync of domain_profiles.
Runs every day at 04:00 UTC (= 06:00 Kyiv winter / 07:00 summer).
"""
import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_stop_event = threading.Event()
_SYNC_HOUR_UTC = 4         # 04:00 UTC = 06:00 Kyiv (UTC+2 winter)
_PARSED_SYNC_HOUR_UTC = 3  # 03:00 UTC — sync corpBQ raw → privateBQ parsed, 1 hour before profiles sync
_RESET_BQ_LIMIT_HOUR_UTC = 0  # 00:00 UTC — daily reset of BQ byte limit back to safe default


def _run_sync():
    """Run full sync in background thread."""
    from services.domain_profiles import sync_domain_profiles
    logger.info("Scheduled daily sync started")
    result = sync_domain_profiles()
    logger.info(f"Scheduled daily sync done: {result}")

    # Flush call stats immediately after sync (don't wait for 5-min tick)
    _flush_call_stats()

    # Invalidate Explorer cache after sync
    try:
        from api.explorer import invalidate_profiles_cache
        invalidate_profiles_cache()
    except Exception:
        pass


def _run_parsed_sync():
    """Sync corpBQ raw JSON → privateBQ parsed tables (sw_parsed, bw_parsed)."""
    from core.bigquery import sync_parsed_from_corp
    logger.info("Scheduled parsed sync started (corpBQ raw → privateBQ parsed)")
    result = sync_parsed_from_corp()
    logger.info(f"Scheduled parsed sync done: {result}")

    # Flush call stats immediately after sync
    _flush_call_stats()


def _flush_call_stats():
    """Flush in-memory BQ call counters to BigQuery."""
    try:
        from core.bigquery import flush_bq_call_stats
        flush_bq_call_stats()
    except Exception as e:
        logger.debug(f"flush_bq_call_stats: {e}")


_BQ_LIMIT_DAILY_RESET_GB = 25   # reset target — enough for a normal day, safe margin


def _get_sync_frequency() -> str:
    """
    Read auto_sync_frequency from BQ app_settings: daily | weekly | monthly | off.
    Backward compat: falls back to auto_sync_enabled (true→daily, false→off).
    """
    try:
        from core.bigquery import get_setting
        freq = get_setting("auto_sync_frequency", "")
        if freq in ("daily", "weekly", "monthly", "off"):
            return freq
        return "daily" if get_setting("auto_sync_enabled", "true") != "false" else "off"
    except Exception:
        return "daily"  # fail-safe


def _should_sync_today(now: datetime) -> bool:
    """Check if sync should run today based on the configured frequency."""
    freq = _get_sync_frequency()
    if freq == "off":
        return False
    if freq == "weekly":
        return now.weekday() == 0      # Monday
    if freq == "monthly":
        return now.day == 1            # 1st of each month
    return True                        # daily


def _reset_bq_limit():
    """Reset BQ byte limit back to safe daily default. Runs at midnight UTC."""
    try:
        from core.bigquery import set_setting, _invalidate_max_bytes_cache
        set_setting("bq_max_bytes_gb", str(_BQ_LIMIT_DAILY_RESET_GB))
        _invalidate_max_bytes_cache()
        logger.info(f"BQ byte limit reset to {_BQ_LIMIT_DAILY_RESET_GB} GB (daily auto-reset)")
    except Exception as e:
        logger.warning(f"BQ limit daily reset failed: {e}")


def _scheduler_loop():
    """Check every 5 minutes if it's time to sync (daily at 00:00, 03:00 and 04:00 UTC)."""
    import time
    while not _stop_event.is_set():
        _flush_call_stats()   # flush counters every 5-min tick
        now = datetime.utcnow()
        if now.hour == _RESET_BQ_LIMIT_HOUR_UTC and now.minute < 5:
            logger.info("Daily BQ limit reset triggered")
            _reset_bq_limit()
            # Sleep 55 minutes to avoid double-trigger within same hour
            _stop_event.wait(timeout=55 * 60)
        elif now.hour == _PARSED_SYNC_HOUR_UTC and now.minute < 5:
            if _should_sync_today(now):
                logger.info("Parsed sync triggered (corpBQ raw → privateBQ parsed)")
                thread = threading.Thread(target=_run_parsed_sync, daemon=True, name="parsed-sync")
                thread.start()
            else:
                logger.info(f"Parsed sync skipped — frequency={_get_sync_frequency()}")
            # Sleep 55 minutes to avoid double-trigger but still wake for profile sync at 04:00
            _stop_event.wait(timeout=55 * 60)
        elif now.hour == _SYNC_HOUR_UTC and now.minute < 5:
            if _should_sync_today(now):
                logger.info("Scheduled profiles sync triggered")
                thread = threading.Thread(target=_run_sync, daemon=True, name="daily-sync")
                thread.start()
            else:
                logger.info(f"Profiles sync skipped — frequency={_get_sync_frequency()}")
            # Sleep 6 hours to avoid double-trigger within same hour
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
    logger.info(f"Sync scheduler started (parsed at {_PARSED_SYNC_HOUR_UTC:02d}:00 UTC, profiles at {_SYNC_HOUR_UTC:02d}:00 UTC)")


def stop_scheduler():
    _stop_event.set()
    logger.info("Sync scheduler stopped")
