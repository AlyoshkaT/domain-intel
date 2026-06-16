"""
Per-service API queues + priority gate for small jobs.

Three independent global semaphores cap concurrent calls per external API:
  sw — SimilarWeb, bw — BuiltWith, ai — Claude (incl. homepage fetch).
A job using only SW never blocks a job using only BW, and slow AI calls
never starve the fast SW/BW lanes.

Priority gate: jobs with ≤ PRIORITY_MAX_DOMAINS domains are "priority".
While at least one priority job is running, non-priority jobs pause before
each NEW API call (in-flight calls finish naturally), so a small urgent job
gets the full API capacity within seconds and the big job resumes after.
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from config.settings import BATCH_CONCURRENCY

logger = logging.getLogger(__name__)

PRIORITY_MAX_DOMAINS = int(os.getenv("PRIORITY_MAX_DOMAINS", "10"))

# Per-service concurrency (defaults to BATCH_CONCURRENCY, override via env)
_LIMITS = {
    "sw": int(os.getenv("SW_CONCURRENCY", str(BATCH_CONCURRENCY))),
    "bw": int(os.getenv("BW_CONCURRENCY", str(BATCH_CONCURRENCY))),
    "ai": int(os.getenv("AI_CONCURRENCY", str(BATCH_CONCURRENCY))),
}

# Lazy init — asyncio primitives must be created inside the running loop
_sems: dict[str, asyncio.Semaphore] = {}
_priority_jobs = 0
_no_priority_event: asyncio.Event | None = None   # set = no priority job running


def _sem(service: str) -> asyncio.Semaphore:
    if service not in _sems:
        _sems[service] = asyncio.Semaphore(_LIMITS.get(service, BATCH_CONCURRENCY))
    return _sems[service]


def _gate() -> asyncio.Event:
    global _no_priority_event
    if _no_priority_event is None:
        _no_priority_event = asyncio.Event()
        _no_priority_event.set()
    return _no_priority_event


def priority_job_started():
    """Call when a priority (small) job starts — big jobs pause at next API call."""
    global _priority_jobs
    _priority_jobs += 1
    _gate().clear()
    logger.info(f"Priority job started (active priority jobs: {_priority_jobs}) — big jobs paused")


def priority_job_finished():
    """Call when a priority job ends — big jobs resume."""
    global _priority_jobs
    _priority_jobs = max(0, _priority_jobs - 1)
    if _priority_jobs == 0:
        _gate().set()
        logger.info("All priority jobs finished — big jobs resumed")


@asynccontextmanager
async def api_slot(service: str, priority: bool = False):
    """
    Acquire one slot in the per-service API queue.
    Non-priority callers first wait until no priority job is active.
    """
    if not priority:
        await _gate().wait()
    async with _sem(service):
        yield
