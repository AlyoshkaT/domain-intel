"""Pipedrive relationship-status endpoints."""
import logging
from datetime import date, timedelta

from fastapi import APIRouter, Request, BackgroundTasks, HTTPException

from api.auth import require_permission
from config.settings import PIPEDRIVE_COMPANY_DOMAIN
from services.pipedrive import (
    sync_pipedrive, get_status_rows, get_timeseries,
    get_sync_frequency, set_sync_frequency, get_webhook_secret, apply_webhook_event,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipedrive", dependencies=[require_permission("pipedrive")])


@router.get("/status")
async def status(as_of: str | None = None):
    """Relationship-status rows. Optional as_of=YYYY-MM-DD recomputes status as of that date."""
    rows = get_status_rows(as_of)
    return {"rows": rows, "count": len(rows), "as_of": as_of,
            "company": PIPEDRIVE_COMPANY_DOMAIN}


@router.get("/timeseries")
async def timeseries(date_from: str | None = None, date_to: str | None = None):
    """Monthly Won/Open/Lost trend. Defaults to the last 12 months."""
    dt = date.fromisoformat(date_to) if date_to else date.today()
    df = date.fromisoformat(date_from) if date_from else (dt - timedelta(days=365))
    return {"series": get_timeseries(df.isoformat(), dt.isoformat()),
            "date_from": df.isoformat(), "date_to": dt.isoformat()}


@router.get("/sync_settings", dependencies=[require_permission("admin")])
async def sync_settings():
    return {"frequency": get_sync_frequency()}


@router.post("/sync_settings", dependencies=[require_permission("admin")])
async def set_sync_settings(request: Request):
    body = await request.json()
    freq = body.get("frequency", "off")
    # The public origin is needed to register the Pipedrive webhook for "online".
    base_url = body.get("base_url") or str(request.base_url)
    try:
        return set_sync_frequency(freq, base_url)
    except Exception as e:
        logger.exception("pipedrive set frequency failed")
        return {"status": "error", "error": str(e)}


@router.post("/sync", dependencies=[require_permission("admin")])
async def sync():
    """Full re-sync from Pipedrive (admin)."""
    try:
        return sync_pipedrive()
    except Exception as e:
        logger.exception("pipedrive sync failed")
        return {"status": "error", "error": str(e)}


# Webhook is exempt from Basic Auth (see api/auth.py); guarded by the URL ?token= secret.
# No permission dependency — declared on a bare router so the parent dep doesn't apply.
hook_router = APIRouter(prefix="/api/pipedrive")


@hook_router.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks, token: str = ""):
    if token != get_webhook_secret():
        raise HTTPException(status_code=403, detail="bad token")
    payload = await request.json()
    # Apply incrementally in the background so we ACK Pipedrive immediately.
    background_tasks.add_task(_safe_apply, payload)
    return {"status": "accepted"}


def _safe_apply(payload: dict):
    try:
        apply_webhook_event(payload)
    except Exception:
        logger.exception("pipedrive webhook apply failed")
