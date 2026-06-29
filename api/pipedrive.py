"""Pipedrive relationship-status endpoints."""
import logging
from datetime import date, timedelta

from fastapi import APIRouter

from api.auth import require_permission
from config.settings import PIPEDRIVE_COMPANY_DOMAIN
from services.pipedrive import sync_pipedrive, get_status_rows, get_timeseries

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


@router.post("/sync", dependencies=[require_permission("admin")])
async def sync():
    """Full re-sync from Pipedrive (admin)."""
    try:
        return sync_pipedrive()
    except Exception as e:
        logger.exception("pipedrive sync failed")
        return {"status": "error", "error": str(e)}
