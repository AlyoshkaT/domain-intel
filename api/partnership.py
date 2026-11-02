"""Partnership (agency ↔ client) dashboard endpoints."""
import logging

from fastapi import APIRouter, Request

from api.auth import require_permission
from services.partnership import (
    get_partnership_rows, sync_partnership, get_frequency, set_frequency,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/partnership", dependencies=[require_permission("partnership")])


@router.get("/data")
async def data():
    """Agency membership + each client's trailing per-channel monthly MRR.
    Reads only the small private tables (no corp scan)."""
    return get_partnership_rows()


@router.get("/settings", dependencies=[require_permission("admin")])
async def settings():
    return {"frequency": get_frequency()}


@router.post("/settings", dependencies=[require_permission("admin")])
async def set_settings(request: Request):
    body = await request.json()
    try:
        return set_frequency(body.get("frequency", "off"))
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/sync", dependencies=[require_permission("admin")])
async def sync():
    """Pull agency membership + MRR from corpBQ now (admin). ~570 MB corp scan."""
    try:
        return sync_partnership()
    except Exception as e:
        logger.exception("partnership sync failed")
        return {"status": "error", "error": str(e)}
