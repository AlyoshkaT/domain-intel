"""Pipedrive relationship-status endpoints."""
import logging

from fastapi import APIRouter

from api.auth import require_permission
from services.pipedrive import sync_pipedrive, get_status_rows

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipedrive", dependencies=[require_permission("explorer")])


@router.get("/status")
async def status():
    """All relationship-status rows for the dashboard."""
    rows = get_status_rows()
    return {"rows": rows, "count": len(rows)}


@router.post("/sync", dependencies=[require_permission("admin")])
async def sync():
    """Full re-sync from Pipedrive (admin)."""
    try:
        return sync_pipedrive()
    except Exception as e:
        logger.exception("pipedrive sync failed")
        return {"status": "error", "error": str(e)}
