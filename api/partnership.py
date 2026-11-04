"""Partnership (agency ↔ client) dashboard endpoints."""
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from api.auth import require_permission
from services.partnership import (
    get_partnership_rows, sync_partnership, get_frequency, set_frequency,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/partnership", dependencies=[require_permission("partnership")])

# Latest async Google-Sheets export result (polled by GET /export/sheets/url).
_sheet_url: str | None = None
_sheet_error: str | None = None


def _rows_from_body(body: dict) -> list[list]:
    """The client sends the finished pivot as rows[0]=header, rest=data."""
    rows = body.get("rows") or []
    if not rows or len(rows) < 2:
        raise HTTPException(status_code=400, detail="No rows")
    return rows


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


# ── Exports ──────────────────────────────────────────────────────────────────
# The dashboard pivot is built client-side (filters, period, channel toggles,
# detail on/off), so the client posts the finished rows and we only serialise
# them — exactly what the user sees.

@router.post("/export/xlsx", dependencies=[require_permission("download")])
async def export_xlsx(request: Request, body: dict):
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    from services.sheets_export import sanitize_for_xlsx
    rows = _rows_from_body(body)
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "partnership_export_xlsx", {"row_count": len(rows) - 1})
    except Exception:
        pass
    df = sanitize_for_xlsx(pd.DataFrame(rows[1:], columns=rows[0]))
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Partnership")
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=partnership_mrr.xlsx"},
    )


@router.post("/export/sheets", dependencies=[require_permission("sheets")])
async def export_sheets(request: Request, body: dict, background_tasks: BackgroundTasks):
    global _sheet_url, _sheet_error
    _sheet_url = None
    _sheet_error = None
    rows = _rows_from_body(body)
    label = str(body.get("label") or f"{len(rows) - 1} clients")
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "partnership_export_sheets", {"row_count": len(rows) - 1, "label": label})
    except Exception:
        pass
    # User's own Drive folder (same lookup as Explorer), so the file lands there.
    folder_id = ""
    try:
        from core.bigquery import get_users
        users = {u["username"]: u for u in get_users()}
        folder_id = users.get(username, {}).get("google_folder") or ""
    except Exception:
        pass

    def do_export():
        global _sheet_url, _sheet_error
        try:
            from services.sheets_export import export_partnership_to_sheets
            _sheet_url = export_partnership_to_sheets(label, rows, folder_id=folder_id)
        except Exception as e:
            _sheet_error = str(e)

    background_tasks.add_task(do_export)
    return {"status": "exporting"}


@router.get("/export/sheets/url")
def sheets_url():
    return {"url": _sheet_url, "error": _sheet_error}
