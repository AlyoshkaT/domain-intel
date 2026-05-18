"""
FastAPI application - main entry point
"""
import io
import json
import logging
import csv
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException, BackgroundTasks
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from core.bigquery import (
    ensure_tables_exist, list_jobs, get_job, get_results,
    reset_stale_jobs, update_job, get_stale_running_jobs,
)
from services.technology_catalog import sync_catalog
from services.redirect_resolver import ensure_redirects_table
from services.credits import fetch_builtwith_credits, get_cached_credits
from services.sheets_export import export_job_to_sheets
from services.domain_profiles import ensure_profiles_table, get_sync_status
from processing.batch import start_job, cancel_job
from processing.pipeline import reload_catalog
from api.auth import auth_middleware, get_user_permissions, require_permission
from api.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Domain Intel API...")
    try:
        ensure_tables_exist()
        ensure_redirects_table()
        ensure_profiles_table()
        logger.info("BigQuery tables verified")
    except Exception as e:
        logger.error(f"BQ init error: {e}")

    # Auto-resume jobs interrupted by server restart
    try:
        from processing.batch import resume_job
        stale_jobs = get_stale_running_jobs()
        resumed, failed_reset = 0, 0
        for job in stale_jobs:
            result = resume_job(job["job_id"])
            if result.get("ok") and result.get("remaining", 0) > 0:
                logger.info(f"Auto-resumed job {job['job_id']}: {result['remaining']} remaining")
                resumed += 1
            elif result.get("ok") and result.get("remaining", 0) == 0:
                logger.info(f"Job {job['job_id']} already fully processed — marked complete")
                resumed += 1
            else:
                # No domain list saved (old job) — fall back to marking failed
                update_job(job["job_id"], status="failed",
                           error_message="Interrupted by server restart (no domain list — cannot resume)")
                failed_reset += 1
        if resumed or failed_reset:
            logger.warning(f"Startup: {resumed} jobs auto-resumed, {failed_reset} marked failed (no domain list)")
    except Exception as e:
        logger.warning(f"Auto-resume error: {e}")

    try:
        await fetch_builtwith_credits()
    except Exception as e:
        logger.warning(f"Credits fetch error: {e}")

    start_scheduler()
    yield
    stop_scheduler()
    logger.info("Shutting down")


app = FastAPI(title="Domain Intel API", version="1.0.0", lifespan=lifespan)

# Auth middleware
app.middleware("http")(auth_middleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=2000)

# Include explorer router
try:
    from api.explorer import router as explorer_router
    app.include_router(explorer_router)
except Exception as e:
    logger.warning(f"Explorer router not loaded: {e}")

try:
    from api.technologies import router as tech_router
    app.include_router(tech_router)
except Exception as e:
    logger.warning(f"Technologies router not loaded: {e}")

try:
    from api.setup import router as setup_router
    app.include_router(setup_router)
except Exception as e:
    logger.warning(f"Setup router not loaded: {e}")

try:
    from api.redirects import router as redirects_router
    app.include_router(redirects_router)
except Exception as e:
    logger.warning(f"Redirects router not loaded: {e}")


# ─── Health ───────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok"}


# ─── BQ activity indicator ────────────────────────────────────────────────────
@app.get("/api/bq_activity")
async def bq_activity():
    from core.bigquery import get_bq_activity
    return get_bq_activity()


# ─── Current user ─────────────────────────────────────────────────────────────
@app.get("/api/me")
async def me(request: Request):
    username = getattr(request.state, "username", "anonymous")
    perms = get_user_permissions(username)
    return {"username": username, "permissions": sorted(perms)}


# ─── Client-side action logger ────────────────────────────────────────────────
@app.post("/api/log")
async def client_log(request: Request):
    """Log a client-side action (e.g. CSV export done in browser)."""
    try:
        body = await request.json()
        action = str(body.get("action", ""))
        details = body.get("details", {})
        if not action:
            return {"ok": False}
        username = getattr(request.state, "username", "unknown")
        from core.bigquery import log_activity
        log_activity(username, action, details)
    except Exception:
        pass
    return {"ok": True}


# ─── Credits ──────────────────────────────────────────────────────────────────
@app.get("/api/credits")
async def credits_endpoint():
    return get_cached_credits()

@app.post("/api/credits/refresh")
async def refresh_credits():
    bw = await fetch_builtwith_credits()
    cached = get_cached_credits()
    return {"builtwith": bw, "similarweb": cached.get("similarweb")}


# ─── Catalog ──────────────────────────────────────────────────────────────────
@app.post("/api/catalog/sync")
async def sync_catalog_endpoint():
    try:
        counts = sync_catalog()
        reload_catalog()
        return {"success": True, "counts": counts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/catalog/status")
async def catalog_status():
    from core.bigquery import client, table_ref
    from services.technology_catalog import CATALOG_TABLE
    try:
        rows = list(client().query(
            f"SELECT sheet, COUNT(*) as cnt FROM `{table_ref(CATALOG_TABLE)}` GROUP BY sheet"
        ).result())
        counts = {r['sheet']: r['cnt'] for r in rows}
        return {"synced": bool(counts), "counts": counts}
    except Exception:
        return {"synced": False, "counts": {}}


# ─── Jobs ─────────────────────────────────────────────────────────────────────
def _parse_domains_from_file(content: bytes, filename: str) -> list[str]:
    domains = []
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        import pandas as pd
        df = pd.read_excel(io.BytesIO(content), header=None)
        for col in df.columns:
            for val in df[col].dropna():
                s = str(val).strip()
                if s and "." in s:
                    domains.append(s)
                    break
            if domains:
                break
        if not domains:
            domains = [str(v).strip() for v in df.iloc[:, 0].dropna() if str(v).strip()]
    else:
        text = content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if row:
                domains.append(row[0].strip())

    from processing.pipeline import _clean_domain
    cleaned = []
    seen = set()
    for d in domains:
        d = _clean_domain(d)
        if d and d not in seen:
            seen.add(d)
            cleaned.append(d)
    return cleaned


@app.post("/api/jobs", dependencies=[require_permission("jobs")])
async def create_job_endpoint(
    request: Request,
    file: UploadFile = File(...),
    services: str = Form(...),
    force_refresh: str = Form(default="false"),
):
    content = await file.read()
    domains = _parse_domains_from_file(content, file.filename or "upload.csv")
    if not domains:
        raise HTTPException(status_code=400, detail="No valid domains found in file")

    services_list = json.loads(services)
    valid_services = ["similarweb", "builtwith", "ai"]
    services_list = [s for s in services_list if s in valid_services]
    if not services_list:
        raise HTTPException(status_code=400, detail="No valid services selected")

    fr = force_refresh.lower() == "true"
    username = getattr(request.state, "username", "unknown")
    job_id = start_job(domains, services_list, file.filename or "upload.csv", force_refresh=fr, username=username)
    try:
        from core.bigquery import log_activity
        log_activity(username, "job_created", {
            "job_id": job_id, "total_domains": len(domains),
            "services": services_list, "filename": file.filename or "upload.csv"
        })
    except Exception:
        pass
    return {"job_id": job_id, "total_domains": len(domains), "services": services_list, "status": "pending"}


@app.get("/api/jobs")
async def list_jobs_endpoint():
    return {"jobs": list_jobs(limit=100)}

@app.get("/api/jobs/{job_id}")
async def get_job_endpoint(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

def _sync_processed_domains(job_id: str):
    """Trigger profiles sync + GSheet auto-export after Cancel/Force Complete (non-blocking)."""
    try:
        from processing.batch import _trigger_profiles_sync
        from core.bigquery import get_results
        results = get_results(job_id)
        domains = [r["domain"] for r in results if r.get("domain") and r.get("status") != "error"]
        if domains:
            _trigger_profiles_sync(job_id, domains)
    except Exception as e:
        logger.warning(f"Post-cancel sync failed for job {job_id}: {e}")

    # Auto GSheet export (same as normal job completion)
    def _do_sheets():
        try:
            from core.bigquery import get_results as _get_results, get_job
            from services.sheets_export import export_job_to_sheets
            from core.bigquery import set_setting
            job = get_job(job_id)
            results = _get_results(job_id)
            if results:
                url = export_job_to_sheets(job_id, job.get("filename", "results"), results)
                if url:
                    set_setting(f"sheet_url_{job_id}", url)
                    logger.info(f"Auto-exported cancelled job {job_id} to Sheets: {url}")
        except Exception as e:
            logger.warning(f"Auto Sheets export failed for cancelled job {job_id}: {e}")

    import threading
    threading.Thread(target=_do_sheets, daemon=True, name=f"sheets-{job_id[:8]}").start()


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job_endpoint(job_id: str):
    cancelled = cancel_job(job_id)
    if not cancelled:
        # Task not found in memory (e.g. after restart) — force status update
        job = get_job(job_id)
        if job and job.get("status") in ("running", "pending"):
            update_job(job_id, status="cancelled", error_message="Manually cancelled")
            cancelled = True
    if cancelled:
        _sync_processed_domains(job_id)
    return {"cancelled": cancelled}

@app.post("/api/jobs/{job_id}/resume", dependencies=[require_permission("jobs")])
async def resume_job_endpoint(request: Request, job_id: str):
    """Resume an interrupted job from where it left off using the stored domain list."""
    from processing.batch import resume_job
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") in ("running", "pending"):
        raise HTTPException(status_code=400, detail="Job is already running")
    username = getattr(request.state, "username", "unknown")
    result = resume_job(job_id, username=username)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "Cannot resume"))
    return result

@app.post("/api/jobs/{job_id}/retry_errors", dependencies=[require_permission("jobs")])
async def retry_errors(request: Request, job_id: str):
    """Create a new job retrying only the error-status domains from a previous job."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    results = get_results(job_id)
    error_domains = [r["domain"] for r in results if r.get("status") == "error"]
    if not error_domains:
        return {"count": 0, "job_id": None}
    services = job.get("services") or []
    username = getattr(request.state, "username", "unknown")
    new_job_id = start_job(error_domains, services, f"retry_{job_id[:8]}.txt", username=username)
    return {"count": len(error_domains), "job_id": new_job_id}

@app.post("/api/admin/sync_parsed_from_corp", dependencies=[require_permission("admin")])
async def sync_parsed_from_corp_endpoint():
    """
    One-time migration: populate privateBQ sw_parsed / bw_parsed from corpBQ raw JSON.
    Run this once after first deploy to avoid waiting for the 03:00 UTC nightly sync.
    After this completes, run /explorer/refresh to rebuild domain_profiles.
    Runs synchronously (may take several minutes).
    """
    from core.bigquery import sync_parsed_from_corp
    result = await asyncio.to_thread(sync_parsed_from_corp)
    return result


@app.post("/api/jobs/{job_id}/sync_from_results", dependencies=[require_permission("jobs")])
async def sync_from_results(job_id: str):
    """Sync domain_profiles directly from analysis_results for this job (bypasses corpBQ).
    Runs synchronously — client waits and receives the result."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    from services.domain_profiles import sync_profiles_from_job_results
    from api.explorer import invalidate_profiles_cache
    result = await asyncio.to_thread(sync_profiles_from_job_results, job_id)
    invalidate_profiles_cache()
    return {"ok": True, **result}


@app.post("/api/jobs/{job_id}/force_complete", dependencies=[require_permission("admin")])
async def force_complete_job(job_id: str):
    """Force-mark a stuck running job as completed with whatever was processed so far."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") not in ("running", "pending"):
        raise HTTPException(status_code=400, detail=f"Job is already '{job.get('status')}'")
    cancel_job(job_id)  # cancel task if still in memory
    done = (job.get("processed_domains") or 0) + (job.get("failed_domains") or 0)
    total = job.get("total_domains") or 0
    status = "completed" if (job.get("failed_domains") or 0) == 0 else "completed_with_errors"
    update_job(job_id, status=status, error_message=f"Force-completed by admin ({done}/{total} processed)")
    _sync_processed_domains(job_id)
    return {"ok": True, "status": status, "processed": done, "total": total}


# ─── Results ──────────────────────────────────────────────────────────────────
@app.get("/api/jobs/{job_id}/results")
async def get_results_endpoint(job_id: str):
    results = get_results(job_id)
    return {"results": results, "total": len(results)}


@app.get("/api/jobs/{job_id}/export/csv", dependencies=[require_permission("download")])
async def export_csv(request: Request, job_id: str):
    results = get_results(job_id)
    if not results:
        raise HTTPException(status_code=404, detail="No results found")
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "job_export_csv", {"job_id": job_id, "row_count": len(results)})
    except Exception:
        pass
    import pandas as pd
    df = pd.DataFrame(results)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=results_{job_id[:8]}.csv"}
    )

@app.get("/api/jobs/{job_id}/export/xlsx", dependencies=[require_permission("download")])
async def export_xlsx(request: Request, job_id: str):
    results = get_results(job_id)
    if not results:
        raise HTTPException(status_code=404, detail="No results found")
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "job_export_xlsx", {"job_id": job_id, "row_count": len(results)})
    except Exception:
        pass
    import pandas as pd
    df = pd.DataFrame(results)
    # Remove timezone from datetime columns
    for col in df.columns:
        if hasattr(df[col], 'dt') and hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_localize(None)
        else:
            try:
                converted = pd.to_datetime(df[col], utc=True)
                if converted.dt.tz is not None:
                    df[col] = converted.dt.tz_localize(None)
            except Exception:
                pass
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=results_{job_id[:8]}.xlsx"}
    )

class SheetsExportRequest(BaseModel):
    folder_id: str = ""  # Google Drive folder URL or ID (overrides env var)

@app.post("/api/jobs/{job_id}/export/sheets", dependencies=[require_permission("sheets")])
async def export_sheets(request: Request, job_id: str,
                        body: SheetsExportRequest = SheetsExportRequest(),
                        background_tasks: BackgroundTasks = BackgroundTasks()):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    results = get_results(job_id)
    if not results:
        raise HTTPException(status_code=404, detail="No results found")
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "job_export_sheets", {"job_id": job_id, "row_count": len(results)})
    except Exception:
        pass

    folder_id = body.folder_id

    def do_export():
        url = export_job_to_sheets(job_id, job.get("filename", "results"), results,
                                   folder_id=folder_id)
        if url:
            from core.bigquery import set_setting
            set_setting(f"sheet_url_{job_id}", url)

    background_tasks.add_task(do_export)
    return {"status": "exporting"}

@app.get("/api/jobs/{job_id}/export/sheets/url")
async def get_sheets_url(job_id: str):
    from core.bigquery import get_setting
    url = get_setting(f"sheet_url_{job_id}")
    return {"url": url or None}


# ─── Serve frontend ───────────────────────────────────────────────────────────
import os
from fastapi.responses import FileResponse

# Find frontend/dist relative to this file
_here = os.path.dirname(os.path.abspath(__file__))
_candidates = [
    os.path.join(_here, "..", "frontend", "dist"),
    os.path.join(os.getcwd(), "frontend", "dist"),
    "/app/frontend/dist",
    os.path.join(_here, "frontend", "dist"),
]
frontend_dist = next((p for p in _candidates if os.path.exists(p)), _candidates[0])
assets_dir = os.path.join(frontend_dist, "assets")
index_html = os.path.join(frontend_dist, "index.html")
import logging as _log
_flog = _log.getLogger(__name__)
_flog.info(f"CWD: {os.getcwd()}")
_flog.info(f"__file__: {os.path.abspath(__file__)}")
for _c in _candidates:
    _flog.info(f"  candidate: {_c} → exists={os.path.exists(_c)}")
_flog.info(f"Frontend dist selected: {frontend_dist} (index.html exists: {os.path.exists(index_html)})")

if os.path.exists(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

@app.get("/")
async def serve_root():
    if os.path.exists(index_html):
        return FileResponse(index_html)
    return {"status": "api-only"}

@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if full_path.startswith("api/"):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not Found")
    if os.path.exists(index_html):
        return FileResponse(index_html)
    return {"status": "api-only", "path": full_path}
