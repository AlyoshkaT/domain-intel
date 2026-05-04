"""
FastAPI application - main entry point
"""
import io
import json
import logging
import csv
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from core.bigquery import ensure_tables_exist, list_jobs, get_job, get_results
from services.technology_catalog import sync_catalog
from services.redirect_resolver import ensure_redirects_table
from services.credits import ensure_app_settings_table, fetch_builtwith_credits, get_cached_credits
from services.sheets_export import export_job_to_sheets
from services.domain_profiles import ensure_profiles_table, get_sync_status
from processing.batch import start_job, cancel_job
from processing.pipeline import reload_catalog
from api.auth import auth_middleware
from api.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Domain Intel API...")
    try:
        ensure_tables_exist()
        ensure_redirects_table()
        ensure_app_settings_table()
        ensure_profiles_table()
        logger.info("BigQuery tables verified")
    except Exception as e:
        logger.error(f"BQ init error: {e}")

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


# ─── Health ───────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok"}


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


@app.post("/api/jobs")
async def create_job_endpoint(
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
    job_id = start_job(domains, services_list, file.filename or "upload.csv", force_refresh=fr)
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

@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job_endpoint(job_id: str):
    return {"cancelled": cancel_job(job_id)}


# ─── Results ──────────────────────────────────────────────────────────────────
@app.get("/api/jobs/{job_id}/results")
async def get_results_endpoint(job_id: str):
    results = get_results(job_id)
    return {"results": results, "total": len(results)}


@app.get("/api/jobs/{job_id}/export/csv")
async def export_csv(job_id: str):
    results = get_results(job_id)
    if not results:
        raise HTTPException(status_code=404, detail="No results found")
    df = pd.DataFrame(results)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=results_{job_id[:8]}.csv"}
    )

@app.get("/api/jobs/{job_id}/export/xlsx")
async def export_xlsx(job_id: str):
    results = get_results(job_id)
    if not results:
        raise HTTPException(status_code=404, detail="No results found")
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

@app.post("/api/jobs/{job_id}/export/sheets")
async def export_sheets(job_id: str, background_tasks: BackgroundTasks):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    results = get_results(job_id)
    if not results:
        raise HTTPException(status_code=404, detail="No results found")

    def do_export():
        url = export_job_to_sheets(job_id, job.get("filename", "results"), results)
        if url:
            from services.credits import _save_setting
            _save_setting(f"sheet_url_{job_id}", url)

    background_tasks.add_task(do_export)
    return {"status": "exporting"}

@app.get("/api/jobs/{job_id}/export/sheets/url")
async def get_sheets_url(job_id: str):
    from services.credits import _get_setting
    url = _get_setting(f"sheet_url_{job_id}")
    return {"url": url}


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
