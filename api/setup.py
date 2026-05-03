"""
Setup API — manage technology catalog, users, cache settings, job history.
"""
import logging
from typing import Literal
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/setup")


# ── Technology Catalog ────────────────────────────────────────────────────────

@router.get("/catalog")
async def get_catalog():
    from services.technology_catalog import get_catalog
    return get_catalog()


@router.post("/catalog/sync")
async def sync_catalog_from_sheets():
    try:
        from services.technology_catalog import sync_catalog
        counts = sync_catalog()
        return {"ok": True, "counts": counts}
    except Exception as e:
        logger.error(f"Catalog sync error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class CatalogEntry(BaseModel):
    sheet: Literal["cms", "ems", "osearch"]
    technology: str
    group_name: str = ""


@router.post("/catalog/add")
async def add_catalog_entry(entry: CatalogEntry):
    from core.bigquery import client, table_ref
    from services.technology_catalog import ensure_catalog_table
    ensure_catalog_table()
    bq = client()
    errors = bq.insert_rows_json(table_ref("technology_catalog"), [{
        "sheet": entry.sheet,
        "technology": entry.technology.strip(),
        "group_name": entry.group_name.strip(),
    }])
    if errors:
        raise HTTPException(status_code=500, detail=str(errors))
    return {"ok": True}


@router.delete("/catalog")
async def remove_catalog_entry(sheet: str, technology: str):
    from core.bigquery import client, table_ref
    from google.cloud import bigquery
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('technology_catalog')}` WHERE sheet = @s AND technology = @t",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("s", "STRING", sheet),
            bigquery.ScalarQueryParameter("t", "STRING", technology),
        ])
    ).result()
    return {"ok": True}


# ── Settings (cache TTL etc.) ─────────────────────────────────────────────────

@router.get("/settings")
async def get_settings():
    from core.bigquery import get_setting
    return {
        "cache_ttl_days": int(get_setting("cache_ttl_days", "90")),
    }


class SettingsUpdate(BaseModel):
    cache_ttl_days: int


@router.post("/settings")
async def update_settings(data: SettingsUpdate):
    if not (1 <= data.cache_ttl_days <= 3650):
        raise HTTPException(status_code=400, detail="TTL must be 1–3650 days")
    from core.bigquery import set_setting
    set_setting("cache_ttl_days", str(data.cache_ttl_days))
    return {"ok": True}


# ── Users ─────────────────────────────────────────────────────────────────────

VALID_PERMISSIONS = {"read", "add", "download", "admin"}


@router.get("/users")
async def list_users():
    from core.bigquery import get_users
    return {"users": get_users()}


class UserCreate(BaseModel):
    username: str
    password: str
    permissions: Literal["read", "add", "download", "admin"]


@router.post("/users")
async def create_user(user: UserCreate):
    if not user.username.strip():
        raise HTTPException(status_code=400, detail="Username required")
    if not user.password.strip():
        raise HTTPException(status_code=400, detail="Password required")
    from core.bigquery import add_user
    add_user(user.username.strip(), user.password, user.permissions)
    return {"ok": True}


@router.delete("/users/{username}")
async def delete_user(username: str):
    from core.bigquery import remove_user
    remove_user(username)
    return {"ok": True}


# ── Job History ───────────────────────────────────────────────────────────────

@router.get("/jobs/count")
async def count_clearable_jobs():
    from core.bigquery import client, table_ref
    bq = client()
    rows = list(bq.query(
        f"SELECT COUNT(*) as cnt FROM `{table_ref('analysis_jobs')}` WHERE status NOT IN ('running','pending')"
    ).result())
    return {"count": rows[0]["cnt"] if rows else 0}


@router.post("/jobs/clear")
async def clear_job_history():
    from core.bigquery import client, table_ref
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('analysis_jobs')}` WHERE status NOT IN ('running','pending')"
    ).result()
    return {"ok": True}
