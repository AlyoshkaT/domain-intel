"""
BQ Explorer API — reads from domain_profiles (fast, pre-built).
"""
import time
from fastapi import APIRouter, BackgroundTasks
from core.bigquery import client, table_ref, BQ_RESULTS_TABLE

router = APIRouter(prefix="/api/explore")

PROFILES_TABLE = "domain_profiles"
FILTERABLE_FIELDS = [
    "domain", "cms_list", "wcms_name", "osearch", "ems_list",
    "ai_category", "ai_is_ecommerce", "sw_category", "sw_primary_region",
]

_profiles_cache: list[dict] | None = None
_profiles_cache_ts: float = 0
CACHE_TTL = 300


async def _get_profiles() -> list[dict]:
    global _profiles_cache, _profiles_cache_ts
    if _profiles_cache is not None and (time.time() - _profiles_cache_ts) < CACHE_TTL:
        return _profiles_cache

    bq = client()
    try:
        rows = list(bq.query(f"""
            SELECT * FROM `{table_ref(PROFILES_TABLE)}`
            ORDER BY sw_visits DESC NULLS LAST
        """).result())
        profiles = [dict(r) for r in rows]
        if not profiles:
            raise Exception("domain_profiles is empty — run sync first")
    except Exception as e:
        # Fallback to analysis_results
        try:
            rows = list(bq.query(f"""
                SELECT domain, sw_visits, cms_list, wcms_name, osearch, osearch_group,
                       ems_list, ai_category, ai_is_ecommerce, ai_industry,
                       sw_category, sw_subcategory, sw_description, sw_title,
                       company_name, sw_primary_region, sw_primary_region_pct
                FROM `{table_ref(BQ_RESULTS_TABLE)}`
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY processed_at DESC) = 1
                ORDER BY sw_visits DESC NULLS LAST
            """).result())
            profiles = [dict(r) for r in rows]
        except Exception:
            return []

    _profiles_cache = profiles
    _profiles_cache_ts = time.time()
    return profiles


def _apply_filters(profiles: list[dict], filters: dict) -> list[dict]:
    result = []
    for p in profiles:
        match = True
        for field, flt in filters.items():
            ftype = flt.get("type")
            val = p.get(field)
            val_str = str(val).lower() if val is not None else ""

            if ftype == "in" and "values" in flt:
                if str(val) not in [str(v) for v in flt.get("values", [])]:
                    match = False; break
            elif ftype == "contains":
                if flt.get("value", "").lower() not in val_str:
                    match = False; break
            elif ftype == "not_contains":
                if flt.get("value", "").lower() in val_str:
                    match = False; break
            elif ftype == "empty":
                if val and str(val).strip():
                    match = False; break
            elif ftype == "not_empty":
                if not val or not str(val).strip():
                    match = False; break
            elif ftype == "in":
                if str(val) not in [str(v) for v in flt.get("values", [])]:
                    match = False; break
            elif ftype == "not_in":
                if str(val) in [str(v) for v in flt.get("values", [])]:
                    match = False; break
            elif ftype == "gt":
                try:
                    if float(val or 0) <= float(flt.get("value", 0)):
                        match = False; break
                except Exception:
                    match = False; break
            elif ftype == "lt":
                try:
                    if float(val or 0) >= float(flt.get("value", 0)):
                        match = False; break
                except Exception:
                    match = False; break
            elif ftype == "between":
                try:
                    v = float(val or 0)
                    if not (float(flt.get("min", 0)) <= v <= float(flt.get("max", 0))):
                        match = False; break
                except Exception:
                    match = False; break

        if match:
            result.append(p)
    return result


@router.get("/stats")
async def explore_stats():
    try:
        profiles = await _get_profiles()
        total        = len(profiles)
        with_cms     = sum(1 for p in profiles if p.get("cms_list"))
        with_traffic = sum(1 for p in profiles if (p.get("sw_visits") or 0) > 0)
        with_ai      = sum(1 for p in profiles if p.get("ai_category") and str(p.get("ai_category")).strip())
        with_ems     = sum(1 for p in profiles if p.get("ems_list"))
        try:
            from core.bigquery import BQ_JOBS_TABLE
            cnt = list(client().query(f"SELECT COUNT(*) as c FROM `{table_ref(BQ_JOBS_TABLE)}`").result())
            total_jobs = cnt[0]["c"] if cnt else 0
        except Exception:
            total_jobs = 0
        return {"total_domains": total, "total_jobs": total_jobs,
                "with_cms": with_cms, "with_traffic": with_traffic,
                "with_ai": with_ai, "with_ems": with_ems}
    except Exception as e:
        return {"error": str(e)}


@router.get("/values/{field}")
async def get_field_values(field: str, q: str = ""):
    if field not in FILTERABLE_FIELDS:
        return {"values": [], "error": "Field not allowed"}
    try:
        profiles = await _get_profiles()
        from collections import Counter
        counts = Counter()
        for p in profiles:
            val = p.get(field)
            if val and str(val).strip():
                counts[str(val)] += 1
        items = [{"value": v, "count": c} for v, c in counts.most_common(200000)]
        if q:
            items = [i for i in items if q.lower() in i["value"].lower()]
        return {"values": items}
    except Exception as e:
        return {"values": [], "error": str(e)}


@router.post("/search")
async def explore_search(body: dict):
    filters = body.get("filters", {})
    limit   = min(int(body.get("limit", 100)), 200000)
    offset  = int(body.get("offset", 0))
    try:
        profiles = await _get_profiles()
        filtered = _apply_filters(profiles, filters) if filters else profiles
        total = len(filtered)
        return {"total": total, "results": filtered[offset:offset + limit]}
    except Exception as e:
        return {"total": 0, "results": [], "error": str(e)}


@router.post("/refresh")
async def refresh_profiles(background_tasks: BackgroundTasks):
    global _profiles_cache
    _profiles_cache = None

    def do_sync():
        global _profiles_cache
        from services.domain_profiles import sync_domain_profiles
        sync_domain_profiles()
        _profiles_cache = None  # Force reload after sync

    background_tasks.add_task(do_sync)
    return {"status": "sync_started", "message": "Синхронізація запущена. Слідкуйте за статусом."}


@router.get("/sync/status")
async def sync_status():
    from services.domain_profiles import get_sync_status
    return get_sync_status()


# ─── Sheets export ────────────────────────────────────────────────────────────
_explore_sheet_url: str | None = None

@router.post("/export/sheets")
async def explore_export_sheets(body: dict, background_tasks: BackgroundTasks):
    global _explore_sheet_url
    _explore_sheet_url = None
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")

    def do_export():
        global _explore_sheet_url
        from services.sheets_export import export_job_to_sheets
        url = export_job_to_sheets("explorer", "Explorer results", results)
        if url:
            _explore_sheet_url = url

    background_tasks.add_task(do_export)
    return {"status": "exporting"}

@router.post("/export/xlsx")
async def explore_export_xlsx(body: dict):
    """Direct XLSX download of filtered results."""
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")
    df = pd.DataFrame(results)
    # Remove timezone from datetime columns
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                conv = pd.to_datetime(df[col], utc=True)
                df[col] = conv.dt.tz_localize(None)
            except Exception:
                pass
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=explorer_results.xlsx"}
    )


@router.post("/export/xlsx")
async def explore_export_xlsx(body: dict):
    """Export filtered results to XLSX."""
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")
    df = pd.DataFrame(results)
    # Strip timezone
    for col in df.columns:
        try:
            if hasattr(df[col], 'dt') and hasattr(df[col].dt, 'tz') and df[col].dt.tz:
                df[col] = df[col].dt.tz_localize(None)
        except Exception:
            pass
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Explorer")
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=explorer_results.xlsx"}
    )


@router.get("/export/sheets/url")
async def explore_sheets_url():
    return {"url": _explore_sheet_url}
