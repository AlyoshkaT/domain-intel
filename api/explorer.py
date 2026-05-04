"""
BQ Explorer API — queries domain_profiles directly in BQ.
Search results cached in memory (5 min TTL) to speed up repeat queries.
"""
import hashlib
import json
import time
from fastapi import APIRouter, BackgroundTasks
from google.cloud import bigquery as bq

from core.bigquery import client, table_ref

router = APIRouter(prefix="/api/explore")
PROFILES_TABLE = "domain_profiles"
FILTERABLE_FIELDS = [
    "domain","cms_list","osearch","ems_list",
    "ai_category","ai_is_ecommerce","sw_category","sw_primary_region",
]

# Cache for field values (lightweight)
_values_cache: dict = {}
_values_cache_ts: float = 0
CACHE_TTL = 300

# Cache for search results (larger, 5-min TTL, max 30 entries)
_search_cache: dict[str, dict] = {}
_SEARCH_CACHE_TTL = 300
_SEARCH_CACHE_MAX = 30


def _make_cache_key(body: dict) -> str:
    """Stable hash of request body for cache keying."""
    serialized = json.dumps(body, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()


def _search_cache_get(key: str):
    entry = _search_cache.get(key)
    if entry and (time.time() - entry["ts"]) < _SEARCH_CACHE_TTL:
        return entry["data"]
    return None


def _search_cache_set(key: str, data: dict):
    # Evict oldest entries if over limit
    if len(_search_cache) >= _SEARCH_CACHE_MAX:
        oldest = min(_search_cache, key=lambda k: _search_cache[k]["ts"])
        del _search_cache[oldest]
    _search_cache[key] = {"data": data, "ts": time.time()}


def _build_where(filters: dict) -> tuple[str, list]:
    """Build WHERE clause and params from filter dict."""
    conditions = []
    params = []
    idx = [0]

    def p(name, type_, val):
        idx[0] += 1
        pname = f"p{idx[0]}_{name}"
        params.append(bq.ScalarQueryParameter(pname, type_, val))
        return f"@{pname}"

    for field, flt in filters.items():
        ftype = flt.get("type")
        if ftype == "contains" and flt.get("value"):
            val = flt["value"]
            conditions.append(f"LOWER({field}) LIKE LOWER({p(field, 'STRING', f'%{val}%')})")
        elif ftype == "not_contains" and flt.get("value"):
            val = flt["value"]
            conditions.append(f"(LOWER({field}) NOT LIKE LOWER({p(field, 'STRING', f'%{val}%')}) OR {field} IS NULL)")
        elif ftype == "empty":
            conditions.append(f"({field} IS NULL OR {field} = '')")
        elif ftype == "not_empty":
            conditions.append(f"({field} IS NOT NULL AND {field} != '')")
        elif ftype == "in" and flt.get("values"):
            phs = [p(field, "STRING", str(v)) for v in flt["values"]]
            conditions.append(f"{field} IN ({','.join(phs)})")
        elif ftype == "not_in" and flt.get("values"):
            phs = [p(field, "STRING", str(v)) for v in flt["values"]]
            conditions.append(f"({field} NOT IN ({','.join(phs)}) OR {field} IS NULL)")
        elif ftype == "gt" and flt.get("value") is not None:
            conditions.append(f"{field} > {p(field,'FLOAT64',float(flt['value']))}")
        elif ftype == "lt" and flt.get("value") is not None:
            conditions.append(f"{field} < {p(field,'FLOAT64',float(flt['value']))}")
        elif ftype == "between" and flt.get("min") is not None and flt.get("max") is not None:
            conditions.append(f"{field} BETWEEN {p(field+'_min','FLOAT64',float(flt['min']))} AND {p(field+'_max','FLOAT64',float(flt['max']))}")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where, params


@router.get("/stats")
async def explore_stats():
    try:
        bq_client = client()
        rows = list(bq_client.query(f"""
            SELECT
                COUNT(*) as total_domains,
                COUNTIF(sw_visits IS NOT NULL AND sw_visits > 0) as with_traffic,
                COUNTIF(cms_list IS NOT NULL AND cms_list != '') as with_cms,
                COUNTIF(ems_list IS NOT NULL AND ems_list != '') as with_ems,
                COUNTIF(ai_category IS NOT NULL AND ai_category != '') as with_ai
            FROM `{table_ref(PROFILES_TABLE)}`
        """).result())
        stats = dict(rows[0]) if rows else {}

        from core.bigquery import BQ_JOBS_TABLE
        cnt = list(bq_client.query(f"SELECT COUNT(*) as c FROM `{table_ref(BQ_JOBS_TABLE)}`").result())
        stats["total_jobs"] = cnt[0]["c"] if cnt else 0
        return stats
    except Exception as e:
        return {"error": str(e)}


@router.get("/values/{field}")
async def get_field_values(field: str, q: str = ""):
    if field not in FILTERABLE_FIELDS:
        return {"values": [], "error": "Field not allowed"}

    cache_key = f"{field}:{q}"
    global _values_cache_ts
    if cache_key in _values_cache and (time.time() - _values_cache_ts) < CACHE_TTL:
        return {"values": _values_cache[cache_key]}

    try:
        bq_client = client()
        where = f"AND LOWER({field}) LIKE LOWER(@q)" if q else ""
        params = [bq.ScalarQueryParameter("q", "STRING", f"%{q}%")] if q else []
        rows = list(bq_client.query(
            f"""SELECT {field} as value, COUNT(*) as cnt
                FROM `{table_ref(PROFILES_TABLE)}`
                WHERE {field} IS NOT NULL AND {field} != '' {where}
                GROUP BY {field} ORDER BY cnt DESC LIMIT 300""",
            job_config=bq.QueryJobConfig(query_parameters=params)
        ).result())
        values = [{"value": r["value"], "count": r["cnt"]} for r in rows]
        _values_cache[cache_key] = values
        _values_cache_ts = time.time()
        return {"values": values}
    except Exception as e:
        return {"values": [], "error": str(e)}


@router.post("/search")
async def explore_search(body: dict):
    filters = body.get("filters", {})
    limit = min(int(body.get("limit", 100)), 200000)
    offset = int(body.get("offset", 0))

    # Check cache first
    ck = _make_cache_key({"filters": filters, "limit": limit, "offset": offset})
    cached = _search_cache_get(ck)
    if cached is not None:
        return cached

    where, params = _build_where(filters)
    job_cfg = bq.QueryJobConfig(query_parameters=params)

    try:
        bq_client = client()

        # Count
        cnt_rows = list(bq_client.query(
            f"SELECT COUNT(*) as total FROM `{table_ref(PROFILES_TABLE)}` {where}",
            job_config=job_cfg
        ).result())
        total = cnt_rows[0]["total"] if cnt_rows else 0

        # Data
        data_rows = list(bq_client.query(
            f"""SELECT domain, sw_visits, cms_list, osearch, osearch_group,
                ems_list, ai_category, ai_is_ecommerce, ai_industry,
                bw_vertical, sw_category, sw_subcategory,
                sw_description, sw_title, company_name,
                sw_primary_region, sw_primary_region_pct
                FROM `{table_ref(PROFILES_TABLE)}` {where}
                ORDER BY sw_visits DESC NULLS LAST
                LIMIT {limit} OFFSET {offset}""",
            job_config=job_cfg
        ).result())

        result = {"total": total, "results": [dict(r) for r in data_rows]}
        _search_cache_set(ck, result)
        return result
    except Exception as e:
        return {"total": 0, "results": [], "error": str(e)}


@router.post("/refresh")
async def refresh_profiles(background_tasks: BackgroundTasks):
    def do_sync():
        from services.domain_profiles import sync_domain_profiles
        sync_domain_profiles()

    background_tasks.add_task(do_sync)
    return {"status": "sync_started"}


@router.get("/sync/status")
async def sync_status():
    from services.domain_profiles import get_sync_status
    return get_sync_status()


# ─── Sheets/XLSX export ───────────────────────────────────────────────────────
_explore_sheet_url: str | None = None
_explore_sheet_error: str | None = None

@router.post("/export/xlsx")
async def explore_export_xlsx(body: dict):
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")
    df = pd.DataFrame(results)
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Explorer")
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=explorer_results.xlsx"}
    )

@router.post("/export/sheets")
async def explore_export_sheets(body: dict, background_tasks: BackgroundTasks):
    global _explore_sheet_url, _explore_sheet_error
    _explore_sheet_url = None
    _explore_sheet_error = None
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")
    label = body.get("label", f"{len(results)} domains")
    def do_export():
        global _explore_sheet_url, _explore_sheet_error
        try:
            from services.sheets_export import export_explorer_to_sheets
            url = export_explorer_to_sheets(label, results)
            if url:
                _explore_sheet_url = url
            else:
                _explore_sheet_error = "Export failed — check GOOGLE_SHEETS_CREDENTIALS_JSON"
        except Exception as e:
            _explore_sheet_error = str(e)
    background_tasks.add_task(do_export)
    return {"status": "exporting"}

@router.get("/export/sheets/url")
async def explore_sheets_url():
    return {"url": _explore_sheet_url, "error": _explore_sheet_error}
