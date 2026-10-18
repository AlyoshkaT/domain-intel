"""
BQ Explorer API — queries domain_profiles directly in BQ.
Search results cached in memory (5 min TTL) to speed up repeat queries.
"""
import hashlib
import json
import time
from fastapi import APIRouter, BackgroundTasks, Request
from google.cloud import bigquery as bq

from core.bigquery import client, table_ref, _bq_touch, _bq_op, _bq_qcfg
from api.auth import require_permission

router = APIRouter(prefix="/api/explore", dependencies=[require_permission("explorer")])
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

# Cache for full profiles list (columnar format to minimise memory)
# {"columns": [...], "rows": [[...], ...]}
_profiles_cache: dict = {}
_profiles_cache_ts: float = 0
_PROFILES_CACHE_TTL = 1800  # 30 min — BQ fetch of 100K rows takes ~30-60s, cache aggressively

# Columns loaded for in-memory filtering. sw_description excluded here because:
# • Explorer doesn't filter/search by description
# • At 100K rows × 300 chars avg it adds ~30MB to every load
# • Fetched separately on demand via /api/explore/domain/<domain>
PROFILE_COLUMNS = [
    "domain", "sw_visits", "cms_list", "osearch", "osearch_group",
    "ems_list", "ai_category", "ai_is_ecommerce", "ai_industry",
    "sw_category", "sw_subcategory", "sw_title",
    "company_name", "sw_primary_region", "sw_primary_region_pct",
    "redirect_from",
]

# Full column list including description — used only in detail/export endpoints
PROFILE_COLUMNS_FULL = PROFILE_COLUMNS + ["sw_description"]


def _region_expr(alias: str = "") -> str:
    """Effective region shown to users = SimilarWeb if present, else the derived
    fallback (ccTLD / AI / Whois). The DB keeps sw_primary_region and derived_region
    SEPARATE; this COALESCE unifies them only at read time for Explorer/dashboard/export."""
    p = f"{alias}." if alias else ""
    return f"COALESCE(NULLIF({p}sw_primary_region, ''), {p}derived_region)"


# Columns a client may filter on. Filter KEYS are interpolated into SQL in
# _build_where (BigQuery has no param for a column name), so they MUST be validated
# against this whitelist — otherwise a crafted filter key is SQL injection.
_ALLOWED_FILTER_FIELDS = {
    "domain", "sw_visits", "sw_category", "sw_subcategory", "sw_description",
    "sw_title", "sw_primary_region", "sw_primary_region_pct", "company_name",
    "cms_list", "osearch", "osearch_group", "ems_list", "bw_vertical",
    "ai_category", "ai_is_ecommerce", "ai_industry",
    "derived_region", "derived_region_source",
}


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
        # Reject unknown field names (SQL-injection guard — the name is interpolated).
        if field not in _ALLOWED_FILTER_FIELDS:
            continue
        ftype = flt.get("type")
        # Region filters operate on the EFFECTIVE value (SW → derived fallback), so
        # the "empty region" slice means truly-still-unknown after the cascade.
        col = _region_expr() if field == "sw_primary_region" else field
        if ftype == "contains" and flt.get("value"):
            val = flt["value"]
            conditions.append(f"LOWER({col}) LIKE LOWER({p(field, 'STRING', f'%{val}%')})")
        elif ftype == "not_contains" and flt.get("value"):
            val = flt["value"]
            conditions.append(f"(LOWER({col}) NOT LIKE LOWER({p(field, 'STRING', f'%{val}%')}) OR {col} IS NULL)")
        elif ftype == "empty":
            conditions.append(f"({col} IS NULL OR {col} = '')")
        elif ftype == "not_empty":
            conditions.append(f"({col} IS NOT NULL AND {col} != '')")
        elif ftype == "in" and flt.get("values"):
            phs = [p(field, "STRING", str(v)) for v in flt["values"]]
            conditions.append(f"{col} IN ({','.join(phs)})")
        elif ftype == "not_in" and flt.get("values"):
            phs = [p(field, "STRING", str(v)) for v in flt["values"]]
            conditions.append(f"({col} NOT IN ({','.join(phs)}) OR {col} IS NULL)")
        elif ftype == "gt" and flt.get("value") is not None:
            conditions.append(f"{col} > {p(field,'FLOAT64',float(flt['value']))}")
        elif ftype == "lt" and flt.get("value") is not None:
            conditions.append(f"{col} < {p(field,'FLOAT64',float(flt['value']))}")
        elif ftype == "between" and flt.get("min") is not None and flt.get("max") is not None:
            conditions.append(f"{col} BETWEEN {p(field+'_min','FLOAT64',float(flt['min']))} AND {p(field+'_max','FLOAT64',float(flt['max']))}")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where, params


@router.get("/profiles")
def get_all_profiles():
    """Return ALL profiles in columnar format for client-side filtering.
    Columnar = one array per row (no repeated keys) → ~3x less memory/JSON."""
    global _profiles_cache, _profiles_cache_ts
    now = time.time()
    if _profiles_cache and (now - _profiles_cache_ts) < _PROFILES_CACHE_TTL:
        return _profiles_cache
    try:
        bq_client = client()
        cols_sql = ", ".join(
            (f"{_region_expr('t')} AS sw_primary_region" if c == "sw_primary_region" else f"t.{c}")
            for c in PROFILE_COLUMNS)
        # sw_fetched / bw_fetched — freshness dates from parsed tables (UI-only, not exported)
        all_columns = PROFILE_COLUMNS + ["sw_fetched", "bw_fetched"]
        # _bq_op keeps the LED lit for the entire duration of this block (can be 5+ min)
        with _bq_op("priv_r"):
            # Use large page_size to reduce HTTP round-trips (default ~10K → 50K per request)
            job = bq_client.query(f"""
                SELECT {cols_sql},
                       FORMAT_TIMESTAMP('%Y-%m-%d', sw.fa) AS sw_fetched,
                       FORMAT_TIMESTAMP('%Y-%m-%d', bw.fa) AS bw_fetched
                FROM `{table_ref(PROFILES_TABLE)}` t
                LEFT JOIN (SELECT domain, MAX(fetched_at) AS fa FROM `{table_ref("sw_parsed")}` GROUP BY domain) sw
                  ON sw.domain = t.domain
                LEFT JOIN (SELECT domain, MAX(fetched_at) AS fa FROM `{table_ref("bw_parsed")}` GROUP BY domain) bw
                  ON bw.domain = t.domain
                ORDER BY t.sw_visits DESC NULLS LAST
            """, job_config=_bq_qcfg())
            result_iter = job.result(page_size=50000)

            # Build compact columnar structure: list of lists, no repeated keys.
            # time.sleep(0) every 1000 rows releases the GIL so the asyncio event
            # loop thread can serve /api/bq_activity polls and keep the LED lit.
            rows = []
            for i, r in enumerate(result_iter):
                row = []
                for col in all_columns:
                    v = r[col]
                    if v is None:
                        row.append(None)
                    elif hasattr(v, 'is_integer'):   # BQ Decimal/float
                        row.append(float(v))
                    else:
                        row.append(v)
                rows.append(row)
                if i % 1000 == 0:
                    time.sleep(0)  # yield GIL → event loop can respond to bq_activity polls

        _profiles_cache = {"columns": all_columns, "rows": rows, "total": len(rows)}
        _profiles_cache_ts = now
        return _profiles_cache
    except Exception as e:
        return {"columns": PROFILE_COLUMNS, "rows": [], "total": 0, "error": str(e)}


def invalidate_profiles_cache():
    """Call this after domain_profiles sync to force fresh load."""
    global _profiles_cache_ts
    _profiles_cache_ts = 0


@router.get("/domain/{domain}")
def get_domain_detail(domain: str):
    """Fetch full profile for a single domain including sw_description."""
    try:
        _bq_touch("priv_r")
        bq_client = client()
        cols_sql = ", ".join(
            (f"{_region_expr()} AS sw_primary_region" if c == "sw_primary_region" else c)
            for c in PROFILE_COLUMNS_FULL)
        rows = list(bq_client.query(
            f"SELECT {cols_sql} FROM `{table_ref(PROFILES_TABLE)}` WHERE domain = @d LIMIT 1",
            job_config=bq.QueryJobConfig(query_parameters=[bq.ScalarQueryParameter("d", "STRING", domain)])
        ).result())
        if not rows:
            return {"error": "not found"}
        r = rows[0]
        return {col: (float(r[col]) if hasattr(r[col], 'is_integer') else r[col])
                for col in PROFILE_COLUMNS_FULL}
    except Exception as e:
        return {"error": str(e)}


@router.get("/stats")
def explore_stats():
    try:
        _bq_touch("priv_r")
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
def get_field_values(field: str, q: str = ""):
    if field not in FILTERABLE_FIELDS:
        return {"values": [], "error": "Field not allowed"}

    cache_key = f"{field}:{q}"
    global _values_cache_ts
    if cache_key in _values_cache and (time.time() - _values_cache_ts) < CACHE_TTL:
        return {"values": _values_cache[cache_key]}

    try:
        _bq_touch("priv_r")
        bq_client = client()
        # Region values reflect the effective (SW → derived) column.
        col = _region_expr() if field == "sw_primary_region" else field
        where = f"AND LOWER({col}) LIKE LOWER(@q)" if q else ""
        params = [bq.ScalarQueryParameter("q", "STRING", f"%{q}%")] if q else []
        rows = list(bq_client.query(
            f"""SELECT {col} as value, COUNT(*) as cnt
                FROM `{table_ref(PROFILES_TABLE)}`
                WHERE {col} IS NOT NULL AND {col} != '' {where}
                GROUP BY {col} ORDER BY cnt DESC LIMIT 300""",
            job_config=bq.QueryJobConfig(query_parameters=params)
        ).result())
        values = [{"value": r["value"], "count": r["cnt"]} for r in rows]
        _values_cache[cache_key] = values
        _values_cache_ts = time.time()
        return {"values": values}
    except Exception as e:
        return {"values": [], "error": str(e)}


@router.post("/search")
def explore_search(body: dict):
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
        _bq_touch("priv_r")
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
                {_region_expr()} AS sw_primary_region, sw_primary_region_pct,
                redirect_from
                FROM `{table_ref(PROFILES_TABLE)}` {where}
                ORDER BY sw_visits DESC NULLS LAST
                LIMIT {limit} OFFSET {offset}""",
            job_config=job_cfg
        ).result())
        _bq_touch("priv_r")

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
        invalidate_profiles_cache()  # force fresh load after sync

    background_tasks.add_task(do_sync)
    return {"status": "sync_started"}


@router.get("/sync/status")
def sync_status():
    from services.domain_profiles import get_sync_status
    return get_sync_status()


@router.post("/region_whois/sweep", dependencies=[require_permission("admin")])
async def region_whois_sweep(body: dict | None = None):
    """Region fallback №3 (manual): look up registrant country via RDAP for a bounded
    batch of domains still missing a region after ccTLD + AI. Gated by REGION_WHOIS_ENABLED.
    Run a profile sync afterwards to surface the results."""
    import asyncio
    from services.region_whois import backfill_whois_regions
    limit = int((body or {}).get("limit") or 0) or None
    with _bq_op("priv_r"):
        return await asyncio.to_thread(backfill_whois_regions, limit)


# ─── Raw technology search (BuiltWith) ────────────────────────────────────────
@router.get("/tech_search")
def tech_search(q: str, limit: int = 50):
    """Autocomplete over the tech dictionary: full tech names containing `q` + counts."""
    from services.tech_index import search_tech
    with _bq_op("priv_r"):
        return {"results": search_tech(q, limit)}


@router.post("/tech_domains")
def tech_domains(body: dict):
    """Return domains that have ANY of the selected exact tech names."""
    from services.tech_index import domains_for_techs
    techs = body.get("techs", []) if isinstance(body, dict) else []
    with _bq_op("priv_r"):
        domains = domains_for_techs(techs)
    return {"domains": domains, "count": len(domains)}


@router.post("/tech_rebuild", dependencies=[require_permission("admin")])
async def tech_rebuild():
    """Full rebuild of the technology index from bw_parsed (admin / manual)."""
    import asyncio
    from services.tech_index import rebuild_tech_index
    with _bq_op("priv_w"):
        result = await asyncio.to_thread(rebuild_tech_index)
    return result


@router.post("/tech_descriptions/refresh", dependencies=[require_permission("admin")])
async def tech_descriptions_refresh():
    """Rebuild the per-technology description+link dictionary from corp (admin / manual)."""
    import asyncio
    from services.tech_index import refresh_tech_descriptions
    with _bq_op("corp_r"):
        result = await asyncio.to_thread(refresh_tech_descriptions)
    return result


# ─── Sheets/XLSX export ───────────────────────────────────────────────────────
_explore_sheet_url: str | None = None
_explore_sheet_error: str | None = None

@router.post("/export/xlsx", dependencies=[require_permission("download")])
async def explore_export_xlsx(request: Request, body: dict):
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "explore_export_xlsx", {"row_count": len(results)})
    except Exception:
        pass
    df = pd.DataFrame(results)
    # Insert Traffic_Rank right after the Traffic (sw_visits) column
    from services.sheets_export import traffic_rank
    ranks = [traffic_rank(r.get("sw_visits")) for r in results]
    if "sw_visits" in df.columns:
        df.insert(df.columns.get_loc("sw_visits") + 1, "traffic_rank", ranks)
    else:
        df["traffic_rank"] = ranks
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Explorer")
    stream.seek(0)
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=explorer_results.xlsx"}
    )

@router.post("/export/sheets", dependencies=[require_permission("sheets")])
async def explore_export_sheets(request: Request, body: dict, background_tasks: BackgroundTasks):
    global _explore_sheet_url, _explore_sheet_error
    _explore_sheet_url = None
    _explore_sheet_error = None
    results = body.get("results", [])
    if not results:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="No results")
    label = body.get("label", f"{len(results)} domains")
    analytics = bool(body.get("analytics", False))
    username = getattr(request.state, "username", "unknown")
    try:
        from core.bigquery import log_activity
        log_activity(username, "explore_export_sheets", {"row_count": len(results), "label": label, "analytics": analytics})
    except Exception:
        pass
    # Look up user's personal Drive folder
    folder_id = ""
    try:
        from core.bigquery import get_users
        users = {u["username"]: u for u in get_users()}
        folder_id = users.get(username, {}).get("google_folder") or ""
    except Exception:
        pass

    def do_export():
        global _explore_sheet_url, _explore_sheet_error
        try:
            from services.sheets_export import export_explorer_to_sheets
            url = export_explorer_to_sheets(label, results, folder_id=folder_id, analytics=analytics)
            _explore_sheet_url = url
        except Exception as e:
            _explore_sheet_error = str(e)
    background_tasks.add_task(do_export)
    return {"status": "exporting"}

@router.get("/export/sheets/url")
def explore_sheets_url():
    return {"url": _explore_sheet_url, "error": _explore_sheet_error}
