"""
Domain Profiles Sync — memory-efficient streaming version.
Parses JSON immediately on load (never stores raw blobs), writes profiles
directly to temp file (never accumulates full rows list in RAM).
"""
import json
import logging
import tempfile
import os
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

from core.bigquery import client, corp_client, table_ref
from config.settings import CORP_PROJECT_ID, CORP_DATASET, GCP_PROJECT_ID, BIGQUERY_DATASET

# How many domains to keep in each parsed-data dict at any moment.
# Lower = less peak RAM, but the dicts are already small (parsed, not raw JSON).
# Keeping full dicts is fine after the raw-JSON parsing fix.

logger = logging.getLogger(__name__)

PROFILES_TABLE = "domain_profiles"
PROFILES_TEMP  = "domain_profiles_tmp"

PROFILES_SCHEMA = [
    bigquery.SchemaField("domain",                "STRING"),
    bigquery.SchemaField("updated_at",            "TIMESTAMP"),
    bigquery.SchemaField("sw_visits",             "FLOAT"),
    bigquery.SchemaField("sw_category",           "STRING"),
    bigquery.SchemaField("sw_subcategory",        "STRING"),
    bigquery.SchemaField("sw_description",        "STRING"),
    bigquery.SchemaField("sw_title",              "STRING"),
    bigquery.SchemaField("sw_primary_region",     "STRING"),
    bigquery.SchemaField("sw_primary_region_pct", "FLOAT"),
    bigquery.SchemaField("company_name",          "STRING"),
    bigquery.SchemaField("cms_list",              "STRING"),
    bigquery.SchemaField("osearch",               "STRING"),
    bigquery.SchemaField("osearch_group",         "STRING"),
    bigquery.SchemaField("ems_list",              "STRING"),
    bigquery.SchemaField("bw_vertical",           "STRING"),
    bigquery.SchemaField("ai_category",           "STRING"),
    bigquery.SchemaField("ai_is_ecommerce",       "STRING"),
    bigquery.SchemaField("ai_industry",           "STRING"),
]

_sync_status = {
    "running": False,
    "last_sync": None,
    "total_domains": 0,
    "error": None,
    "progress": "",
}


def normalize_domain(domain: str) -> str:
    """Normalize domain: lowercase, remove www., strip spaces."""
    if not domain:
        return ""
    d = domain.strip().lower()
    d = d.removeprefix("http://").removeprefix("https://")
    d = d.split("/")[0].split("?")[0].split("#")[0].split(":")[0]
    if d.startswith("www."):
        d = d[4:]
    return d.strip(".")


def ensure_profiles_table():
    bq = client()
    table_obj = bigquery.Table(
        f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{PROFILES_TABLE}",
        schema=PROFILES_SCHEMA
    )
    try:
        bq.get_table(table_obj)
    except Exception:
        bq.create_table(table_obj)
        logger.info(f"Created table {PROFILES_TABLE}")


def _safe_json(val) -> Optional[dict]:
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None


def _parse_sw(data: dict | None) -> dict:
    if not data:
        return {}
    try:
        visits = 0
        eng = data.get("Engagments", {})
        if eng.get("Visits"):
            visits = float(eng["Visits"])
        else:
            monthly = data.get("EstimatedMonthlyVisits", {})
            if monthly:
                visits = float(list(monthly.values())[-1])
        cat_rank = data.get("CategoryRank", {})
        category = cat_rank.get("Category") or data.get("Category") or ""
        sw_cat = category.split("/")[0] if "/" in category else category
        sw_sub = category.split("/")[1] if "/" in category else ""
        top = data.get("TopCountryShares") or []
        region = top[0].get("CountryCode", "") if top else ""
        region_pct = round(top[0].get("Value", 0) * 100, 1) if top else None
        return {
            "sw_visits": visits,
            "sw_category": sw_cat,
            "sw_subcategory": sw_sub,
            "sw_description": (data.get("Description") or "")[:500],
            "sw_title": data.get("Title") or data.get("SiteName") or "",
            "sw_primary_region": region,
            "sw_primary_region_pct": region_pct,
            "company_name": data.get("Title") or data.get("SiteName") or "",
        }
    except Exception:
        return {}


def _parse_bw(data: dict | None, catalog: dict) -> dict:
    if not data:
        return {}
    try:
        from services.technology_catalog import match_technologies
        matched = match_technologies(data, catalog)
        vertical = ""
        try:
            results = data.get("Results", [])
            if results:
                vertical = results[0].get("Result", {}).get("Vertical", "") or ""
        except Exception:
            pass
        return {
            "cms_list":      matched.get("cms_list", ""),
            "osearch":       matched.get("osearch", ""),
            "osearch_group": matched.get("osearch_group", ""),
            "ems_list":      matched.get("ems_list", ""),
            "bw_vertical":   vertical,
        }
    except Exception:
        return {}


_PROFILE_SCHEMA = {"domain","updated_at","sw_visits","sw_category","sw_subcategory",
                   "sw_description","sw_title","sw_primary_region","sw_primary_region_pct",
                   "company_name","cms_list","osearch","osearch_group","ems_list",
                   "bw_vertical","ai_category","ai_is_ecommerce","ai_industry"}


def _build_profile(domain: str, sw_raw, bw_raw,
                   ai_rec: dict, catalog: dict, updated_at: str) -> dict:
    """Legacy helper — accepts raw JSON. Use _build_profile_parsed for efficiency."""
    sw = _parse_sw(_safe_json(sw_raw))
    bw = _parse_bw(_safe_json(bw_raw), catalog)
    return _assemble_profile(domain, sw, bw, ai_rec, updated_at)


def _build_profile_parsed(domain: str, sw: dict, bw: dict,
                           ai_rec: dict, updated_at: str) -> dict:
    """Build profile from already-parsed dicts (no JSON parsing, memory efficient)."""
    return _assemble_profile(domain, sw, bw, ai_rec, updated_at)


def _assemble_profile(domain: str, sw: dict, bw: dict,
                      ai_rec: dict, updated_at: str) -> dict:
    return {k: v for k, v in {
        "domain":                domain,
        "updated_at":            updated_at,
        "sw_visits":             sw.get("sw_visits"),
        "sw_category":           sw.get("sw_category", ""),
        "sw_subcategory":        sw.get("sw_subcategory", ""),
        "sw_description":        sw.get("sw_description", ""),
        "sw_title":              sw.get("sw_title", ""),
        "sw_primary_region":     sw.get("sw_primary_region", ""),
        "sw_primary_region_pct": sw.get("sw_primary_region_pct"),
        "company_name":          sw.get("company_name", ""),
        "cms_list":              bw.get("cms_list", ""),
        "osearch":               bw.get("osearch", ""),
        "osearch_group":         bw.get("osearch_group", ""),
        "ems_list":              bw.get("ems_list", ""),
        "bw_vertical":           bw.get("bw_vertical", ""),
        "ai_category":           ai_rec.get("ai_category", "") or "",
        "ai_is_ecommerce":       ai_rec.get("ai_is_ecommerce", "") or "",
        "ai_industry":           ai_rec.get("ai_industry", "") or "",
    }.items() if k in _PROFILE_SCHEMA}


def sync_domain_profiles() -> dict:
    global _sync_status
    _sync_status["running"] = True
    _sync_status["error"] = None
    _sync_status["progress"] = "Починаємо..."
    t0 = time.time()

    try:
        ensure_profiles_table()
        corp = corp_client()
        our  = client()

        sw_table      = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.similarweb_raw_data`"
        bw_table      = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.builtwith_raw_data`"
        corp_ai_table = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses`"

        # Load catalog once
        _sync_status["progress"] = "Завантажуємо каталог..."
        from services.technology_catalog import get_catalog
        catalog = get_catalog()

        # Fetch domain lists
        _sync_status["progress"] = "Отримуємо список доменів..."
        sw_domains      = set(normalize_domain(r["domain"]) for r in corp.query(f"SELECT DISTINCT domain FROM {sw_table}").result())
        bw_domains      = set(normalize_domain(r["domain"]) for r in corp.query(f"SELECT DISTINCT domain FROM {bw_table}").result())
        corp_ai_domains = set(normalize_domain(r["domain"]) for r in corp.query(f"SELECT DISTINCT domain FROM {corp_ai_table}").result())
        all_domains = {d for d in sw_domains | bw_domains | corp_ai_domains if d}
        logger.info(f"Unique normalized domains: {len(all_domains)}")

        # ── Memory-efficient data loading ──────────────────────────────────
        # IMPORTANT: we parse JSON immediately and store only the small parsed
        # dicts — raw response_json blobs (up to 100KB each for BW) are never
        # accumulated in RAM.  This reduces peak memory by ~100x.

        _sync_status["progress"] = f"Завантажуємо SW ({len(sw_domains):,})..."
        sw_parsed: dict[str, dict] = {}
        for r in corp.query(f"""
            SELECT domain, response_json FROM {sw_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(REGEXP_REPLACE(domain, r'^www\\.', ''))
                                       ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                sw_parsed[key] = _parse_sw(_safe_json(r["response_json"]))
        logger.info(f"SW parsed: {len(sw_parsed)}")

        _sync_status["progress"] = f"Завантажуємо BW ({len(bw_domains):,})..."
        bw_parsed: dict[str, dict] = {}
        for r in corp.query(f"""
            SELECT domain, response_json FROM {bw_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(REGEXP_REPLACE(domain, r'^www\\.', ''))
                                       ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                bw_parsed[key] = _parse_bw(_safe_json(r["response_json"]), catalog)
        logger.info(f"BW parsed: {len(bw_parsed)}")

        _sync_status["progress"] = "Завантажуємо Corp AI (claude_responses)..."
        ai_data: dict[str, dict] = {}
        for r in corp.query(f"""
            SELECT domain, response_json, fetched_at FROM {corp_ai_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if not key:
                continue
            rj = r["response_json"]
            data = rj if isinstance(rj, dict) else _safe_json(rj)
            if data:
                is_ecom = data.get("is_ecommerce")
                ai_data[key] = {
                    "ai_category":     data.get("category", ""),
                    "ai_is_ecommerce": "Так" if is_ecom is True or str(is_ecom).lower() in ("true", "1", "yes") else "Ні",
                    "ai_industry":     data.get("subcategory", ""),
                }
        logger.info(f"AI data total: {len(ai_data)} domains")

        # ── Stream profiles directly to temp file (no rows[] list in RAM) ──
        updated_at = datetime.now(timezone.utc).isoformat()
        domains_list = sorted(all_domains)
        total_count = len(domains_list)

        _sync_status["progress"] = f"Записуємо {total_count:,} профілів на диск..."
        tmp_file = None
        written = 0
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
                tmp_file = f.name
                for i, domain in enumerate(domains_list):
                    try:
                        profile = _build_profile_parsed(
                            domain,
                            sw_parsed.get(domain, {}),
                            bw_parsed.get(domain, {}),
                            ai_data.get(domain, {}),
                            updated_at,
                        )
                        f.write(json.dumps(profile, default=str) + "\n")
                        written += 1
                    except Exception as e:
                        logger.warning(f"Build error for {domain}: {e}")

                    if (i + 1) % 10000 == 0:
                        pct = int((i + 1) / total_count * 100)
                        _sync_status["progress"] = f"Записуємо: {i+1:,}/{total_count:,} ({pct}%)"

            logger.info(f"Written {written} profiles to {tmp_file} in {time.time()-t0:.0f}s")

            # Release parsed data dicts — free memory before BQ upload
            sw_parsed.clear()
            bw_parsed.clear()
            ai_data.clear()

            _sync_status["progress"] = f"Завантажуємо {written:,} профілів у BigQuery..."
            job_config = bigquery.LoadJobConfig(
                schema=PROFILES_SCHEMA,
                write_disposition="WRITE_TRUNCATE",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            with open(tmp_file, "rb") as f:
                load_job = our.load_table_from_file(
                    f, table_ref(PROFILES_TABLE), job_config=job_config
                )
            _sync_status["progress"] = "BigQuery завантажує файл..."
            load_job.result()
        finally:
            if tmp_file and os.path.exists(tmp_file):
                os.unlink(tmp_file)

        elapsed = time.time() - t0
        _sync_status["last_sync"] = updated_at
        _sync_status["total_domains"] = written
        _sync_status["progress"] = f"✅ {written:,} доменів за {elapsed/60:.1f} хв."
        logger.info(f"Sync done: {written} domains in {elapsed:.0f}s")
        return {"total": written, "status": "ok"}

    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        _sync_status["error"] = str(e)
        _sync_status["progress"] = f"❌ {str(e)[:100]}"
        return {"error": str(e)}
    finally:
        _sync_status["running"] = False


def get_sync_status() -> dict:
    return dict(_sync_status)
