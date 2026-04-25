"""
Domain Profiles Sync — optimized, with domain normalization.
"""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

from core.bigquery import client, corp_client, table_ref, BQ_AI_CACHE
from config.settings import CORP_PROJECT_ID, CORP_DATASET, GCP_PROJECT_ID, BIGQUERY_DATASET

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


def _parse_wc(data: dict | None) -> dict:
    if not data:
        return {}
    try:
        results = data.get("results", [])
        if results:
            cms = next(
                (r for r in results if any(c in ["CMS","E-commerce"] for c in r.get("categories",[]))),
                results[0]
            )
            return {"wcms_name": cms.get("name", "")}
        return {"wcms_name": data.get("result", {}).get("name", "")}
    except Exception:
        return {}


def _build_profile(domain: str, sw_raw, bw_raw, wc_raw,
                   ai_rec: dict, catalog: dict, updated_at: str) -> dict:
    sw = _parse_sw(_safe_json(sw_raw))
    bw = _parse_bw(_safe_json(bw_raw), catalog)
    wc = _parse_wc(_safe_json(wc_raw))
    return {
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
        "wcms_name":             wc.get("wcms_name", ""),
        "ai_category":           ai_rec.get("ai_category", "") or "",
        "ai_is_ecommerce":       ai_rec.get("ai_is_ecommerce", "") or "",
        "ai_industry":           ai_rec.get("ai_industry", "") or "",
    }


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
        wc_table      = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.whatcms_raw_data`"
        ai_table      = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BQ_AI_CACHE}`"
        corp_ai_table = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses`"

        # Load catalog once
        _sync_status["progress"] = "Завантажуємо каталог..."
        from services.technology_catalog import get_catalog
        catalog = get_catalog()

        # Fetch domain lists
        _sync_status["progress"] = "Отримуємо список доменів..."
        sw_domains      = set(normalize_domain(r["domain"]) for r in corp.query(f"SELECT DISTINCT domain FROM {sw_table}").result())
        bw_domains      = set(normalize_domain(r["domain"]) for r in corp.query(f"SELECT DISTINCT domain FROM {bw_table}").result())
        ai_domains      = set(normalize_domain(r["domain"]) for r in our.query(f"SELECT DISTINCT domain FROM {ai_table}").result())
        corp_ai_domains = set(normalize_domain(r["domain"]) for r in corp.query(f"SELECT DISTINCT domain FROM {corp_ai_table}").result())
        all_domains = {d for d in sw_domains | bw_domains | ai_domains | corp_ai_domains if d}
        logger.info(f"Unique normalized domains: {len(all_domains)}")

        # Fetch latest data per domain (normalize keys)
        _sync_status["progress"] = f"Завантажуємо SW ({len(sw_domains):,})..."
        sw_data: dict[str, any] = {}
        for r in corp.query(f"""
            SELECT domain, response_json, fetched_at FROM {sw_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(REGEXP_REPLACE(domain, r'^www\\.', ''))
                                       ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                sw_data[key] = r["response_json"]

        _sync_status["progress"] = f"Завантажуємо BW ({len(bw_domains):,})..."
        bw_data: dict[str, any] = {}
        for r in corp.query(f"""
            SELECT domain, response_json, fetched_at FROM {bw_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(REGEXP_REPLACE(domain, r'^www\\.', ''))
                                       ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                bw_data[key] = r["response_json"]

        _sync_status["progress"] = "Завантажуємо WC + AI..."
        wc_data: dict[str, any] = {}
        for r in our.query(f"""
            SELECT domain, response_json FROM {wc_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                wc_data[key] = r["response_json"]

        ai_data: dict[str, dict] = {}
        for r in our.query(f"""
            SELECT domain, ai_category, ai_is_ecommerce, ai_industry FROM {ai_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                ai_data[key] = dict(r)

        # Build profiles in parallel
        _sync_status["progress"] = f"Обробляємо {len(all_domains):,} доменів (8 потоків)..."
        updated_at = datetime.now(timezone.utc).isoformat()
        domains_list = list(all_domains)

        def build_one(domain: str) -> dict:
            return _build_profile(
                domain,
                sw_data.get(domain),
                bw_data.get(domain),
                None,
                ai_data.get(domain, {}),
                catalog,
                updated_at,
            )

        rows = []
        processed = 0
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(build_one, d): d for d in domains_list}
            for future in as_completed(futures):
                try:
                    rows.append(future.result())
                except Exception as e:
                    logger.warning(f"Build error: {e}")
                processed += 1
                if processed % 5000 == 0:
                    pct = int(processed / len(domains_list) * 100)
                    _sync_status["progress"] = f"Обробка: {processed:,}/{len(domains_list):,} ({pct}%)"

        logger.info(f"Built {len(rows)} profiles in {time.time()-t0:.0f}s")

        # Write via temp table → CREATE OR REPLACE
        _sync_status["progress"] = f"Записуємо {len(rows):,} профілів..."
        tmp_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{PROFILES_TEMP}"
        tmp_obj = bigquery.Table(tmp_ref, schema=PROFILES_SCHEMA)
        try:
            our.delete_table(tmp_ref)
        except Exception:
            pass
        our.create_table(tmp_obj)

        BATCH = 1000
        for i in range(0, len(rows), BATCH):
            our.insert_rows_json(tmp_ref, rows[i:i+BATCH])
            if i % 20000 == 0:
                pct = int(i / len(rows) * 100)
                _sync_status["progress"] = f"Запис: {i:,}/{len(rows):,} ({pct}%)"

        _sync_status["progress"] = "Фіналізуємо таблицю..."
        time.sleep(15)  # wait streaming buffer
        our.query(f"""
            CREATE OR REPLACE TABLE `{table_ref(PROFILES_TABLE)}`
            AS SELECT * FROM `{tmp_ref}`
        """).result()
        try:
            our.delete_table(tmp_ref)
        except Exception:
            pass

        elapsed = time.time() - t0
        _sync_status["last_sync"] = updated_at
        _sync_status["total_domains"] = len(rows)
        _sync_status["progress"] = f"✅ {len(rows):,} доменів за {elapsed/60:.1f} хв."
        logger.info(f"Sync done: {len(rows)} domains in {elapsed:.0f}s")
        return {"total": len(rows), "status": "ok"}

    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        _sync_status["error"] = str(e)
        _sync_status["progress"] = f"❌ {str(e)[:100]}"
        return {"error": str(e)}
    finally:
        _sync_status["running"] = False


def get_sync_status() -> dict:
    return dict(_sync_status)
