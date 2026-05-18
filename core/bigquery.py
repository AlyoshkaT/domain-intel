"""
BigQuery client - reads credentials from ENV (Railway) or file (local dev)
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery
from google.oauth2 import service_account

from config.settings import (
    GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_LOCATION,
    GOOGLE_APPLICATION_CREDENTIALS,
    GOOGLE_CORP_CREDENTIALS, CORP_PROJECT_ID, CORP_DATASET,
    BQ_BUILTWITH_CACHE, BQ_SIMILARWEB_CACHE,
    BQ_JOBS_TABLE, BQ_RESULTS_TABLE
)

BQ_JOB_DOMAINS_TABLE = "job_domain_lists"

SW_PARSED_TABLE = "sw_parsed"
BW_PARSED_TABLE = "bw_parsed"

logger = logging.getLogger(__name__)

_SW_PARSED_SCHEMA = [
    bigquery.SchemaField("domain",               "STRING"),
    bigquery.SchemaField("fetched_at",           "TIMESTAMP"),
    bigquery.SchemaField("sw_visits",            "FLOAT64"),
    bigquery.SchemaField("sw_category",          "STRING"),
    bigquery.SchemaField("sw_subcategory",       "STRING"),
    bigquery.SchemaField("sw_description",       "STRING"),
    bigquery.SchemaField("sw_title",             "STRING"),
    bigquery.SchemaField("sw_primary_region",    "STRING"),
    bigquery.SchemaField("sw_primary_region_pct","FLOAT64"),
    bigquery.SchemaField("company_name",         "STRING"),
    # Extended fields for autonomous work
    bigquery.SchemaField("sw_top_countries",     "STRING"),   # JSON: top-5 [{country, value}]
    bigquery.SchemaField("sw_monthly_visits",    "STRING"),   # JSON: {"2024-01": 12000, ...}
    bigquery.SchemaField("sw_global_rank",       "INT64"),    # global traffic rank
    bigquery.SchemaField("sw_engagement",        "STRING"),   # JSON: {bounce_rate, pages_per_visit, avg_visit_duration}
]

_BW_PARSED_SCHEMA = [
    bigquery.SchemaField("domain",               "STRING"),
    bigquery.SchemaField("fetched_at",           "TIMESTAMP"),
    bigquery.SchemaField("bw_vertical",          "STRING"),
    bigquery.SchemaField("bw_cms_raw",           "STRING"),
    bigquery.SchemaField("bw_ecommerce",         "STRING"),
    bigquery.SchemaField("bw_email_marketing",   "STRING"),
    bigquery.SchemaField("bw_technologies",      "STRING"),   # JSON list of known tech names
    bigquery.SchemaField("techs_compact",        "STRING"),   # compact format for catalog matching
    bigquery.SchemaField("technologies_json",    "STRING"),   # ALL techs: [{n,t,v,l}] incl. unknown
]


def _make_client(env_var: str, file_path: str, project_id: str) -> bigquery.Client:
    """Load credentials from ENV variable (Railway) or file (local dev)."""
    scopes = ["https://www.googleapis.com/auth/bigquery"]

    # Try ENV variable first (Railway)
    json_str = os.getenv(env_var, "")
    if json_str:
        try:
            info = json.loads(json_str)
            creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
            return bigquery.Client(project=project_id, credentials=creds)
        except Exception as e:
            logger.warning(f"Failed to load credentials from ENV {env_var}: {e}")

    # Fallback to file (local dev)
    if file_path and os.path.exists(file_path):
        creds = service_account.Credentials.from_service_account_file(file_path, scopes=scopes)
        return bigquery.Client(project=project_id, credentials=creds)

    return bigquery.Client(project=project_id)


_client: Optional[bigquery.Client] = None
def client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = _make_client("GOOGLE_CREDENTIALS_JSON", GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID)
    return _client


_corp_client: Optional[bigquery.Client] = None
def corp_client() -> bigquery.Client:
    global _corp_client
    if _corp_client is None:
        _corp_client = _make_client("GOOGLE_CORP_CREDENTIALS_JSON", GOOGLE_CORP_CREDENTIALS, CORP_PROJECT_ID)
    return _corp_client


def table_ref(table_name: str) -> str:
    return f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

def corp_table_ref(table_name: str) -> str:
    return f"{CORP_PROJECT_ID}.{CORP_DATASET}.{table_name}"


JOBS_SCHEMA = [
    bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("total_domains", "INTEGER"),
    bigquery.SchemaField("processed_domains", "INTEGER"),
    bigquery.SchemaField("failed_domains", "INTEGER"),
    bigquery.SchemaField("services", "STRING"),
    bigquery.SchemaField("filename", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
]

RESULTS_SCHEMA = [
    bigquery.SchemaField("job_id", "STRING"),
    bigquery.SchemaField("domain", "STRING"),
    bigquery.SchemaField("processed_at", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("sw_visits", "FLOAT"),
    bigquery.SchemaField("cms_list", "STRING"),
    bigquery.SchemaField("osearch_group", "STRING"),
    bigquery.SchemaField("osearch", "STRING"),
    bigquery.SchemaField("ems_list", "STRING"),
    bigquery.SchemaField("ai_category", "STRING"),
    bigquery.SchemaField("ai_is_ecommerce", "STRING"),
    bigquery.SchemaField("ai_industry", "STRING"),
    bigquery.SchemaField("bw_vertical", "STRING"),
    bigquery.SchemaField("bw_industry", "STRING"),
    bigquery.SchemaField("sw_category", "STRING"),
    bigquery.SchemaField("sw_subcategory", "STRING"),
    bigquery.SchemaField("sw_description", "STRING"),
    bigquery.SchemaField("sw_title", "STRING"),
    bigquery.SchemaField("sw_primary_region", "STRING"),
    bigquery.SchemaField("sw_primary_region_pct", "FLOAT"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("osearch_parse", "STRING"),
    bigquery.SchemaField("sw_top_countries", "STRING"),
    bigquery.SchemaField("bw_technologies", "STRING"),
    bigquery.SchemaField("bw_cms_raw", "STRING"),
    bigquery.SchemaField("bw_ecommerce", "STRING"),
    bigquery.SchemaField("bw_email_marketing", "STRING"),
    bigquery.SchemaField("error_detail", "STRING"),
]


_JOB_DOMAINS_SCHEMA = [
    bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("domains_json", "STRING"),   # JSON array of domain strings
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]


def _ensure_or_migrate_table(bq: bigquery.Client, table_name: str, schema: list, sentinel_col: str) -> None:
    """
    Create table if missing, or recreate if `sentinel_col` is absent (schema migration).
    Safe to call on empty tables — drops & recreates if schema is stale.
    """
    full_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    tbl_obj = bigquery.Table(full_ref, schema=schema)
    try:
        existing = bq.get_table(full_ref)
        existing_cols = {f.name for f in existing.schema}
        if sentinel_col not in existing_cols:
            logger.info(f"Migrating {table_name}: missing column '{sentinel_col}', recreating")
            bq.delete_table(full_ref)
            bq.create_table(tbl_obj)
            logger.info(f"Recreated {table_name} with updated schema")
        else:
            logger.info(f"Table {table_name} schema OK")
    except Exception:
        bq.create_table(tbl_obj)
        logger.info(f"Created table {table_name}")


def ensure_tables_exist():
    bq = client()
    tables_to_create = {
        BQ_JOBS_TABLE: JOBS_SCHEMA,
        BQ_RESULTS_TABLE: RESULTS_SCHEMA,
        BQ_JOB_DOMAINS_TABLE: _JOB_DOMAINS_SCHEMA,
    }
    for table_name, schema in tables_to_create.items():
        table_ref_obj = bigquery.Table(f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}", schema=schema)
        try:
            bq.get_table(table_ref_obj)
            logger.info(f"Table {table_name} already exists")
        except Exception:
            bq.create_table(table_ref_obj)
            logger.info(f"Created table {table_name}")
    # Parsed tables use migration logic to add new columns on first deploy
    _ensure_or_migrate_table(bq, SW_PARSED_TABLE, _SW_PARSED_SCHEMA, "sw_engagement")
    _ensure_or_migrate_table(bq, BW_PARSED_TABLE, _BW_PARSED_SCHEMA, "technologies_json")
    bq_corp = corp_client()
    for table_name in [BQ_BUILTWITH_CACHE, BQ_SIMILARWEB_CACHE]:
        try:
            bq_corp.get_table(f"{CORP_PROJECT_ID}.{CORP_DATASET}.{table_name}")
        except Exception as e:
            logger.warning(f"Corp table {table_name} not found: {e}")
    # Ensure user management tables and run migrations
    try:
        _ensure_users_table()
    except Exception as e:
        logger.error(f"_ensure_users_table error: {e}")
    try:
        _ensure_activity_logs_table()
    except Exception as e:
        logger.error(f"_ensure_activity_logs_table error: {e}")
    try:
        _ensure_sw_usage_table()
    except Exception as e:
        logger.error(f"_ensure_sw_usage_table error: {e}")


# ── In-memory prefetch cache (populated at job start for batch speed) ─────────
# Structure: { table_name: { domain: response_dict | None } }
# None means "we looked it up and it wasn't there" (explicit miss).
_prefetch_cache: dict[str, dict[str, Optional[dict]]] = {}
_PREFETCH_SENTINEL = object()  # distinct from None for "not prefetched"


def prefetch_corp_cache(domains: list[str], tables: list[str]) -> None:
    """
    Batch-fetch the latest cached row for each (table, domain) pair.
    Called once at job start — replaces N×T individual BQ queries with T queries.
    After this, get_cached() will serve results from memory.
    """
    if not domains or not tables:
        return
    bq = corp_client()
    t_start = time.time()

    # Deduplicate + limit (BQ IN clause can handle thousands of values fine)
    unique_domains = list(dict.fromkeys(domains))
    # Build parameterised IN list
    ph = ", ".join(f"@d{i}" for i in range(len(unique_domains)))
    params = [bigquery.ScalarQueryParameter(f"d{i}", "STRING", d) for i, d in enumerate(unique_domains)]

    for table in tables:
        _prefetch_cache.setdefault(table, {})
        try:
            query = f"""
                SELECT domain, response_json
                FROM (
                    SELECT domain, response_json,
                           ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) AS rn
                    FROM `{corp_table_ref(table)}`
                    WHERE domain IN ({ph})
                )
                WHERE rn = 1
            """
            rows = list(bq.query(
                query,
                job_config=bigquery.QueryJobConfig(query_parameters=params)
            ).result())
            hit_count = 0
            for row in rows:
                d = row["domain"]
                data = row["response_json"]
                if not isinstance(data, dict):
                    data = json.loads(data)
                _prefetch_cache[table][d] = data
                hit_count += 1
            # Mark explicit misses so get_cached() won't fall through to BQ
            for d in unique_domains:
                if d not in _prefetch_cache[table]:
                    _prefetch_cache[table][d] = None
            elapsed = time.time() - t_start
            logger.info(f"Prefetch {table}: {hit_count}/{len(unique_domains)} hits in {elapsed:.1f}s")
        except Exception as e:
            logger.error(f"Prefetch error ({table}): {e}")


def clear_prefetch_cache() -> None:
    """Clear the in-memory prefetch cache after a job finishes."""
    _prefetch_cache.clear()


# ── Parsed cache (privateBQ sw_parsed / bw_parsed) ───────────────────────────

_parsed_sw_cache: dict[str, Optional[dict]] = {}
_parsed_bw_cache: dict[str, Optional[dict]] = {}


def save_sw_parsed(domain: str, parsed: dict) -> None:
    """Stream-insert one parsed SW row into privateBQ sw_parsed."""
    bq = client()
    row = {
        "domain": domain,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "sw_visits": parsed.get("sw_visits"),
        "sw_category": parsed.get("sw_category", ""),
        "sw_subcategory": parsed.get("sw_subcategory", ""),
        "sw_description": parsed.get("sw_description", ""),
        "sw_title": parsed.get("sw_title", ""),
        "sw_primary_region": parsed.get("sw_primary_region", ""),
        "sw_primary_region_pct": parsed.get("sw_primary_region_pct"),
        "company_name": parsed.get("company_name", ""),
        # Extended fields
        "sw_top_countries": parsed.get("sw_top_countries", "[]"),
        "sw_monthly_visits": parsed.get("sw_monthly_visits", "{}"),
        "sw_global_rank": parsed.get("sw_global_rank"),
        "sw_engagement": parsed.get("sw_engagement", "{}"),
    }
    try:
        errors = bq.insert_rows_json(table_ref(SW_PARSED_TABLE), [row])
        if errors:
            logger.error(f"save_sw_parsed error ({domain}): {errors}")
        else:
            _parsed_sw_cache[domain] = parsed
            logger.debug(f"save_sw_parsed OK: {domain}")
    except Exception as e:
        logger.error(f"save_sw_parsed exception ({domain}): {e}")


def save_bw_parsed(domain: str, bw_dict: dict) -> None:
    """Stream-insert one parsed BW row into privateBQ bw_parsed."""
    bq = client()
    row = {
        "domain": domain,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "bw_vertical": bw_dict.get("bw_vertical", ""),
        "bw_cms_raw": bw_dict.get("bw_cms_raw", ""),
        "bw_ecommerce": bw_dict.get("bw_ecommerce", ""),
        "bw_email_marketing": bw_dict.get("bw_email_marketing", ""),
        "bw_technologies": bw_dict.get("bw_technologies", "[]"),
        "techs_compact": bw_dict.get("techs_compact", ""),
        "technologies_json": bw_dict.get("technologies_json", "[]"),
    }
    try:
        errors = bq.insert_rows_json(table_ref(BW_PARSED_TABLE), [row])
        if errors:
            logger.error(f"save_bw_parsed error ({domain}): {errors}")
        else:
            _parsed_bw_cache[domain] = bw_dict
            logger.debug(f"save_bw_parsed OK: {domain}")
    except Exception as e:
        logger.error(f"save_bw_parsed exception ({domain}): {e}")


def prefetch_parsed(domains: list[str]) -> None:
    """
    Batch-fetch latest sw_parsed + bw_parsed rows from privateBQ for given domains.
    Called once at job start — replaces per-domain BQ reads with 2 queries.
    """
    if not domains:
        return
    bq = client()
    t_start = time.time()
    unique_domains = list(dict.fromkeys(domains))
    ph = ", ".join(f"@d{i}" for i in range(len(unique_domains)))
    params = [bigquery.ScalarQueryParameter(f"d{i}", "STRING", d) for i, d in enumerate(unique_domains)]

    # SW
    try:
        rows = list(bq.query(
            f"""
            SELECT domain, sw_visits, sw_category, sw_subcategory, sw_description,
                   sw_title, sw_primary_region, sw_primary_region_pct, company_name,
                   sw_top_countries, sw_monthly_visits, sw_global_rank, sw_engagement
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) AS rn
                FROM `{table_ref(SW_PARSED_TABLE)}`
                WHERE domain IN ({ph})
            ) WHERE rn = 1
            """,
            job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result())
        sw_hits = 0
        for row in rows:
            d = row["domain"]
            _parsed_sw_cache[d] = {
                "sw_visits": row["sw_visits"],
                "sw_category": row["sw_category"] or "",
                "sw_subcategory": row["sw_subcategory"] or "",
                "sw_description": row["sw_description"] or "",
                "sw_title": row["sw_title"] or "",
                "sw_primary_region": row["sw_primary_region"] or "",
                "sw_primary_region_pct": row["sw_primary_region_pct"],
                "company_name": row["company_name"] or "",
                "sw_top_countries": row["sw_top_countries"] or "[]",
                "sw_monthly_visits": row["sw_monthly_visits"] or "{}",
                "sw_global_rank": row["sw_global_rank"],
                "sw_engagement": row["sw_engagement"] or "{}",
            }
            sw_hits += 1
        for d in unique_domains:
            if d not in _parsed_sw_cache:
                _parsed_sw_cache[d] = None
        logger.info(f"prefetch_parsed SW: {sw_hits}/{len(unique_domains)} hits in {time.time()-t_start:.1f}s")
    except Exception as e:
        logger.error(f"prefetch_parsed SW error: {e}")

    # BW
    t_bw = time.time()
    try:
        rows = list(bq.query(
            f"""
            SELECT domain, bw_vertical, bw_cms_raw, bw_ecommerce, bw_email_marketing,
                   bw_technologies, techs_compact, technologies_json
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) AS rn
                FROM `{table_ref(BW_PARSED_TABLE)}`
                WHERE domain IN ({ph})
            ) WHERE rn = 1
            """,
            job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result())
        bw_hits = 0
        for row in rows:
            d = row["domain"]
            _parsed_bw_cache[d] = {
                "bw_vertical": row["bw_vertical"] or "",
                "bw_cms_raw": row["bw_cms_raw"] or "",
                "bw_ecommerce": row["bw_ecommerce"] or "",
                "bw_email_marketing": row["bw_email_marketing"] or "",
                "bw_technologies": row["bw_technologies"] or "[]",
                "techs_compact": row["techs_compact"] or "",
                "technologies_json": row["technologies_json"] or "[]",
            }
            bw_hits += 1
        for d in unique_domains:
            if d not in _parsed_bw_cache:
                _parsed_bw_cache[d] = None
        logger.info(f"prefetch_parsed BW: {bw_hits}/{len(unique_domains)} hits in {time.time()-t_bw:.1f}s")
    except Exception as e:
        logger.error(f"prefetch_parsed BW error: {e}")


def get_sw_parsed(domain: str) -> Optional[dict]:
    """Return parsed SW data from in-memory cache or fallback BQ query."""
    if domain in _parsed_sw_cache:
        return _parsed_sw_cache[domain]
    # Slow path: individual BQ query
    bq = client()
    try:
        rows = list(bq.query(
            f"""
            SELECT sw_visits, sw_category, sw_subcategory, sw_description,
                   sw_title, sw_primary_region, sw_primary_region_pct, company_name,
                   sw_top_countries, sw_monthly_visits, sw_global_rank, sw_engagement
            FROM `{table_ref(SW_PARSED_TABLE)}`
            WHERE domain = @domain
            ORDER BY fetched_at DESC LIMIT 1
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("domain", "STRING", domain)]
            )
        ).result())
        if rows:
            r = rows[0]
            result = {
                "sw_visits": r["sw_visits"],
                "sw_category": r["sw_category"] or "",
                "sw_subcategory": r["sw_subcategory"] or "",
                "sw_description": r["sw_description"] or "",
                "sw_title": r["sw_title"] or "",
                "sw_primary_region": r["sw_primary_region"] or "",
                "sw_primary_region_pct": r["sw_primary_region_pct"],
                "company_name": r["company_name"] or "",
                "sw_top_countries": r["sw_top_countries"] or "[]",
                "sw_monthly_visits": r["sw_monthly_visits"] or "{}",
                "sw_global_rank": r["sw_global_rank"],
                "sw_engagement": r["sw_engagement"] or "{}",
            }
            _parsed_sw_cache[domain] = result
            return result
        _parsed_sw_cache[domain] = None
        return None
    except Exception as e:
        logger.error(f"get_sw_parsed error ({domain}): {e}")
        return None


def get_bw_parsed(domain: str) -> Optional[dict]:
    """Return parsed BW data from in-memory cache or fallback BQ query."""
    if domain in _parsed_bw_cache:
        return _parsed_bw_cache[domain]
    # Slow path: individual BQ query
    bq = client()
    try:
        rows = list(bq.query(
            f"""
            SELECT bw_vertical, bw_cms_raw, bw_ecommerce, bw_email_marketing,
                   bw_technologies, techs_compact, technologies_json
            FROM `{table_ref(BW_PARSED_TABLE)}`
            WHERE domain = @domain
            ORDER BY fetched_at DESC LIMIT 1
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("domain", "STRING", domain)]
            )
        ).result())
        if rows:
            r = rows[0]
            result = {
                "bw_vertical": r["bw_vertical"] or "",
                "bw_cms_raw": r["bw_cms_raw"] or "",
                "bw_ecommerce": r["bw_ecommerce"] or "",
                "bw_email_marketing": r["bw_email_marketing"] or "",
                "bw_technologies": r["bw_technologies"] or "[]",
                "techs_compact": r["techs_compact"] or "",
                "technologies_json": r["technologies_json"] or "[]",
            }
            _parsed_bw_cache[domain] = result
            return result
        _parsed_bw_cache[domain] = None
        return None
    except Exception as e:
        logger.error(f"get_bw_parsed error ({domain}): {e}")
        return None


def was_parsed_prefetched(domain: str) -> bool:
    """
    Returns True if this domain was included in a prefetch_parsed() call
    (even if no data was found — i.e., the value is None/miss).
    Used in pipeline to skip expensive corpBQ fallback during batch jobs.
    """
    return domain in _parsed_sw_cache or domain in _parsed_bw_cache


def clear_parsed_cache() -> None:
    """Clear the in-memory parsed cache after a job finishes."""
    _parsed_sw_cache.clear()
    _parsed_bw_cache.clear()


def sync_parsed_from_corp() -> dict:
    """
    Daily sync: MERGE corpBQ raw JSON → privateBQ sw_parsed + bw_parsed.
    Extracts parsed fields in SQL so we never transfer full JSON blobs.
    Intended to run once per day at 03:00 UTC, 1 hour before domain_profiles sync.
    """
    t0 = time.time()
    logger.info("sync_parsed_from_corp: starting SW merge")

    sw_tbl = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.similarweb_raw_data`"
    bw_tbl = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.builtwith_raw_data`"
    our_sw = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{SW_PARSED_TABLE}`"
    our_bw = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BW_PARSED_TABLE}`"

    bq = client()

    # ── SW MERGE ─────────────────────────────────────────────────────────────
    try:
        sw_merge = f"""
            MERGE {our_sw} T
            USING (
                SELECT
                    domain,
                    CURRENT_TIMESTAMP() AS fetched_at,
                    COALESCE(
                        SAFE_CAST(JSON_VALUE(response_json, '$.Engagments.Visits') AS FLOAT64),
                        0
                    ) AS sw_visits,
                    COALESCE(
                        JSON_VALUE(response_json, '$.CategoryRank.Category'),
                        JSON_VALUE(response_json, '$.Category'), ''
                    ) AS sw_category_raw,
                    COALESCE(SUBSTR(JSON_VALUE(response_json, '$.Description'), 1, 500), '') AS sw_description,
                    COALESCE(
                        JSON_VALUE(response_json, '$.Title'),
                        JSON_VALUE(response_json, '$.SiteName'), ''
                    ) AS sw_title,
                    COALESCE(JSON_VALUE(response_json, '$.TopCountryShares[0].CountryCode'), '') AS sw_primary_region,
                    SAFE_CAST(JSON_VALUE(response_json, '$.TopCountryShares[0].Value') AS FLOAT64) AS sw_region_val,
                    -- Extended: top-5 countries as JSON array
                    IFNULL(TO_JSON_STRING(ARRAY(
                        SELECT AS STRUCT
                            JSON_VALUE(c, '$.CountryCode') AS country,
                            ROUND(SAFE_CAST(JSON_VALUE(c, '$.Value') AS FLOAT64), 4) AS value
                        FROM UNNEST(JSON_QUERY_ARRAY(response_json, '$.TopCountryShares')) AS c
                        LIMIT 5
                    )), '[]') AS sw_top_countries,
                    -- Extended: monthly visits history
                    IFNULL(JSON_QUERY(response_json, '$.EstimatedMonthlyVisits'), '{{}}') AS sw_monthly_visits,
                    -- Extended: global rank
                    SAFE_CAST(JSON_VALUE(response_json, '$.GlobalRank.Rank') AS INT64) AS sw_global_rank,
                    -- Extended: engagement metrics
                    TO_JSON_STRING(STRUCT(
                        SAFE_CAST(JSON_VALUE(response_json, '$.Engagments.BounceRate') AS FLOAT64) AS bounce_rate,
                        SAFE_CAST(JSON_VALUE(response_json, '$.Engagments.PagePerVisit') AS FLOAT64) AS pages_per_visit,
                        SAFE_CAST(JSON_VALUE(response_json, '$.Engagments.TimeOnSite') AS FLOAT64) AS avg_visit_duration
                    )) AS sw_engagement
                FROM {sw_tbl}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            ) S
            ON T.domain = S.domain
            WHEN MATCHED AND S.fetched_at > T.fetched_at THEN UPDATE SET
                T.fetched_at           = S.fetched_at,
                T.sw_visits            = S.sw_visits,
                T.sw_category          = IF(STRPOS(S.sw_category_raw, '/') > 0, SPLIT(S.sw_category_raw, '/')[OFFSET(0)], S.sw_category_raw),
                T.sw_subcategory       = IF(STRPOS(S.sw_category_raw, '/') > 0, SPLIT(S.sw_category_raw, '/')[OFFSET(1)], ''),
                T.sw_description       = S.sw_description,
                T.sw_title             = S.sw_title,
                T.sw_primary_region    = S.sw_primary_region,
                T.sw_primary_region_pct = ROUND((COALESCE(S.sw_region_val, 0)) * 100, 1),
                T.company_name         = S.sw_title,
                T.sw_top_countries     = S.sw_top_countries,
                T.sw_monthly_visits    = S.sw_monthly_visits,
                T.sw_global_rank       = S.sw_global_rank,
                T.sw_engagement        = S.sw_engagement
            WHEN NOT MATCHED THEN INSERT (
                domain, fetched_at, sw_visits, sw_category, sw_subcategory,
                sw_description, sw_title, sw_primary_region, sw_primary_region_pct, company_name,
                sw_top_countries, sw_monthly_visits, sw_global_rank, sw_engagement
            ) VALUES (
                S.domain, S.fetched_at, S.sw_visits,
                IF(STRPOS(S.sw_category_raw, '/') > 0, SPLIT(S.sw_category_raw, '/')[OFFSET(0)], S.sw_category_raw),
                IF(STRPOS(S.sw_category_raw, '/') > 0, SPLIT(S.sw_category_raw, '/')[OFFSET(1)], ''),
                S.sw_description, S.sw_title, S.sw_primary_region,
                ROUND((COALESCE(S.sw_region_val, 0)) * 100, 1),
                S.sw_title,
                S.sw_top_countries, S.sw_monthly_visits, S.sw_global_rank, S.sw_engagement
            )
        """
        sw_job = bq.query(sw_merge)
        sw_job.result()
        sw_elapsed = time.time() - t0
        logger.info(f"sync_parsed_from_corp: SW merge done in {sw_elapsed:.1f}s, "
                    f"rows_affected={sw_job.num_dml_affected_rows}")
    except Exception as e:
        logger.error(f"sync_parsed_from_corp SW error: {e}", exc_info=True)
        return {"error": str(e)}

    # ── BW MERGE ─────────────────────────────────────────────────────────────
    logger.info("sync_parsed_from_corp: starting BW merge")
    t_bw = time.time()
    _SEP_FIELD = "\x01"
    _SEP_TECH  = "\x02"
    try:
        bw_merge = f"""
            MERGE {our_bw} T
            USING (
                SELECT
                    domain,
                    CURRENT_TIMESTAMP() AS fetched_at,
                    COALESCE(JSON_VALUE(response_json, '$.Results[0].Result.Vertical'), '') AS bw_vertical,
                    '' AS bw_cms_raw,
                    '' AS bw_ecommerce,
                    '' AS bw_email_marketing,
                    '[]' AS bw_technologies,
                    IFNULL((
                        SELECT STRING_AGG(
                            JSON_VALUE(tech, '$.Name')
                            || '{_SEP_FIELD}'
                            || IFNULL(JSON_VALUE(tech, '$.LastDetected'), '0'),
                            '{_SEP_TECH}'
                        )
                        FROM UNNEST(JSON_QUERY_ARRAY(response_json, '$.Results[0].Result.Paths')) AS path,
                        UNNEST(JSON_QUERY_ARRAY(path, '$.Technologies')) AS tech
                        WHERE JSON_VALUE(tech, '$.Name') IS NOT NULL
                    ), '') AS techs_compact,
                    -- Extended: ALL technologies as rich JSON array [{n, t, l}]
                    IFNULL((
                        SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(
                            JSON_VALUE(tech, '$.Name') AS n,
                            IFNULL(JSON_VALUE(tech, '$.Tag[0]'), '') AS t,
                            SAFE_CAST(JSON_VALUE(tech, '$.LastDetected') AS INT64) AS l
                        )))
                        FROM UNNEST(JSON_QUERY_ARRAY(response_json, '$.Results[0].Result.Paths')) AS path,
                        UNNEST(JSON_QUERY_ARRAY(path, '$.Technologies')) AS tech
                        WHERE JSON_VALUE(tech, '$.Name') IS NOT NULL
                    ), '[]') AS technologies_json
                FROM {bw_tbl}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            ) S
            ON T.domain = S.domain
            WHEN MATCHED AND S.fetched_at > T.fetched_at THEN UPDATE SET
                T.fetched_at         = S.fetched_at,
                T.bw_vertical        = S.bw_vertical,
                T.bw_cms_raw         = S.bw_cms_raw,
                T.bw_ecommerce       = S.bw_ecommerce,
                T.bw_email_marketing = S.bw_email_marketing,
                T.bw_technologies    = S.bw_technologies,
                T.techs_compact      = S.techs_compact,
                T.technologies_json  = S.technologies_json
            WHEN NOT MATCHED THEN INSERT (
                domain, fetched_at, bw_vertical, bw_cms_raw, bw_ecommerce,
                bw_email_marketing, bw_technologies, techs_compact, technologies_json
            ) VALUES (
                S.domain, S.fetched_at, S.bw_vertical, S.bw_cms_raw, S.bw_ecommerce,
                S.bw_email_marketing, S.bw_technologies, S.techs_compact, S.technologies_json
            )
        """
        bw_job = bq.query(bw_merge)
        bw_job.result()
        bw_elapsed = time.time() - t_bw
        logger.info(f"sync_parsed_from_corp: BW merge done in {bw_elapsed:.1f}s, "
                    f"rows_affected={bw_job.num_dml_affected_rows}")
    except Exception as e:
        logger.error(f"sync_parsed_from_corp BW error: {e}", exc_info=True)
        return {"error": str(e)}

    total_elapsed = time.time() - t0
    logger.info(f"sync_parsed_from_corp: done in {total_elapsed:.1f}s total")
    return {
        "status": "ok",
        "sw_rows": sw_job.num_dml_affected_rows,
        "bw_rows": bw_job.num_dml_affected_rows,
        "elapsed": round(total_elapsed, 1),
    }


def get_cached(table: str, domain: str, ttl_days: int = 90, force: bool = False, ignore_ttl: bool = False) -> Optional[dict]:
    if force:
        return None

    # Fast path: serve from in-memory prefetch cache if available
    if table in _prefetch_cache and domain in _prefetch_cache[table]:
        data = _prefetch_cache[table][domain]
        if data is None:
            logger.debug(f"Prefetch MISS: {table} / {domain}")
        else:
            logger.debug(f"Prefetch HIT: {table} / {domain}")
        return data

    # Slow path: individual BQ query (used when prefetch wasn't called)
    bq = corp_client()
    t_start = time.time()
    logger.info(f"Cache lookup: {table} / {domain}")
    ttl_clause = f"AND fetched_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ttl_days} DAY)" if not ignore_ttl else ""
    query = f"""
        SELECT response_json, fetched_at FROM `{corp_table_ref(table)}`
        WHERE domain = @domain {ttl_clause}
        ORDER BY fetched_at DESC LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("domain", "STRING", domain)]
    )
    try:
        rows = list(bq.query(query, job_config=job_config).result())
        elapsed = time.time() - t_start
        if rows:
            logger.info(f"Cache HIT: {table} / {domain} ({elapsed:.1f}s)")
            data = rows[0]["response_json"]
            return data if isinstance(data, dict) else json.loads(data)
        logger.info(f"Cache MISS: {table} / {domain} ({elapsed:.1f}s)")
        return None
    except Exception as e:
        logger.error(f"Cache read error ({table}, {domain}): {e}")
        return None


def save_cache(table: str, domain: str, data: dict):
    bq = corp_client()
    row = {"domain": domain, "fetched_at": datetime.now(timezone.utc).isoformat(), "response_json": json.dumps(data)}
    try:
        errors = bq.insert_rows_json(corp_table_ref(table), [row])
        if errors:
            logger.error(f"Cache write error ({table}, {domain}): {errors}")
        else:
            logger.info(f"Cache saved: {table} / {domain}")
    except Exception as e:
        logger.error(f"Cache write exception ({table}, {domain}): {e}")


def create_job(job_id: str, total_domains: int, services: list[str], filename: str):
    bq = client()
    created_at = datetime.now(timezone.utc).isoformat()
    services_json = json.dumps(services).replace("'", "''")
    filename_escaped = (filename or "").replace("'", "''")
    bq.query(f"""
        INSERT INTO `{table_ref(BQ_JOBS_TABLE)}`
        (job_id, created_at, updated_at, status, total_domains,
         processed_domains, failed_domains, services, filename, error_message)
        VALUES ('{job_id}', '{created_at}', '{created_at}', 'pending',
         {total_domains}, 0, 0, '{services_json}', '{filename_escaped}', NULL)
    """).result()


def get_stale_running_jobs() -> list[dict]:
    """Return all jobs currently in running/pending state (survived server restart)."""
    bq = client()
    try:
        rows = list(bq.query(
            f"SELECT * FROM `{table_ref(BQ_JOBS_TABLE)}` WHERE status IN ('running','pending')"
        ).result())
        result = []
        for row in rows:
            r = dict(row)
            r["services"] = json.loads(r.get("services") or "[]")
            result.append(r)
        return result
    except Exception as e:
        logger.error(f"get_stale_running_jobs error: {e}")
        return []


def save_job_domains(job_id: str, domains: list[str]) -> None:
    """Persist the original domain list so the job can be resumed after restart."""
    bq = client()
    row = {
        "job_id": job_id,
        "domains_json": json.dumps(domains),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        errors = bq.insert_rows_json(table_ref(BQ_JOB_DOMAINS_TABLE), [row])
        if errors:
            logger.error(f"save_job_domains error: {errors}")
    except Exception as e:
        logger.error(f"save_job_domains exception: {e}")


def get_job_domains(job_id: str) -> list[str]:
    """Retrieve the original domain list for a job (for resume)."""
    bq = client()
    try:
        rows = list(bq.query(
            f"SELECT domains_json FROM `{table_ref(BQ_JOB_DOMAINS_TABLE)}` "
            f"WHERE job_id = @job_id ORDER BY created_at DESC LIMIT 1",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
            )
        ).result())
        if rows:
            return json.loads(rows[0]["domains_json"])
    except Exception as e:
        logger.error(f"get_job_domains error: {e}")
    return []


def get_processed_domains_for_job(job_id: str) -> set[str]:
    """Return the set of domains that already have a result row for this job."""
    bq = client()
    try:
        rows = list(bq.query(
            f"SELECT domain FROM `{table_ref(BQ_RESULTS_TABLE)}` WHERE job_id = @job_id",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
            )
        ).result())
        return {r["domain"] for r in rows}
    except Exception as e:
        logger.error(f"get_processed_domains_for_job error: {e}")
        return set()


def reset_stale_jobs() -> int:
    """Mark running/pending jobs as failed — fallback when no domain list is available."""
    bq = client()
    try:
        rows = list(bq.query(
            f"SELECT COUNT(*) as c FROM `{table_ref(BQ_JOBS_TABLE)}` WHERE status IN ('running','pending')"
        ).result())
        count = int(rows[0]["c"]) if rows else 0
        if count:
            bq.query(
                f"UPDATE `{table_ref(BQ_JOBS_TABLE)}` "
                f"SET status='failed', error_message='Interrupted by server restart', "
                f"updated_at=CURRENT_TIMESTAMP() "
                f"WHERE status IN ('running','pending')"
            ).result()
            logger.info(f"Reset {count} stale jobs (running/pending → failed)")
        return count
    except Exception as e:
        logger.error(f"reset_stale_jobs error: {e}")
        return 0


def update_job(job_id: str, **kwargs):
    bq = client()
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()
    set_parts = []
    for k, v in kwargs.items():
        if v is None: set_parts.append(f"{k} = NULL")
        elif isinstance(v, (int, float)): set_parts.append(f"{k} = {v}")
        else: set_parts.append(f"{k} = '{str(v).replace(chr(39), chr(39)*2)}'")
    try:
        bq.query(f"UPDATE `{table_ref(BQ_JOBS_TABLE)}` SET {', '.join(set_parts)} WHERE job_id = '{job_id}'").result()
    except Exception as e:
        logger.error(f"update_job error: {e}")


def get_job(job_id: str) -> Optional[dict]:
    bq = client()
    rows = list(bq.query(
        f"SELECT * FROM `{table_ref(BQ_JOBS_TABLE)}` WHERE job_id = @job_id LIMIT 1",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)])
    ).result())
    if rows:
        row = dict(rows[0])
        row["services"] = json.loads(row.get("services") or "[]")
        return row
    return None


def list_jobs(limit: int = 50) -> list[dict]:
    bq = client()
    rows = list(bq.query(f"SELECT * FROM `{table_ref(BQ_JOBS_TABLE)}` ORDER BY created_at DESC LIMIT {limit}").result())
    result = []
    for row in rows:
        r = dict(row)
        r["services"] = json.loads(r.get("services") or "[]")
        result.append(r)
    return result


def save_result(result: dict):
    """Save one domain result via streaming insert (~100ms vs 3-5s for DML INSERT)."""
    bq = client()
    # Normalise: BQ insert_rows_json needs JSON-serialisable values only
    row = {k: (None if v is None else v) for k, v in result.items()}
    try:
        errors = bq.insert_rows_json(table_ref(BQ_RESULTS_TABLE), [row])
        if errors:
            logger.error(f"Result write error: {errors}")
        else:
            logger.info(f"Result saved OK: {result.get('domain')}")
    except Exception as e:
        logger.error(f"Result write error: {e}")


def get_results(job_id: str) -> list[dict]:
    bq = client()
    return [dict(row) for row in bq.query(
        f"SELECT * FROM `{table_ref(BQ_RESULTS_TABLE)}` WHERE job_id = @job_id ORDER BY processed_at DESC",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)])
    ).result()]


# ── App Settings ──────────────────────────────────────────────────────────────

_SETTINGS_SCHEMA = [
    bigquery.SchemaField("key", "STRING"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

_settings_cache: dict[str, str] = {}
_settings_cached_at: float = 0
_SETTINGS_TTL = 30  # seconds in-memory cache


def _ensure_settings_table():
    bq = client()
    tbl = bigquery.Table(table_ref("app_settings"), schema=_SETTINGS_SCHEMA)
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)
        logger.info("Created table app_settings")


def get_setting(key: str, default: str = "") -> str:
    global _settings_cache, _settings_cached_at
    now = time.time()
    if now - _settings_cached_at < _SETTINGS_TTL and key in _settings_cache:
        return _settings_cache[key]
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT key, value FROM `{table_ref('app_settings')}`"
        ).result())
        _settings_cache = {r["key"]: r["value"] for r in rows}
        _settings_cached_at = now
        return _settings_cache.get(key, default)
    except Exception:
        return _settings_cache.get(key, default)


def set_setting(key: str, value: str):
    """Upsert a setting via MERGE (pure DML — avoids streaming buffer conflicts)."""
    global _settings_cache, _settings_cached_at
    _ensure_settings_table()
    bq = client()
    tref = table_ref("app_settings")
    updated_at = datetime.now(timezone.utc).isoformat()
    bq.query(f"""
        MERGE `{tref}` T
        USING (SELECT @key AS key, @value AS value, CAST(@updated_at AS TIMESTAMP) AS updated_at) S
        ON T.key = S.key
        WHEN MATCHED THEN
            UPDATE SET T.value = S.value, T.updated_at = S.updated_at
        WHEN NOT MATCHED THEN
            INSERT (key, value, updated_at) VALUES (S.key, S.value, S.updated_at)
    """, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("key", "STRING", key),
        bigquery.ScalarQueryParameter("value", "STRING", value),
        bigquery.ScalarQueryParameter("updated_at", "STRING", updated_at),
    ])).result()
    _settings_cache[key] = value
    _settings_cached_at = 0  # invalidate cache


def get_cache_ttl() -> int:
    """Returns cache TTL in days (default 90)."""
    return int(get_setting("cache_ttl_days", "90"))


# ── App Users ─────────────────────────────────────────────────────────────────

_USERS_SCHEMA = [
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("password", "STRING"),
    bigquery.SchemaField("permissions", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

_ACTIVITY_LOGS_SCHEMA = [
    bigquery.SchemaField("logged_at", "TIMESTAMP"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("action", "STRING"),
    bigquery.SchemaField("details", "STRING"),
]

_SW_USAGE_COUNTER_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("api", "STRING"),
    bigquery.SchemaField("calls", "INTEGER"),
]


def _ensure_users_table():
    bq = client()
    tbl = bigquery.Table(table_ref("app_users"), schema=_USERS_SCHEMA)
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)
        logger.info("Created table app_users")
    # Migrate: add new columns if not exists
    for col, col_type in [("permissions", "STRING"), ("email", "STRING"), ("google_folder", "STRING"), ("display_name", "STRING"), ("first_name", "STRING"), ("last_name", "STRING")]:
        try:
            bq.query(
                f"ALTER TABLE `{table_ref('app_users')}` ADD COLUMN IF NOT EXISTS {col} {col_type}"
            ).result()
        except Exception as e:
            logger.warning(f"Migration add column {col} to app_users: {e}")


def _ensure_activity_logs_table():
    bq = client()
    tbl = bigquery.Table(table_ref("activity_logs"), schema=_ACTIVITY_LOGS_SCHEMA)
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)
        logger.info("Created table activity_logs")


def _ensure_sw_usage_table():
    bq = client()
    tbl = bigquery.Table(table_ref("sw_usage_counter"), schema=_SW_USAGE_COUNTER_SCHEMA)
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)
        logger.info("Created table sw_usage_counter")


def get_users() -> list[dict]:
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT * FROM `{table_ref('app_users')}` ORDER BY created_at"
        ).result())
        return [{
            "username": r["username"],
            "permissions": dict(r).get("permissions"),
            "created_at": str(r["created_at"]),
            "email": dict(r).get("email"),
            "google_folder": dict(r).get("google_folder"),
            "display_name": dict(r).get("display_name"),
            "first_name": dict(r).get("first_name"),
            "last_name": dict(r).get("last_name"),
        } for r in rows]
    except Exception as e:
        logger.error(f"get_users error: {e}")
        raise


def get_bq_users_for_auth() -> dict[str, str]:
    """Returns {username: password} for auth middleware."""
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT username, password FROM `{table_ref('app_users')}`"
        ).result())
        return {r["username"]: r["password"] for r in rows}
    except Exception:
        return {}


def get_bq_users_permissions() -> dict[str, str]:
    """Returns {username: permissions_string} for permission checking."""
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT username, permissions FROM `{table_ref('app_users')}`"
        ).result())
        return {r["username"]: (r["permissions"] or "") for r in rows}
    except Exception:
        return {}


def add_user(username: str, password: str, permissions: str,
             email: str = None, google_folder: str = None, display_name: str = None,
             first_name: str = None, last_name: str = None):
    _ensure_users_table()
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('app_users')}` WHERE username = @u",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("u", "STRING", username)])
    ).result()
    row = {
        "username": username, "password": password,
        "permissions": permissions,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if email is not None:
        row["email"] = email
    if google_folder is not None:
        row["google_folder"] = google_folder
    if display_name is not None:
        row["display_name"] = display_name
    if first_name is not None:
        row["first_name"] = first_name
    if last_name is not None:
        row["last_name"] = last_name
    errors = bq.insert_rows_json(table_ref("app_users"), [row])
    if errors:
        logger.error(f"add_user errors: {errors}")
        raise RuntimeError(f"BQ insert error: {errors[0].get('errors', errors[0])}")


def update_user(username: str, **kwargs):
    """Update specific fields for an existing user. Accepted fields:
    permissions, email, google_folder, display_name, password."""
    allowed = {"permissions", "email", "google_folder", "display_name", "first_name", "last_name", "password"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    bq = client()
    set_parts = []
    params = [bigquery.ScalarQueryParameter("u", "STRING", username)]
    for i, (k, v) in enumerate(updates.items()):
        pname = f"p{i}"
        if v is None:
            set_parts.append(f"{k} = NULL")
        else:
            set_parts.append(f"{k} = @{pname}")
            params.append(bigquery.ScalarQueryParameter(pname, "STRING", str(v)))
    try:
        bq.query(
            f"UPDATE `{table_ref('app_users')}` SET {', '.join(set_parts)} WHERE username = @u",
            job_config=bigquery.QueryJobConfig(query_parameters=params)
        ).result()
    except Exception as e:
        logger.error(f"update_user error: {e}")


def remove_user(username: str):
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('app_users')}` WHERE username = @u",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("u", "STRING", username)])
    ).result()


# ── Activity Logs ─────────────────────────────────────────────────────────────

def log_activity(username: str, action: str, details: dict = None):
    """Log a user action via streaming insert."""
    try:
        bq = client()
        # BQ streaming insert: use float timestamp (seconds since epoch) for TIMESTAMP columns
        row = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "username": username or "unknown",
            "action": action,
        }
        if details is not None:
            row["details"] = json.dumps(details)
        errors = bq.insert_rows_json(table_ref("activity_logs"), [row])
        if errors:
            logger.error(f"log_activity insert errors: {errors}")
        else:
            logger.info(f"log_activity OK: {username} / {action}")
    except Exception as e:
        logger.error(f"log_activity error: {e}")


def clear_activity_logs() -> int:
    """
    Clear all rows from activity_logs.
    Uses load_table_from_file(WRITE_TRUNCATE) with empty data instead of
    DML DELETE, because BQ DML cannot modify rows still in the streaming
    buffer — this approach always works regardless of buffer state.
    Returns approximate row count that was in the table before clearing.
    """
    import io as _io
    try:
        bq = client()
        tbl_id = table_ref("activity_logs")

        # Count existing rows first so we can report a number
        try:
            count_row = list(bq.query(f"SELECT COUNT(*) AS n FROM `{tbl_id}`").result())[0]
            before = int(count_row["n"])
        except Exception:
            before = 0

        # Overwrite table with empty JSONL file (WRITE_TRUNCATE)
        job_config = bigquery.LoadJobConfig(
            schema=_ACTIVITY_LOGS_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = bq.load_table_from_file(_io.BytesIO(b""), tbl_id, job_config=job_config)
        job.result()
        logger.info(f"clear_activity_logs: truncated table (had ~{before} rows)")
        return before
    except Exception as e:
        logger.error(f"clear_activity_logs error: {e}")
        raise


def get_activity_logs(limit: int = 200) -> list[dict]:
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT logged_at, username, action, details"
            f" FROM `{table_ref('activity_logs')}` ORDER BY logged_at DESC LIMIT {limit}"
        ).result())
        return [{
            "logged_at": str(r["logged_at"]),
            "username": r["username"],
            "action": r["action"],
            "details": r["details"],
        } for r in rows]
    except Exception as e:
        logger.error(f"get_activity_logs error: {e}")
        return []


# ── SW Usage Counter ──────────────────────────────────────────────────────────

def increment_api_usage(username: str, api: str, calls: int = 1):
    """Upsert (date, username, api) → increment calls. Uses MERGE for atomic upsert."""
    try:
        bq = client()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sql = f"""
            MERGE `{table_ref('sw_usage_counter')}` AS T
            USING (SELECT @date AS date, @username AS username, @api AS api) AS S
            ON T.date = S.date AND T.username = S.username AND T.api = S.api
            WHEN MATCHED THEN
                UPDATE SET T.calls = T.calls + @calls
            WHEN NOT MATCHED THEN
                INSERT (date, username, api, calls) VALUES (S.date, S.username, S.api, @calls)
        """
        params = [
            bigquery.ScalarQueryParameter("date", "DATE", today),
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("api", "STRING", api),
            bigquery.ScalarQueryParameter("calls", "INT64", calls),
        ]
        bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    except Exception as e:
        logger.error(f"increment_api_usage error: {e}")


def get_api_usage_summary() -> list[dict]:
    """Returns usage grouped by date + api."""
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT date, username, api, calls"
            f" FROM `{table_ref('sw_usage_counter')}` ORDER BY date DESC, username, api"
        ).result())
        return [{
            "date": str(r["date"]),
            "username": r["username"],
            "api": r["api"],
            "calls": r["calls"],
        } for r in rows]
    except Exception as e:
        logger.error(f"get_api_usage_summary error: {e}")
        return []
