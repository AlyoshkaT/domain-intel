"""
BigQuery client - all read/write operations
"""
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import os

from config.settings import (
    GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_LOCATION,
    GOOGLE_APPLICATION_CREDENTIALS,
    GOOGLE_CORP_CREDENTIALS, CORP_PROJECT_ID, CORP_DATASET,
    BQ_BUILTWITH_CACHE, BQ_SIMILARWEB_CACHE, BQ_WHATCMS_CACHE,
    BQ_JOBS_TABLE, BQ_RESULTS_TABLE
)

logger = logging.getLogger(__name__)

BQ_AI_CACHE = "ai_cache"


def _make_client(creds_path: str, project_id: str) -> bigquery.Client:
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        return bigquery.Client(project=project_id, credentials=credentials)
    return bigquery.Client(project=project_id)


# ── Main client (domain-intel-dev) ────────────────────────────────────────────
_client: Optional[bigquery.Client] = None

def client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = _make_client(GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID)
    return _client


# ── Corp client (esoteric-parsec-147012) — for cache tables ───────────────────
_corp_client: Optional[bigquery.Client] = None

def corp_client() -> bigquery.Client:
    global _corp_client
    if _corp_client is None:
        _corp_client = _make_client(GOOGLE_CORP_CREDENTIALS, CORP_PROJECT_ID)
    return _corp_client


def table_ref(table_name: str) -> str:
    return f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"


def corp_table_ref(table_name: str) -> str:
    return f"{CORP_PROJECT_ID}.{CORP_DATASET}.{table_name}"


# ─── Schema definitions ───────────────────────────────────────────────────────

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
    bigquery.SchemaField("wcms_name", "STRING"),
    bigquery.SchemaField("wcms_confidence", "FLOAT"),
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


# ─── Table initialization ─────────────────────────────────────────────────────

def ensure_tables_exist():
    bq = client()

    cache_schema = [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
        bigquery.SchemaField("response_json", "STRING"),
    ]

    ai_cache_schema = [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
        bigquery.SchemaField("ai_category", "STRING"),
        bigquery.SchemaField("ai_is_ecommerce", "STRING"),
        bigquery.SchemaField("ai_industry", "STRING"),
    ]

    tables_to_create = {
        BQ_JOBS_TABLE: JOBS_SCHEMA,
        BQ_RESULTS_TABLE: RESULTS_SCHEMA,
        BQ_WHATCMS_CACHE: cache_schema,
        BQ_AI_CACHE: ai_cache_schema,
    }

    for table_name, schema in tables_to_create.items():
        table_ref_obj = bigquery.Table(
            f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}", schema=schema
        )
        try:
            bq.get_table(table_ref_obj)
            logger.info(f"Table {table_name} already exists")
        except Exception:
            bq.create_table(table_ref_obj)
            logger.info(f"Created table {table_name}")

    # Verify corp cache tables exist
    bq_corp = corp_client()
    for table_name in [BQ_BUILTWITH_CACHE, BQ_SIMILARWEB_CACHE]:
        try:
            bq_corp.get_table(f"{CORP_PROJECT_ID}.{CORP_DATASET}.{table_name}")
            logger.info(f"Table {table_name} already exists")
        except Exception as e:
            logger.warning(f"Corp cache table {table_name} not found: {e}")


# ─── Cache read/write (corp BQ) ───────────────────────────────────────────────

def get_cached(table: str, domain: str, ttl_days: int = 90, force: bool = False, ignore_ttl: bool = False) -> Optional[dict]:
    if force:
        return None
    bq = corp_client()
    t_start = time.time()
    logger.info(f"Cache lookup: {table} / {domain}")

    ttl_clause = f"AND fetched_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {ttl_days} DAY)" if not ignore_ttl else ""
    query = f"""
        SELECT response_json, fetched_at
        FROM `{corp_table_ref(table)}`
        WHERE domain = @domain
          {ttl_clause}
        ORDER BY fetched_at DESC
        LIMIT 1
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
            if isinstance(data, dict):
                return data
            return json.loads(data)
        else:
            logger.info(f"Cache MISS: {table} / {domain} ({elapsed:.1f}s)")
            return None
    except Exception as e:
        logger.error(f"Cache read error ({table}, {domain}): {e}")
        return None


def save_cache(table: str, domain: str, data: dict):
    """Save API response to corporate cache table (streaming insert)."""
    bq = corp_client()
    row = {
        "domain": domain,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "response_json": json.dumps(data),
    }
    try:
        errors = bq.insert_rows_json(corp_table_ref(table), [row])
        if errors:
            logger.error(f"Cache write error ({table}, {domain}): {errors}")
        else:
            logger.info(f"Cache saved: {table} / {domain}")
    except Exception as e:
        logger.error(f"Cache write exception ({table}, {domain}): {e}")


# ─── AI Cache (our BQ) ────────────────────────────────────────────────────────

def get_ai_cached(domain: str, ignore_ttl: bool = False) -> Optional[dict]:
    """Read AI classification from our own BQ cache."""
    bq = client()
    ttl_clause = "AND fetched_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)" if not ignore_ttl else ""
    try:
        rows = list(bq.query(f"""
            SELECT ai_category, ai_is_ecommerce, ai_industry
            FROM `{table_ref(BQ_AI_CACHE)}`
            WHERE domain = @domain
            {ttl_clause}
            ORDER BY fetched_at DESC LIMIT 1
        """, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("domain", "STRING", domain)]
        )).result())
        if rows:
            logger.info(f"AI cache HIT: {domain}")
            return dict(rows[0])
        return None
    except Exception as e:
        logger.error(f"AI cache read error ({domain}): {e}")
        return None


def save_ai_cache(domain: str, ai_category: str, ai_is_ecommerce: str, ai_industry: str):
    """Save AI classification to our own BQ cache."""
    bq = client()
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        params = [
            bigquery.ScalarQueryParameter("domain", "STRING", domain),
            bigquery.ScalarQueryParameter("fetched_at", "TIMESTAMP", fetched_at),
            bigquery.ScalarQueryParameter("ai_category", "STRING", ai_category or ""),
            bigquery.ScalarQueryParameter("ai_is_ecommerce", "STRING", ai_is_ecommerce or ""),
            bigquery.ScalarQueryParameter("ai_industry", "STRING", ai_industry or ""),
        ]
        sql = f"""
            INSERT INTO `{table_ref(BQ_AI_CACHE)}`
            (domain, fetched_at, ai_category, ai_is_ecommerce, ai_industry)
            VALUES (@domain, @fetched_at, @ai_category, @ai_is_ecommerce, @ai_industry)
        """
        bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        logger.info(f"AI cache saved: {domain}")
    except Exception as e:
        logger.error(f"AI cache save error ({domain}): {e}")


# ─── Jobs ─────────────────────────────────────────────────────────────────────

def create_job(job_id: str, total_domains: int, services: list[str], filename: str):
    bq = client()
    created_at = datetime.now(timezone.utc).isoformat()
    services_json = json.dumps(services).replace("'", "''")
    filename_escaped = (filename or "").replace("'", "''")
    sql = f"""
        INSERT INTO `{table_ref(BQ_JOBS_TABLE)}`
        (job_id, created_at, updated_at, status, total_domains,
         processed_domains, failed_domains, services, filename, error_message)
        VALUES
        ('{job_id}', '{created_at}', '{created_at}', 'pending',
         {total_domains}, 0, 0, '{services_json}', '{filename_escaped}', NULL)
    """
    bq.query(sql).result()


def update_job(job_id: str, **kwargs):
    bq = client()
    kwargs["updated_at"] = datetime.now(timezone.utc).isoformat()

    set_parts = []
    for k, v in kwargs.items():
        if v is None:
            set_parts.append(f"{k} = NULL")
        elif isinstance(v, (int, float)):
            set_parts.append(f"{k} = {v}")
        else:
            escaped = str(v).replace("'", "''")
            set_parts.append(f"{k} = '{escaped}'")

    sql = f"""
        UPDATE `{table_ref(BQ_JOBS_TABLE)}`
        SET {', '.join(set_parts)}
        WHERE job_id = '{job_id}'
    """
    try:
        bq.query(sql).result()
    except Exception as e:
        logger.error(f"update_job error: {e}")


def get_job(job_id: str) -> Optional[dict]:
    bq = client()
    query = f"SELECT * FROM `{table_ref(BQ_JOBS_TABLE)}` WHERE job_id = @job_id LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    )
    rows = list(bq.query(query, job_config=job_config).result())
    if rows:
        row = dict(rows[0])
        row["services"] = json.loads(row.get("services") or "[]")
        return row
    return None


def list_jobs(limit: int = 50) -> list[dict]:
    bq = client()
    query = f"""
        SELECT * FROM `{table_ref(BQ_JOBS_TABLE)}`
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    rows = list(bq.query(query).result())
    result = []
    for row in rows:
        r = dict(row)
        r["services"] = json.loads(r.get("services") or "[]")
        result.append(r)
    return result


# ─── Results ──────────────────────────────────────────────────────────────────

def save_result(result: dict):
    """Save result via parameterized INSERT — safe for any content."""
    bq = client()
    try:
        cols = []
        placeholders = []
        params = []
        for k, v in result.items():
            cols.append(k)
            if v is None:
                placeholders.append("NULL")
            elif isinstance(v, float):
                placeholders.append(f"@p_{k}")
                params.append(bigquery.ScalarQueryParameter(f"p_{k}", "FLOAT64", v))
            elif isinstance(v, int):
                placeholders.append(f"@p_{k}")
                params.append(bigquery.ScalarQueryParameter(f"p_{k}", "INT64", v))
            else:
                placeholders.append(f"@p_{k}")
                params.append(bigquery.ScalarQueryParameter(f"p_{k}", "STRING", str(v)))

        sql = f"INSERT INTO `{table_ref(BQ_RESULTS_TABLE)}` ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
        bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        logger.info(f"Result saved OK: {result.get('domain')}")
    except Exception as e:
        logger.error(f"Result write error: {e}")


def get_results(job_id: str) -> list[dict]:
    bq = client()
    query = f"""
        SELECT * FROM `{table_ref(BQ_RESULTS_TABLE)}`
        WHERE job_id = @job_id
        ORDER BY processed_at DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
    )
    return [dict(row) for row in bq.query(query, job_config=job_config).result()]
