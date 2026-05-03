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

logger = logging.getLogger(__name__)


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


def ensure_tables_exist():
    bq = client()
    tables_to_create = {
        BQ_JOBS_TABLE: JOBS_SCHEMA,
        BQ_RESULTS_TABLE: RESULTS_SCHEMA,
    }
    for table_name, schema in tables_to_create.items():
        table_ref_obj = bigquery.Table(f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}", schema=schema)
        try:
            bq.get_table(table_ref_obj)
            logger.info(f"Table {table_name} already exists")
        except Exception:
            bq.create_table(table_ref_obj)
            logger.info(f"Created table {table_name}")
    bq_corp = corp_client()
    for table_name in [BQ_BUILTWITH_CACHE, BQ_SIMILARWEB_CACHE]:
        try:
            bq_corp.get_table(f"{CORP_PROJECT_ID}.{CORP_DATASET}.{table_name}")
        except Exception as e:
            logger.warning(f"Corp table {table_name} not found: {e}")


def get_cached(table: str, domain: str, ttl_days: int = 90, force: bool = False, ignore_ttl: bool = False) -> Optional[dict]:
    if force:
        return None
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
    bq = client()
    try:
        cols, placeholders, params = [], [], []
        for k, v in result.items():
            cols.append(k)
            if v is None: placeholders.append("NULL")
            elif isinstance(v, float): placeholders.append(f"@p_{k}"); params.append(bigquery.ScalarQueryParameter(f"p_{k}", "FLOAT64", v))
            elif isinstance(v, int): placeholders.append(f"@p_{k}"); params.append(bigquery.ScalarQueryParameter(f"p_{k}", "INT64", v))
            else: placeholders.append(f"@p_{k}"); params.append(bigquery.ScalarQueryParameter(f"p_{k}", "STRING", str(v)))
        sql = f"INSERT INTO `{table_ref(BQ_RESULTS_TABLE)}` ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
        bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
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
    global _settings_cache, _settings_cached_at
    _ensure_settings_table()
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('app_settings')}` WHERE key = @key",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("key", "STRING", key)])
    ).result()
    errors = bq.insert_rows_json(table_ref("app_settings"), [{
        "key": key, "value": value,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }])
    if errors:
        logger.error(f"set_setting errors: {errors}")
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


def _ensure_users_table():
    bq = client()
    tbl = bigquery.Table(table_ref("app_users"), schema=_USERS_SCHEMA)
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)
        logger.info("Created table app_users")


def get_users() -> list[dict]:
    try:
        bq = client()
        rows = list(bq.query(
            f"SELECT username, permissions, created_at FROM `{table_ref('app_users')}` ORDER BY created_at"
        ).result())
        return [{"username": r["username"], "permissions": r["permissions"],
                 "created_at": str(r["created_at"])} for r in rows]
    except Exception as e:
        logger.error(f"get_users error: {e}")
        return []


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


def add_user(username: str, password: str, permissions: str):
    _ensure_users_table()
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('app_users')}` WHERE username = @u",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("u", "STRING", username)])
    ).result()
    errors = bq.insert_rows_json(table_ref("app_users"), [{
        "username": username, "password": password,
        "permissions": permissions,
        "created_at": datetime.now(timezone.utc).isoformat()
    }])
    if errors:
        logger.error(f"add_user errors: {errors}")


def remove_user(username: str):
    bq = client()
    bq.query(
        f"DELETE FROM `{table_ref('app_users')}` WHERE username = @u",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("u", "STRING", username)])
    ).result()
