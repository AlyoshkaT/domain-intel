"""
Credits service — tracks remaining API credits for BuiltWith and SimilarWeb.
Stores in app_settings table in our BQ.
"""
import httpx
import logging
from datetime import datetime, timezone
from typing import Optional
from google.cloud import bigquery

from core.bigquery import client, table_ref

logger = logging.getLogger(__name__)

APP_SETTINGS_TABLE = "app_settings"

APP_SETTINGS_SCHEMA = [
    bigquery.SchemaField("key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

# In-memory cache to avoid BQ reads on every request
_credits_cache: dict = {}


def ensure_app_settings_table():
    bq = client()
    from config.settings import GCP_PROJECT_ID, BIGQUERY_DATASET
    table_obj = bigquery.Table(
        f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{APP_SETTINGS_TABLE}",
        schema=APP_SETTINGS_SCHEMA
    )
    try:
        bq.get_table(table_obj)
    except Exception:
        bq.create_table(table_obj)
        logger.info(f"Created table {APP_SETTINGS_TABLE}")


def _save_setting(key: str, value: str):
    """Save or update a setting in BQ."""
    bq = client()
    updated_at = datetime.now(timezone.utc).isoformat()
    try:
        # Try UPDATE first
        result = bq.query(f"""
            UPDATE `{table_ref(APP_SETTINGS_TABLE)}`
            SET value = @value, updated_at = @updated_at
            WHERE key = @key
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("key", "STRING", key),
            bigquery.ScalarQueryParameter("value", "STRING", value),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", updated_at),
        ])).result()

        # If no rows updated — INSERT
        bq.query(f"""
            INSERT INTO `{table_ref(APP_SETTINGS_TABLE)}` (key, value, updated_at)
            SELECT @key, @value, @updated_at
            WHERE NOT EXISTS (
                SELECT 1 FROM `{table_ref(APP_SETTINGS_TABLE)}` WHERE key = @key
            )
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("key", "STRING", key),
            bigquery.ScalarQueryParameter("value", "STRING", value),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", updated_at),
        ])).result()
    except Exception as e:
        logger.error(f"app_settings save error ({key}): {e}")


def _get_setting(key: str) -> Optional[str]:
    bq = client()
    try:
        rows = list(bq.query(f"""
            SELECT value FROM `{table_ref(APP_SETTINGS_TABLE)}`
            WHERE key = @key LIMIT 1
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("key", "STRING", key),
        ])).result())
        return rows[0]["value"] if rows else None
    except Exception:
        return None


# ─── BuiltWith credits ────────────────────────────────────────────────────────

async def fetch_builtwith_credits() -> Optional[int]:
    """Fetch remaining BuiltWith credits from whoami endpoint."""
    from config.settings import BUILTWITH_API_KEY
    if not BUILTWITH_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(
                "https://api.builtwith.com/whoamiv1/api.json",
                params={"KEY": BUILTWITH_API_KEY}
            )
            resp.raise_for_status()
            data = resp.json()
            remaining = data.get("credits", {}).get("remaining")
            if remaining is not None:
                remaining = int(remaining)
                _credits_cache["builtwith"] = remaining
                _save_setting("builtwith_credits_remaining", str(remaining))
                logger.info(f"BuiltWith credits: {remaining}")
            return remaining
    except Exception as e:
        logger.error(f"BuiltWith credits fetch error: {e}")
        return None


def update_similarweb_credits_from_headers(headers: dict):
    """Extract and save SimilarWeb credits from response headers."""
    remaining = headers.get("x-ratelimit-requests-remaining")
    if remaining is not None:
        try:
            remaining = int(remaining)
            _credits_cache["similarweb"] = remaining
            _save_setting("similarweb_credits_remaining", str(remaining))
            logger.info(f"SimilarWeb credits updated: {remaining}")
        except ValueError:
            pass


def get_cached_credits() -> dict:
    """Get credits from in-memory cache, fallback to BQ."""
    result = {}

    # BuiltWith
    if "builtwith" in _credits_cache:
        result["builtwith"] = _credits_cache["builtwith"]
    else:
        val = _get_setting("builtwith_credits_remaining")
        if val is not None:
            result["builtwith"] = int(val)
            _credits_cache["builtwith"] = int(val)

    # SimilarWeb
    if "similarweb" in _credits_cache:
        result["similarweb"] = _credits_cache["similarweb"]
    else:
        val = _get_setting("similarweb_credits_remaining")
        if val is not None:
            result["similarweb"] = int(val)
            _credits_cache["similarweb"] = int(val)

    return result
