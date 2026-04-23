"""
WhatCMS API service
"""
import asyncio
import httpx
import logging
from typing import Optional
from config.settings import WHATCMS_API_KEY, REQUEST_TIMEOUT, WHATCMS_MAX_RETRIES
from core.bigquery import client, table_ref
from google.cloud import bigquery as bq_lib
import json
from datetime import datetime, timezone

WHATCMS_CACHE_TABLE = "whatcms_raw_data"

def _get_whatcms_cached(domain: str) -> dict | None:
    """Read WhatCMS cache from our own BQ (not corp)."""
    bq = client()
    try:
        rows = list(bq.query(f"""
            SELECT response_json FROM `{table_ref(WHATCMS_CACHE_TABLE)}`
            WHERE domain = '{domain}'
            # AND fetched_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            ORDER BY fetched_at DESC LIMIT 1
        """).result())
        if rows:
            data = rows[0]["response_json"]
            return data if isinstance(data, dict) else json.loads(data)
        return None
    except Exception:
        return None

def _save_whatcms_cache(domain: str, data: dict):
    """Save WhatCMS cache to our own BQ."""
    bq = client()
    escaped = json.dumps(data).replace("'", "''")
    fetched_at = datetime.now(timezone.utc).isoformat()
    try:
        bq.query(f"""
            INSERT INTO `{table_ref(WHATCMS_CACHE_TABLE)}`
            (domain, fetched_at, response_json)
            VALUES ('{domain}', '{fetched_at}', '{escaped}')
        """).result()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"WhatCMS cache save error: {e}")

logger = logging.getLogger(__name__)


async def fetch_whatcms(domain: str, force_refresh: bool = False, api_call: bool = True) -> Optional[dict]:
    cached = None
    if not force_refresh:
        cached = _get_whatcms_cached(domain)
    if cached:
        logger.debug(f"WhatCMS cache hit: {domain}")
        return cached
    if not api_call:
        return None  # є кеш miss але API не вибрано

    if not WHATCMS_API_KEY:
        logger.warning("WHATCMS_API_KEY not set")
        return None

    for attempt in range(WHATCMS_MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.get(
                    "https://whatcms.org/API/Tech",
                    params={"key": WHATCMS_API_KEY, "url": domain}
                )
                if resp.status_code == 429:
                    wait = (attempt + 1) * 5
                    logger.warning(f"WhatCMS rate limit, waiting {wait}s (attempt {attempt+1})")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                _save_whatcms_cache(domain, data)
                return data
        except Exception as e:
            logger.error(f"WhatCMS error for {domain} (attempt {attempt+1}): {e}")
            if attempt < WHATCMS_MAX_RETRIES:
                await asyncio.sleep(3)

    return None


def parse_whatcms(data: dict) -> dict:
    if not data:
        return {}
    # Новий формат: results[] — масив технологій
    results = data.get("results", [])
    if results:
        # Шукаємо CMS або E-commerce технологію
        cms = next(
            (r for r in results if any(
                c in ["CMS", "E-commerce"] for c in r.get("categories", [])
            )),
            results[0]  # якщо не знайшли — беремо першу
        )
        return {
            "wcms_name": cms.get("name", ""),
            "wcms_confidence": 100.0,  # WhatCMS не повертає confidence
        }
    # Старий формат: result.name
    result = data.get("result", {})
    return {
        "wcms_name": result.get("name", ""),
        "wcms_confidence": float(result.get("confidence", 0) or 0),
    }

