"""
Redirect resolver service
Checks HTTP redirects and manages domain_redirects table in BQ.
"""
import httpx
import logging
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

from core.bigquery import client, table_ref
from google.cloud import bigquery

logger = logging.getLogger(__name__)

REDIRECTS_TABLE = "domain_redirects"

REDIRECTS_SCHEMA = [
    bigquery.SchemaField("original", "STRING"),
    bigquery.SchemaField("resolved", "STRING"),
    bigquery.SchemaField("type", "STRING"),      # www / subdomain / http_redirect
    bigquery.SchemaField("detected_at", "TIMESTAMP"),
    bigquery.SchemaField("job_id", "STRING"),
]


def ensure_redirects_table():
    """Create domain_redirects table if not exists."""
    bq = client()
    from config.settings import GCP_PROJECT_ID, BIGQUERY_DATASET
    table_obj = bigquery.Table(
        f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{REDIRECTS_TABLE}",
        schema=REDIRECTS_SCHEMA
    )
    try:
        bq.get_table(table_obj)
    except Exception:
        bq.create_table(table_obj)
        logger.info(f"Created table {REDIRECTS_TABLE}")


def get_known_redirect(domain: str) -> Optional[str]:
    """Check if we already know where this domain redirects to."""
    bq = client()
    try:
        rows = list(bq.query(f"""
            SELECT resolved FROM `{table_ref(REDIRECTS_TABLE)}`
            WHERE original = '{domain}'
            ORDER BY detected_at DESC
            LIMIT 1
        """).result())
        if rows:
            return rows[0]["resolved"]
        return None
    except Exception as e:
        logger.error(f"Redirect lookup error for {domain}: {e}")
        return None


def save_redirect(original: str, resolved: str, redirect_type: str, job_id: str):
    """Save redirect record to BQ."""
    bq = client()
    detected_at = datetime.now(timezone.utc).isoformat()
    original_e = original.replace("'", "''")
    resolved_e = resolved.replace("'", "''")
    job_id_e = job_id.replace("'", "''")
    sql = f"""
        INSERT INTO `{table_ref(REDIRECTS_TABLE)}`
        (original, resolved, type, detected_at, job_id)
        VALUES
        ('{original_e}', '{resolved_e}', '{redirect_type}', '{detected_at}', '{job_id_e}')
    """
    try:
        bq.query(sql).result()
        logger.info(f"Redirect saved: {original} → {resolved} ({redirect_type})")
    except Exception as e:
        logger.error(f"Redirect save error: {e}")


async def check_http_redirect(domain: str, timeout: int = 5) -> Optional[str]:
    """
    Check if domain redirects to another domain via HTTP.
    Returns resolved domain or None if no redirect.
    """
    for scheme in ["https", "http"]:
        url = f"{scheme}://{domain}"
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=False,
                headers={"User-Agent": "Mozilla/5.0 (compatible; DomainIntel/1.0)"}
            ) as client_http:
                resp = await client_http.get(url)
                if resp.status_code in (301, 302, 303, 307, 308):
                    location = resp.headers.get("location", "")
                    if location:
                        resolved = _extract_domain(location)
                        if resolved and resolved != domain:
                            logger.info(f"HTTP redirect: {domain} → {resolved}")
                            return resolved
            return None  # No redirect
        except Exception:
            continue
    return None


def _extract_domain(url: str) -> Optional[str]:
    try:
        if not url.startswith("http"):
            url = "https://" + url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        # Перевірка що це реальний домен (має крапку і мінімум 4 символи)
        if not domain or "." not in domain or len(domain) < 4:
            return None
        return domain
    except Exception:
        return None


async def resolve_domain(domain: str, job_id: str) -> tuple[str, str]:
    """
    Full domain resolution pipeline.
    Returns (resolved_domain, redirect_type).
    redirect_type: 'none' | 'www' | 'subdomain' | 'http_redirect'
    """
    # 1. Check known redirects in BQ
    known = get_known_redirect(domain)
    if known:
        logger.info(f"Known redirect: {domain} → {known}")
        return known, "known"

    # 2. Check HTTP redirect
    resolved = await check_http_redirect(domain)
    if resolved and resolved != domain:
        # Determine type
        orig_parts = domain.split(".")
        res_parts = resolved.split(".")

        if orig_parts[-2:] == res_parts[-2:]:
            redirect_type = "subdomain"
        else:
            redirect_type = "http_redirect"

        save_redirect(domain, resolved, redirect_type, job_id)
        return resolved, redirect_type

    return domain, "none"
