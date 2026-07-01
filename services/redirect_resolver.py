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


# In-memory redirect cache, populated once per job by prefetch_redirects().
# Replaces 1 BQ query per domain (each billed the 10MB minimum → ~$0.89/15K job)
# with a single batched query.
_redirect_cache: dict[str, Optional[str]] = {}
_redirect_prefetch_active = False
_REDIRECT_CHUNK = 5000


def _clean_resolved(resolved: Optional[str]) -> Optional[str]:
    """Reject stored redirects with a port (legacy bug → 'site.com:443')."""
    if resolved and ":" in resolved:
        return None
    return resolved or None


def prefetch_redirects(domains: list[str]) -> None:
    """
    Batch-load known redirects for the whole job in one (chunked) query.
    After this, get_known_redirect() serves from memory — no per-domain BQ query.
    """
    global _redirect_prefetch_active
    if not domains:
        return
    bq = client()
    uniq = list(dict.fromkeys(domains))
    hits = 0
    try:
        for i in range(0, len(uniq), _REDIRECT_CHUNK):
            chunk = uniq[i:i + _REDIRECT_CHUNK]
            ph = ", ".join(f"@d{j}" for j in range(len(chunk)))
            params = [bigquery.ScalarQueryParameter(f"d{j}", "STRING", d) for j, d in enumerate(chunk)]
            rows = list(bq.query(
                f"""
                SELECT original, resolved FROM (
                    SELECT original, resolved,
                           ROW_NUMBER() OVER (PARTITION BY original ORDER BY detected_at DESC) rn
                    FROM `{table_ref(REDIRECTS_TABLE)}`
                    WHERE original IN ({ph})
                ) WHERE rn = 1
                """,
                job_config=bigquery.QueryJobConfig(query_parameters=params)
            ).result())
            for r in rows:
                _redirect_cache[r["original"]] = _clean_resolved(r["resolved"])
                hits += 1
        # Mark misses so get_known_redirect won't fall through to BQ
        for d in uniq:
            if d not in _redirect_cache:
                _redirect_cache[d] = None
        _redirect_prefetch_active = True
        logger.info(f"prefetch_redirects: {hits} known redirects for {len(uniq)} domains (1 query)")
    except Exception as e:
        logger.error(f"prefetch_redirects error: {e}")


def clear_redirect_cache() -> None:
    """Clear redirect cache after a job finishes."""
    global _redirect_prefetch_active
    _redirect_cache.clear()
    _redirect_prefetch_active = False


def get_known_redirect(domain: str) -> Optional[str]:
    """Check if we already know where this domain redirects to."""
    # Fast path: from the batch prefetch cache (no BQ query)
    if domain in _redirect_cache:
        return _redirect_cache[domain]
    # During a job, a miss means it wasn't in the prefetch → skip the per-domain query
    if _redirect_prefetch_active:
        return None
    bq = client()
    try:
        rows = list(bq.query(f"""
            SELECT resolved FROM `{table_ref(REDIRECTS_TABLE)}`
            WHERE original = '{domain}'
            ORDER BY detected_at DESC
            LIMIT 1
        """).result())
        if rows:
            return _clean_resolved(rows[0]["resolved"])
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
        _redirect_cache[original] = _clean_resolved(resolved)  # keep in-job cache consistent
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
        # Use .hostname (not .netloc) — .netloc includes port e.g. "site.com:443"
        domain = (parsed.hostname or "").lower()
        if domain.startswith("www."):
            domain = domain[4:]
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
    import asyncio as _asyncio

    # 1. Check known redirects in BQ (run in thread — sync BQ query)
    known = await _asyncio.to_thread(get_known_redirect, domain)
    if known:
        logger.info(f"Known redirect: {domain} → {known}")
        return known, "known"

    # 2. Check HTTP redirect
    resolved = await check_http_redirect(domain)
    if resolved and resolved != domain:
        orig_parts = domain.split(".")
        res_parts  = resolved.split(".")

        if orig_parts[-2:] == res_parts[-2:]:
            redirect_type = "subdomain"
        else:
            redirect_type = "http_redirect"

        _asyncio.get_event_loop().run_in_executor(
            None, save_redirect, domain, resolved, redirect_type, job_id)
        return resolved, redirect_type

    return domain, "none"
