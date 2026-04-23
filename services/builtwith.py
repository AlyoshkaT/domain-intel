"""
BuiltWith API service
"""
import httpx
import json
import logging
from typing import Optional
from config.settings import (
    BUILTWITH_API_KEY, BUILTWITH_RAPIDAPI_KEY,
    REQUEST_TIMEOUT, RATE_LIMIT_WAIT
)
from core.bigquery import get_cached, save_cache

logger = logging.getLogger(__name__)

from config.settings import BQ_BUILTWITH_CACHE
BUILTWITH_CACHE_TABLE = BQ_BUILTWITH_CACHE

# Technology tag mappings
CMS_TAGS = ["cms", "blog-software", "ecommerce"]
ECOMMERCE_TAGS = ["ecommerce", "shopping-cart"]
EMAIL_TAGS = ["email"]
SEARCH_TAGS = ["site-search", "search"]


async def fetch_builtwith(domain: str, mode: str = "direct") -> Optional[dict]:
    """Fetch BuiltWith data. Returns cached if available."""
    cached = get_cached(BUILTWITH_CACHE_TABLE, domain)
    if cached:
        logger.debug(f"BuiltWith cache hit: {domain}")
        return cached

    if mode == "direct" and BUILTWITH_API_KEY:
        data = await _fetch_direct(domain)
    elif mode == "rapidapi" and BUILTWITH_RAPIDAPI_KEY:
        data = await _fetch_rapidapi(domain)
    else:
        logger.warning(f"BuiltWith: no API key configured (mode={mode})")
        return None

    if data:
        save_cache(BUILTWITH_CACHE_TABLE, domain, data)
    return data


async def _fetch_direct(domain: str) -> Optional[dict]:
    url = "https://api.builtwith.com/v21/api.json"
    params = {"KEY": BUILTWITH_API_KEY, "LOOKUP": domain}
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            if resp.status_code == 429:
                logger.warning(f"BuiltWith rate limit for {domain}")
                return None
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"BuiltWith direct error for {domain}: {e}")
        return None


async def _fetch_rapidapi(domain: str) -> Optional[dict]:
    url = "https://builtwith.p.rapidapi.com/v21/api.json"
    headers = {
        "X-RapidAPI-Key": BUILTWITH_RAPIDAPI_KEY,
        "X-RapidAPI-Host": "builtwith.p.rapidapi.com",
    }
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.get(url, params={"LOOKUP": domain}, headers=headers)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"BuiltWith RapidAPI error for {domain}: {e}")
        return None


def _get_technologies(data: dict) -> list[dict]:
    """Extract flat list of technologies from BuiltWith response."""
    techs = []
    try:
        results = data.get("Results", [])
        for result in results:
            paths = result.get("Result", {}).get("Paths", [])
            for path in paths:
                for tech in path.get("Technologies", []):
                    techs.append(tech)
    except Exception:
        pass
    return techs


def parse_builtwith(data: dict) -> dict:
    """Extract key fields from BuiltWith response."""
    if not data:
        return {}

    techs = _get_technologies(data)
    tech_names = [t.get("Name", "") for t in techs]
    tech_tags = [(t.get("Tag", "").lower(), t.get("Name", "")) for t in techs]

    cms = next((name for tag, name in tech_tags if any(c in tag for c in CMS_TAGS)), "")
    ecommerce = next((name for tag, name in tech_tags if any(e in tag for e in ECOMMERCE_TAGS)), "")
    email_mkt = next((name for tag, name in tech_tags if any(e in tag for e in EMAIL_TAGS)), "")
    on_site_search = next((name for tag, name in tech_tags if any(s in tag for s in SEARCH_TAGS)), "")

    return {
        "bw_cms": cms,
        "bw_ecommerce": ecommerce,
        "bw_email_marketing": email_mkt,
        "bw_on_site_search": on_site_search,
        "bw_technologies": json.dumps(tech_names[:30]),  # top 30
    }


def extract_on_site_search(data: dict) -> str:
    """Extract OnSiteSearch specifically from BuiltWith JSON."""
    if not data:
        return ""
    parsed = parse_builtwith(data)
    return parsed.get("bw_on_site_search", "")


async def get_builtwith_credits() -> Optional[int]:
    """Get remaining BuiltWith API credits."""
    if not BUILTWITH_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.get(
                "https://api.builtwith.com/credits/v1/api.json",
                params={"KEY": BUILTWITH_API_KEY}
            )
            data = resp.json()
            return data.get("Remaining") or data.get("remaining")
    except Exception as e:
        logger.error(f"BuiltWith credits error: {e}")
        return None
