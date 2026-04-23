"""
SimilarWeb API service — via RapidAPI
POST https://similarweb-api1.p.rapidapi.com/v1/visitsInfo
"""
import httpx
import json
import logging
from typing import Optional
from config.settings import SIMILARWEB_RAPIDAPI_KEY, REQUEST_TIMEOUT
from core.bigquery import get_cached, save_cache

logger = logging.getLogger(__name__)

SIMILARWEB_CACHE_TABLE = "similarweb_raw_data"
SIMILARWEB_URL = "https://similarweb-api1.p.rapidapi.com/v1/visitsInfo"


async def fetch_similarweb(domain: str) -> Optional[dict]:
    """Fetch SimilarWeb data via RapidAPI. Returns cached if available."""
    if not SIMILARWEB_RAPIDAPI_KEY:
        logger.warning("SIMILARWEB_RAPIDAPI_KEY not set")
        return None

    headers = {
        "x-rapidapi-key": SIMILARWEB_RAPIDAPI_KEY,
        "x-rapidapi-host": "similarweb-api1.p.rapidapi.com",
        "Content-Type": "application/json",
    }
    payload = {"q": domain}

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            resp = await client.post(SIMILARWEB_URL, json=payload, headers=headers)
            if resp.status_code == 429:
                logger.warning(f"SimilarWeb rate limit for {domain}")
                return None
            resp.raise_for_status()

            # Extract credits from headers
            try:
                from services.credits import update_similarweb_credits_from_headers
                update_similarweb_credits_from_headers(dict(resp.headers))
            except Exception:
                pass

            data = resp.json()
            save_cache(SIMILARWEB_CACHE_TABLE, domain, data)
            return data
    except Exception as e:
        logger.error(f"SimilarWeb error for {domain}: {e}")
        return None


def parse_similarweb(data: dict) -> dict:
    """Extract key fields from SimilarWeb RapidAPI response."""
    if not data:
        return {}

    # Visits
    visits = 0
    engagments = data.get("Engagments", {})
    if engagments.get("Visits"):
        visits = float(engagments["Visits"])
    else:
        monthly = data.get("EstimatedMonthlyVisits", {})
        if monthly:
            visits = float(list(monthly.values())[-1])

    # Category
    category_rank = data.get("CategoryRank", {})
    category = (
        category_rank.get("Category") or
        data.get("Category") or ""
    )

    # Subcategory — split from category by "/"
    sw_category = category.split("/")[0] if "/" in category else category
    sw_subcategory = category.split("/")[1] if "/" in category else ""

    # Top countries
    top_countries = data.get("TopCountryShares") or []
    top_countries_clean = [
        {
            "country": c.get("CountryCode", ""),
            "value": round(c.get("Value", 0), 4)
        }
        for c in top_countries[:5]
    ]

    # Primary region
    primary_region = ""
    primary_region_pct = None
    if top_countries_clean:
        primary_region = top_countries_clean[0]["country"]
        primary_region_pct = round(top_countries_clean[0]["value"] * 100, 1)

    return {
        "sw_visits": visits,
        "sw_category": sw_category,
        "sw_subcategory": sw_subcategory,
        "sw_description": data.get("Description") or "",
        "sw_title": data.get("Title") or data.get("SiteName") or "",
        "sw_top_countries": json.dumps(top_countries_clean),
        "sw_primary_region": primary_region,
        "sw_primary_region_pct": primary_region_pct,
        "company_name": data.get("Title") or data.get("SiteName") or "",
    }
