"""
SimilarWeb API service — via RapidAPI
POST https://similarweb-api1.p.rapidapi.com/v1/visitsInfo
"""
import asyncio
import httpx
import json
import logging
from typing import Optional
from config.settings import SIMILARWEB_RAPIDAPI_KEY, REQUEST_TIMEOUT, RATE_LIMIT_WAIT, SW_CONCURRENCY
from core.bigquery import get_cached, save_cache

logger = logging.getLogger(__name__)

SIMILARWEB_CACHE_TABLE = "similarweb_raw_data"
SIMILARWEB_URL = "https://similarweb-api1.p.rapidapi.com/v1/visitsInfo"

# Global semaphore — limits concurrent SW API calls regardless of BATCH_CONCURRENCY.
# Prevents flooding the RapidAPI endpoint and triggering 429s.
# Value tunable via SW_CONCURRENCY env var (default 3).
_sw_semaphore: asyncio.Semaphore | None = None

def _get_sw_semaphore() -> asyncio.Semaphore:
    global _sw_semaphore
    if _sw_semaphore is None:
        _sw_semaphore = asyncio.Semaphore(SW_CONCURRENCY)
    return _sw_semaphore


# Sentinel returned when SW API is rate-limited (429) — distinguishable from genuine 0 traffic.
SW_RATE_LIMITED = {"_sw_rate_limited": True}

# Global cooldown: when ANY call hits 429, every SW call waits until this moment
# before firing — so one rate-limit signal throttles the whole queue, instead of
# every domain independently hammering the API and getting its own 429.
_sw_cooldown_until: float = 0.0


async def _respect_cooldown():
    import time as _t
    wait = _sw_cooldown_until - _t.monotonic()
    if wait > 0:
        await asyncio.sleep(min(wait, 30))


def _set_cooldown(seconds: float):
    import time as _t
    global _sw_cooldown_until
    target = _t.monotonic() + seconds
    if target > _sw_cooldown_until:
        _sw_cooldown_until = target


async def fetch_similarweb(domain: str, _retries: int = 5) -> Optional[dict]:
    """
    Fetch SimilarWeb data via RapidAPI with rate-limit retry + concurrency cap.
    Returns SW_RATE_LIMITED sentinel (not None) on persistent 429 so callers can
    distinguish "rate limited" from "genuinely no data".
    """
    if not SIMILARWEB_RAPIDAPI_KEY:
        logger.warning("SIMILARWEB_RAPIDAPI_KEY not set")
        return None

    headers = {
        "x-rapidapi-key": SIMILARWEB_RAPIDAPI_KEY,
        "x-rapidapi-host": "similarweb-api1.p.rapidapi.com",
        "Content-Type": "application/json",
    }
    payload = {"q": domain}

    async with _get_sw_semaphore():
        for attempt in range(_retries):
            try:
                # Respect a global cooldown set by a recent 429 from any domain
                await _respect_cooldown()
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    resp = await client.post(SIMILARWEB_URL, json=payload, headers=headers)

                    if resp.status_code == 429:
                        # Per-domain exponential backoff: 10s, 20s, 40s, 80s, 160s
                        wait = RATE_LIMIT_WAIT * (2 ** attempt)
                        # Global cooldown is a SHORT shared nudge so concurrent calls
                        # don't all retry at once — capped so one domain's deep backoff
                        # never freezes the whole SW lane (that throttled us to ~17/h).
                        _set_cooldown(min(wait, RATE_LIMIT_WAIT))
                        logger.warning(
                            f"SW 429 for {domain} — backoff {wait}s "
                            f"(attempt {attempt+1}/{_retries})"
                        )
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()

                    try:
                        from services.credits import update_similarweb_credits_from_headers
                        update_similarweb_credits_from_headers(dict(resp.headers))
                    except Exception:
                        pass

                    data = resp.json()
                    await asyncio.to_thread(save_cache, SIMILARWEB_CACHE_TABLE, domain, data)
                    try:
                        from core.bigquery import save_sw_parsed
                        parsed = parse_similarweb(data)
                        await asyncio.to_thread(save_sw_parsed, domain, parsed)
                    except Exception as _e:
                        logger.warning(f"save_sw_parsed failed for {domain}: {_e}")
                    return data

            except httpx.TimeoutException:
                logger.warning(f"SW timeout for {domain} (attempt {attempt+1}/{_retries})")
                if attempt < _retries - 1:
                    await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"SimilarWeb error for {domain}: {e}")
                break

    logger.warning(f"SW failed for {domain} after {_retries} attempts — returning rate-limited sentinel")
    return SW_RATE_LIMITED


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

    # Extended: monthly visits history (all months from raw dict)
    monthly_raw = data.get("EstimatedMonthlyVisits", {}) or {}
    sw_monthly_visits = json.dumps(monthly_raw) if monthly_raw else "{}"

    # Extended: global rank
    gr = data.get("GlobalRank")
    if isinstance(gr, dict):
        sw_global_rank = gr.get("Rank")
    elif isinstance(gr, (int, float)):
        sw_global_rank = int(gr)
    else:
        sw_global_rank = None

    # Extended: engagement metrics
    eng = engagments or {}
    sw_engagement = json.dumps({
        "bounce_rate": eng.get("BounceRate"),
        "pages_per_visit": eng.get("PagePerVisit"),
        "avg_visit_duration": eng.get("TimeOnSite"),
    })

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
        # Extended fields
        "sw_monthly_visits": sw_monthly_visits,
        "sw_global_rank": sw_global_rank,
        "sw_engagement": sw_engagement,
    }
