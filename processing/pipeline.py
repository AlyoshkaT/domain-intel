"""
Pipeline orchestrator - processes one domain through selected services.
Produces all 20 columns as per spec.
Cache is read for ALL services regardless of selection (enrichment).
API calls are made ONLY for selected services.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

from config.settings import DELAY_BETWEEN_API_CALLS
from core.bigquery import get_cached, save_cache, get_ai_cached, save_ai_cache
from services.similarweb import fetch_similarweb, parse_similarweb
from services.builtwith import fetch_builtwith, parse_builtwith
from services.whatcms import fetch_whatcms, parse_whatcms
from services.claude_ai import classify_domain, fetch_homepage_text
from services.technology_catalog import get_catalog, match_technologies
from services.redirect_resolver import resolve_domain

logger = logging.getLogger(__name__)

_catalog_cache: dict | None = None


def _get_catalog() -> dict:
    global _catalog_cache
    if _catalog_cache is None:
        _catalog_cache = get_catalog()
        logger.info(f"Catalog loaded: cms={len(_catalog_cache.get('cms',[]))}, "
                    f"osearch={len(_catalog_cache.get('osearch',[]))}, "
                    f"ems={len(_catalog_cache.get('ems',[]))}")
    return _catalog_cache


def reload_catalog():
    global _catalog_cache
    _catalog_cache = None


def _clean_domain(domain: str) -> str:
    """
    Normalize domain input to clean root domain.
    Examples:
      https://rozetka.com.ua/ua/phones/ → rozetka.com.ua
      www.example.com                   → example.com
      HTTP://EXAMPLE.COM                → example.com
      example.com/                      → example.com
    """
    if not domain:
        return ""
    
    domain = domain.strip().lower()
    
    # Remove protocol
    domain = domain.removeprefix("http://").removeprefix("https://")
    
    # Remove auth (user:pass@)
    if "@" in domain:
        domain = domain.split("@")[-1]
    
    # Remove path, query, fragment — keep only host
    domain = domain.split("/")[0]
    domain = domain.split("?")[0]
    domain = domain.split("#")[0]
    domain = domain.split(":")[0]  # Remove port
    
    # Remove www.
    if domain.startswith("www."):
        domain = domain[4:]
    
    # Remove trailing dots
    domain = domain.strip(".")
    
    # Basic validation — must have at least one dot and no spaces
    if "." not in domain or " " in domain or len(domain) < 4:
        return ""
    
    return domain


async def process_domain(
    domain: str,
    job_id: str,
    services: list[str],
    force_refresh: bool = False,
) -> dict:
    """
    Process a single domain through selected services.
    Cache is always read for enrichment (ignore_ttl=True).
    API calls only for selected services.
    """
    domain = _clean_domain(domain)

    result = {
        "job_id": job_id,
        "domain": domain,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "status": "ok",
        "error_detail": None,
        "sw_visits": None,
        "cms_list": None,
        "wcms_name": None,
        "wcms_confidence": None,
        "osearch_group": None,
        "osearch": None,
        "ems_list": None,
        "ai_category": None,
        "ai_is_ecommerce": None,
        "ai_industry": None,
        "bw_vertical": None,
        "bw_industry": None,
        "sw_category": None,
        "sw_subcategory": None,
        "sw_description": None,
        "sw_title": None,
        "sw_primary_region": None,
        "sw_primary_region_pct": None,
        "company_name": None,
        "osearch_parse": None,
        "sw_top_countries": None,
        "bw_technologies": None,
        "bw_cms_raw": None,
        "bw_ecommerce": None,
        "bw_email_marketing": None,
    }

    try:
        # ── Resolve domain (redirects) ─────────────────────────────────────────
        resolved_domain, redirect_type = await resolve_domain(domain, job_id)
        if resolved_domain != domain:
            logger.info(f"Using resolved domain: {resolved_domain} (was {domain})")
            result["domain"] = resolved_domain
        working_domain = resolved_domain

        catalog = _get_catalog()

        # ── SimilarWeb ────────────────────────────────────────────────────────
        # Cache read always (enrichment), API only if selected
        sw_data = None
        if not force_refresh:
            sw_data = get_cached("similarweb_raw_data", working_domain, ignore_ttl=True)
        if sw_data is None and "similarweb" in services:
            sw_data = await fetch_similarweb(working_domain)
        if sw_data:
            parsed = parse_similarweb(sw_data)
            result["sw_visits"] = parsed.get("sw_visits")
            result["sw_category"] = parsed.get("sw_category")
            result["sw_subcategory"] = parsed.get("sw_subcategory")
            result["sw_description"] = parsed.get("sw_description")
            result["sw_title"] = parsed.get("sw_title")
            result["sw_top_countries"] = parsed.get("sw_top_countries")
            result["sw_primary_region"] = parsed.get("sw_primary_region")
            result["sw_primary_region_pct"] = parsed.get("sw_primary_region_pct")
            result["company_name"] = parsed.get("company_name")
        if "similarweb" in services:
            await asyncio.sleep(DELAY_BETWEEN_API_CALLS / 1000)

        # ── BuiltWith ─────────────────────────────────────────────────────────
        # Cache read always (enrichment), API only if selected
        bw_data = None
        if not force_refresh:
            bw_data = get_cached("builtwith_raw_data", working_domain, ignore_ttl=True)
        if bw_data is None and "builtwith" in services:
            bw_data = await fetch_builtwith(working_domain)
        if bw_data:
            parsed = parse_builtwith(bw_data)
            bw_techs_json = parsed.get("bw_technologies", "[]")
            result["bw_technologies"] = bw_techs_json
            result["bw_cms_raw"] = parsed.get("bw_cms")
            result["bw_ecommerce"] = parsed.get("bw_ecommerce")
            result["bw_email_marketing"] = parsed.get("bw_email_marketing")
            try:
                bw_results = bw_data.get("Results", [])
                if bw_results:
                    vertical = bw_results[0].get("Result", {}).get("Vertical", "")
                    result["bw_vertical"] = vertical
                    result["bw_industry"] = vertical
            except Exception:
                pass
            matched = match_technologies(bw_data, catalog)
            result["cms_list"] = matched.get("cms_list")
            result["osearch"] = matched.get("osearch")
            result["osearch_group"] = matched.get("osearch_group")
            result["ems_list"] = matched.get("ems_list")
        if "builtwith" in services:
            await asyncio.sleep(DELAY_BETWEEN_API_CALLS / 1000)

        # ── WhatCMS ───────────────────────────────────────────────────────────
        # Cache read always (enrichment), API only if selected
        wcms_data = await fetch_whatcms(
            working_domain,
            force_refresh=force_refresh,
            api_call=("whatcms" in services)
        )
        if wcms_data:
            parsed = parse_whatcms(wcms_data)
            result["wcms_name"] = parsed.get("wcms_name")
            result["wcms_confidence"] = parsed.get("wcms_confidence")
        if "whatcms" in services:
            await asyncio.sleep(DELAY_BETWEEN_API_CALLS / 1000)

        # ── Claude AI ─────────────────────────────────────────────────────────
        # Cache read always (enrichment), API only if selected
        ai_cached = None
        if not force_refresh:
            ai_cached = get_ai_cached(working_domain, ignore_ttl=True)

        if ai_cached:
            result["ai_category"] = ai_cached.get("ai_category")
            result["ai_is_ecommerce"] = ai_cached.get("ai_is_ecommerce")
            result["ai_industry"] = ai_cached.get("ai_industry")
        elif "ai" in services:
            homepage_text = await fetch_homepage_text(working_domain)
            ai_result = await classify_domain(
                domain=working_domain,
                sw_title=result.get("sw_title") or "",
                sw_description=result.get("sw_description") or "",
                sw_category=result.get("sw_category") or "",
                bw_cms=result.get("cms_list") or "",
                bw_ecommerce=result.get("bw_ecommerce") or "",
                homepage_text=homepage_text,
            )
            if ai_result:
                result["ai_category"] = ai_result.get("ai_category")
                result["ai_is_ecommerce"] = ai_result.get("ai_is_ecommerce")
                result["ai_industry"] = ai_result.get("ai_industry")
                # Save to AI cache
                save_ai_cache(
                    working_domain,
                    ai_result.get("ai_category", ""),
                    ai_result.get("ai_is_ecommerce", ""),
                    ai_result.get("ai_industry", ""),
                )

    except Exception as e:
        logger.error(f"Pipeline error for {domain}: {e}")
        result["status"] = "error"
        result["error_detail"] = str(e)

    return result
