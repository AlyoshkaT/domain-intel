"""
Pipeline orchestrator - processes one domain through selected services.
Cache always read for enrichment. API calls only if service selected.
AI results saved to corpBQ claude_responses.
WhatCMS removed.
"""
import asyncio
import logging
from datetime import datetime, timezone

from config.settings import DELAY_BETWEEN_API_CALLS
from core.bigquery import get_cached, get_sw_parsed, get_bw_parsed, save_bw_parsed
from services.similarweb import fetch_similarweb, parse_similarweb
from services.builtwith import fetch_builtwith, parse_builtwith
from services.claude_ai import (
    classify_domain, fetch_homepage_text,
    get_corp_ai_cached, save_corp_ai_result, _make_input_hash
)
from services.technology_catalog import get_catalog, match_technologies
from services.redirect_resolver import resolve_domain

logger = logging.getLogger(__name__)

_catalog_cache: dict | None = None


def _get_catalog() -> dict:
    global _catalog_cache
    if _catalog_cache is None:
        _catalog_cache = get_catalog()
        logger.info(f"Catalog: cms={len(_catalog_cache.get('cms',[]))}, "
                    f"osearch={len(_catalog_cache.get('osearch',[]))}, "
                    f"ems={len(_catalog_cache.get('ems',[]))}")
    return _catalog_cache


def reload_catalog():
    global _catalog_cache
    _catalog_cache = None


def _clean_domain(domain: str) -> str:
    if not domain:
        return ""
    domain = domain.strip().lower()
    domain = domain.removeprefix("http://").removeprefix("https://")
    if "@" in domain:
        domain = domain.split("@")[-1]
    domain = domain.split("/")[0].split("?")[0].split("#")[0].split(":")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    domain = domain.strip(".")
    if "." not in domain or " " in domain or len(domain) < 4:
        return ""
    return domain


async def process_domain(
    domain: str,
    job_id: str,
    services: list[str],
    force_refresh: bool = False,
    username: str = "",
    skip_redirect: bool = False,
) -> dict:
    domain = _clean_domain(domain)

    result = {
        "job_id": job_id, "domain": domain,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "status": "ok", "error_detail": None,
        "sw_visits": None, "sw_category": None, "sw_subcategory": None,
        "sw_description": None, "sw_title": None, "sw_primary_region": None,
        "sw_primary_region_pct": None, "sw_top_countries": None, "company_name": None,
        "cms_list": None, "osearch_group": None, "osearch": None, "ems_list": None,
        "bw_vertical": None, "bw_industry": None, "bw_technologies": None,
        "bw_cms_raw": None, "bw_ecommerce": None, "bw_email_marketing": None,
        "ai_category": None, "ai_is_ecommerce": None, "ai_industry": None,
    }

    try:
        if skip_redirect:
            # Domains from Explorer/DB are already canonical — no need to resolve
            working_domain = domain
        else:
            resolved_domain, _ = await resolve_domain(domain, job_id)
            if resolved_domain != domain:
                logger.info(f"Redirect: {domain} → {resolved_domain}")
                result["domain"] = resolved_domain
            working_domain = resolved_domain
        catalog = _get_catalog()

        # ── SimilarWeb + BuiltWith ────────────────────────────────────────────
        sw_want = "similarweb" in services
        bw_want = "builtwith" in services

        # Cache strategy:
        # - force_refresh=True  → ignore privateBQ cache for selected services; non-selected still enriched
        # - force_refresh=False → check privateBQ parsed cache first; fetch from API only on miss
        # Non-selected services ALWAYS try privateBQ first, then fall back to corpBQ for enrichment.

        if sw_want:
            sw_parsed_cache = None if force_refresh else get_sw_parsed(working_domain)
        else:
            # Not selected — try privateBQ first, fall back to corpBQ raw for enrichment
            sw_parsed_cache = get_sw_parsed(working_domain)
            if sw_parsed_cache is None:
                sw_raw = get_cached("similarweb_raw_data", working_domain, ignore_ttl=True)
                if sw_raw:
                    sw_parsed_cache = parse_similarweb(sw_raw)

        if bw_want:
            bw_parsed_cache = None if force_refresh else get_bw_parsed(working_domain)
        else:
            bw_parsed_cache = get_bw_parsed(working_domain)
            if bw_parsed_cache is None:
                bw_raw = get_cached("builtwith_raw_data", working_domain, ignore_ttl=True)
                if bw_raw:
                    p = parse_builtwith(bw_raw)
                    try:
                        bw_res = bw_raw.get("Results", [])
                        vertical = bw_res[0].get("Result", {}).get("Vertical", "") if bw_res else ""
                    except Exception:
                        vertical = ""
                    bw_parsed_cache = {
                        "bw_vertical": vertical,
                        "bw_cms_raw": p.get("bw_cms", ""),
                        "bw_ecommerce": p.get("bw_ecommerce", ""),
                        "bw_email_marketing": p.get("bw_email_marketing", ""),
                        "bw_technologies": p.get("bw_technologies", "[]"),
                        "techs_compact": "",
                    }

        sw_needs_fetch = sw_parsed_cache is None and sw_want
        bw_needs_fetch = bw_parsed_cache is None and bw_want

        # Raw API data (only used when fetching from API)
        sw_data = None
        bw_data = None

        if sw_needs_fetch and bw_needs_fetch:
            # Both uncached and requested — fetch concurrently
            sw_data, bw_data = await asyncio.gather(
                fetch_similarweb(working_domain),
                fetch_builtwith(working_domain),
            )
            if sw_data and username:
                try:
                    from core.bigquery import increment_api_usage
                    increment_api_usage(username, "similarweb")
                except Exception:
                    pass
        else:
            # At most one API call needed — keep it simple/sequential
            if sw_needs_fetch:
                sw_data = await fetch_similarweb(working_domain)
                if sw_data and username:
                    try:
                        from core.bigquery import increment_api_usage
                        increment_api_usage(username, "similarweb")
                    except Exception:
                        pass
                if sw_want:
                    await asyncio.sleep(DELAY_BETWEEN_API_CALLS / 1000)
            if bw_needs_fetch:
                bw_data = await fetch_builtwith(working_domain)

        # ── Parse SimilarWeb result ───────────────────────────────────────────
        if sw_data:
            sw_parsed_cache = parse_similarweb(sw_data)

        if sw_parsed_cache:
            p = sw_parsed_cache
            result.update({k: p.get(k) for k in [
                "sw_visits", "sw_category", "sw_subcategory", "sw_description",
                "sw_title", "sw_top_countries", "sw_primary_region", "sw_primary_region_pct",
                "company_name",
            ]})

        # ── Parse BuiltWith result ────────────────────────────────────────────
        if bw_data:
            # Fresh from API — parse and save to privateBQ bw_parsed
            p = parse_builtwith(bw_data)
            result["bw_technologies"]    = p.get("bw_technologies", "[]")
            result["bw_cms_raw"]         = p.get("bw_cms")
            result["bw_ecommerce"]       = p.get("bw_ecommerce")
            result["bw_email_marketing"] = p.get("bw_email_marketing")
            try:
                bw_res = bw_data.get("Results", [])
                if bw_res:
                    v = bw_res[0].get("Result", {}).get("Vertical", "")
                    result["bw_vertical"] = result["bw_industry"] = v
            except Exception:
                pass
            m = match_technologies(bw_data, catalog)
            result["cms_list"] = m.get("cms_list")
            result["osearch"]  = m.get("osearch")
            result["osearch_group"] = m.get("osearch_group")
            result["ems_list"] = m.get("ems_list")
            # Build techs_compact and save to privateBQ bw_parsed
            _SEP_FIELD, _SEP_TECH = "\x01", "\x02"
            try:
                paths = bw_data.get("Results", [{}])[0].get("Result", {}).get("Paths", [])
                techs = [
                    f"{t.get('Name', '')}{_SEP_FIELD}{t.get('LastDetected', '0') or '0'}"
                    for path in paths for t in path.get("Technologies", []) if t.get("Name")
                ]
                techs_compact = _SEP_TECH.join(techs)
            except Exception:
                techs_compact = ""
            asyncio.create_task(asyncio.to_thread(save_bw_parsed, working_domain, {
                "bw_vertical": result.get("bw_vertical", ""),
                "bw_cms_raw": result.get("bw_cms_raw", ""),
                "bw_ecommerce": result.get("bw_ecommerce", ""),
                "bw_email_marketing": result.get("bw_email_marketing", ""),
                "bw_technologies": result.get("bw_technologies", "[]"),
                "techs_compact": techs_compact,
            }))
        elif bw_parsed_cache:
            # Served from privateBQ parsed cache
            result["bw_technologies"]    = bw_parsed_cache.get("bw_technologies", "[]")
            result["bw_cms_raw"]         = bw_parsed_cache.get("bw_cms_raw")
            result["bw_ecommerce"]       = bw_parsed_cache.get("bw_ecommerce")
            result["bw_email_marketing"] = bw_parsed_cache.get("bw_email_marketing")
            result["bw_vertical"] = result["bw_industry"] = bw_parsed_cache.get("bw_vertical", "")
            # Catalog matching from techs_compact
            from services.domain_profiles import _match_bw_compact
            m = _match_bw_compact(
                bw_parsed_cache.get("bw_vertical", ""),
                bw_parsed_cache.get("techs_compact", ""),
                catalog,
            )
            result["cms_list"]      = m.get("cms_list")
            result["osearch"]       = m.get("osearch")
            result["osearch_group"] = m.get("osearch_group")
            result["ems_list"]      = m.get("ems_list")

        if bw_want:
            await asyncio.sleep(DELAY_BETWEEN_API_CALLS / 1000)

        # ── Claude AI — read corpBQ cache, write to corpBQ ────────────────────
        # Read from cache unless force_refresh AND ai is selected.
        # Non-selected AI always enriches from cache.
        ai_cached = None
        if not (force_refresh and "ai" in services):
            ai_cached = get_corp_ai_cached(working_domain)

        if ai_cached:
            result["ai_category"]     = ai_cached.get("ai_category")
            result["ai_is_ecommerce"] = ai_cached.get("ai_is_ecommerce")
            result["ai_industry"]     = ai_cached.get("ai_industry")
        elif "ai" in services:
            homepage_text = await fetch_homepage_text(working_domain)
            input_hash = _make_input_hash(working_domain, result.get("sw_title") or "", result.get("sw_description") or "")
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
                result["ai_category"]     = ai_result.get("ai_category")
                result["ai_is_ecommerce"] = ai_result.get("ai_is_ecommerce")
                result["ai_industry"]     = ai_result.get("ai_industry")
                asyncio.create_task(asyncio.to_thread(save_corp_ai_result, working_domain, ai_result, input_hash))

    except Exception as e:
        logger.error(f"Pipeline error for {domain}: {e}")
        result["status"] = "error"
        result["error_detail"] = str(e)

    return result
