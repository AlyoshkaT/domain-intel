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
from core.bigquery import get_cached, get_sw_parsed, get_bw_parsed, save_bw_parsed, was_parsed_prefetched
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
    priority: bool = False,
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
            # Not selected — try privateBQ first.
            # Fallback to corpBQ raw only for single-domain calls (no batch prefetch ran).
            # In batch jobs, prefetch_parsed() sets explicit sentinels — skip corpBQ to avoid
            # N individual 1.9GB table scans (one per domain).
            sw_parsed_cache = get_sw_parsed(working_domain)
            if sw_parsed_cache is None and not was_parsed_prefetched(working_domain):
                # Single-domain path (e.g., direct API call) — corpBQ fallback is fine
                sw_raw = get_cached("similarweb_raw_data", working_domain, ignore_ttl=True)
                if sw_raw:
                    sw_parsed_cache = parse_similarweb(sw_raw)

        if bw_want:
            bw_parsed_cache = None if force_refresh else get_bw_parsed(working_domain)
        else:
            bw_parsed_cache = get_bw_parsed(working_domain)
            if bw_parsed_cache is None and not was_parsed_prefetched(working_domain):
                # Single-domain path only — skip corpBQ in batch jobs
                bw_raw = get_cached("builtwith_raw_data", working_domain, ignore_ttl=True)
                if bw_raw:
                    p = parse_builtwith(bw_raw)
                    try:
                        bw_res = bw_raw.get("Results", [])
                        vertical = bw_res[0].get("Result", {}).get("Vertical", "") if bw_res else ""
                    except Exception:
                        vertical = ""
                    # Also build technologies_json from raw corpBQ blob (backward-compat fallback)
                    _tj = "[]"
                    try:
                        import json as _json
                        _paths = bw_raw.get("Results", [{}])[0].get("Result", {}).get("Paths", [])
                        _all = [t for _p in _paths for t in _p.get("Technologies", []) if t.get("Name")]
                        _recs = []
                        for t in _all:
                            _tag = t.get("Tag", [])
                            _ft = (_tag[0] if isinstance(_tag, list) and _tag
                                   else _tag if isinstance(_tag, str) else "")
                            _rec = {"n": t.get("Name", ""), "t": _ft}
                            _fd = t.get("FirstDetected")
                            if _fd is not None:
                                try:
                                    _rec["f"] = int(_fd)
                                except (ValueError, TypeError):
                                    pass
                            _ld = t.get("LastDetected")
                            if _ld is not None:
                                try:
                                    _rec["l"] = int(_ld)
                                except (ValueError, TypeError):
                                    pass
                            _recs.append(_rec)
                        _tj = _json.dumps(_recs)
                    except Exception:
                        pass
                    bw_parsed_cache = {
                        "bw_vertical": vertical,
                        "bw_cms_raw": p.get("bw_cms", ""),
                        "bw_ecommerce": p.get("bw_ecommerce", ""),
                        "bw_email_marketing": p.get("bw_email_marketing", ""),
                        "bw_technologies": p.get("bw_technologies", "[]"),
                        "techs_compact": "",
                        "technologies_json": _tj,
                    }

        sw_needs_fetch = sw_parsed_cache is None and sw_want
        bw_needs_fetch = bw_parsed_cache is None and bw_want

        # Raw API data (only used when fetching from API)
        sw_data = None
        bw_data = None

        from processing.limits import api_slot

        async def _fetch_sw():
            async with api_slot("sw", priority):
                return await fetch_similarweb(working_domain)

        async def _fetch_bw():
            async with api_slot("bw", priority):
                return await fetch_builtwith(working_domain)

        if sw_needs_fetch and bw_needs_fetch:
            # Both uncached and requested — fetch concurrently (separate API queues)
            sw_data, bw_data = await asyncio.gather(_fetch_sw(), _fetch_bw())
            if sw_data and username:
                try:
                    from core.bigquery import increment_api_usage
                    increment_api_usage(username, "similarweb")
                except Exception:
                    pass
        else:
            # At most one API call needed — keep it simple/sequential
            if sw_needs_fetch:
                sw_data = await _fetch_sw()
                if sw_data and username:
                    try:
                        from core.bigquery import increment_api_usage
                        increment_api_usage(username, "similarweb")
                    except Exception:
                        pass
                if sw_want:
                    await asyncio.sleep(DELAY_BETWEEN_API_CALLS / 1000)
            if bw_needs_fetch:
                bw_data = await _fetch_bw()

        # ── Parse SimilarWeb result ───────────────────────────────────────────
        from services.similarweb import SW_RATE_LIMITED
        if sw_data and sw_data is not SW_RATE_LIMITED:
            sw_parsed_cache = parse_similarweb(sw_data)
        elif sw_data is SW_RATE_LIMITED:
            # API returned 429 after all retries — mark domain so user knows to re-run
            result["error_detail"] = (result.get("error_detail") or "") + "[SW:rate_limited] "
            logger.warning(f"SW rate-limited for {working_domain} — marked in result")

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
            # Build techs_compact + technologies_json and save to privateBQ bw_parsed
            _SEP_FIELD, _SEP_TECH = "\x01", "\x02"
            techs_compact = ""
            technologies_json_str = "[]"
            try:
                import json as _json
                paths = bw_data.get("Results", [{}])[0].get("Result", {}).get("Paths", [])
                all_techs = [t for path in paths for t in path.get("Technologies", []) if t.get("Name")]
                techs_compact = _SEP_TECH.join(
                    f"{t.get('Name', '')}{_SEP_FIELD}{t.get('LastDetected', '0') or '0'}"
                    for t in all_techs
                )
                # Rich JSON: all techs with name, first tag, last-detected timestamp
                tech_records = []
                for t in all_techs:
                    tag_raw = t.get("Tag", [])
                    first_tag = (tag_raw[0] if isinstance(tag_raw, list) and tag_raw
                                 else tag_raw if isinstance(tag_raw, str) else "")
                    rec = {"n": t.get("Name", ""), "t": first_tag}
                    fd = t.get("FirstDetected")
                    if fd is not None:
                        try:
                            rec["f"] = int(fd)
                        except (ValueError, TypeError):
                            pass
                    ld = t.get("LastDetected")
                    if ld is not None:
                        try:
                            rec["l"] = int(ld)
                        except (ValueError, TypeError):
                            pass
                    tech_records.append(rec)
                technologies_json_str = _json.dumps(tech_records)
            except Exception:
                pass
            asyncio.create_task(asyncio.to_thread(save_bw_parsed, working_domain, {
                "bw_vertical": result.get("bw_vertical", ""),
                "bw_cms_raw": result.get("bw_cms_raw", ""),
                "bw_ecommerce": result.get("bw_ecommerce", ""),
                "bw_email_marketing": result.get("bw_email_marketing", ""),
                "bw_technologies": result.get("bw_technologies", "[]"),
                "techs_compact": techs_compact,
                "technologies_json": technologies_json_str,
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
            async with api_slot("ai", priority):
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
                # Mirror into privateBQ ai_parsed so profile syncs never need corpBQ for fresh results
                from core.bigquery import save_ai_parsed
                asyncio.create_task(asyncio.to_thread(save_ai_parsed, working_domain, {
                    "ai_category":     ai_result.get("ai_category", ""),
                    "ai_is_ecommerce": ai_result.get("ai_is_ecommerce", ""),
                    "ai_industry":     ai_result.get("ai_industry", ""),
                }))

    except Exception as e:
        logger.error(f"Pipeline error for {domain}: {e}")
        result["status"] = "error"
        result["error_detail"] = str(e)

    return result
