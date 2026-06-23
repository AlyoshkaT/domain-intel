"""
Domain Profiles Sync — memory-efficient streaming version.
Parses JSON immediately on load (never stores raw blobs), writes profiles
directly to temp file (never accumulates full rows list in RAM).
"""
import json
import logging
import tempfile
import os
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

from core.bigquery import client, corp_client, table_ref, _bq_op, track_bq_call, _bq_qcfg
from config.settings import CORP_PROJECT_ID, CORP_DATASET, GCP_PROJECT_ID, BIGQUERY_DATASET

# How many domains to keep in each parsed-data dict at any moment.
# Lower = less peak RAM, but the dicts are already small (parsed, not raw JSON).
# Keeping full dicts is fine after the raw-JSON parsing fix.

logger = logging.getLogger(__name__)

PROFILES_TABLE = "domain_profiles"
PROFILES_TEMP  = "domain_profiles_tmp"

PROFILES_SCHEMA = [
    bigquery.SchemaField("domain",                "STRING"),
    bigquery.SchemaField("updated_at",            "TIMESTAMP"),
    bigquery.SchemaField("sw_visits",             "FLOAT"),
    bigquery.SchemaField("sw_category",           "STRING"),
    bigquery.SchemaField("sw_subcategory",        "STRING"),
    bigquery.SchemaField("sw_description",        "STRING"),
    bigquery.SchemaField("sw_title",              "STRING"),
    bigquery.SchemaField("sw_primary_region",     "STRING"),
    bigquery.SchemaField("sw_primary_region_pct", "FLOAT"),
    bigquery.SchemaField("company_name",          "STRING"),
    bigquery.SchemaField("cms_list",              "STRING"),
    bigquery.SchemaField("osearch",               "STRING"),
    bigquery.SchemaField("osearch_group",         "STRING"),
    bigquery.SchemaField("ems_list",              "STRING"),
    bigquery.SchemaField("bw_vertical",           "STRING"),
    bigquery.SchemaField("ai_category",           "STRING"),
    bigquery.SchemaField("ai_is_ecommerce",       "STRING"),
    bigquery.SchemaField("ai_industry",           "STRING"),
]

_sync_status = {
    "running": False,
    "last_sync": None,
    "total_domains": 0,
    "error": None,
    "progress": "",
    "pct": 0,          # 0-100, for progress bar
    "mode": "full",    # "full" | "incremental"
}


def normalize_domain(domain: str) -> str:
    """Normalize domain: lowercase, remove www., strip spaces."""
    if not domain:
        return ""
    d = domain.strip().lower()
    d = d.removeprefix("http://").removeprefix("https://")
    d = d.split("/")[0].split("?")[0].split("#")[0].split(":")[0]
    if d.startswith("www."):
        d = d[4:]
    return d.strip(".")


def ensure_profiles_table():
    bq = client()
    table_obj = bigquery.Table(
        f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{PROFILES_TABLE}",
        schema=PROFILES_SCHEMA
    )
    try:
        bq.get_table(table_obj)
    except Exception:
        bq.create_table(table_obj)
        logger.info(f"Created table {PROFILES_TABLE}")


def _safe_json(val) -> Optional[dict]:
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None


def _parse_sw(data: dict | None) -> dict:
    if not data:
        return {}
    try:
        visits = 0
        eng = data.get("Engagments", {})
        if eng.get("Visits"):
            visits = float(eng["Visits"])
        else:
            monthly = data.get("EstimatedMonthlyVisits", {})
            if monthly:
                visits = float(list(monthly.values())[-1])
        cat_rank = data.get("CategoryRank", {})
        category = cat_rank.get("Category") or data.get("Category") or ""
        sw_cat = category.split("/")[0] if "/" in category else category
        sw_sub = category.split("/")[1] if "/" in category else ""
        top = data.get("TopCountryShares") or []
        region = top[0].get("CountryCode", "") if top else ""
        region_pct = round(top[0].get("Value", 0) * 100, 1) if top else None
        return {
            "sw_visits": visits,
            "sw_category": sw_cat,
            "sw_subcategory": sw_sub,
            "sw_description": (data.get("Description") or "")[:500],
            "sw_title": data.get("Title") or data.get("SiteName") or "",
            "sw_primary_region": region,
            "sw_primary_region_pct": region_pct,
            "company_name": data.get("Title") or data.get("SiteName") or "",
        }
    except Exception:
        return {}


def _parse_bw(data: dict | None, catalog: dict) -> dict:
    if not data:
        return {}
    try:
        from services.technology_catalog import match_technologies
        matched = match_technologies(data, catalog)
        vertical = ""
        try:
            results = data.get("Results", [])
            if results:
                vertical = results[0].get("Result", {}).get("Vertical", "") or ""
        except Exception:
            pass
        return {
            "cms_list":      matched.get("cms_list", ""),
            "osearch":       matched.get("osearch", ""),
            "osearch_group": matched.get("osearch_group", ""),
            "ems_list":      matched.get("ems_list", ""),
            "bw_vertical":   vertical,
        }
    except Exception:
        return {}


_PROFILE_SCHEMA = {"domain","updated_at","sw_visits","sw_category","sw_subcategory",
                   "sw_description","sw_title","sw_primary_region","sw_primary_region_pct",
                   "company_name","cms_list","osearch","osearch_group","ems_list",
                   "bw_vertical","ai_category","ai_is_ecommerce","ai_industry"}

_SEP_TECH = "\x02"   # separator between tech entries in compact BW string
_SEP_FIELD = "\x01"  # separator between name and lastDetected


def _signal_strength(name: str) -> int:
    """How strong is a BuiltWith detection as evidence the site *uses* a platform.
    3 = on-page script / core integration (e.g. "Klaviyo", "MailChimp for Shopify")
    2 = plugin / form / widget connector (often dormant, e.g. "MailChimp for WordPress")
    1 = SPF / DNS / mail record only (weakest, e.g. "MailChimp SPF", "Pardot Mail")
    """
    n = name.lower()
    if "spf" in n or "dkim" in n or "dmarc" in n or n.endswith(" mail") or " dns" in n:
        return 1
    if any(k in n for k in (" for wordpress", " for woocommerce", " for wix",
                            "wordpress", "plugin", "widget", "form", "subscribe",
                            "sign up", "opt-in", "opt in", "optin", " by ")):
        return 2
    return 3


def _select_match(entries, bw_index):
    """Pick the best catalog match for one dimension.
    Order: signal strength first, then recency (lastDetected), then a deterministic
    alphabetical fallback — so co-present ties never depend on catalog row order.
    Returns (matched_bw_name, group) or ("", "").
    """
    def sort_key(e):
        if isinstance(e, dict):
            return ((e.get("group") or e.get("technology") or "").lower(),
                    (e.get("technology") or "").lower())
        return (e.lower(), e.lower())

    best_rank = None  # (strength, last)
    best = ("", "")
    for entry in sorted(entries, key=sort_key):
        tech = entry["technology"] if isinstance(entry, dict) else entry
        grp  = entry.get("group", "") if isinstance(entry, dict) else ""
        hit = bw_index.get(tech.lower())
        if not hit:
            continue
        name, last = hit
        rank = (_signal_strength(name), last)
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best = (name, grp)
    return best


def _match_bw_compact(bw_vertical: str, techs_compact: str, catalog: dict) -> dict:
    """
    Match BW techs from compact SQL-extracted string instead of full JSON blob.
    techs_compact format: "WordPress\x011234567\x02Shopify\x011234999\x02..."
    This avoids downloading 50-200KB BW JSON per domain from BigQuery.
    """
    bw_index: dict[str, tuple[str, int]] = {}
    for entry in techs_compact.split(_SEP_TECH):
        if _SEP_FIELD in entry:
            name, last_s = entry.split(_SEP_FIELD, 1)
            if name:
                key = name.lower()
                last = int(last_s) if last_s.lstrip("-").isdigit() else 0
                if key not in bw_index or last > bw_index[key][1]:
                    bw_index[key] = (name, last)

    cms_name, cms_grp = _select_match(catalog.get("cms", []), bw_index)
    cms_match = cms_grp if cms_grp else cms_name

    osearch_match, osearch_group = _select_match(catalog.get("osearch", []), bw_index)

    ems_name, ems_grp = _select_match(catalog.get("ems", []), bw_index)
    ems_match = ems_grp if ems_grp else ems_name

    return {
        "cms_list":      cms_match,
        "osearch":       osearch_match,
        "osearch_group": osearch_group,
        "ems_list":      ems_match,
        "bw_vertical":   bw_vertical or "",
    }


def _build_profile(domain: str, sw_raw, bw_raw,
                   ai_rec: dict, catalog: dict, updated_at: str) -> dict:
    """Legacy helper — accepts raw JSON. Use _build_profile_parsed for efficiency."""
    sw = _parse_sw(_safe_json(sw_raw))
    bw = _parse_bw(_safe_json(bw_raw), catalog)
    return _assemble_profile(domain, sw, bw, ai_rec, updated_at)


def _build_profile_parsed(domain: str, sw: dict, bw: dict,
                           ai_rec: dict, updated_at: str) -> dict:
    """Build profile from already-parsed dicts (no JSON parsing, memory efficient)."""
    return _assemble_profile(domain, sw, bw, ai_rec, updated_at)


def _assemble_profile(domain: str, sw: dict, bw: dict,
                      ai_rec: dict, updated_at: str) -> dict:
    return {k: v for k, v in {
        "domain":                domain,
        "updated_at":            updated_at,
        "sw_visits":             sw.get("sw_visits"),
        "sw_category":           sw.get("sw_category", ""),
        "sw_subcategory":        sw.get("sw_subcategory", ""),
        "sw_description":        sw.get("sw_description", ""),
        "sw_title":              sw.get("sw_title", ""),
        "sw_primary_region":     sw.get("sw_primary_region", ""),
        "sw_primary_region_pct": sw.get("sw_primary_region_pct"),
        "company_name":          sw.get("company_name", ""),
        "cms_list":              bw.get("cms_list", ""),
        "osearch":               bw.get("osearch", ""),
        "osearch_group":         bw.get("osearch_group", ""),
        "ems_list":              bw.get("ems_list", ""),
        "bw_vertical":           bw.get("bw_vertical", ""),
        "ai_category":           ai_rec.get("ai_category", "") or "",
        "ai_is_ecommerce":       ai_rec.get("ai_is_ecommerce", "") or "",
        "ai_industry":           ai_rec.get("ai_industry", "") or "",
    }.items() if k in _PROFILE_SCHEMA}


def sync_domain_profiles() -> dict:
    global _sync_status
    _sync_status.update({"running": True, "error": None, "progress": "Починаємо...", "pct": 0, "mode": "full"})
    t0 = time.time()

    try:
        ensure_profiles_table()
        corp = corp_client()
        our  = client()

        corp_ai_table = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses`"

        # Load catalog once
        _sync_status["progress"] = "Завантажуємо каталог..."
        from services.technology_catalog import get_catalog
        catalog = get_catalog()

        # ── Memory-efficient data loading ──────────────────────────────────
        # pct budget:  SW 5-35 | BW 35-70 | AI 70-80 | write 80-95 | upload 95-100
        # We set pct BEFORE each blocking .result() so the bar moves immediately.

        # ── SQL-side extraction — only needed fields, NOT full JSON blobs ──────
        # BW JSON can be 50-200KB per domain × 100K domains = up to 20GB transfer.
        # We extract only the fields we need in SQL; Python receives small values.

        # Phase 1 — SW (5 → 35%): read from privateBQ sw_parsed (cheap, already parsed)
        _sync_status.update({"progress": "1/4 · SW запит…", "pct": 5})
        sw_parsed: dict[str, dict] = {}
        sw_priv_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sw_parsed`"
        sw_job = our.query(f"""
            SELECT
                domain,
                sw_visits,
                sw_category AS sw_category_raw,
                sw_subcategory,
                sw_description,
                sw_title,
                sw_primary_region AS sw_region,
                sw_primary_region_pct / 100 AS sw_region_val
            FROM {sw_priv_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """, job_config=_bq_qcfg(max_bytes=False))
        _sync_status.update({"progress": "1/4 · SW отримуємо…", "pct": 8})
        with _bq_op("priv_r"):
            for n, r in enumerate(sw_job.result()):
                if n % 2000 == 0:
                    time.sleep(0)  # yield GIL → event loop can serve bq_activity polls
                key = normalize_domain(r["domain"])
                if not key:
                    continue
                sw_parsed[key] = {
                    "sw_visits":             r["sw_visits"] or 0,
                    "sw_category":           r["sw_category_raw"] or "",
                    "sw_subcategory":        r["sw_subcategory"] or "",
                    "sw_description":        r["sw_description"] or "",
                    "sw_title":              r["sw_title"] or "",
                    "sw_primary_region":     r["sw_region"] or "",
                    "sw_primary_region_pct": round((r["sw_region_val"] or 0) * 100, 1) or None,
                    "company_name":          r["sw_title"] or "",
                }
                if len(sw_parsed) % 10000 == 0:
                    _sync_status.update({"progress": f"1/4 · SW: {len(sw_parsed):,}…", "pct": 10 + min(24, len(sw_parsed) // 1000)})
        _sync_status.update({"progress": f"1/4 · SW: {len(sw_parsed):,} доменів ✓", "pct": 35})
        track_bq_call("priv_sw")
        logger.info(f"SW parsed: {len(sw_parsed)}")

        # Safety guard: if privateBQ is empty (e.g. first deploy before sync_parsed_from_corp runs),
        # abort the full sync to avoid overwriting domain_profiles with blank SW/BW data.
        # Run POST /api/admin/sync_parsed_from_corp first to populate privateBQ.
        if len(sw_parsed) == 0:
            msg = ("privateBQ sw_parsed is empty — aborting full sync to protect existing data. "
                   "Run POST /api/admin/sync_parsed_from_corp first, then retry.")
            logger.error(msg)
            _sync_status.update({"error": msg, "progress": f"❌ {msg[:80]}", "running": False})
            return {"error": msg}

        # Phase 2 — BW (35 → 70%): read from privateBQ bw_parsed (cheap, already extracted)
        _sync_status.update({"progress": "2/4 · BW запит (privateBQ)…", "pct": 35})
        bw_parsed: dict[str, dict] = {}
        bw_priv_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.bw_parsed`"
        bw_job = our.query(f"""
            SELECT domain, bw_vertical, techs_compact
            FROM {bw_priv_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """, job_config=_bq_qcfg(max_bytes=False))
        _sync_status.update({"progress": "2/4 · BW отримуємо…", "pct": 38})
        with _bq_op("priv_r"):
            for n, r in enumerate(bw_job.result()):
                if n % 2000 == 0:
                    time.sleep(0)  # yield GIL
                key = normalize_domain(r["domain"])
                if key:
                    bw_parsed[key] = _match_bw_compact(
                        r["bw_vertical"] or "",
                        r["techs_compact"] or "",
                        catalog,
                    )
                    if len(bw_parsed) % 10000 == 0:
                        _sync_status.update({"progress": f"2/4 · BW: {len(bw_parsed):,}…", "pct": 40 + min(28, len(bw_parsed) // 1000)})
        _sync_status.update({"progress": f"2/4 · BW: {len(bw_parsed):,} доменів ✓", "pct": 70})
        track_bq_call("priv_bw")
        logger.info(f"BW parsed: {len(bw_parsed)}")

        # Phase 3 — AI (70 → 80%): read from privateBQ ai_parsed (cheap).
        # Fallback to corpBQ full scan only if ai_parsed is empty (not yet backfilled).
        _sync_status.update({"progress": "3/4 · AI запит (privateBQ)…", "pct": 70})
        ai_data: dict[str, dict] = {}
        ai_priv_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.ai_parsed`"
        try:
            ai_job = our.query(f"""
                SELECT domain, ai_category, ai_is_ecommerce, ai_industry
                FROM {ai_priv_table}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            """, job_config=_bq_qcfg(max_bytes=False))
            with _bq_op("priv_r"):
                for r in ai_job.result():
                    key = normalize_domain(r["domain"])
                    if key:
                        ai_data[key] = {
                            "ai_category":     r["ai_category"] or "",
                            "ai_is_ecommerce": r["ai_is_ecommerce"] or "Ні",
                            "ai_industry":     r["ai_industry"] or "",
                        }
        except Exception as e:
            logger.warning(f"ai_parsed read failed ({e}) — falling back to corpBQ")

        if not ai_data:
            logger.info("ai_parsed empty — falling back to corpBQ claude_responses full scan")
            _sync_status.update({"progress": "3/4 · AI запит (corpBQ fallback)…", "pct": 71})
            ai_job = corp.query(f"""
                SELECT
                    domain,
                    COALESCE(JSON_VALUE(response_json, '$.category'), '')        AS ai_category,
                    LOWER(COALESCE(JSON_VALUE(response_json, '$.is_ecommerce'), 'false')) AS ai_is_ecom,
                    COALESCE(JSON_VALUE(response_json, '$.subcategory'), '')      AS ai_industry
                FROM {corp_ai_table}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            """, job_config=_bq_qcfg(max_bytes=False))
            with _bq_op("corp_r"):
                for r in ai_job.result():
                    key = normalize_domain(r["domain"])
                    if key:
                        ai_data[key] = {
                            "ai_category":     r["ai_category"] or "",
                            "ai_is_ecommerce": "Так" if r["ai_is_ecom"] in ("true", "1", "yes") else "Ні",
                            "ai_industry":     r["ai_industry"] or "",
                        }
            track_bq_call("corp_ai")
        _sync_status.update({"progress": f"3/4 · AI: {len(ai_data):,} доменів ✓", "pct": 80})
        logger.info(f"AI data total: {len(ai_data)} domains")

        all_domains = {d for d in sw_parsed.keys() | bw_parsed.keys() | ai_data.keys() if d}
        logger.info(f"Unique normalized domains: {len(all_domains)}")

        # ── Phase 4: Stream profiles directly to temp file (80 → 95%) ──
        updated_at = datetime.now(timezone.utc).isoformat()
        domains_list = sorted(all_domains)
        total_count = len(domains_list)

        _sync_status.update({"progress": f"4/4 · Записуємо {total_count:,} профілів…", "pct": 80})
        tmp_file = None
        written = 0
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
                tmp_file = f.name
                for i, domain in enumerate(domains_list):
                    try:
                        profile = _build_profile_parsed(
                            domain,
                            sw_parsed.get(domain, {}),
                            bw_parsed.get(domain, {}),
                            ai_data.get(domain, {}),
                            updated_at,
                        )
                        f.write(json.dumps(profile, default=str) + "\n")
                        written += 1
                    except Exception as e:
                        logger.warning(f"Build error for {domain}: {e}")

                    if (i + 1) % 5000 == 0 or (i + 1) == total_count:
                        # map write progress to 80-95% range
                        pct = 80 + int((i + 1) / total_count * 15)
                        _sync_status["progress"] = f"4/4 · {i+1:,}/{total_count:,} ({int((i+1)/total_count*100)}%)"
                        _sync_status["pct"] = pct

            logger.info(f"Written {written} profiles to {tmp_file} in {time.time()-t0:.0f}s")

            # Release parsed data dicts — free memory before BQ upload
            sw_parsed.clear()
            bw_parsed.clear()
            ai_data.clear()

            _sync_status.update({"progress": f"Завантажуємо {written:,} профілів у BigQuery…", "pct": 95})
            _bq_touch("priv_w")
            job_config = bigquery.LoadJobConfig(
                schema=PROFILES_SCHEMA,
                write_disposition="WRITE_TRUNCATE",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            with open(tmp_file, "rb") as f:
                load_job = our.load_table_from_file(
                    f, table_ref(PROFILES_TABLE), job_config=job_config
                )
            _sync_status.update({"progress": "BigQuery завантажує файл…", "pct": 97})
            load_job.result()
            track_bq_call("priv_ai")
        finally:
            if tmp_file and os.path.exists(tmp_file):
                os.unlink(tmp_file)

        elapsed = time.time() - t0
        _sync_status.update({
            "last_sync": updated_at,
            "total_domains": written,
            "progress": f"✅ {written:,} доменів за {elapsed/60:.1f} хв.",
            "pct": 100,
        })
        logger.info(f"Sync done: {written} domains in {elapsed:.0f}s")
        return {"total": written, "status": "ok"}

    except Exception as e:
        logger.error(f"Sync error: {e}", exc_info=True)
        _sync_status["error"] = str(e)
        _sync_status["progress"] = f"❌ {str(e)[:100]}"
        return {"error": str(e)}
    finally:
        _sync_status["running"] = False


def get_sync_status() -> dict:
    return dict(_sync_status)


def sync_domain_profiles_incremental(domains: list[str]) -> dict:
    """
    Fast upsert: rebuild profiles only for the given domains.
    Uses MERGE so existing rows are updated, new rows are inserted.
    Typical job of 100-200 domains: ~10-30 seconds.
    """
    if not domains:
        return {"total": 0, "status": "ok", "skipped": "empty domain list"}

    # Normalise
    norm_domains = [d for d in (normalize_domain(x) for x in domains) if d]
    if not norm_domains:
        return {"total": 0, "status": "ok", "skipped": "no valid domains"}

    logger.info(f"Incremental sync: {len(norm_domains)} domains")
    t0 = time.time()

    try:
        ensure_profiles_table()
        corp = corp_client()
        our  = client()

        corp_ai_table = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses`"

        from services.technology_catalog import get_catalog
        catalog = get_catalog()

        # Build a quoted list for IN clause (safe — domains are normalized)
        dom_list_sql = ", ".join(f"'{d}'" for d in norm_domains)
        in_clause = f"domain IN ({dom_list_sql})"

        from core.bigquery import _bq_touch
        _bq_touch("priv_r")
        sw_priv_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.sw_parsed`"
        sw_parsed: dict[str, dict] = {}
        for r in our.query(f"""
            SELECT
                domain,
                sw_visits,
                sw_category AS sw_category_raw,
                sw_subcategory,
                sw_description,
                sw_title,
                sw_primary_region AS sw_region,
                sw_primary_region_pct / 100 AS sw_region_val
            FROM {sw_priv_table}
            WHERE {in_clause}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if not key:
                continue
            sw_parsed[key] = {
                "sw_visits":             r["sw_visits"] or 0,
                "sw_category":           r["sw_category_raw"] or "",
                "sw_subcategory":        r["sw_subcategory"] or "",
                "sw_description":        r["sw_description"] or "",
                "sw_title":              r["sw_title"] or "",
                "sw_primary_region":     r["sw_region"] or "",
                "sw_primary_region_pct": round((r["sw_region_val"] or 0) * 100, 1) or None,
                "company_name":          r["sw_title"] or "",
            }

        bw_priv_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.bw_parsed`"
        bw_parsed: dict[str, dict] = {}
        for r in our.query(f"""
            SELECT domain, bw_vertical, techs_compact
            FROM {bw_priv_table}
            WHERE {in_clause}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """).result():
            key = normalize_domain(r["domain"])
            if key:
                bw_parsed[key] = _match_bw_compact(r["bw_vertical"] or "", r["techs_compact"] or "", catalog)

        # AI: privateBQ ai_parsed first (free for corpBQ), corp fallback only for missing domains
        ai_data: dict[str, dict] = {}
        ai_priv_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.ai_parsed`"
        try:
            for r in our.query(f"""
                SELECT domain, ai_category, ai_is_ecommerce, ai_industry
                FROM {ai_priv_table}
                WHERE {in_clause}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            """).result():
                key = normalize_domain(r["domain"])
                if key:
                    ai_data[key] = {
                        "ai_category":     r["ai_category"] or "",
                        "ai_is_ecommerce": r["ai_is_ecommerce"] or "Ні",
                        "ai_industry":     r["ai_industry"] or "",
                    }
        except Exception as e:
            logger.warning(f"incremental: ai_parsed read failed ({e}) — corp fallback for all domains")

        missing_ai = [d for d in norm_domains if d not in ai_data]
        if missing_ai:
            _bq_touch("corp_r")
            missing_sql = ", ".join(f"'{d}'" for d in missing_ai)
            ai_in = f"LOWER(REGEXP_REPLACE(domain, r'^www\\.', '')) IN ({missing_sql})"
            for r in corp.query(f"""
                SELECT
                    domain,
                    COALESCE(JSON_VALUE(response_json, '$.category'), '') AS ai_category,
                    LOWER(COALESCE(JSON_VALUE(response_json, '$.is_ecommerce'), 'false')) AS ai_is_ecom,
                    COALESCE(JSON_VALUE(response_json, '$.subcategory'), '') AS ai_industry
                FROM {corp_ai_table}
                WHERE {ai_in}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
            """).result():
                key = normalize_domain(r["domain"])
                if key:
                    ai_data[key] = {
                        "ai_category":     r["ai_category"] or "",
                        "ai_is_ecommerce": "Так" if r["ai_is_ecom"] in ("true", "1", "yes") else "Ні",
                        "ai_industry":     r["ai_industry"] or "",
                    }

        updated_at = datetime.now(timezone.utc).isoformat()
        tmp_file = None
        written = 0

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            tmp_file = f.name
            for domain in norm_domains:
                try:
                    profile = _build_profile_parsed(
                        domain,
                        sw_parsed.get(domain, {}),
                        bw_parsed.get(domain, {}),
                        ai_data.get(domain, {}),
                        updated_at,
                    )
                    f.write(json.dumps(profile, default=str) + "\n")
                    written += 1
                except Exception as e:
                    logger.warning(f"Incremental build error for {domain}: {e}")

        # Load into a temp table, then MERGE into domain_profiles
        _bq_touch("priv_w")
        tmp_table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.domain_profiles_incr_tmp"
        try:
            job_config = bigquery.LoadJobConfig(
                schema=PROFILES_SCHEMA,
                write_disposition="WRITE_TRUNCATE",
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            with open(tmp_file, "rb") as f:
                load_job = our.load_table_from_file(f, tmp_table_id, job_config=job_config)
            load_job.result()

            # MERGE — update existing or insert new
            profiles_full = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{PROFILES_TABLE}`"
            tmp_full = f"`{tmp_table_id}`"
            merge_cols = [f.name for f in PROFILES_SCHEMA if f.name != "domain"]
            set_clause = ", ".join(f"T.{c} = S.{c}" for c in merge_cols)
            ins_cols = ", ".join(f.name for f in PROFILES_SCHEMA)
            ins_vals = ", ".join(f"S.{f.name}" for f in PROFILES_SCHEMA)
            our.query(f"""
                MERGE {profiles_full} T
                USING {tmp_full} S ON T.domain = S.domain
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
            """).result()
        finally:
            if tmp_file and os.path.exists(tmp_file):
                os.unlink(tmp_file)
            # Drop temp table
            try:
                our.delete_table(tmp_table_id, not_found_ok=True)
            except Exception:
                pass

        elapsed = time.time() - t0
        logger.info(f"Incremental sync done: {written} domains in {elapsed:.1f}s")
        return {"total": written, "status": "ok", "elapsed": round(elapsed, 1)}

    except Exception as e:
        logger.error(f"Incremental sync error: {e}", exc_info=True)
        return {"error": str(e)}


def sync_profiles_from_job_results(job_id: str) -> dict:
    """
    Sync domain_profiles directly from analysis_results for a given job.
    Bypasses corpBQ cache — useful when save_cache failed silently during job.
    Only updates non-empty fields (COALESCE logic preserves existing values).
    """
    t0 = time.time()
    try:
        ensure_profiles_table()
        bq = client()
        results_full   = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.analysis_results`"
        profiles_full  = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{PROFILES_TABLE}`"

        # MERGE: for each field — use new value only if non-empty, else keep existing
        bq.query(f"""
            MERGE {profiles_full} T
            USING (
                SELECT
                    domain,
                    processed_at                                        AS updated_at,
                    sw_visits,
                    NULLIF(TRIM(COALESCE(sw_category, '')), '')         AS sw_category,
                    NULLIF(TRIM(COALESCE(sw_subcategory, '')), '')      AS sw_subcategory,
                    NULLIF(TRIM(COALESCE(sw_description, '')), '')      AS sw_description,
                    NULLIF(TRIM(COALESCE(sw_title, '')), '')            AS sw_title,
                    NULLIF(TRIM(COALESCE(sw_primary_region, '')), '')   AS sw_primary_region,
                    sw_primary_region_pct,
                    NULLIF(TRIM(COALESCE(company_name, '')), '')        AS company_name,
                    NULLIF(TRIM(COALESCE(cms_list, '')), '')            AS cms_list,
                    NULLIF(TRIM(COALESCE(osearch, '')), '')             AS osearch,
                    NULLIF(TRIM(COALESCE(osearch_group, '')), '')       AS osearch_group,
                    NULLIF(TRIM(COALESCE(ems_list, '')), '')            AS ems_list,
                    NULLIF(TRIM(COALESCE(bw_vertical, '')), '')         AS bw_vertical,
                    NULLIF(TRIM(COALESCE(ai_category, '')), '')         AS ai_category,
                    NULLIF(TRIM(COALESCE(ai_is_ecommerce, '')), '')     AS ai_is_ecommerce,
                    NULLIF(TRIM(COALESCE(ai_industry, '')), '')         AS ai_industry
                FROM {results_full}
                WHERE job_id = @job_id AND status != 'error'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY processed_at DESC) = 1
            ) S
            ON T.domain = S.domain
            WHEN MATCHED THEN UPDATE SET
                updated_at            = S.updated_at,
                sw_visits             = COALESCE(S.sw_visits,             T.sw_visits),
                sw_category           = COALESCE(S.sw_category,           T.sw_category),
                sw_subcategory        = COALESCE(S.sw_subcategory,        T.sw_subcategory),
                sw_description        = COALESCE(S.sw_description,        T.sw_description),
                sw_title              = COALESCE(S.sw_title,              T.sw_title),
                sw_primary_region     = COALESCE(S.sw_primary_region,     T.sw_primary_region),
                sw_primary_region_pct = COALESCE(S.sw_primary_region_pct, T.sw_primary_region_pct),
                company_name          = COALESCE(S.company_name,          T.company_name),
                cms_list              = COALESCE(S.cms_list,              T.cms_list),
                osearch               = COALESCE(S.osearch,               T.osearch),
                osearch_group         = COALESCE(S.osearch_group,         T.osearch_group),
                ems_list              = COALESCE(S.ems_list,              T.ems_list),
                bw_vertical           = COALESCE(S.bw_vertical,           T.bw_vertical),
                ai_category           = COALESCE(S.ai_category,           T.ai_category),
                ai_is_ecommerce       = COALESCE(S.ai_is_ecommerce,       T.ai_is_ecommerce),
                ai_industry           = COALESCE(S.ai_industry,           T.ai_industry)
            WHEN NOT MATCHED THEN INSERT (
                domain, updated_at, sw_visits, sw_category, sw_subcategory,
                sw_description, sw_title, sw_primary_region, sw_primary_region_pct,
                company_name, cms_list, osearch, osearch_group, ems_list,
                bw_vertical, ai_category, ai_is_ecommerce, ai_industry
            ) VALUES (
                S.domain, S.updated_at, S.sw_visits, S.sw_category, S.sw_subcategory,
                S.sw_description, S.sw_title, S.sw_primary_region, S.sw_primary_region_pct,
                S.company_name, S.cms_list, S.osearch, S.osearch_group, S.ems_list,
                S.bw_vertical, S.ai_category, S.ai_is_ecommerce, S.ai_industry
            )
        """, job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("job_id", "STRING", job_id)
        ])).result()

        elapsed = time.time() - t0
        logger.info(f"sync_from_results job={job_id} done in {elapsed:.1f}s")
        return {"status": "ok", "elapsed": round(elapsed, 1)}
    except Exception as e:
        logger.error(f"sync_from_results error job={job_id}: {e}", exc_info=True)
        return {"error": str(e)}


def rematch_catalog() -> dict:
    """
    Re-run catalog matching (CMS / OSearch / EMS) for all domains in domain_profiles
    using the current technology_catalog in BQ.
    Only updates cms_list / osearch / osearch_group / ems_list — does NOT touch SW/AI fields.
    Uses bw_parsed table as source of techs_compact.
    """
    t0 = time.time()
    try:
        from services.technology_catalog import get_catalog
        catalog = get_catalog()

        our = client()
        bw_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.bw_parsed`"
        profiles_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{PROFILES_TABLE}`"

        rows = list(our.query(f"""
            SELECT domain, bw_vertical, techs_compact
            FROM {bw_table}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
        """).result())

        if not rows:
            return {"status": "ok", "updated": 0, "elapsed": 0}

        logger.info(f"rematch_catalog: {len(rows)} domains to rematch")

        # Build NDJSON with new catalog matches (deduplicated by normalized domain)
        import io as _io, json as _json
        deduped: dict[str, dict] = {}
        for r in rows:
            domain = normalize_domain(r["domain"])
            if not domain:
                continue
            # Skip domains without BW techs — nothing to (re)match, and we must not
            # touch them so their existing values are preserved (see MERGE below,
            # which is now an authoritative overwrite for the domains it does touch).
            if not (r["techs_compact"] or "").strip():
                continue
            m = _match_bw_compact(r["bw_vertical"] or "", r["techs_compact"] or "", catalog)
            deduped[domain] = {
                "domain":        domain,
                "cms_list":      m.get("cms_list", ""),
                "osearch":       m.get("osearch", ""),
                "osearch_group": m.get("osearch_group", ""),
                "ems_list":      m.get("ems_list", ""),
            }
        ndjson_lines = [_json.dumps(v) for v in deduped.values()]

        tmp_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}`.catalog_rematch_tmp"
        tmp_schema = [
            bigquery.SchemaField("domain",        "STRING"),
            bigquery.SchemaField("cms_list",      "STRING"),
            bigquery.SchemaField("osearch",       "STRING"),
            bigquery.SchemaField("osearch_group", "STRING"),
            bigquery.SchemaField("ems_list",      "STRING"),
        ]
        tmp_ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.catalog_rematch_tmp"
        job_cfg = bigquery.LoadJobConfig(
            schema=tmp_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        load_job = our.load_table_from_file(
            _io.BytesIO("\n".join(ndjson_lines).encode()),
            tmp_ref,
            job_config=job_cfg,
        )
        load_job.result()

        # MERGE into domain_profiles — authoritative overwrite.
        # Every domain in S comes from bw_parsed WITH non-empty techs_compact, so the
        # recomputed match is definitive: overwrite directly (including with '') so that
        # technologies removed from the catalog get cleared instead of lingering.
        merge_sql = f"""
            MERGE {profiles_table} T
            USING `{tmp_ref}` S ON T.domain = S.domain
            WHEN MATCHED THEN UPDATE SET
                T.cms_list      = S.cms_list,
                T.osearch       = S.osearch,
                T.osearch_group = S.osearch_group,
                T.ems_list      = S.ems_list
        """
        merge_job = our.query(merge_sql)
        merge_job.result()
        updated = merge_job.num_dml_affected_rows or len(ndjson_lines)

        # Drop temp table
        try:
            our.delete_table(tmp_ref)
        except Exception:
            pass

        elapsed = round(time.time() - t0, 1)
        logger.info(f"rematch_catalog done: {updated} domains updated in {elapsed}s")
        return {"status": "ok", "updated": updated, "elapsed": elapsed}
    except Exception as e:
        logger.error(f"rematch_catalog error: {e}", exc_info=True)
        return {"error": str(e)}
