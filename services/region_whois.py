"""
Region fallback — level №3: registrant country via RDAP (the modern, free,
JSON Whois). Last resort, used only for the residue that ccTLD (№1) and the AI
homepage pass (№2) could not resolve — typically generic TLDs (.com/.net/…).

OFF by default (REGION_WHOIS_ENABLED). The lookup is a per-domain network call, so
it runs as a bounded, rate-limited SWEEP, not inline in jobs. Results are cached in
privateBQ `region_whois`; the profile sync reads that cache for free and feeds it
into the cascade. Yield is modest — GDPR redaction hides most registrant countries —
so a miss is normal and simply leaves the domain without a derived region.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from google.cloud import bigquery

from core.bigquery import client, table_ref, _bq_touch
from config.settings import (
    REGION_WHOIS_ENABLED, REGION_WHOIS_TIMEOUT, REGION_WHOIS_SLEEP, REGION_WHOIS_BATCH,
    GCP_PROJECT_ID, BIGQUERY_DATASET,
)

logger = logging.getLogger(__name__)

WHOIS_TABLE = "region_whois"
_WHOIS_SCHEMA = [
    bigquery.SchemaField("domain",     "STRING"),
    bigquery.SchemaField("country",    "STRING"),   # ISO-2 upper, or '' if looked-up-but-unknown
    bigquery.SchemaField("fetched_at", "TIMESTAMP"),
]

# Common country NAME → ISO alpha-2 (RDAP vCard "adr" often gives a name, not a code).
_NAME_TO_ISO2 = {
    "ukraine": "UA", "united states": "US", "united states of america": "US",
    "usa": "US", "germany": "DE", "deutschland": "DE", "poland": "PL", "polska": "PL",
    "united kingdom": "GB", "great britain": "GB", "france": "FR", "spain": "ES",
    "espana": "ES", "italy": "IT", "italia": "IT", "netherlands": "NL", "czechia": "CZ",
    "czech republic": "CZ", "romania": "RO", "bulgaria": "BG", "greece": "GR",
    "portugal": "PT", "austria": "AT", "switzerland": "CH", "belgium": "BE",
    "sweden": "SE", "norway": "NO", "finland": "FI", "denmark": "DK", "ireland": "IE",
    "canada": "CA", "australia": "AU", "india": "IN", "turkey": "TR", "turkiye": "TR",
    "brazil": "BR", "mexico": "MX", "hungary": "HU", "slovakia": "SK", "slovenia": "SI",
    "croatia": "HR", "serbia": "RS", "lithuania": "LT", "latvia": "LV", "estonia": "EE",
    "united arab emirates": "AE", "philippines": "PH", "china": "CN", "japan": "JP",
    "israel": "IL", "south africa": "ZA", "new zealand": "NZ", "russia": "RU",
    "russian federation": "RU", "belarus": "BY", "kazakhstan": "KZ", "moldova": "MD",
    "georgia": "GE", "azerbaijan": "AZ", "armenia": "AM",
}


def _to_iso2(raw: str) -> str:
    v = (raw or "").strip()
    if not v:
        return ""
    if len(v) == 2 and v.isalpha():
        return v.upper()
    return _NAME_TO_ISO2.get(v.lower(), "")


def ensure_whois_table():
    bq = client()
    ref = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{WHOIS_TABLE}"
    try:
        bq.get_table(ref)
    except Exception:
        bq.create_table(bigquery.Table(ref, schema=_WHOIS_SCHEMA))
        logger.info(f"Created table {WHOIS_TABLE}")


def load_whois_countries(domains: Optional[list[str]] = None) -> dict[str, str]:
    """Latest non-empty registrant country per domain from the cache. Returns
    {domain: ISO2}. `domains` None → whole table (it's small: only residue domains)."""
    ensure_whois_table()
    bq = client()
    tbl = f"`{table_ref(WHOIS_TABLE)}`"
    job_cfg = None
    where = "WHERE country IS NOT NULL AND country != ''"
    if domains:
        uniq = list(dict.fromkeys(d for d in domains if d))
        if not uniq:
            return {}
        ph = ", ".join(f"@d{i}" for i in range(len(uniq)))
        where += f" AND domain IN ({ph})"
        job_cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter(f"d{i}", "STRING", d) for i, d in enumerate(uniq)])
    _bq_touch("priv_r")
    rows = bq.query(f"""
        SELECT domain, country FROM {tbl}
        {where}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY fetched_at DESC) = 1
    """, job_config=job_cfg).result()
    return {r["domain"]: r["country"] for r in rows if r["country"]}


def _extract_country(rdap: dict) -> str:
    """Pull a registrant (fallback: administrative) country out of an RDAP response."""
    entities = rdap.get("entities") or []
    # Prefer registrant, then administrative — never registrar (that's not the audience).
    for want in ("registrant", "administrative"):
        for ent in entities:
            roles = [r.lower() for r in (ent.get("roles") or [])]
            if want not in roles:
                continue
            vcard = ent.get("vcardArray")
            if not (isinstance(vcard, list) and len(vcard) == 2):
                continue
            for prop in vcard[1]:
                # prop = [name, params, type, value]
                if not (isinstance(prop, list) and len(prop) >= 4 and prop[0] == "adr"):
                    continue
                params = prop[1] if isinstance(prop[1], dict) else {}
                cc = _to_iso2(params.get("cc", "") or params.get("country", ""))
                if cc:
                    return cc
                val = prop[3]
                if isinstance(val, list) and val:
                    cc = _to_iso2(str(val[-1]))   # last adr component = country
                    if cc:
                        return cc
    return ""


def lookup_whois_country(domain: str) -> str:
    """Single RDAP lookup via the IANA bootstrap (rdap.org → the right registry).
    Returns ISO-2 or '' (redacted / unknown / no RDAP)."""
    try:
        with httpx.Client(timeout=REGION_WHOIS_TIMEOUT, follow_redirects=True,
                          headers={"Accept": "application/rdap+json"}) as c:
            resp = c.get(f"https://rdap.org/domain/{domain}")
            if resp.status_code != 200:
                return ""
            return _extract_country(resp.json())
    except Exception as e:
        logger.debug(f"RDAP lookup failed for {domain}: {e}")
        return ""


def backfill_whois_regions(limit: Optional[int] = None) -> dict:
    """Bounded sweep: look up registrant country for domains that STILL have no region
    after №1/№2 (sw_primary_region empty AND derived_region NULL) and aren't cached yet.
    Writes results (including empty = 'looked up, unknown') so they aren't retried."""
    if not REGION_WHOIS_ENABLED:
        return {"status": "disabled",
                "note": "set REGION_WHOIS_ENABLED=true to enable the RDAP sweep"}
    ensure_whois_table()
    bq = client()
    n = int(limit or REGION_WHOIS_BATCH)
    profiles = f"`{table_ref('domain_profiles')}`"
    whois = f"`{table_ref(WHOIS_TABLE)}`"
    _bq_touch("priv_r")
    candidates = [r["domain"] for r in bq.query(f"""
        SELECT p.domain
        FROM {profiles} p
        LEFT JOIN (SELECT DISTINCT domain FROM {whois}) w ON w.domain = p.domain
        WHERE COALESCE(p.sw_primary_region, '') = ''
          AND p.derived_region IS NULL
          AND w.domain IS NULL
        LIMIT {n}
    """).result()]

    if not candidates:
        return {"status": "ok", "checked": 0, "found": 0, "note": "no residue to look up"}

    rows, found = [], 0
    now = datetime.now(timezone.utc).isoformat()
    for d in candidates:
        cc = lookup_whois_country(d)
        rows.append({"domain": d, "country": cc, "fetched_at": now})
        if cc:
            found += 1
        time.sleep(REGION_WHOIS_SLEEP)

    _bq_touch("priv_w")
    errors = bq.insert_rows_json(table_ref(WHOIS_TABLE), rows)
    if errors:
        logger.error(f"region_whois insert errors: {errors}")
        return {"status": "error", "checked": len(rows), "found": found, "errors": str(errors)[:200]}
    logger.info(f"whois sweep: checked {len(rows)}, found {found} countries")
    return {"status": "ok", "checked": len(rows), "found": found,
            "note": "run a profile sync to surface the new regions"}
