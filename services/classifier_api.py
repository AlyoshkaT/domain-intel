"""
Owner's website-classifier API client (categories v2).

We no longer classify categories ourselves and no longer write to the owner's
`claude_responses`. Instead we POST domains to the owner's classifier; it reads the
site HTML, classifies with their Jev model, and writes the answer to their table —
which appears in the `latest_categories_claude` view we now read. So this client
just TRIGGERS classification and returns whatever the response gives; the
authoritative category is read back from the view by the profile sync.

Constraints (per the owner):
  • analysis_mode must be "categories", cache_policy must be "any" (else 403).
  • ≤ 25 websites per request. batch_id is prefixed with "domain-intel_" on their side.
  • Key comes from settings.CLASSIFIER_API_KEY (set in .env / Railway by the user).

⚠️ Do NOT run the 2026-10-17 backfill domains through this until the owner confirms
their cache is fixed — the API would return our own stale answer for them.
"""
import logging
import time
from typing import Iterable

import httpx

from config.settings import CLASSIFIER_API_URL, CLASSIFIER_API_KEY, CLASSIFIER_TIMEOUT

logger = logging.getLogger(__name__)

_BATCH = 25


def _chunks(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _parse_result(payload: dict) -> dict[str, dict]:
    """Best-effort extraction of {domain → {category, subcategory}} from the response.
    The exact shape is in the owner's docs/API.md; we read it defensively and also
    rely on the view for the authoritative value, so an unexpected shape is non-fatal."""
    out: dict[str, dict] = {}
    items = None
    if isinstance(payload, dict):
        for k in ("results", "websites", "data", "items"):
            if isinstance(payload.get(k), list):
                items = payload[k]
                break
        if items is None and isinstance(payload.get("results"), dict):
            items = [{"domain": d, **v} for d, v in payload["results"].items()]
    if items is None and isinstance(payload, list):
        items = payload
    for it in (items or []):
        if not isinstance(it, dict):
            continue
        dom = (it.get("domain") or it.get("website") or it.get("url") or "").strip().lower()
        cat = it.get("category") or (it.get("categories") or {}).get("category") if isinstance(it.get("categories"), dict) else it.get("category")
        if dom and cat:
            out[dom] = {"category": cat, "subcategory": it.get("subcategory") or ""}
    return out


def classify_domains(domains: list[str], batch_id: str = "domain-intel") -> dict:
    """Send domains to the owner's classifier in batches of 25. Returns a summary
    {sent, batches, results: {domain: {category, subcategory}}, errors}. Does not
    write anything on our side — read categories back from the view after."""
    if not CLASSIFIER_API_KEY:
        return {"status": "no_key",
                "note": "set CLASSIFIER_API_KEY in .env / Railway (owner's classifier key)"}
    doms = [d.strip().lower() for d in domains if d and d.strip()]
    if not doms:
        return {"status": "empty"}

    headers = {"X-API-Key": CLASSIFIER_API_KEY, "Content-Type": "application/json"}
    results: dict[str, dict] = {}
    errors = 0
    sent = 0
    with httpx.Client(timeout=CLASSIFIER_TIMEOUT) as cli:
        for i, chunk in enumerate(_chunks(doms, _BATCH)):
            body = {
                "websites": chunk,
                "analysis_mode": "categories",
                "cache_policy": "any",
                "batch_id": f"{batch_id}-{i}",
            }
            try:
                r = cli.post(CLASSIFIER_API_URL, headers=headers, json=body)
                if r.status_code != 200:
                    errors += 1
                    logger.error(f"classifier API {r.status_code}: {r.text[:200]}")
                    continue
                results.update(_parse_result(r.json()))
                sent += len(chunk)
            except Exception as e:
                errors += 1
                logger.error(f"classifier API call failed: {e}")
            time.sleep(0.2)  # be polite between batches
    logger.info(f"classify_domains: sent {sent}/{len(doms)}, {errors} batch errors, "
                f"{len(results)} parsed (categories also land in the owner's view)")
    return {"status": "ok", "sent": sent, "batches": i + 1 if doms else 0,
            "results": results, "errors": errors}
