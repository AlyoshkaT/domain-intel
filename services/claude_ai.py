"""
Claude AI (Haiku) classification service.

READ  → esoteric-parsec-147012.es_analysis.claude_responses
        using QUALIFY ROW_NUMBER() to get latest per domain (same logic as
        latest_categories_claude VIEW, but extracts all fields incl. is_ecommerce)
WRITE → esoteric-parsec-147012.es_analysis.claude_responses
        (append-only log: domain, fetched_at, response_json JSON, input_hash)

Note: latest_categories_claude is just a VIEW on claude_responses that
      loses is_ecommerce — we read the source directly.
"""
import hashlib
import httpx
import json
import logging
from typing import Optional
from config.settings import ANTHROPIC_API_KEY, CORP_PROJECT_ID, CORP_DATASET, AI_API_TIMEOUT, HOMEPAGE_TIMEOUT

logger = logging.getLogger(__name__)

# Both read and write use claude_responses (the source of truth)
CORP_AI_TABLE     = f"`{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses`"
CORP_AI_TABLE_ID  = f"{CORP_PROJECT_ID}.{CORP_DATASET}.claude_responses"

# Prefetch cache key
CORP_AI_CACHE_KEY = "claude_responses"

CATEGORIES = [
    "product_ecom", "service_ecom", "marketplace",
    "non_transactional", "saas", "media", "finance",
    "healthcare", "education", "travel", "real_estate",
    "b2b", "logistics", "food", "automotive", "gaming",
    "non_profit", "government", "community", "high_risk", "other"
]

SUBCATEGORIES = [
    "fashion_accessories", "electronics", "home_garden", "food_grocery",
    "beauty_cosmetics", "sports_hobby_mil", "automotive_parts",
    "industrial_professional", "corporate_b2b", "saas", "news_media",
    "adult_content", "gambling", "finance_banking", "healthcare_medical",
    "education_elearning", "travel_hospitality", "real_estate",
    "entertainment_gaming", "community_forum", "other"
]


def _make_input_hash(domain: str, sw_title: str, sw_description: str) -> str:
    raw = f"{domain}|{sw_title[:100]}|{sw_description[:200]}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _parse_ai_row(data: dict) -> Optional[dict]:
    """Convert claude_responses response_json → our ai_ fields (all fields incl. is_ecommerce)."""
    category = data.get("category")
    if not category:
        return None
    is_ecom = data.get("is_ecommerce")
    return {
        "ai_category":     category,
        "ai_industry":     data.get("subcategory", ""),
        "ai_is_ecommerce": "Yes" if is_ecom is True or str(is_ecom).lower() in ("true", "1", "yes") else "No",
        "ai_region":       (data.get("region") or "").strip().upper(),
    }


def get_corp_ai_cached(domain: str) -> Optional[dict]:
    """Read AI classification from claude_responses (latest per domain, all fields)."""
    from core.bigquery import _prefetch_cache

    # Fast path: prefetch cache (populated at job start)
    if CORP_AI_CACHE_KEY in _prefetch_cache and domain in _prefetch_cache[CORP_AI_CACHE_KEY]:
        raw = _prefetch_cache[CORP_AI_CACHE_KEY][domain]
        if raw is None:
            logger.debug(f"Prefetch AI MISS: {domain}")
            return None
        data = raw if isinstance(raw, dict) else json.loads(raw)
        result = _parse_ai_row(data)
        if result:
            logger.debug(f"Prefetch AI HIT: {domain}")
        return result

    # Slow path: QUALIFY query — same logic as latest_categories_claude view
    from core.bigquery import corp_client, _bq_touch
    _bq_touch("corp_r")
    import google.cloud.bigquery as gcbq
    bq = corp_client()
    try:
        rows = list(bq.query(
            f"""SELECT
                  JSON_VALUE(response_json, '$.category')     AS category,
                  JSON_VALUE(response_json, '$.subcategory')  AS subcategory,
                  JSON_VALUE(response_json, '$.is_ecommerce') AS is_ecommerce,
                  JSON_VALUE(response_json, '$.region')       AS region
                FROM {CORP_AI_TABLE}
                WHERE domain = @domain
                QUALIFY ROW_NUMBER() OVER (ORDER BY fetched_at DESC) = 1""",
            job_config=gcbq.QueryJobConfig(
                query_parameters=[gcbq.ScalarQueryParameter("domain", "STRING", domain)]
            )
        ).result())
        if rows:
            result = _parse_ai_row(dict(rows[0]))
            if result:
                logger.info(f"Corp AI cache HIT: {domain}")
            return result
        return None
    except Exception as e:
        logger.error(f"Corp AI cache read error ({domain}): {e}")
        return None


def save_corp_ai_result(domain: str, result: dict, input_hash: str = ""):
    """DISABLED — DO NOT write to corp claude_responses.

    claude_responses is the OWNER's classifier cache that feeds their market_analysis
    reports. Our rows polluted it (incident 2026-10-17: backfill overrode their
    categories for ~150 domains). We now keep AI results ONLY in our private
    ai_parsed. This is a no-op kept so existing callers don't break.
    See memory: feedback_corp_bq_readonly / never write claude_responses.
    """
    return


AI_MODEL = "claude-haiku-5-5"
AI_MAX_TOKENS = 300


def build_classification_prompt(domain: str, sw_title: str = "", sw_description: str = "",
                                sw_category: str = "", bw_cms: str = "", bw_ecommerce: str = "",
                                homepage_text: str = "") -> str:
    """Build the classification prompt — shared by the live and Batch-API paths."""
    cats_str = ", ".join(CATEGORIES)
    subs_str = ", ".join(SUBCATEGORIES)
    context_parts = []
    if sw_title:       context_parts.append(f"Title: {sw_title}")
    if sw_description: context_parts.append(f"Description: {sw_description[:300]}")
    if sw_category:    context_parts.append(f"SW category: {sw_category}")
    if bw_cms:         context_parts.append(f"CMS: {bw_cms}")
    if bw_ecommerce:   context_parts.append(f"E-commerce platform: {bw_ecommerce}")
    if homepage_text:  context_parts.append(f"Homepage text:\n{homepage_text[:1000]}")
    context = "\n".join(context_parts) or f"Domain: {domain}"
    return f"""Classify this website. Domain: {domain}

{context}

For "region": the ISO 3166-1 alpha-2 country code (UPPERCASE, e.g. UA, DE, PL) of
the site's PRIMARY target market. Infer it ONLY from geo-stable signals that do not
change with the visitor's location: phone country code (+380→UA, +49→DE), currency
and prices (₴/грн→UA, zł→PL), physical address or postal code, and shipping/delivery
country mentions. Do NOT use the interface language — many sites switch language
based on the visitor's IP, so language is unreliable. If none of these signals are
present, return "" (empty string). Never guess from the TLD.

Respond ONLY with JSON (no markdown):
{{
  "category": "<one of: {cats_str}>",
  "subcategory": "<one of: {subs_str}>",
  "is_ecommerce": true or false,
  "region": "<ISO-2 code or empty string>",
  "category_reasoning": "<1 sentence>",
  "ecommerce_reasoning": "<1 sentence>"
}}"""


def parse_classification_text(text: str) -> Optional[dict]:
    """Parse a model JSON reply into our ai_* fields — shared by live + batch."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        parsed = json.loads(text.strip())
    except json.JSONDecodeError:
        return None
    is_ecom = parsed.get("is_ecommerce", False)
    return {
        "ai_category":            parsed.get("category", "other"),
        "ai_is_ecommerce":        "Yes" if is_ecom is True or str(is_ecom).lower() in ("true", "1", "yes") else "No",
        "ai_industry":            parsed.get("subcategory", "other"),
        "ai_region":              (parsed.get("region") or "").strip().upper(),
        "ai_category_reasoning":  parsed.get("category_reasoning", ""),
        "ai_ecommerce_reasoning": parsed.get("ecommerce_reasoning", ""),
    }


async def classify_domain(
    domain: str,
    sw_title: str = "",
    sw_description: str = "",
    sw_category: str = "",
    bw_cms: str = "",
    bw_ecommerce: str = "",
    homepage_text: str = "",
) -> Optional[dict]:
    """Classify domain using Claude Haiku. Returns dict with ai_* fields."""
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set")
        return None

    prompt = build_classification_prompt(domain, sw_title, sw_description, sw_category,
                                         bw_cms, bw_ecommerce, homepage_text)

    try:
        async with httpx.AsyncClient(timeout=AI_API_TIMEOUT) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": AI_MODEL,
                    "max_tokens": 300,
                    # Classification needs no reasoning — disable thinking so the whole
                    # token budget goes to the JSON (Haiku 5.5 has thinking ON by default,
                    # which would eat max_tokens and truncate the JSON) and cost stays low.
                    "thinking": {"type": "disabled"},
                    "messages": [{"role": "user", "content": prompt}],
                }
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["content"][0]["text"].strip()

            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()

            parsed = json.loads(text)
            is_ecom = parsed.get("is_ecommerce", False)
            return {
                "ai_category":              parsed.get("category", "other"),
                "ai_is_ecommerce":          "Yes" if is_ecom is True or str(is_ecom).lower() in ("true", "1", "yes") else "No",
                "ai_industry":              parsed.get("subcategory", "other"),
                "ai_region":                (parsed.get("region") or "").strip().upper(),
                "ai_category_reasoning":    parsed.get("category_reasoning", ""),
                "ai_ecommerce_reasoning":   parsed.get("ecommerce_reasoning", ""),
            }
    except json.JSONDecodeError as e:
        logger.error(f"Claude AI JSON parse error for {domain}: {e}")
        return None
    except Exception as e:
        logger.error(f"Claude AI error for {domain}: {e}")
        return None


import re as _re
# A common JS anti-bot interstitial hardcodes a hash, sets cookie
# challenge_passed=<hash>, then reloads. We can't run JS, but the hash is in the
# page — replay it as a cookie to fetch the real content (otherwise AI sees nothing
# and e.g. can't tell a Ukrainian shop is UA).
_CHALLENGE_HASH_RE = _re.compile(r'defaultHash\s*=\s*["\']([0-9a-fA-F]{16,})["\']')


async def fetch_homepage_text(domain: str) -> str:
    for url in [f"https://{domain}", f"http://{domain}"]:
        try:
            async with httpx.AsyncClient(
                timeout=HOMEPAGE_TIMEOUT, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; DomainIntel/1.0)"}
            ) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    continue
                html = resp.text
                if "challenge_passed" in html and len(html) < 5000:
                    m = _CHALLENGE_HASH_RE.search(html)
                    if m:
                        try:
                            resp2 = await client.get(url, cookies={"challenge_passed": m.group(1)})
                            if resp2.status_code == 200 and len(resp2.text) > len(html):
                                html = resp2.text
                        except Exception:
                            pass
                return _extract_text(html)
        except Exception:
            continue
    return ""


def _extract_text(html: str) -> str:
    import re
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    meta = re.findall(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', html, re.IGNORECASE)
    tags = re.findall(r'<(?:h[1-3]|p|title)[^>]*>(.*?)</(?:h[1-3]|p|title)>', html, re.DOTALL | re.IGNORECASE)
    text_parts = [re.sub(r'<[^>]+>', '', t).strip() for t in tags if len(re.sub(r'<[^>]+>', '', t).strip()) > 10]
    return (" ".join(meta) + " " + " ".join(text_parts))[:2000]
