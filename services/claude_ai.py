"""
Claude AI (Haiku) classification service
"""
import httpx
import json
import logging
from typing import Optional
from config.settings import ANTHROPIC_API_KEY, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

CATEGORIES_17 = [
    "E-commerce / Retail", "SaaS / Software", "Media / Publishing",
    "Finance / Banking", "Healthcare", "Education", "Travel / Hospitality",
    "Real Estate", "B2B Services", "Logistics / Supply Chain",
    "Food / Restaurant", "Automotive", "Gaming / Entertainment",
    "Non-profit / NGO", "Government", "Community / Forum", "Other"
]


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

    categories_str = "\n".join(f"- {c}" for c in CATEGORIES_17)
    context_parts = []
    if sw_title:
        context_parts.append(f"Site title: {sw_title}")
    if sw_description:
        context_parts.append(f"Description: {sw_description}")
    if sw_category:
        context_parts.append(f"SimilarWeb category: {sw_category}")
    if bw_cms:
        context_parts.append(f"CMS: {bw_cms}")
    if bw_ecommerce:
        context_parts.append(f"E-commerce platform: {bw_ecommerce}")
    if homepage_text:
        context_parts.append(f"Homepage content (excerpt):\n{homepage_text[:1500]}")

    context = "\n".join(context_parts) or f"Domain: {domain}"

    prompt = f"""Analyze this website and classify it. Domain: {domain}

Available information:
{context}

Respond ONLY with a JSON object (no markdown, no explanation):
{{
  "category": "<one of the 17 categories below>",
  "is_ecommerce": "<Yes|No|Unknown>",
  "industry": "<short industry description, e.g. 'Online Fashion Retail' or 'B2B SaaS (CRM)'>"
}}

Categories to choose from:
{categories_str}"""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": prompt}],
                }
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["content"][0]["text"].strip()

            # Strip markdown fences if present
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()

            parsed = json.loads(text)
            return {
                "ai_category": parsed.get("category", ""),
                "ai_is_ecommerce": parsed.get("is_ecommerce", "Unknown"),
                "ai_industry": parsed.get("industry", ""),
            }
    except json.JSONDecodeError as e:
        logger.error(f"Claude AI JSON parse error for {domain}: {e}")
        return None
    except Exception as e:
        logger.error(f"Claude AI error for {domain}: {e}")
        return None


async def fetch_homepage_text(domain: str) -> str:
    """Fetch and parse homepage text for AI classification."""
    urls_to_try = [f"https://{domain}", f"http://{domain}"]
    for url in urls_to_try:
        try:
            async with httpx.AsyncClient(
                timeout=10,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; DomainIntel/1.0)"}
            ) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return _extract_text(resp.text)
        except Exception:
            continue
    return ""


def _extract_text(html: str) -> str:
    """Simple text extraction without BeautifulSoup dependency."""
    import re
    # Remove scripts and styles
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Extract meta description
    meta = re.findall(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', html, re.IGNORECASE)
    meta_text = " ".join(meta)
    # Extract headings and paragraphs
    tags = re.findall(r'<(?:h[1-3]|p|title)[^>]*>(.*?)</(?:h[1-3]|p|title)>', html, re.DOTALL | re.IGNORECASE)
    text_parts = [re.sub(r'<[^>]+>', '', t).strip() for t in tags]
    text_parts = [t for t in text_parts if len(t) > 10]
    combined = meta_text + " " + " ".join(text_parts)
    return combined[:2000]
