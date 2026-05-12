"""
Credits service — tracks remaining API credits for BuiltWith and SimilarWeb.
Stores in app_settings table in our BQ.
"""
import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# In-memory cache to avoid BQ reads on every request
_credits_cache: dict = {}


def _save_setting(key: str, value: str):
    """Save or update a setting in BQ (delegates to core set_setting for MERGE upsert)."""
    try:
        from core.bigquery import set_setting
        set_setting(key, value)
    except Exception as e:
        logger.error(f"app_settings save error ({key}): {e}")


# ─── BuiltWith credits ────────────────────────────────────────────────────────

async def fetch_builtwith_credits() -> Optional[int]:
    """Fetch remaining BuiltWith credits from whoami endpoint."""
    from config.settings import BUILTWITH_API_KEY
    if not BUILTWITH_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(
                "https://api.builtwith.com/whoamiv1/api.json",
                params={"KEY": BUILTWITH_API_KEY}
            )
            resp.raise_for_status()
            data = resp.json()
            remaining = data.get("credits", {}).get("remaining")
            if remaining is not None:
                remaining = int(remaining)
                _credits_cache["builtwith"] = remaining
                from core.bigquery import set_setting
                set_setting("builtwith_credits_remaining", str(remaining))
                logger.info(f"BuiltWith credits: {remaining}")
            return remaining
    except Exception as e:
        logger.error(f"BuiltWith credits fetch error: {e}")
        return None


def update_similarweb_credits_from_headers(headers: dict):
    """Extract and save SimilarWeb credits from response headers."""
    remaining = headers.get("x-ratelimit-requests-remaining")
    if remaining is not None:
        try:
            remaining = int(remaining)
            _credits_cache["similarweb"] = remaining
            from core.bigquery import set_setting
            set_setting("similarweb_credits_remaining", str(remaining))
            logger.info(f"SimilarWeb credits updated: {remaining}")
        except ValueError:
            pass
        except Exception as e:
            logger.error(f"SimilarWeb credits save error: {e}")



def get_cached_credits() -> dict:
    """Get credits from in-memory cache, fallback to BQ."""
    from core.bigquery import get_setting
    result = {}

    # BuiltWith
    if "builtwith" in _credits_cache:
        result["builtwith"] = _credits_cache["builtwith"]
    else:
        val = get_setting("builtwith_credits_remaining")
        if val is not None:
            try:
                result["builtwith"] = int(val)
                _credits_cache["builtwith"] = int(val)
            except ValueError:
                pass

    # SimilarWeb
    if "similarweb" in _credits_cache:
        result["similarweb"] = _credits_cache["similarweb"]
    else:
        val = get_setting("similarweb_credits_remaining")
        if val is not None:
            try:
                result["similarweb"] = int(val)
                _credits_cache["similarweb"] = int(val)
            except ValueError:
                pass

    return result
