"""
Simple authentication middleware.
Priority: AUTH_USERS env var → BQ app_users table → allow all.
"""
import base64
import os
import time
from fastapi import Request
from fastapi.responses import Response

_bq_users_cache: dict[str, str] = {}
_bq_users_cached_at: float = 0
_BQ_CACHE_TTL = 60  # seconds


def _load_env_users() -> dict[str, str]:
    raw = os.getenv("AUTH_USERS", "")
    if not raw:
        return {}
    users = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" in pair:
            u, p = pair.split(":", 1)
            users[u.strip()] = p.strip()
    return users


def _load_bq_users() -> dict[str, str]:
    global _bq_users_cache, _bq_users_cached_at
    now = time.time()
    if now - _bq_users_cached_at < _BQ_CACHE_TTL:
        return _bq_users_cache
    try:
        from core.bigquery import get_bq_users_for_auth
        users = get_bq_users_for_auth()
        _bq_users_cache = users
        _bq_users_cached_at = now
        return users
    except Exception:
        return _bq_users_cache


def get_auth_users() -> dict[str, str]:
    env_users = _load_env_users()
    if env_users:
        return env_users
    return _load_bq_users()


def invalidate_users_cache():
    global _bq_users_cached_at
    _bq_users_cached_at = 0


async def auth_middleware(request: Request, call_next):
    """Basic Auth middleware. Skips if no users configured."""
    if request.url.path in ["/api/health"]:
        return await call_next(request)

    users = get_auth_users()
    if not users:
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Basic "):
        return Response(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Domain Intel"'}
        )

    try:
        decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception:
        return Response(content="Unauthorized", status_code=401,
                        headers={"WWW-Authenticate": 'Basic realm="Domain Intel"'})

    if users.get(username) != password:
        return Response(content="Unauthorized", status_code=401,
                        headers={"WWW-Authenticate": 'Basic realm="Domain Intel"'})

    return await call_next(request)
