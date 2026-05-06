"""
Simple authentication middleware.
Priority: AUTH_USERS env var → BQ app_users table → allow all.
"""
import base64
import os
import time
from fastapi import Request, HTTPException, Depends
from fastapi.responses import Response

_bq_users_cache: dict[str, str] = {}          # username → password
_bq_permissions_cache: dict[str, str] = {}    # username → "explorer,jobs,..."
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
    global _bq_users_cache, _bq_permissions_cache, _bq_users_cached_at
    now = time.time()
    if now - _bq_users_cached_at < _BQ_CACHE_TTL:
        return _bq_users_cache
    try:
        from core.bigquery import get_bq_users_for_auth, get_bq_users_permissions
        users = get_bq_users_for_auth()
        _bq_permissions_cache = get_bq_users_permissions()
        _bq_users_cache = users
        _bq_users_cached_at = now
        return users
    except Exception:
        return _bq_users_cache


def get_auth_users() -> dict[str, str]:
    """Merge env users + BQ users. ENV takes priority on conflict."""
    bq_users = _load_bq_users()
    env_users = _load_env_users()
    return {**bq_users, **env_users}


def get_user_permissions(username: str) -> set[str]:
    """Return set of permissions for the given username.
    If no users configured (anonymous mode) → full access."""
    # Refresh cache if needed
    _load_bq_users()
    # ENV users always get full access (they're configured outside BQ)
    env_users = _load_env_users()
    if username in env_users:
        return {"explorer", "jobs", "download", "sheets", "admin"}
    # No auth configured → full access
    all_users = get_auth_users()
    if not all_users or username == "anonymous":
        return {"explorer", "jobs", "download", "sheets", "admin"}
    perm_str = _bq_permissions_cache.get(username, "")
    perms = set(p.strip() for p in perm_str.split(",") if p.strip())
    # admin implies everything
    if "admin" in perms:
        return {"explorer", "jobs", "download", "sheets", "admin"}
    return perms


def require_permission(perm: str):
    """FastAPI dependency: raises 403 if user lacks the required permission."""
    def checker(request: Request):
        username = getattr(request.state, "username", "anonymous")
        perms = get_user_permissions(username)
        if perm not in perms:
            raise HTTPException(
                status_code=403,
                detail=f"Недостатньо прав: потрібен дозвіл '{perm}'"
            )
    return Depends(checker)


def invalidate_users_cache():
    global _bq_users_cached_at
    _bq_users_cached_at = 0


async def auth_middleware(request: Request, call_next):
    """Basic Auth middleware. Skips if no users configured."""
    if request.url.path in ["/api/health"]:
        return await call_next(request)

    users = get_auth_users()
    if not users:
        request.state.username = "anonymous"
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

    request.state.username = username
    return await call_next(request)
