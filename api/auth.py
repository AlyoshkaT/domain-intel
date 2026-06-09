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
    # Load passwords — critical, must succeed
    try:
        from core.bigquery import get_bq_users_for_auth
        users = get_bq_users_for_auth()
        _bq_users_cache = users
        _bq_users_cached_at = now
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to load BQ users: {e}")
        return _bq_users_cache
    # Load permissions — separate, non-critical (failure keeps old cache)
    try:
        from core.bigquery import get_bq_users_permissions
        _bq_permissions_cache = get_bq_users_permissions()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to load BQ permissions: {e}")
    return _bq_users_cache


def get_auth_users() -> dict[str, str]:
    """Merge env users + BQ users. ENV takes priority on conflict."""
    bq_users = _load_bq_users()
    env_users = _load_env_users()
    return {**bq_users, **env_users}


_ALL_PERMS = {"explorer", "jobs", "download", "sheets", "admin"}

def get_user_permissions(username: str) -> set[str]:
    """Return set of permissions for the given username.
    If no users configured (anonymous mode) → full access."""
    _load_bq_users()
    # ENV users always get full access
    env_users = _load_env_users()
    if username in env_users:
        return _ALL_PERMS.copy()
    # No auth configured at all → full access
    all_users = get_auth_users()
    if not all_users or username == "anonymous":
        return _ALL_PERMS.copy()
    # Permissions cache may be empty if load failed — fall back to full access
    # so existing users don't get locked out
    if not _bq_permissions_cache:
        return _ALL_PERMS.copy()
    # Bootstrap: if NO user in the system has admin permission yet,
    # treat all authenticated BQ users as admins (first-time setup)
    any_admin = any("admin" in (v or "") for v in _bq_permissions_cache.values())
    if not any_admin:
        return _ALL_PERMS.copy()
    perm_str = _bq_permissions_cache.get(username, "")
    if not perm_str:
        return _ALL_PERMS.copy()  # user exists but no permissions recorded → full access (legacy)
    perms = set(p.strip() for p in perm_str.split(",") if p.strip())
    if "admin" in perms:
        return _ALL_PERMS.copy()
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


_login_logged: dict[str, float] = {}   # username → last logged timestamp
_LOGIN_LOG_INTERVAL = 3600             # log once per hour per user

def _maybe_log_login(username: str):
    now = time.time()
    if now - _login_logged.get(username, 0) < _LOGIN_LOG_INTERVAL:
        return
    _login_logged[username] = now
    try:
        from core.bigquery import log_activity
        log_activity(username, "login", {})
    except Exception:
        pass


async def auth_middleware(request: Request, call_next):
    """Basic Auth middleware. Skips if no users configured."""
    path = request.url.path

    # Static assets and health — always public (no auth dialog for JS/CSS/favicon)
    if (path.startswith("/assets/") or
            path.endswith(".json") and not path.startswith("/api/") or
            path in ("/api/health", "/favicon.ico", "/favicon.png", "/robots.txt")):
        return await call_next(request)

    users = get_auth_users()
    if not users:
        request.state.username = "anonymous"
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Basic "):
        # API calls: return plain 401 (no WWW-Authenticate) — SPA handles it silently.
        # HTML pages: return 401 with WWW-Authenticate to trigger the browser native dialog.
        is_api = path.startswith("/api/")
        headers = {} if is_api else {"WWW-Authenticate": 'Basic realm="Domain Intel"'}
        return Response(content="Unauthorized", status_code=401, headers=headers)

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
    _maybe_log_login(username)
    return await call_next(request)
