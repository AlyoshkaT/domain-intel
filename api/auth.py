"""
Simple authentication middleware.
Users defined in .env as AUTH_USERS=user1:pass1,user2:pass2
"""
import base64
import os
from fastapi import Request, HTTPException
from fastapi.responses import Response

def get_auth_users() -> dict[str, str]:
    """Parse AUTH_USERS from env: 'user1:pass1,user2:pass2'"""
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


async def auth_middleware(request: Request, call_next):
    """Basic Auth middleware. Skips if AUTH_USERS not set."""
    users = get_auth_users()
    if not users:
        # Auth disabled — allow all
        return await call_next(request)

    # Allow health check without auth
    if request.url.path in ["/api/health"]:
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
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if users.get(username) != password:
        return Response(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Domain Intel"'}
        )

    return await call_next(request)
