"""Supabase auth for the FastAPI backend.

Verifies the JWT that the frontend's Supabase client sends in the
``Authorization: Bearer <token>`` header. Supabase signs access tokens with
the project's JWT secret (HS256), so we can verify them locally without a
network round-trip per request.

Graceful degradation: if ``SUPABASE_JWT_SECRET`` is not set the backend still
starts and serves the public projection endpoints. The user-specific endpoints
that depend on :func:`get_current_user` then respond with 503 so the frontend
can hide auth features rather than crash.
"""
from __future__ import annotations

import os
from typing import Optional

from fastapi import Header, HTTPException

try:
    import jwt  # PyJWT
    from jwt import PyJWTError
except Exception:  # pragma: no cover - dependency missing
    jwt = None  # type: ignore[assignment]
    PyJWTError = Exception  # type: ignore[assignment,misc]

SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")
# Supabase access tokens carry the "authenticated" audience.
_JWT_AUDIENCE = "authenticated"


def auth_configured() -> bool:
    """True when the backend can verify Supabase tokens."""
    return bool(jwt and SUPABASE_JWT_SECRET)


class AuthUser:
    """The authenticated Supabase user, derived from the JWT claims."""

    def __init__(self, id: str, email: Optional[str], claims: dict) -> None:
        self.id = id
        self.email = email
        self.claims = claims


def get_current_user(authorization: Optional[str] = Header(None)) -> AuthUser:
    """FastAPI dependency: require and verify a Supabase access token.

    Raises 503 when auth isn't configured, 401 when the token is missing or
    invalid.
    """
    if not auth_configured():
        raise HTTPException(
            status_code=503,
            detail="Auth is not configured on the server (SUPABASE_JWT_SECRET unset).",
        )
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token.")

    token = authorization.split(" ", 1)[1].strip()
    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience=_JWT_AUDIENCE,
        )
    except PyJWTError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token missing subject claim.")

    return AuthUser(id=user_id, email=payload.get("email"), claims=payload)
