"""Supabase database access for user-specific features.

Uses the service-role key for server-side writes/reads against the
``saved_picks``, ``pick_results`` and ``alerts`` tables (see
``supabase_schema.sql``). Because the API authenticates the user itself (see
``auth.py``) and scopes every query by ``user_id``, the service-role client is
appropriate here.

Graceful degradation: if Supabase isn't configured (missing env vars or the
``supabase`` package isn't installed) :func:`get_db` returns ``None`` and the
user endpoints respond with 503.
"""
from __future__ import annotations

import os
from typing import Optional

try:
    from supabase import Client, create_client
except Exception:  # pragma: no cover - dependency missing
    Client = None  # type: ignore[assignment,misc]
    create_client = None  # type: ignore[assignment]

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

_client: "Optional[Client]" = None


def db_configured() -> bool:
    return bool(create_client and SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


def get_db() -> "Optional[Client]":
    """Return a memoised Supabase service client, or ``None`` if unconfigured."""
    global _client
    if not db_configured():
        return None
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _client
