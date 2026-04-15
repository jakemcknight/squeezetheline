"""
Supabase-backed auth for Squeeze the Line.

Users sign up with email + password (handled by Supabase). Admin status is
controlled by an allow-list of emails in Streamlit secrets — keeps the
permissions model dead simple without needing a separate roles table.

Required secrets / env vars:
    SUPABASE_URL          - Your Supabase project URL (https://xxx.supabase.co)
    SUPABASE_ANON_KEY     - Public anon key from Supabase API settings
    ADMIN_EMAILS          - Comma-separated list of admin emails
"""

import os
from typing import Optional

import streamlit as st
from supabase import create_client, Client


def _get_secret(name: str, default: str = "") -> str:
    """Read a value from env var, falling back to st.secrets, then default."""
    val = os.environ.get(name)
    if val:
        return val
    try:
        return st.secrets[name]
    except Exception:
        return default


@st.cache_resource
def get_supabase() -> Optional[Client]:
    """Build (or reuse) the Supabase client. Cached for the app lifetime."""
    url = _get_secret("SUPABASE_URL")
    key = _get_secret("SUPABASE_ANON_KEY")
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception as e:
        print(f"Supabase init failed: {e}")
        return None


def get_admin_emails() -> list[str]:
    raw = _get_secret("ADMIN_EMAILS", "")
    return [e.strip().lower() for e in raw.split(",") if e.strip()]


def sign_up(email: str, password: str) -> tuple[bool, str]:
    """Sign up a new user. Returns (success, message)."""
    sb = get_supabase()
    if not sb:
        return False, "Auth not configured."
    try:
        sb.auth.sign_up({"email": email, "password": password})
        return True, "Account created. Check your email to verify before signing in."
    except Exception as e:
        return False, f"Sign up failed: {e}"


def sign_in(email: str, password: str) -> tuple[bool, str]:
    """Sign in an existing user. Returns (success, message)."""
    sb = get_supabase()
    if not sb:
        return False, "Auth not configured."
    try:
        resp = sb.auth.sign_in_with_password({"email": email, "password": password})
        if resp.user is None:
            return False, "Invalid credentials."
        # Stash the session in Streamlit so it survives reruns
        st.session_state["sb_user"] = {
            "id": resp.user.id,
            "email": resp.user.email,
        }
        st.session_state["sb_session"] = {
            "access_token": resp.session.access_token if resp.session else None,
            "refresh_token": resp.session.refresh_token if resp.session else None,
        }
        return True, "Signed in."
    except Exception as e:
        return False, f"Sign in failed: {e}"


def sign_out():
    """Clear the user session."""
    sb = get_supabase()
    try:
        if sb:
            sb.auth.sign_out()
    except Exception:
        pass
    for k in ("sb_user", "sb_session", "selected_player", "view_picks"):
        st.session_state.pop(k, None)


def current_user() -> Optional[dict]:
    """Return the currently signed-in user dict, or None."""
    return st.session_state.get("sb_user")


def is_admin() -> bool:
    """Return True if the current user's email is in ADMIN_EMAILS."""
    user = current_user()
    if not user:
        return False
    return (user.get("email", "").lower() in get_admin_emails())


def is_authenticated() -> bool:
    return current_user() is not None
