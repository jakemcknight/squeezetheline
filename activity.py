"""
User activity tracking.

Logs every login, nav change, player view, pick save, AI query, etc.
to a Supabase `user_activity` table so we can see who's using the
platform and what they're doing.

Writes go through the service-role key (needed to bypass RLS). Reads
for the admin analytics view use the anon key.

Each log call runs in a background thread so it never blocks the UI.
"""

import datetime
import os
import threading
from typing import Optional

import pandas as pd


# Actions we log (string constants to avoid typos)
ACTION_LOGIN = "login"
ACTION_SIGN_UP = "sign_up"
ACTION_PAGE_VIEW = "page_view"        # user switched top-nav tab
ACTION_PLAYER_VIEW = "player_view"    # opened a player detail page
ACTION_SAVE_PICK = "save_pick"        # manually saved an over/under
ACTION_AI_ANALYSIS = "ai_analysis"    # asked Claude about a prop
ACTION_MANUAL_REFRESH = "manual_refresh"


def _admin_client():
    """Service-role client needed to bypass RLS on inserts."""
    import os
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        try:
            import streamlit as st
            url = url or st.secrets.get("SUPABASE_URL", "")
            key = key or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY", "")
        except Exception:
            pass
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception:
        return None


def _anon_client():
    from auth import get_supabase
    return get_supabase()


def _current_user_info() -> Optional[dict]:
    """Read the signed-in user from Streamlit session state."""
    try:
        import streamlit as st
        user = st.session_state.get("sb_user")
        if user and user.get("id"):
            return user
    except Exception:
        pass
    return None


def log(action: str, details: Optional[dict] = None, user: Optional[dict] = None):
    """Fire-and-forget activity logger.

    Runs the Supabase insert in a background thread so it never blocks
    the UI. Silently no-ops if we can't reach Supabase or there's no user.
    """
    user = user or _current_user_info()
    if not user:
        return
    payload = {
        "user_id": user["id"],
        "user_email": user.get("email", ""),
        "action": action,
        "details": details or {},
    }

    def _do_insert():
        sb = _admin_client()
        if not sb:
            return
        try:
            sb.table("user_activity").insert(payload).execute()
        except Exception as e:
            print(f"[activity] insert failed: {e}")

    threading.Thread(target=_do_insert, daemon=True).start()


def log_once_per_session(key: str, action: str, details: Optional[dict] = None):
    """Log an action only once per Streamlit session (e.g. login event)."""
    try:
        import streamlit as st
        state_key = f"_activity_logged_{key}"
        if st.session_state.get(state_key):
            return
        st.session_state[state_key] = True
    except Exception:
        return
    log(action, details)


# --- Query helpers for the admin analytics view ---

def fetch_recent_activity(limit: int = 500) -> pd.DataFrame:
    """Most-recent N events."""
    sb = _anon_client()
    if not sb:
        return pd.DataFrame()
    try:
        resp = sb.table("user_activity").select("*").order("created_at", desc=True).limit(limit).execute()
        return pd.DataFrame(resp.data or [])
    except Exception:
        return pd.DataFrame()


def fetch_activity_since(days: int = 30) -> pd.DataFrame:
    """All events in the last N days."""
    sb = _anon_client()
    if not sb:
        return pd.DataFrame()
    since = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat() + "Z"
    try:
        resp = sb.table("user_activity").select("*").gte("created_at", since).order("created_at", desc=True).limit(10000).execute()
        return pd.DataFrame(resp.data or [])
    except Exception:
        return pd.DataFrame()


def summarize(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"events": 0, "unique_users": 0, "logins": 0, "player_views": 0, "ai_queries": 0}
    return {
        "events": len(df),
        "unique_users": df["user_email"].nunique(),
        "logins": int((df["action"] == ACTION_LOGIN).sum()),
        "player_views": int((df["action"] == ACTION_PLAYER_VIEW).sum()),
        "ai_queries": int((df["action"] == ACTION_AI_ANALYSIS).sum()),
    }


def dau_series(df: pd.DataFrame) -> pd.DataFrame:
    """Daily active users (count of distinct emails that did anything each day)."""
    if df.empty:
        return pd.DataFrame(columns=["date", "dau"])
    df = df.copy()
    df["date"] = pd.to_datetime(df["created_at"]).dt.date
    dau = df.groupby("date")["user_email"].nunique().reset_index(name="dau")
    return dau.sort_values("date")


def per_user_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Per-user breakdown: last seen, total events, active days."""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["created_at"]).dt.date
    summary = df.groupby("user_email").agg(
        events=("id", "count"),
        active_days=("date", "nunique"),
        last_seen=("created_at", "max"),
        first_seen=("created_at", "min"),
    ).reset_index()
    summary["last_seen"] = pd.to_datetime(summary["last_seen"]).dt.strftime("%Y-%m-%d %H:%M UTC")
    summary["first_seen"] = pd.to_datetime(summary["first_seen"]).dt.strftime("%Y-%m-%d")
    return summary.sort_values("last_seen", ascending=False)
