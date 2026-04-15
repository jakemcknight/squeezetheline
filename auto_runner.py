"""
In-app auto-refresh / auto-grade.

NBA.com blocks GitHub Actions IP ranges, so instead of running the daily
pipeline from a cron job, we trigger it on the first relevant page load
inside Streamlit Cloud (whose IPs do work with NBA.com).

Two checks happen on every app load (cached so they only do real work
when the criteria are met):

1. Auto-refresh — if it's past 10am ET today and Supabase has no picks
   for today, generate them.
2. Auto-grade — if it's past 2am ET and Supabase has any pending picks
   from a previous date that should now be gradable, grade them.
"""

import datetime
from zoneinfo import ZoneInfo

import streamlit as st


ET = ZoneInfo("America/New_York")
REFRESH_HOUR_ET = 10  # 10am
GRADE_HOUR_ET = 2     # 2am


def _now_et() -> datetime.datetime:
    return datetime.datetime.now(tz=ET)


def _today_et() -> datetime.date:
    return _now_et().date()


def _supabase_anon():
    """Read-only Supabase client (uses the anon key)."""
    from auth import get_supabase
    return get_supabase()


def _supabase_admin():
    """Service-role client for writes — only available if SUPABASE_SERVICE_ROLE_KEY is set."""
    import os
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL") or st.secrets.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        return None
    try:
        return create_client(url, key)
    except Exception:
        return None


def _todays_picks_exist() -> bool:
    sb = _supabase_anon()
    if not sb:
        return True  # if Supabase isn't configured, don't try to refresh
    try:
        resp = sb.table("auto_picks").select("id", count="exact").eq("date", str(_today_et())).limit(1).execute()
        return (resp.count or 0) > 0
    except Exception:
        return True  # be conservative on error


def _get_pending_pick_dates() -> list[str]:
    """Return dates for which we still have un-graded picks (excluding today)."""
    sb = _supabase_anon()
    if not sb:
        return []
    try:
        resp = sb.table("auto_picks").select("date").eq("result", "pending").lt("date", str(_today_et())).execute()
        dates = sorted(set(r["date"] for r in (resp.data or [])))
        return dates
    except Exception:
        return []


def maybe_auto_refresh() -> dict:
    """If conditions are met, generate today's picks. Returns a status dict."""
    now = _now_et()
    if now.hour < REFRESH_HOUR_ET:
        return {"action": "skip", "reason": f"too early (before {REFRESH_HOUR_ET}am ET)"}
    if _todays_picks_exist():
        return {"action": "skip", "reason": "picks already saved for today"}
    if _supabase_admin() is None:
        return {"action": "skip", "reason": "no service-role key configured (admin only)"}
    if st.session_state.get("_auto_refresh_in_progress"):
        return {"action": "skip", "reason": "another tab/session is already running"}

    st.session_state["_auto_refresh_in_progress"] = True
    try:
        from auto_picks import generate_and_save_picks
        n = generate_and_save_picks(_today_et())
        return {"action": "ran", "saved": n}
    except Exception as e:
        return {"action": "error", "error": str(e)}
    finally:
        st.session_state["_auto_refresh_in_progress"] = False


def maybe_auto_grade() -> dict:
    """If conditions are met, grade pending picks. Returns a status dict."""
    now = _now_et()
    if now.hour < GRADE_HOUR_ET:
        return {"action": "skip", "reason": f"too early (before {GRADE_HOUR_ET}am ET)"}
    pending_dates = _get_pending_pick_dates()
    if not pending_dates:
        return {"action": "skip", "reason": "no pending picks to grade"}
    if _supabase_admin() is None:
        return {"action": "skip", "reason": "no service-role key configured (admin only)"}
    if st.session_state.get("_auto_grade_in_progress"):
        return {"action": "skip", "reason": "another tab/session is already running"}

    st.session_state["_auto_grade_in_progress"] = True
    try:
        # Backfill latest box scores so we have yesterday's stats
        from backfill import backfill
        backfill()
        from auto_picks import grade_pending_picks
        n = grade_pending_picks(_today_et())
        return {"action": "ran", "graded": n}
    except Exception as e:
        return {"action": "error", "error": str(e)}
    finally:
        st.session_state["_auto_grade_in_progress"] = False


def run_daily_jobs(silent: bool = True) -> dict:
    """Top-level entry point. Returns the status of both jobs."""
    refresh = maybe_auto_refresh()
    grade = maybe_auto_grade()
    return {"refresh": refresh, "grade": grade}
