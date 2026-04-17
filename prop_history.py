"""
Historical prop line tracking.

Snapshots every prop line every day to Supabase, then grades them against
the actual player stats once games finish. Over time this gives us
real "hit rate vs the actual book line" data — not just hit rate vs
some arbitrary number.

Schema: see SQL in setup notes; table is `historical_props`.

Usage:
    snapshot_props(date, props_df)            # called from auto refresh
    grade_props(historical_data)              # called from auto grade
    get_player_line_history(player, stat)     # used by app + AI prompts
"""

import datetime
from typing import Optional

import pandas as pd

# Mapping from The Odds API "type" values to our stat keys
PROP_TYPE_TO_STAT = {
    "Total Points": "points",
    "Total Rebounds": "rebounds",
    "Total Assists": "assists",
    "Total PRA": "pra",
    "Total 3PM": "threes",
    "Total Steals": "steals",
    "Total Blocks": "blocks",
}


def _admin_client():
    """Service-role Supabase client. Required for writes."""
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
    """Anon Supabase client for reads."""
    from auth import get_supabase
    return get_supabase()


def snapshot_props(game_date: datetime.date, props_df: pd.DataFrame, book: str = "draftkings") -> int:
    """Upsert every prop line for the day to historical_props.

    Idempotent — re-running the same day overwrites existing rows for
    that (date, player, stat, book) combo. Status defaults to 'pending'
    and gets filled in by grade_props() later.
    """
    sb = _admin_client()
    if sb is None or props_df.empty:
        return 0

    rows = []
    for _, row in props_df.iterrows():
        stat_key = PROP_TYPE_TO_STAT.get(row.get("type"))
        if not stat_key:
            continue
        rows.append({
            "date": str(game_date),
            "player": row["name"] if "name" in props_df.columns else row.get("player"),
            "stat": stat_key,
            "line": float(row["spread"]),
            "book": book,
            "actual": None,
            "result": None,
            "status": "pending",
        })
    if not rows:
        return 0
    sb.table("historical_props").upsert(
        rows, on_conflict="date,player,stat,book"
    ).execute()
    return len(rows)


def grade_props(historical_data: pd.DataFrame, up_to_date: Optional[datetime.date] = None) -> int:
    """Look up each pending prop's player + date in the box scores and fill in
    the actual stat + result (over / under / push / dnp)."""
    sb = _admin_client()
    if sb is None or historical_data.empty:
        return 0

    df = historical_data.rename(columns={
        "player": "name", "team_code": "team", "opponent_code": "opponent",
        "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
        "threefm": "threes", "stl": "steals", "blk": "blocks",
    })
    for c in ("points", "rebounds", "assists", "minutes", "threes", "steals", "blocks"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    if all(c in df.columns for c in ("points", "rebounds", "assists")):
        df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    df["date_string"] = df.get("date_string", df.get("game_gameday"))

    cutoff = str(up_to_date or datetime.date.today())
    pending = sb.table("historical_props").select("*").eq("status", "pending").lte("date", cutoff).execute()
    pending_props = pending.data or []
    if not pending_props:
        return 0

    graded = 0
    for prop in pending_props:
        match = df[(df["name"] == prop["player"]) & (df["date_string"] == prop["date"])]
        if match.empty:
            # Player simply has no record on that date — could be a DNP or
            # a non-game day. Mark as DNP after a 7-day grace period so we
            # stop trying to grade it.
            prop_date = pd.to_datetime(prop["date"])
            if (pd.Timestamp.today() - prop_date).days >= 7:
                sb.table("historical_props").update({"status": "dnp"}).eq("id", prop["id"]).execute()
            continue

        stat = prop["stat"]
        if stat not in match.columns:
            continue
        actual = float(match.iloc[0][stat])
        line = prop["line"]
        if actual > line:
            result = "over"
        elif actual < line:
            result = "under"
        else:
            result = "push"
        sb.table("historical_props").update({
            "actual": actual,
            "result": result,
            "status": "graded",
        }).eq("id", prop["id"]).execute()
        graded += 1

    return graded


def get_player_line_history(
    player: str,
    stat: str,
    near_line: Optional[float] = None,
    line_window: float = 1.0,
    limit: int = 200,
) -> dict:
    """Return hit-rate stats for a player + stat across all tracked lines.

    If `near_line` is provided, also returns hit rate for lines within
    ±line_window of that value (useful for "this exact line size" stats).
    """
    sb = _anon_client()
    if sb is None:
        return {"available": False}
    try:
        resp = (
            sb.table("historical_props")
            .select("date, line, actual, result")
            .eq("player", player)
            .eq("stat", stat)
            .eq("status", "graded")
            .order("date", desc=True)
            .limit(limit)
            .execute()
        )
        rows = resp.data or []
    except Exception:
        return {"available": False}

    if not rows:
        return {
            "available": True,
            "all_games": 0,
            "all_overs": 0,
            "all_unders": 0,
            "near_games": 0,
            "near_overs": 0,
            "near_unders": 0,
        }

    overs = sum(1 for r in rows if r["result"] == "over")
    unders = sum(1 for r in rows if r["result"] == "under")

    near_overs = near_unders = near_games = 0
    if near_line is not None:
        for r in rows:
            if abs(float(r["line"]) - near_line) <= line_window:
                near_games += 1
                if r["result"] == "over":
                    near_overs += 1
                elif r["result"] == "under":
                    near_unders += 1

    return {
        "available": True,
        "all_games": len(rows),
        "all_overs": overs,
        "all_unders": unders,
        "near_games": near_games,
        "near_overs": near_overs,
        "near_unders": near_unders,
        "near_line": near_line,
        "line_window": line_window,
    }


def total_tracked() -> dict:
    """Top-level counters for the admin diagnostic panel."""
    sb = _anon_client()
    if sb is None:
        return {"available": False}
    try:
        total = sb.table("historical_props").select("id", count="exact").limit(1).execute()
        graded = sb.table("historical_props").select("id", count="exact").eq("status", "graded").limit(1).execute()
        return {
            "available": True,
            "total": total.count or 0,
            "graded": graded.count or 0,
        }
    except Exception:
        return {"available": False}
