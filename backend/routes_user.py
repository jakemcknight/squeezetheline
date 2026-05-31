"""User-specific, auth-protected endpoints.

These wrap the Supabase tables defined in ``supabase_schema.sql``. Every query
is scoped to the authenticated user's id (from the verified JWT), so a user can
only ever read or write their own rows.

All routes here depend on :func:`auth.get_current_user`; when Supabase isn't
configured the dependency / DB helper short-circuits with 503 so the frontend
degrades gracefully.
"""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException

from auth import AuthUser, get_current_user
from db import get_db
from models import (
    Alert,
    AlertRequest,
    HistoryStats,
    PickHistory,
    SavedPick,
    SavePickRequest,
)

router = APIRouter(prefix="/api", tags=["user"])


def _require_db():
    db = get_db()
    if db is None:
        raise HTTPException(
            status_code=503,
            detail="Supabase is not configured on the server.",
        )
    return db


def _row_to_saved_pick(row: dict) -> SavedPick:
    """Map a ``saved_picks`` row (optionally with a joined result) to the model."""
    results = row.get("pick_results") or []
    result_row = results[0] if isinstance(results, list) and results else (
        results if isinstance(results, dict) else None
    )
    return SavedPick(
        id=str(row["id"]),
        pick_id=row["pick_id"],
        player=row["player"],
        team_abbr=row.get("team_abbr", ""),
        opponent_abbr=row.get("opponent_abbr", ""),
        sport=row["sport"],
        stat_type=row["stat_type"],
        line=row["line"],
        projection=row["projection"],
        edge=row["edge"],
        confidence=row["confidence"],
        side=row.get("side", "none"),
        recommendation=row.get("recommendation", ""),
        game_time=row.get("game_time"),
        saved_at=row["saved_at"],
        result=(result_row or {}).get("result", "pending"),
        actual_value=(result_row or {}).get("actual_value"),
        settled_at=(result_row or {}).get("settled_at"),
    )


@router.post("/picks/save", response_model=SavedPick)
def save_pick(
    body: SavePickRequest,
    user: AuthUser = Depends(get_current_user),
) -> SavedPick:
    """Save a pick to the user's tracking list (idempotent per pick_id)."""
    db = _require_db()
    payload = {
        "user_id": user.id,
        "pick_id": body.pick_id,
        "player": body.player,
        "team_abbr": body.team_abbr,
        "opponent_abbr": body.opponent_abbr,
        "sport": body.sport,
        "stat_type": body.stat_type,
        "line": body.line,
        "projection": body.projection,
        "edge": body.edge,
        "confidence": body.confidence,
        "side": body.side,
        "recommendation": body.recommendation,
        "game_time": body.game_time,
    }
    # Upsert on (user_id, pick_id) so re-saving the same pick is a no-op update.
    resp = (
        db.table("saved_picks")
        .upsert(payload, on_conflict="user_id,pick_id")
        .execute()
    )
    if not resp.data:
        raise HTTPException(status_code=500, detail="Failed to save pick.")
    return _row_to_saved_pick(resp.data[0])


@router.get("/picks/saved", response_model=List[SavedPick])
def get_saved_picks(
    user: AuthUser = Depends(get_current_user),
) -> List[SavedPick]:
    """The user's currently tracked picks, newest first."""
    db = _require_db()
    resp = (
        db.table("saved_picks")
        .select("*, pick_results(*)")
        .eq("user_id", user.id)
        .order("saved_at", desc=True)
        .execute()
    )
    return [_row_to_saved_pick(r) for r in (resp.data or [])]


@router.get("/picks/history", response_model=PickHistory)
def get_pick_history(
    user: AuthUser = Depends(get_current_user),
) -> PickHistory:
    """Performance history: settled picks plus aggregate win/loss stats."""
    db = _require_db()
    resp = (
        db.table("saved_picks")
        .select("*, pick_results(*)")
        .eq("user_id", user.id)
        .order("saved_at", desc=True)
        .execute()
    )
    picks = [_row_to_saved_pick(r) for r in (resp.data or [])]

    wins = sum(1 for p in picks if p.result == "win")
    losses = sum(1 for p in picks if p.result == "loss")
    pushes = sum(1 for p in picks if p.result == "push")
    pending = sum(1 for p in picks if p.result == "pending")
    decided = wins + losses
    stats = HistoryStats(
        total=len(picks),
        wins=wins,
        losses=losses,
        pushes=pushes,
        pending=pending,
        win_rate=round(wins / decided, 4) if decided else 0.0,
    )
    return PickHistory(stats=stats, picks=picks)


@router.post("/alerts", response_model=Alert)
def create_alert(
    body: AlertRequest,
    user: AuthUser = Depends(get_current_user),
) -> Alert:
    """Create a line-movement alert for the user."""
    db = _require_db()
    payload = {
        "user_id": user.id,
        "pick_id": body.pick_id,
        "player": body.player,
        "stat_type": body.stat_type,
        "sport": body.sport,
        "direction": body.direction,
        "threshold": body.threshold,
        "note": body.note,
        "active": True,
    }
    resp = db.table("alerts").insert(payload).execute()
    if not resp.data:
        raise HTTPException(status_code=500, detail="Failed to create alert.")
    row = resp.data[0]
    return Alert(
        id=str(row["id"]),
        pick_id=row["pick_id"],
        player=row["player"],
        stat_type=row["stat_type"],
        sport=row["sport"],
        direction=row["direction"],
        threshold=row["threshold"],
        note=row.get("note"),
        active=row.get("active", True),
        created_at=row["created_at"],
    )
