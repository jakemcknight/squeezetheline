"""
Parlay builder + tracker.

Lets users build multi-leg parlays from picks on the board, calculate
combined odds, flag correlations, and store the tickets in Supabase
for tracking ROI.

Required Supabase table: parlays  (id, user_email, name, legs[jsonb],
                                   combined_odds, stake, status, created_at)

A "leg" is a dict: {player, stat, side, line, team, opponent, hit_pct,
                    confidence, score, line_odds (default -110)}
"""

import os
import datetime
import math
from typing import Optional


def _admin_client():
    """Service-role Supabase client for writes."""
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


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal (multiplier on stake including return of stake)."""
    if american > 0:
        return 1 + (american / 100.0)
    return 1 + (100.0 / abs(american))


def decimal_to_american(decimal: float) -> int:
    """Convert decimal back to American (rounded)."""
    if decimal <= 1.0:
        return 0
    if decimal >= 2.0:
        return int(round((decimal - 1) * 100))
    return int(round(-100 / (decimal - 1)))


def combined_odds(legs: list[dict]) -> dict:
    """Compute combined parlay odds from individual leg odds (defaulting -110)."""
    if not legs:
        return {"decimal": 1.0, "american": 0, "implied_pct": 0.0}
    decimal = 1.0
    for leg in legs:
        odds = leg.get("line_odds", -110)
        decimal *= american_to_decimal(odds)
    american = decimal_to_american(decimal)
    implied = (1.0 / decimal) * 100.0
    return {
        "decimal": round(decimal, 4),
        "american": american,
        "implied_pct": round(implied, 2),
        "payout_per_dollar": round(decimal - 1, 4),
    }


def estimated_hit_pct(legs: list[dict]) -> float:
    """Naive: P(parlay hits) = product of leg hit%s. Doesn't account for
    correlation — use the warnings to interpret this carefully."""
    if not legs:
        return 0.0
    p = 1.0
    for leg in legs:
        hit = leg.get("hit_pct") or leg.get("confidence") or 50
        p *= float(hit) / 100.0
    return round(p * 100.0, 2)


def detect_correlations(legs: list[dict]) -> list[str]:
    """Flag pairs of legs that are likely correlated.

    Heuristics:
      - Same game, same direction (over/over or under/under): general game-flow correlation
      - Same player, multiple stats: definitely correlated (rarely a real parlay)
      - Same team, same direction: weaker correlation
    """
    warnings = []
    n = len(legs)
    for i in range(n):
        for j in range(i + 1, n):
            a, b = legs[i], legs[j]
            # Same player, multiple legs
            if a["player"] == b["player"]:
                warnings.append(
                    f"{a['player']}: same player on multiple legs (stats are correlated)."
                )
                continue
            # Identify the game by the {team, opponent} unordered pair
            game_a = frozenset({a.get("team", ""), a.get("opponent", "")})
            game_b = frozenset({b.get("team", ""), b.get("opponent", "")})
            same_game = game_a == game_b and "" not in game_a
            same_team = a.get("team") == b.get("team")
            same_dir = a.get("side") == b.get("side")
            if same_game and same_dir:
                warnings.append(
                    f"{a['player']} & {b['player']}: same game, same direction "
                    f"({a['side']}) — game-flow correlation."
                )
            elif same_team and same_dir:
                warnings.append(
                    f"{a['player']} & {b['player']}: same team ({a['team']}), same direction "
                    f"({a['side']}) — team-pace correlation."
                )
    return warnings


def save_parlay(user_email: str, name: str, legs: list[dict],
                stake: float = 10.0) -> Optional[dict]:
    """Save a parlay to Supabase. Returns the saved row dict."""
    sb = _admin_client()
    if sb is None:
        return None
    odds = combined_odds(legs)
    payload = {
        "user_email": user_email,
        "name": name,
        "legs": legs,
        "combined_odds_american": odds["american"],
        "combined_odds_decimal": odds["decimal"],
        "implied_pct": odds["implied_pct"],
        "estimated_hit_pct": estimated_hit_pct(legs),
        "stake": stake,
        "status": "open",
        "created_at": datetime.datetime.utcnow().isoformat() + "Z",
    }
    try:
        resp = sb.table("parlays").insert(payload).execute()
        return (resp.data or [None])[0]
    except Exception as e:
        print(f"[parlays] save failed: {e}")
        return None


def fetch_user_parlays(user_email: str) -> list[dict]:
    sb = _anon_client()
    if sb is None:
        return []
    try:
        resp = (
            sb.table("parlays")
            .select("*")
            .eq("user_email", user_email)
            .order("created_at", desc=True)
            .limit(200)
            .execute()
        )
        return resp.data or []
    except Exception:
        return []


def delete_parlay(parlay_id: str):
    sb = _admin_client()
    if sb is None:
        return
    try:
        sb.table("parlays").delete().eq("id", parlay_id).execute()
    except Exception as e:
        print(f"[parlays] delete failed: {e}")
