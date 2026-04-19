"""
Auto-generated daily picks stored in Supabase.

Logic:
- Every Strong Over and Strong Under (per analysis.py filters) becomes an
  auto pick for the day.
- The top 5 of each (by composite score) are flagged as `is_top_pick=True`
  so we can track them as a tighter 'best bets' subset separately.

Flow:
1. auto_refresh.py runs daily at 10am ET (GitHub Actions), calls
   `generate_and_save_picks()` to fetch data + write picks to Supabase.
2. auto_grade.py runs daily at 2am ET (GitHub Actions), calls
   `grade_pending_picks()` to look up box scores and mark each pick
   as won / lost / push.

Required environment variables when running the scripts:
- ODDS_API_KEY
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY  (NOT the anon key — needs write access)

The Streamlit app reads with the anon key + RLS policies.
"""

import os
import datetime
from typing import Optional

import pandas as pd
from supabase import create_client, Client

from analysis import (
    analyze_stat,
    filter_strong_overs,
    filter_strong_unders,
)


STAT_CONFIGS = [
    ("points", "Total Points"),
    ("rebounds", "Total Rebounds"),
    ("assists", "Total Assists"),
    ("pra", "Total PRA"),
    ("threes", "Total 3PM"),
    ("steals", "Total Steals"),
    ("blocks", "Total Blocks"),
]

TOP_N = 5


def get_admin_client() -> Optional[Client]:
    """Return a Supabase client with service_role privileges (write access)."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set for auto-picks writes."
        )
    return create_client(url, key)


def _composite_score(row, side: str) -> float:
    """Same scoring as the Top Picks panel in the app."""
    d = row.get("delta", 0) or 0
    d5 = row.get("delta_5g", 0) or 0
    d10 = row.get("delta_10g", 0) or 0
    avg_delta = (abs(d) + abs(d5) + abs(d10)) / 3
    hit = row.get("hit%", 0) or 0
    hist = row.get("history_hit%", 0) or 0
    edge = (hit - 50) + (hist - 50) if side == "over" else (50 - hit) + (50 - hist)
    return avg_delta * (edge / 10 if edge > 0 else 0)


def _build_pick(row, side: str, stat_key: str, game_date: datetime.date, is_top: bool) -> dict:
    # Best-effort ML prediction (skips silently if model not trained)
    ml_pred = None
    try:
        from model import predict_player_stat, load_model
        if load_model(stat_key) is not None:
            recent = {
                "avg_5": float(row.get(f"{stat_key}_5g") or 0),
                "avg_10": float(row.get(f"{stat_key}_10g") or 0),
                "avg_25": float(row.get(stat_key) or 0),
                "min_avg_10": 28.0,
            }
            pred = predict_player_stat(
                player=row["name"], stat=stat_key,
                opponent=row.get("opponent", "") or "",
                team=row.get("team-code", "") or "",
                home=True, rest_days=int(row.get("rest_days") or 2),
                recent_averages=recent,
            )
            if pred is not None:
                ml_pred = float(pred)
    except Exception:
        pass

    def _safe_int(v):
        try:
            return int(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None
        except Exception:
            return None

    def _safe_float(v):
        try:
            return float(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None
        except Exception:
            return None

    return {
        "date": str(game_date),
        "player": row["name"],
        "stat": stat_key,
        "line": float(row["spread"]),
        "side": side,
        "team": row.get("team-code", ""),
        "opponent": row.get("opponent", ""),
        "delta": float(row.get("delta", 0) or 0),
        "delta_10g": _safe_float(row.get("delta_10g")),
        "hit_pct": float(row.get("hit%", 0) or 0),
        "history_hit_pct": float(row.get("history_hit%", 0) or 0),
        "def_rank": _safe_int(row.get("rank")),
        "vs_opp_career": row.get("vs_opp_career", "") or "",
        "ml_prediction": ml_pred,
        "score": float(_composite_score(row, side)),
        "is_top_pick": bool(is_top),
        "actual": None,
        "result": "pending",
    }


def generate_picks_for_date(game_date: datetime.date) -> list[dict]:
    """Run the full analysis pipeline and return the list of auto-picks."""
    # Lazy imports so we don't pull in all dependencies when only reading
    from scrapers.odds_api import get_todays_games, get_all_props
    from scrapers.nba import get_current_season_stats, get_player_positions
    from scrapers.basketball_ref import get_defense_by_position
    from scrapers.injuries import get_injury_report
    from data import prepare_stats, prepare_props

    print(f"Generating auto-picks for {game_date}...")
    todays_games = get_todays_games(game_date)
    stats = get_current_season_stats()
    positions = get_player_positions()
    df = prepare_stats(stats, positions)
    props = get_all_props(game_date)
    props = prepare_props(props)
    defense = get_defense_by_position()

    # Injury status — we'll use it to exclude OUT/DBT from auto picks
    injuries = get_injury_report()
    injury_join = (
        injuries[["name", "status_short"]].drop_duplicates(subset="name")
        if not injuries.empty
        else pd.DataFrame(columns=["name", "status_short"])
    )

    all_overs = []
    all_unders = []
    for stat_key, prop_type in STAT_CONFIGS:
        result = analyze_stat(stat_key, prop_type, df, props, todays_games, defense, game_date=game_date)
        if result.empty:
            continue
        if not injury_join.empty:
            result = result.merge(injury_join, on="name", how="left")
            result = result[~result["status_short"].fillna("").isin({"OUT", "DBT"})]

        overs = filter_strong_overs(result).copy()
        unders = filter_strong_unders(result).copy()
        overs["stat_key"] = stat_key
        unders["stat_key"] = stat_key
        overs["_score"] = overs.apply(lambda r: _composite_score(r, "over"), axis=1)
        unders["_score"] = unders.apply(lambda r: _composite_score(r, "under"), axis=1)
        all_overs.append(overs)
        all_unders.append(unders)

    overs_df = pd.concat(all_overs, ignore_index=True) if all_overs else pd.DataFrame()
    unders_df = pd.concat(all_unders, ignore_index=True) if all_unders else pd.DataFrame()

    # Identify top 5 across all stats for each side
    top_over_keys = set()
    if not overs_df.empty:
        top_over_keys = set(
            overs_df.sort_values("_score", ascending=False)
            .head(TOP_N)
            .apply(lambda r: (r["name"], r["stat_key"]), axis=1)
        )
    top_under_keys = set()
    if not unders_df.empty:
        top_under_keys = set(
            unders_df.sort_values("_score", ascending=False)
            .head(TOP_N)
            .apply(lambda r: (r["name"], r["stat_key"]), axis=1)
        )

    picks = []
    for _, row in overs_df.iterrows():
        is_top = (row["name"], row["stat_key"]) in top_over_keys
        picks.append(_build_pick(row, "over", row["stat_key"], game_date, is_top))
    for _, row in unders_df.iterrows():
        is_top = (row["name"], row["stat_key"]) in top_under_keys
        picks.append(_build_pick(row, "under", row["stat_key"], game_date, is_top))

    print(f"  Generated {len(picks)} picks ({sum(p['is_top_pick'] for p in picks)} flagged as top)")
    return picks


def save_picks_to_supabase(picks: list[dict]):
    """Upsert picks to the auto_picks table. Uses (date, player, stat, side)
    as the unique key so re-running on the same day is idempotent."""
    if not picks:
        return 0
    sb = get_admin_client()
    # Upsert lets us re-run without duplicating
    res = sb.table("auto_picks").upsert(
        picks, on_conflict="date,player,stat,side"
    ).execute()
    return len(res.data or [])


def generate_and_save_picks(game_date: Optional[datetime.date] = None) -> int:
    """Top-level entry point for auto_refresh.py."""
    game_date = game_date or datetime.date.today()
    picks = generate_picks_for_date(game_date)
    n = save_picks_to_supabase(picks)
    print(f"Saved {n} picks to Supabase.")
    return n


def grade_pending_picks(up_to_date: Optional[datetime.date] = None) -> int:
    """Mark each pending pick as won/lost/push using the backfilled box scores."""
    from data import load_historical_data

    sb = get_admin_client()
    hist = load_historical_data()
    if hist.empty:
        print("No historical data available — can't grade.")
        return 0

    hist = hist.rename(columns={
        "player": "name", "team_code": "team", "opponent_code": "opponent",
        "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
        "threefm": "threes", "stl": "steals", "blk": "blocks",
    })
    for c in ("points", "rebounds", "assists", "threes", "steals", "blocks"):
        if c in hist.columns:
            hist[c] = pd.to_numeric(hist[c], errors="coerce").fillna(0)
    if all(c in hist.columns for c in ("points", "rebounds", "assists")):
        hist["pra"] = hist["points"] + hist["rebounds"] + hist["assists"]

    cutoff = str(up_to_date or datetime.date.today())
    pending = sb.table("auto_picks").select("*").eq("result", "pending").lte("date", cutoff).execute()
    pending_picks = pending.data or []
    print(f"Found {len(pending_picks)} pending picks to grade (up to {cutoff})...")

    updates = []
    for pick in pending_picks:
        match = hist[(hist["name"] == pick["player"]) & (hist["date_string"] == pick["date"])]
        if match.empty:
            continue
        stat_col = pick["stat"]
        if stat_col not in match.columns:
            continue
        actual = float(match.iloc[0][stat_col])
        line = pick["line"]
        if pick["side"] == "over":
            result = "won" if actual > line else ("push" if actual == line else "lost")
        else:
            result = "won" if actual < line else ("push" if actual == line else "lost")
        updates.append({
            "id": pick["id"],
            "actual": actual,
            "result": result,
        })

    graded = 0
    for u in updates:
        sb.table("auto_picks").update(
            {"actual": u["actual"], "result": u["result"]}
        ).eq("id", u["id"]).execute()
        graded += 1

    print(f"Graded {graded} picks.")
    return graded


def fetch_auto_picks(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    top_only: bool = False,
) -> list[dict]:
    """Read auto picks from Supabase using the anon key (for the app)."""
    from auth import get_supabase  # anon client used by the Streamlit app
    sb = get_supabase()
    if not sb:
        return []
    q = sb.table("auto_picks").select("*").order("date", desc=True).order("score", desc=True)
    if date_from:
        q = q.gte("date", date_from)
    if date_to:
        q = q.lte("date", date_to)
    if top_only:
        q = q.eq("is_top_pick", True)
    resp = q.execute()
    return resp.data or []


def summarize_picks(picks: list[dict]) -> dict:
    graded = [p for p in picks if p.get("result") in ("won", "lost", "push")]
    won = [p for p in graded if p["result"] == "won"]
    lost = [p for p in graded if p["result"] == "lost"]
    push = [p for p in graded if p["result"] == "push"]
    return {
        "total": len(picks),
        "pending": len([p for p in picks if p.get("result") == "pending"]),
        "won": len(won),
        "lost": len(lost),
        "push": len(push),
        "win_rate": (len(won) / max(len(won) + len(lost), 1)) * 100,
    }
