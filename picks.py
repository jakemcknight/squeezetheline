"""
Pick tracking for Squeeze the Line.

Stores picks on disk (data/picks.json), supports adding, removing, and
auto-grading once the game's stats are available.

A 'pick' is a (date, player, stat, line, side) tuple plus metadata. After
the player's game finishes, we look up their actual stat for that date and
mark the pick as Won/Lost/Push.
"""

import json
import os
import datetime
from typing import Optional

import pandas as pd

from data import DATA_DIR

PICKS_PATH = os.path.join(DATA_DIR, "picks.json")


def load_picks() -> list[dict]:
    if not os.path.exists(PICKS_PATH):
        return []
    with open(PICKS_PATH) as f:
        return json.load(f)


def save_picks(picks: list[dict]):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PICKS_PATH, "w") as f:
        json.dump(picks, f, indent=2, default=str)


def add_pick(
    date: datetime.date,
    player: str,
    stat: str,
    line: float,
    side: str,
    team: str = "",
    opponent: str = "",
    odds: Optional[int] = None,
    book: str = "",
) -> dict:
    """Add a new pick. Returns the saved pick dict."""
    picks = load_picks()
    pick = {
        "id": f"{date}_{player}_{stat}_{side}".lower().replace(" ", "_"),
        "date": str(date),
        "player": player,
        "stat": stat,
        "line": float(line),
        "side": side,  # "over" or "under"
        "team": team,
        "opponent": opponent,
        "odds": odds,
        "book": book,
        "actual": None,
        "result": "pending",  # pending | won | lost | push
        "created_at": datetime.datetime.now().isoformat(),
    }
    # Replace any existing pick with the same id (de-dupe)
    picks = [p for p in picks if p.get("id") != pick["id"]]
    picks.append(pick)
    save_picks(picks)
    return pick


def remove_pick(pick_id: str):
    picks = load_picks()
    picks = [p for p in picks if p.get("id") != pick_id]
    save_picks(picks)


def grade_picks(historical_data: pd.DataFrame) -> int:
    """Grade all pending picks against the historical_data dataframe.

    Looks up each pick's player + date in the historical data and records
    the actual stat value, then sets result to won/lost/push.

    Returns the number of picks that were graded.
    """
    if historical_data.empty:
        return 0

    # Normalize the historical data column names
    df = historical_data.rename(columns={
        "player": "name", "team_code": "team", "opponent_code": "opponent",
        "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
        "threefm": "threes", "stl": "steals", "blk": "blocks",
    })
    for c in ("points", "rebounds", "assists", "threes", "steals", "blocks"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    if all(c in df.columns for c in ("points", "rebounds", "assists")):
        df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    df["date_string"] = df.get("date_string", df.get("game_gameday"))

    picks = load_picks()
    graded = 0
    for pick in picks:
        if pick.get("result") and pick["result"] != "pending":
            continue
        match = df[(df["name"] == pick["player"]) & (df["date_string"] == pick["date"])]
        if match.empty:
            continue
        stat = pick["stat"]
        if stat not in match.columns:
            continue
        actual = float(match.iloc[0][stat])
        pick["actual"] = actual
        if pick["side"] == "over":
            pick["result"] = "won" if actual > pick["line"] else (
                "push" if actual == pick["line"] else "lost"
            )
        else:
            pick["result"] = "won" if actual < pick["line"] else (
                "push" if actual == pick["line"] else "lost"
            )
        graded += 1
    save_picks(picks)
    return graded


def picks_summary() -> dict:
    """Compute aggregate stats for graded picks."""
    picks = load_picks()
    graded = [p for p in picks if p.get("result") in ("won", "lost", "push")]
    pending = [p for p in picks if p.get("result") == "pending"]
    won = [p for p in graded if p["result"] == "won"]
    lost = [p for p in graded if p["result"] == "lost"]
    push = [p for p in graded if p["result"] == "push"]
    return {
        "total": len(picks),
        "pending": len(pending),
        "graded": len(graded),
        "won": len(won),
        "lost": len(lost),
        "push": len(push),
        "win_rate": (len(won) / max(len(won) + len(lost), 1)) * 100,
    }
