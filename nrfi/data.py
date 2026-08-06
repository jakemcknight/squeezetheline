"""MLB Stats API access + local caching for the NRFI board.

All data comes from statsapi.mlb.com (free, no key). Two heavy datasets are
cached under nrfi/cache/ and refreshed at most once per day:

  * season_linescores_<year>.json — every regular-season game with its
    per-inning linescore. This gives us, per team, whether they scored /
    allowed a run in the 1st inning of each game, plus the league base rate.
  * pitcher_logs_<year>.json — game logs for the probable pitchers we've
    looked up (accumulated across runs). Joined against the linescores, a
    starter's TRUE first-inning runs-allowed rate falls out directly: the
    starter always pitches the 1st, so opponent 1st-inning runs in his
    starts are runs he allowed.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

BASE = "https://statsapi.mlb.com/api/v1"
CACHE_DIR = Path(__file__).resolve().parent / "cache"
SEASON_START = "03-01"  # wide window; gameType=R filters to regular season

session = requests.Session()
session.headers["User-Agent"] = "squeezetheline-nrfi/1.0"


def _get(url: str, params: Optional[dict] = None, retries: int = 3) -> dict:
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as err:  # noqa: BLE001 — retry any transport/JSON hiccup
            last_err = err
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"MLB Stats API request failed after {retries} tries: {url} ({last_err})")


def _cache_load(name: str) -> Optional[dict]:
    path = CACHE_DIR / name
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
    return None


def _cache_save(name: str, payload: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / name).write_text(json.dumps(payload))


# ---------------------------------------------------------------------------
# Today's slate
# ---------------------------------------------------------------------------

def fetch_slate(day: date) -> List[dict]:
    """Return today's games with probable pitchers (may be empty)."""
    data = _get(
        f"{BASE}/schedule",
        {"sportId": 1, "date": day.isoformat(), "hydrate": "probablePitcher"},
    )
    games: List[dict] = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            games.append(
                {
                    "gamePk": g["gamePk"],
                    "status": g.get("status", {}).get("detailedState", "?"),
                    "venue": g.get("venue", {}).get("name", ""),
                    "away_id": g["teams"]["away"]["team"]["id"],
                    "away_name": g["teams"]["away"]["team"]["name"],
                    "home_id": g["teams"]["home"]["team"]["id"],
                    "home_name": g["teams"]["home"]["team"]["name"],
                    "away_pitcher": g["teams"]["away"].get("probablePitcher"),
                    "home_pitcher": g["teams"]["home"].get("probablePitcher"),
                }
            )
    return games


# ---------------------------------------------------------------------------
# Season linescores (cached, refreshed daily)
# ---------------------------------------------------------------------------

def fetch_season_first_innings(season: int, today: date) -> Dict[int, dict]:
    """Map gamePk -> compact first-inning record for every FINAL regular-season
    game before today. Cached; refetched when the cache is from a prior day."""
    cache_name = f"season_linescores_{season}.json"
    cached = _cache_load(cache_name)
    if cached and cached.get("fetched") == today.isoformat():
        return {int(k): v for k, v in cached["games"].items()}

    data = _get(
        f"{BASE}/schedule",
        {
            "sportId": 1,
            "startDate": f"{season}-{SEASON_START}",
            "endDate": today.isoformat(),
            "gameType": "R",
            "hydrate": "linescore",
        },
    )
    games: Dict[int, dict] = {}
    for d in data.get("dates", []):
        for g in d.get("games", []):
            if g.get("status", {}).get("abstractGameState") != "Final":
                continue
            if g.get("officialDate") == today.isoformat():
                continue  # keep the sample strictly pre-today
            innings = g.get("linescore", {}).get("innings", [])
            if not innings or innings[0].get("num") != 1:
                continue
            first = innings[0]
            away_r = first.get("away", {}).get("runs")
            home_r = first.get("home", {}).get("runs")
            if away_r is None or home_r is None:
                continue  # incomplete linescore (rare) — skip
            games[g["gamePk"]] = {
                "away_id": g["teams"]["away"]["team"]["id"],
                "home_id": g["teams"]["home"]["team"]["id"],
                "away_first": away_r,
                "home_first": home_r,
            }
    _cache_save(cache_name, {"fetched": today.isoformat(), "games": games})
    return games


# ---------------------------------------------------------------------------
# Pitcher game logs (cached, accumulated; refreshed daily)
# ---------------------------------------------------------------------------

def fetch_pitcher_starts(
    pitcher_ids: List[int], season: int, today: date
) -> Dict[int, List[dict]]:
    """Map pitcher id -> list of {gamePk, isHome} for their starts this season
    (games before today only). One API call per uncached pitcher per day."""
    cache_name = f"pitcher_logs_{season}.json"
    cached = _cache_load(cache_name) or {}
    fresh = cached.get("fetched") == today.isoformat()
    logs: Dict[str, List[dict]] = cached.get("logs", {}) if fresh else {}

    for pid in pitcher_ids:
        if str(pid) in logs:
            continue
        data = _get(
            f"{BASE}/people/{pid}/stats",
            {"stats": "gameLog", "group": "pitching", "season": season},
        )
        starts: List[dict] = []
        for block in data.get("stats", []):
            for split in block.get("splits", []):
                if split.get("stat", {}).get("gamesStarted") != 1:
                    continue
                if split.get("date", "") >= today.isoformat():
                    continue
                game_pk = split.get("game", {}).get("gamePk")
                if game_pk is not None:
                    starts.append({"gamePk": game_pk, "isHome": bool(split.get("isHome"))})
        logs[str(pid)] = starts

    _cache_save(cache_name, {"fetched": today.isoformat(), "logs": logs})
    return {int(k): v for k, v in logs.items()}
