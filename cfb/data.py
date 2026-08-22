"""CFBD (CollegeFootballData.com) access + local caching for the CFB board.

Everything here needs a free CFBD API key (Bearer token), resolved from:
  1. the CFBD_API_KEY env var,
  2. backend/.env (CFBD_API_KEY=... line),
  3. repo-root .env.
Grab one in ~1 minute at https://collegefootballdata.com/key — no card.

Endpoints used (all cached under cfb/cache/, refreshed daily):
  * /calendar?year=Y                  — week windows, to pick "this week"
  * /games?year=Y&week=W&seasonType=… — the week's slate (filtered to FBS)
  * /ratings/sp?year=Y                — SP+ ratings (preseason early on);
                                        falls back to Y-1 final SP+ if the
                                        new season's aren't published yet
  * /player/returning?year=Y          — returning production (confidence
                                        input only; preseason SP+ already
                                        bakes it into the rating itself)
  * /games?year=Y&seasonType=regular  — completed games, for in-season
                                        rating updates

Without a key, __main__ falls back to building the slate from The Odds API
events and seeding ratings from cfb/stub_ratings.json (clearly-labeled demo
numbers, never real SP+ — confidence is forced Low so no BET can flag).
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

BASE = "https://api.collegefootballdata.com"
CACHE_DIR = Path(__file__).resolve().parent / "cache"
CACHE_TTL_HOURS = 12

_REPO_ROOT = Path(__file__).resolve().parent.parent

session = requests.Session()
session.headers["User-Agent"] = "squeezetheline-cfb/1.0"


def resolve_cfbd_key() -> str:
    key = os.environ.get("CFBD_API_KEY", "")
    if key:
        return key
    for env_file in (_REPO_ROOT / "backend" / ".env", _REPO_ROOT / ".env"):
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                m = re.match(r"\s*CFBD_API_KEY\s*=\s*(\S+)", line)
                if m:
                    return m.group(1).strip().strip('"').strip("'")
    return ""


def _get(path: str, params: dict, api_key: str, retries: int = 3) -> list:
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = session.get(
                f"{BASE}{path}",
                params=params,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as err:  # noqa: BLE001 — retry any transport hiccup
            last_err = err
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"CFBD request failed after {retries} tries: {path} ({last_err})")


def _cached(name: str, fetch, ttl_hours: float = CACHE_TTL_HOURS) -> list:
    path = CACHE_DIR / name
    if path.exists():
        try:
            payload = json.loads(path.read_text())
            if time.time() - payload.get("ts", 0) < ttl_hours * 3600:
                return payload["data"]
        except (json.JSONDecodeError, OSError):
            pass
    data = fetch()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"ts": time.time(), "data": data}))
    return data


def _g(row: dict, *keys, default=None):
    """CFBD has shipped both camelCase and snake_case over API versions —
    take the first key that exists so either parses."""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return default


# ---------------------------------------------------------------------------
# Calendar / week selection
# ---------------------------------------------------------------------------

def fetch_calendar(year: int, api_key: str) -> List[dict]:
    return _cached(f"calendar_{year}.json",
                   lambda: _get("/calendar", {"year": year}, api_key))


def pick_current_week(calendar: List[dict], today: date) -> Optional[dict]:
    """The week to board: the first regular-season week whose window hasn't
    fully ended yet (so on a Tuesday you get the upcoming Saturday)."""
    best = None
    for wk in calendar:
        if _g(wk, "seasonType", "season_type", default="regular") != "regular":
            continue
        end_raw = _g(wk, "lastGameStart", "last_game_start", "endDate", "end_date")
        if not end_raw:
            continue
        end = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00")).date()
        if end >= today and (best is None or end < best[0]):
            best = (end, wk)
    return best[1] if best else None


# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------

def _parse_game(g: dict) -> dict:
    return {
        "id": _g(g, "id"),
        "week": _g(g, "week"),
        "start": _g(g, "startDate", "start_date", default=""),
        "neutral": bool(_g(g, "neutralSite", "neutral_site", default=False)),
        "completed": bool(_g(g, "completed", default=False)),
        "home": _g(g, "homeTeam", "home_team", default=""),
        "away": _g(g, "awayTeam", "away_team", default=""),
        "home_class": _g(g, "homeClassification", "home_classification",
                         "homeDivision", "home_division", default=""),
        "away_class": _g(g, "awayClassification", "away_classification",
                         "awayDivision", "away_division", default=""),
        "home_points": _g(g, "homePoints", "home_points"),
        "away_points": _g(g, "awayPoints", "away_points"),
    }


def fetch_week_games(year: int, week: int, api_key: str) -> List[dict]:
    """This week's FBS slate (any game with at least one FBS side)."""
    raw = _cached(
        f"games_{year}_wk{week}.json",
        lambda: _get("/games", {"year": year, "week": week,
                                "seasonType": "regular"}, api_key),
        ttl_hours=6,
    )
    games = [_parse_game(g) for g in raw]
    return [g for g in games if "fbs" in (g["home_class"], g["away_class"])]


def fetch_completed_games(year: int, api_key: str) -> List[dict]:
    """All completed regular-season games this year, for rating updates."""
    raw = _cached(
        f"games_{year}_all.json",
        lambda: _get("/games", {"year": year, "seasonType": "regular"}, api_key),
        ttl_hours=6,
    )
    out = []
    for g in (_parse_game(x) for x in raw):
        if g["completed"] and g["home_points"] is not None and g["away_points"] is not None:
            out.append(g)
    return out


# ---------------------------------------------------------------------------
# Ratings + returning production
# ---------------------------------------------------------------------------

def fetch_sp_ratings(year: int, api_key: str) -> tuple[Dict[str, dict], str]:
    """SP+ per team: {team: {overall, off, def, st}}. Early in a season CFBD
    serves the preseason projections under the new year; if nothing is
    published yet we fall back to the prior year's final SP+ (noted)."""
    for yr, label in ((year, f"SP+ {year} (preseason-seeded)"),
                      (year - 1, f"SP+ {year - 1} final (fallback seed)")):
        raw = _cached(f"sp_{yr}.json",
                      lambda yr=yr: _get("/ratings/sp", {"year": yr}, api_key))
        ratings: Dict[str, dict] = {}
        for row in raw:
            team = _g(row, "team", default="")
            if not team or team == "nationalAverages":
                continue
            off = _g(row, "offense", default={}) or {}
            dfn = _g(row, "defense", default={}) or {}
            st = _g(row, "specialTeams", "special_teams", default={}) or {}
            ratings[team] = {
                "overall": float(_g(row, "rating", default=0.0) or 0.0),
                "off": float(_g(off, "rating", default=0.0) or 0.0),
                "def": float(_g(dfn, "rating", default=0.0) or 0.0),
                "st": float(_g(st, "rating", default=0.0) or 0.0),
            }
        if ratings:
            return ratings, label
    raise RuntimeError("CFBD returned no SP+ ratings for either year")


def fetch_returning_production(year: int, api_key: str) -> Dict[str, float]:
    """{team: fraction of production returning} — confidence input only."""
    try:
        raw = _cached(f"returning_{year}.json",
                      lambda: _get("/player/returning", {"year": year}, api_key))
    except RuntimeError:
        return {}
    out: Dict[str, float] = {}
    for row in raw:
        team = _g(row, "team", default="")
        pct = _g(row, "percentPPA", "percent_ppa",
                 "totalPercentPPA", "usage", default=None)
        if team and pct is not None:
            pct = float(pct)
            out[team] = pct / 100.0 if pct > 1.5 else pct
    return out


# ---------------------------------------------------------------------------
# Stub fallback (no CFBD key)
# ---------------------------------------------------------------------------

def load_stub_ratings() -> tuple[Dict[str, dict], str]:
    """Illustrative demo ratings bundled with the repo — NOT real SP+.
    Used only when no CFBD key is configured, purely to exercise the code
    path; every game gets confidence Low so nothing can flag as a BET."""
    path = Path(__file__).resolve().parent / "stub_ratings.json"
    data = json.loads(path.read_text())
    return data["teams"], "STUB demo ratings (NOT real SP+ — get a CFBD key)"


def slate_from_odds_events(events: List[dict], today: date,
                           horizon_days: int = 9) -> List[dict]:
    """No-CFBD fallback slate: upcoming Odds API events within the horizon,
    shaped like fetch_week_games output (neutral-site info unavailable)."""
    out = []
    now = datetime.now(timezone.utc)
    for ev in events:
        try:
            start = datetime.fromisoformat(ev["commence_time"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        if 0 <= (start - now).days <= horizon_days:
            out.append({
                "id": None, "week": None, "start": ev["commence_time"],
                "neutral": False, "completed": False,
                "home": ev["home"], "away": ev["away"],
                "home_class": "fbs", "away_class": "fbs",
                "home_points": None, "away_points": None,
            })
    return out
