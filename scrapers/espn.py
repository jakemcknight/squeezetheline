"""
Generic ESPN basketball scrapers (injuries, rosters/positions, season stats).

ESPN exposes free, no-auth JSON endpoints for every basketball league under a
common shape, differing only by a league slug:
    nba | wnba | mens-college-basketball | womens-college-basketball

This module is league-parameterized so it can back any of them. It's the
primary data source for sports that aren't covered by nba_api (e.g. WNBA).

Cost note: ESPN has no single league-wide game-log endpoint, so building
season stats means walking teams -> rosters -> per-athlete game logs. That's
~one request per team plus one per player (≈200 calls for a 15-team WNBA
slate). It's only run on an explicit admin "refresh" and the result is cached,
but it is meaningfully slower than the NBA one-shot LeagueGameLog call.
"""

import datetime
from typing import Optional

import pandas as pd
import requests
from unidecode import unidecode

# Site API — teams, rosters, injuries
SITE_API = "https://site.api.espn.com/apis/site/v2/sports/basketball"
# Web API — per-athlete game logs
WEB_API = "https://site.web.api.espn.com/apis/common/v3/sports/basketball"

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; squeezetheline/1.0)"}
_TIMEOUT = 15

# ESPN position abbreviations -> the 5-position scheme the analysis/defense code uses.
# ESPN returns either generic (G/F/C) or specific (PG/SG/SF/PF) abbreviations.
POSITION_MAP = {
    "PG": "PG", "SG": "SG", "SF": "SF", "PF": "PF", "C": "C",
    "G": "PG", "F": "SF", "G-F": "SG", "F-G": "SF", "F-C": "PF", "C-F": "C",
}

# Match injuries.py so statuses render consistently across sources.
STATUS_SHORT = {
    "Day-To-Day": "DTD",
    "Out": "OUT",
    "Doubtful": "DBT",
    "Questionable": "Q",
    "Probable": "PROB",
    "Active": "ACT",
    "Suspended": "SUS",
}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


# --- Injuries ---------------------------------------------------------------

def get_injuries(league: str) -> pd.DataFrame:
    """Fetch the injury report for an ESPN basketball league.

    Returns a DataFrame: name, team, status, status_short, comment, date.
    Same schema as scrapers.injuries.get_injury_report so callers are
    interchangeable.
    """
    url = f"{SITE_API}/{league}/injuries"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch ESPN injuries for {league}: {e}")
        return pd.DataFrame(columns=["name", "team", "status", "status_short", "comment", "date"])

    rows = []
    for team_block in data.get("injuries", []):
        team_name = team_block.get("displayName", "")
        for inj in team_block.get("injuries", []):
            athlete = inj.get("athlete") or {}
            name = athlete.get("displayName", "").strip()
            if not name:
                continue
            status = inj.get("status", "")
            rows.append({
                "name": unidecode(name),
                "team": team_name,
                "status": status,
                "status_short": STATUS_SHORT.get(status, status),
                "comment": inj.get("shortComment", ""),
                "date": inj.get("date", ""),
            })
    cols = ["name", "team", "status", "status_short", "comment", "date"]
    return pd.DataFrame(rows, columns=cols)


# --- Teams & rosters --------------------------------------------------------

def get_teams(league: str) -> list[dict]:
    """Return [{id, abbreviation, displayName}] for every team in the league."""
    url = f"{SITE_API}/{league}/teams"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch ESPN teams for {league}: {e}")
        return []
    teams = []
    for entry in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
        t = entry.get("team", {})
        if t.get("id"):
            teams.append({
                "id": t["id"],
                "abbreviation": t.get("abbreviation", ""),
                "displayName": t.get("displayName", ""),
            })
    return teams


def _athlete_url(league: str, athlete_id: str, slug: str) -> str:
    web_league = {"mens-college-basketball": "mens-college-basketball"}.get(league, league)
    if slug:
        return f"https://www.espn.com/{web_league}/player/_/id/{athlete_id}/{slug}"
    return f"https://www.espn.com/{web_league}/player/_/id/{athlete_id}"


def get_player_positions(league: str, session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Walk every team roster and return player positions.

    Returns a DataFrame: name, position (PG/SG/SF/PF/C), player_id, player_url.
    Mirrors scrapers.nba.get_player_positions output so data.prepare_stats and
    the player-detail headshot logic work unchanged.
    """
    sess = session or _session()
    rows = []
    for team in get_teams(league):
        url = f"{SITE_API}/{league}/teams/{team['id']}/roster"
        try:
            resp = sess.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            athletes = resp.json().get("athletes", [])
        except Exception as e:
            print(f"  espn roster failed for {team['abbreviation']}: {type(e).__name__}")
            continue
        for a in athletes:
            name = (a.get("displayName") or "").strip()
            if not name:
                continue
            pos = (a.get("position") or {})
            abbr = pos.get("abbreviation") or ""
            rows.append({
                "name": unidecode(name),
                "position": POSITION_MAP.get(abbr, "SF"),
                "player_id": a.get("id"),
                "player_url": _athlete_url(league, a.get("id", ""), a.get("slug", "")),
            })
    return pd.DataFrame(rows, columns=["name", "position", "player_id", "player_url"])


# --- Season stats -----------------------------------------------------------

def _stat_value(stats: list, names: list, key: str, made_only: bool = False) -> float:
    """Read one stat from an event's parallel stats/names arrays.

    ESPN reports made-attempted fields like "3PM-3PA" as "2-5"; made_only
    pulls just the leading 'made' number.
    """
    try:
        idx = names.index(key)
        raw = stats[idx]
    except (ValueError, IndexError):
        return 0.0
    if raw in (None, "", "--"):
        return 0.0
    if made_only and isinstance(raw, str) and "-" in raw:
        raw = raw.split("-")[0]
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def get_player_gamelog(league: str, athlete_id: str, season: int,
                       session: Optional[requests.Session] = None) -> list[dict]:
    """Fetch one athlete's game log for a season, parsed into stat rows.

    Returns rows with: gameday, opponent, minutes, points, rebounds, assists,
    threes, steals, blocks (player name/team are attached by the caller).
    """
    sess = session or _session()
    url = f"{WEB_API}/{league}/athletes/{athlete_id}/gamelog"
    try:
        resp = sess.get(url, params={"season": season}, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    names = data.get("names", [])
    events_meta = data.get("events", {}) or {}
    out = []
    for season_type in data.get("seasonTypes", []) or []:
        for category in season_type.get("categories", []) or []:
            for ev in category.get("events", []) or []:
                eid = ev.get("eventId")
                stats = ev.get("stats", [])
                if not eid or not stats:
                    continue
                meta = events_meta.get(eid, {})
                opp = (meta.get("opponent") or {}).get("abbreviation", "")
                gd = meta.get("gameDate", "")
                out.append({
                    "gameday": gd,
                    "opponent": opp,
                    "minutes": _stat_value(stats, names, "minutes"),
                    "points": _stat_value(stats, names, "points"),
                    "rebounds": _stat_value(stats, names, "totalRebounds"),
                    "assists": _stat_value(stats, names, "assists"),
                    "threes": _stat_value(
                        stats, names,
                        "threePointFieldGoalsMade-threePointFieldGoalsAttempted",
                        made_only=True,
                    ),
                    "steals": _stat_value(stats, names, "steals"),
                    "blocks": _stat_value(stats, names, "blocks"),
                })
    return out


def get_season_stats(league: str, season: Optional[int] = None) -> pd.DataFrame:
    """Build a full per-game stat table for the league/season via ESPN.

    Returns the same columns scrapers.nba.get_current_season_stats produces:
    name, team-code, opponent, gameday, minutes, points, rebounds, assists,
    threes, steals, blocks, pra.

    See module docstring re: request cost (one call per player).
    """
    if season is None:
        season = datetime.date.today().year
    sess = _session()

    rows = []
    for team in get_teams(league):
        roster_url = f"{SITE_API}/{league}/teams/{team['id']}/roster"
        try:
            athletes = sess.get(roster_url, timeout=_TIMEOUT).json().get("athletes", [])
        except Exception:
            continue
        team_code = team["abbreviation"]
        for a in athletes:
            aid = a.get("id")
            name = (a.get("displayName") or "").strip()
            if not aid or not name:
                continue
            for g in get_player_gamelog(league, aid, season, session=sess):
                rows.append({"name": unidecode(name), "team-code": team_code, **g})

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["gameday"] = pd.to_datetime(df["gameday"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df[df["gameday"].notna()]
    for col in ("minutes", "points", "rebounds", "assists", "threes", "steals", "blocks"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    return df
