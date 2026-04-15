"""
Fetch current-season player stats from NBA.com via nba_api.

Completely free, no API key needed. Uses LeagueGameLog to pull all player
game logs for the current season in a single request.

NBA.com is known to flake on cloud IPs (GitHub Actions, AWS, etc.) so
the calls below use a longer timeout and retry on transient failures.
"""

import time

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog, PlayerIndex

from config import SEASON_YEAR, NBA_API_TEAM_CODE_MAP

NBA_TIMEOUT = 90  # seconds — default is 30, which often times out from cloud IPs
NBA_MAX_RETRIES = 5
NBA_RETRY_BACKOFF = 5  # seconds (linear: 5, 10, 15, ...)


def _with_retries(callable_, *args, **kwargs):
    """Retry an nba_api call on transient timeouts/connection errors."""
    last_err = None
    for attempt in range(1, NBA_MAX_RETRIES + 1):
        try:
            return callable_(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt == NBA_MAX_RETRIES:
                break
            wait = NBA_RETRY_BACKOFF * attempt
            print(f"  nba_api attempt {attempt} failed ({type(e).__name__}); retrying in {wait}s...")
            time.sleep(wait)
    raise last_err

# Map nba_api general positions to the specific positions used by
# the defense-vs-position data (PG, SG, SF, PF, C)
POSITION_MAP = {
    "G": "PG",
    "F": "SF",
    "C": "C",
    "G-F": "SG",
    "F-G": "SF",
    "F-C": "PF",
    "C-F": "C",
}


def _map_team_code(code: str) -> str:
    return NBA_API_TEAM_CODE_MAP.get(code, code)


def _extract_opponent(matchup: str, team_abbr: str) -> str:
    """Extract opponent code from a MATCHUP like 'LAL vs. BOS' or 'LAL @ BOS'."""
    if not isinstance(matchup, str):
        return ""
    cleaned = matchup.replace("vs.", "|").replace("@", "|")
    for part in cleaned.split("|"):
        code = part.strip()
        if code and code != team_abbr:
            return code
    return ""


def get_current_season_stats() -> pd.DataFrame:
    """
    Fetch all player game logs for the current NBA season.

    Returns a DataFrame with columns matching what the analysis pipeline
    expects: name, team-code, gameday, minutes, points, rebounds, assists.
    """
    season = f"{SEASON_YEAR - 1}-{str(SEASON_YEAR)[-2:]}"
    log = _with_retries(
        LeagueGameLog,
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="P",
        timeout=NBA_TIMEOUT,
    )
    data = log.get_dict()["resultSets"][0]
    raw = pd.DataFrame(data["rowSet"], columns=data["headers"])

    df = pd.DataFrame()
    df["name"] = raw["PLAYER_NAME"]
    df["team-code"] = raw["TEAM_ABBREVIATION"].apply(_map_team_code)
    df["opponent"] = raw.apply(
        lambda r: _map_team_code(_extract_opponent(r["MATCHUP"], r["TEAM_ABBREVIATION"])),
        axis=1,
    )
    df["gameday"] = pd.to_datetime(raw["GAME_DATE"])
    df["minutes"] = pd.to_numeric(raw["MIN"], errors="coerce").fillna(0)
    df["points"] = pd.to_numeric(raw["PTS"], errors="coerce").fillna(0)
    df["rebounds"] = pd.to_numeric(raw["REB"], errors="coerce").fillna(0)
    df["assists"] = pd.to_numeric(raw["AST"], errors="coerce").fillna(0)
    df["threes"] = pd.to_numeric(raw["FG3M"], errors="coerce").fillna(0)
    df["steals"] = pd.to_numeric(raw["STL"], errors="coerce").fillna(0)
    df["blocks"] = pd.to_numeric(raw["BLK"], errors="coerce").fillna(0)
    df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    return df


def get_player_positions() -> pd.DataFrame:
    """
    Fetch player positions for the current season from NBA.com.

    Returns a DataFrame with columns: name, position (PG/SG/SF/PF/C).
    """
    season = f"{SEASON_YEAR - 1}-{str(SEASON_YEAR)[-2:]}"
    pi = _with_retries(PlayerIndex, season=season, timeout=NBA_TIMEOUT)
    data = pi.get_dict()["resultSets"][0]
    raw = pd.DataFrame(data["rowSet"], columns=data["headers"])

    df = pd.DataFrame()
    df["name"] = raw["PLAYER_FIRST_NAME"] + " " + raw["PLAYER_LAST_NAME"]
    df["position"] = raw["POSITION"].map(POSITION_MAP).fillna("SF")
    df["player_id"] = raw["PERSON_ID"].apply(
        lambda x: int(x) if pd.notna(x) else None
    )
    df["player_url"] = raw.apply(
        lambda r: f"https://www.nba.com/player/{int(r['PERSON_ID'])}/{r['PLAYER_SLUG']}"
        if pd.notna(r.get("PERSON_ID")) and pd.notna(r.get("PLAYER_SLUG"))
        else "",
        axis=1,
    )
    return df
