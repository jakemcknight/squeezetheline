"""
Fetch current-season player stats from NBA.com via nba_api.

Completely free, no API key needed. Uses LeagueGameLog to pull all player
game logs for the current season in a single request.
"""

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog, PlayerIndex

from config import SEASON_YEAR, NBA_API_TEAM_CODE_MAP

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


def get_current_season_stats() -> pd.DataFrame:
    """
    Fetch all player game logs for the current NBA season.

    Returns a DataFrame with columns matching what the analysis pipeline
    expects: name, team-code, gameday, minutes, points, rebounds, assists.
    """
    season = f"{SEASON_YEAR - 1}-{str(SEASON_YEAR)[-2:]}"
    log = LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="P",
    )
    data = log.get_dict()["resultSets"][0]
    raw = pd.DataFrame(data["rowSet"], columns=data["headers"])

    df = pd.DataFrame()
    df["name"] = raw["PLAYER_NAME"]
    df["team-code"] = raw["TEAM_ABBREVIATION"].apply(_map_team_code)
    df["gameday"] = pd.to_datetime(raw["GAME_DATE"])
    df["minutes"] = pd.to_numeric(raw["MIN"], errors="coerce").fillna(0)
    df["points"] = pd.to_numeric(raw["PTS"], errors="coerce").fillna(0)
    df["rebounds"] = pd.to_numeric(raw["REB"], errors="coerce").fillna(0)
    df["assists"] = pd.to_numeric(raw["AST"], errors="coerce").fillna(0)
    return df


def get_player_positions() -> pd.DataFrame:
    """
    Fetch player positions for the current season from NBA.com.

    Returns a DataFrame with columns: name, position (PG/SG/SF/PF/C).
    """
    season = f"{SEASON_YEAR - 1}-{str(SEASON_YEAR)[-2:]}"
    pi = PlayerIndex(season=season)
    data = pi.get_dict()["resultSets"][0]
    raw = pd.DataFrame(data["rowSet"], columns=data["headers"])

    df = pd.DataFrame()
    df["name"] = raw["PLAYER_FIRST_NAME"] + " " + raw["PLAYER_LAST_NAME"]
    df["position"] = raw["POSITION"].map(POSITION_MAP).fillna("SF")
    df["player_url"] = raw.apply(
        lambda r: f"https://www.nba.com/player/{int(r['PERSON_ID'])}/{r['PLAYER_SLUG']}"
        if pd.notna(r.get("PERSON_ID")) and pd.notna(r.get("PLAYER_SLUG"))
        else "",
        axis=1,
    )
    return df
