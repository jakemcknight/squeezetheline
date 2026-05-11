"""
Fetch current-season player stats from NBA.com via nba_api.

Completely free, no API key needed. Uses LeagueGameLog to pull all player
game logs for the current season in a single request.

NBA.com is known to flake on cloud IPs (GitHub Actions, AWS, etc.) so
the calls below use a longer timeout and retry on transient failures.
"""

import time
from typing import Optional

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
    Fetch all player game logs for the current NBA season — combining
    Regular Season, Play-In Tournament, and Playoffs so analytics still
    work after the regular season ends in mid-April.

    Returns a DataFrame with columns matching what the analysis pipeline
    expects: name, team-code, gameday, minutes, points, rebounds, assists.
    """
    season = f"{SEASON_YEAR - 1}-{str(SEASON_YEAR)[-2:]}"
    parts = []
    for season_type in ("Regular Season", "PlayIn", "Playoffs"):
        try:
            log = _with_retries(
                LeagueGameLog,
                season=season,
                season_type_all_star=season_type,
                player_or_team_abbreviation="P",
                timeout=NBA_TIMEOUT,
            )
            data = log.get_dict()["resultSets"][0]
            df_part = pd.DataFrame(data["rowSet"], columns=data["headers"])
            if not df_part.empty:
                parts.append(df_part)
        except Exception as e:
            # Some types may not exist for the season yet
            print(f"  get_current_season_stats: skipped {season_type} ({type(e).__name__})")
            continue
    if not parts:
        return pd.DataFrame()
    raw = pd.concat(parts, ignore_index=True)

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


def get_live_box_score(player_name: str) -> Optional[dict]:
    """Return live in-game stats for a player if their game is currently running.

    Uses nba_api's scoreboard + boxscore_v3 endpoints. Returns:
      {pts, reb, ast, threes, steals, blocks, minutes, period, time_remaining}
    or None if no live game found for the player.
    """
    try:
        from nba_api.live.nba.endpoints import scoreboard, boxscore
    except ImportError:
        return None

    try:
        sb = scoreboard.ScoreBoard(timeout=10)
        games = sb.games.get_dict()
    except Exception:
        return None

    # Try to find a live game; look up the boxscore for each
    for g in games or []:
        if g.get("gameStatus") not in (2,):  # 2 = in progress
            continue
        game_id = g.get("gameId")
        try:
            bs = boxscore.BoxScore(game_id=game_id, timeout=10)
            data = bs.get_dict()
        except Exception:
            continue
        for team_key in ("homeTeam", "awayTeam"):
            for p in (data.get("game", {}).get(team_key, {}) or {}).get("players", []) or []:
                full = f"{p.get('firstName','')} {p.get('familyName','')}".strip()
                if full.lower() != player_name.lower():
                    continue
                stats = p.get("statistics", {}) or {}
                return {
                    "pts": int(stats.get("points", 0)),
                    "reb": int(stats.get("reboundsTotal", 0)),
                    "ast": int(stats.get("assists", 0)),
                    "threes": int(stats.get("threePointersMade", 0)),
                    "steals": int(stats.get("steals", 0)),
                    "blocks": int(stats.get("blocks", 0)),
                    "minutes": str(stats.get("minutes", "0")),
                    "period": int(data["game"].get("period", 1)),
                    "time_remaining": data["game"].get("gameClock", ""),
                    "team": team_key,
                }
    return None


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
