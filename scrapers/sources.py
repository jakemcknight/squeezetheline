"""
Sport-agnostic data dispatcher.

The app shouldn't care *which* scraper backs a given sport — it just asks for
"season stats for sport_key X". This module routes each request to the right
source based on config.SPORTS:

    stats_source = "nba_api"  -> scrapers/nba.py   (NBA)
    stats_source = "espn"     -> scrapers/wnba.py  (WNBA, via ESPN)
    stats_source = None       -> empty frames      (e.g. NCAA, not yet wired)

Defense rankings and injuries are already sport-parameterized in their own
modules; they're re-exposed here so app.py has a single import surface.
"""

import pandas as pd

from config import SPORTS, DEFAULT_SPORT
from scrapers.basketball_ref import get_defense_by_position as _defense
from scrapers.injuries import get_injury_report as _injuries

_STATS_COLUMNS = [
    "name", "team-code", "opponent", "gameday", "minutes",
    "points", "rebounds", "assists", "threes", "steals", "blocks", "pra",
]
_POSITION_COLUMNS = ["name", "position", "player_id", "player_url"]

# Odds API sport key -> stats_source declared in config.
_STATS_SOURCE_BY_KEY = {cfg["key"]: cfg.get("stats_source") for cfg in SPORTS.values()}
_DEFAULT_KEY = SPORTS[DEFAULT_SPORT]["key"]


def _stats_source(sport_key: str) -> str:
    return _STATS_SOURCE_BY_KEY.get(sport_key, _STATS_SOURCE_BY_KEY.get(_DEFAULT_KEY))


def get_season_stats(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Per-game player stats for the sport's current season.

    Columns: name, team-code, opponent, gameday, minutes, points, rebounds,
    assists, threes, steals, blocks, pra. Empty when no source is wired.
    """
    source = _stats_source(sport_key)
    if source == "nba_api":
        from scrapers.nba import get_current_season_stats
        return get_current_season_stats()
    if source == "espn":
        from scrapers.wnba import get_current_season_stats
        return get_current_season_stats()
    # No source wired (e.g. NCAA) — see scrapers/ncaa.py.
    return pd.DataFrame(columns=_STATS_COLUMNS)


def get_positions(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Player positions for the sport. Columns: name, position, player_id, player_url."""
    source = _stats_source(sport_key)
    if source == "nba_api":
        from scrapers.nba import get_player_positions
        return get_player_positions()
    if source == "espn":
        from scrapers.wnba import get_player_positions
        return get_player_positions()
    return pd.DataFrame(columns=_POSITION_COLUMNS)


def get_defense_by_position(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Defense-vs-position rankings (NBA only; empty elsewhere — see basketball_ref)."""
    return _defense(sport_key)


def get_injury_report(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Injury report for the sport (ESPN)."""
    return _injuries(sport_key)
