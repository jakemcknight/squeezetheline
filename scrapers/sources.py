"""
Sport-agnostic data dispatcher.

The app shouldn't care *which* scraper backs a given sport — it just asks for
"season stats for sport_key X". This module routes each request to the right
source based on config.SPORTS:

    stats_source = "nba_api"  -> scrapers/nba.py     (NBA, one-shot league log)
    stats_source = "espn"     -> ESPN-backed module per sport key:
                                   basketball_wnba       -> scrapers/wnba.py
                                   baseball_mlb          -> scrapers/mlb.py
                                   americanfootball_nfl  -> scrapers/nfl.py
                                   americanfootball_ncaaf-> scrapers/ncaaf.py
    stats_source = None       -> empty frames        (NCAAB — odds + injuries
                                                      only; see scrapers/ncaa.py)

ESPN-backed sports are *slate-scoped*: pass the slate's team codes and prop
player names so only those rosters/game logs are fetched (the request count
scales with the slate, not the league). NBA ignores those filters — its single
LeagueGameLog call already returns the whole league cheaply.

Defense rankings and injuries are already sport-parameterized in their own
modules; they're re-exposed here so app.py has a single import surface. Injuries
work for every sport with an espn_league (basketball, football, baseball) via
the sport-generalized scrapers/espn.py.
"""

import pandas as pd

from config import SPORTS, DEFAULT_SPORT, stat_keys_for
from scrapers.basketball_ref import get_defense_by_position as _defense
from scrapers.injuries import get_injury_report as _injuries

_POSITION_COLUMNS = ["name", "position", "player_id", "player_url"]

# Odds API sport key -> stats_source declared in config.
_STATS_SOURCE_BY_KEY = {cfg["key"]: cfg.get("stats_source") for cfg in SPORTS.values()}
_DEFAULT_KEY = SPORTS[DEFAULT_SPORT]["key"]

# Odds API sport key -> the ESPN-backed stats module for that sport.
_ESPN_MODULE_BY_KEY = {
    "basketball_wnba": "scrapers.wnba",
    "baseball_mlb": "scrapers.mlb",
    "americanfootball_nfl": "scrapers.nfl",
    "americanfootball_ncaaf": "scrapers.ncaaf",
}


def _stats_source(sport_key: str) -> str:
    return _STATS_SOURCE_BY_KEY.get(sport_key, _STATS_SOURCE_BY_KEY.get(_DEFAULT_KEY))


def _empty_stats(sport_key: str) -> pd.DataFrame:
    cols = ["name", "team-code", "opponent", "gameday", "minutes"] + stat_keys_for(sport_key)
    return pd.DataFrame(columns=list(dict.fromkeys(cols)))


def _espn_module(sport_key: str):
    import importlib
    name = _ESPN_MODULE_BY_KEY.get(sport_key, "scrapers.wnba")
    return importlib.import_module(name)


def get_season_stats(sport_key: str = "basketball_nba",
                     team_codes=None, player_names=None) -> pd.DataFrame:
    """Per-game player stats for the sport's current season.

    Columns: name, team-code, opponent, gameday, minutes + the sport's stat
    columns (see config.SPORT_STAT_CONFIGS). Empty when no source is wired.

    `team_codes`/`player_names` scope ESPN-backed fetches to the slate (ignored
    by the NBA source, which is a single cheap league-wide call).
    """
    source = _stats_source(sport_key)
    if source == "nba_api":
        from scrapers.nba import get_current_season_stats
        return get_current_season_stats()
    if source == "espn":
        mod = _espn_module(sport_key)
        # WNBA's wrapper takes no slate args (small league, full walk); the
        # baseball/football wrappers accept team/player scoping.
        try:
            return mod.get_current_season_stats(team_codes=team_codes, player_names=player_names)
        except TypeError:
            return mod.get_current_season_stats()
    # No source wired (e.g. NCAAB) — see scrapers/ncaa.py.
    return _empty_stats(sport_key)


def get_positions(sport_key: str = "basketball_nba", team_codes=None) -> pd.DataFrame:
    """Player positions for the sport. Columns: name, position, player_id, player_url."""
    source = _stats_source(sport_key)
    if source == "nba_api":
        from scrapers.nba import get_player_positions
        return get_player_positions()
    if source == "espn":
        mod = _espn_module(sport_key)
        try:
            return mod.get_player_positions(team_codes=team_codes)
        except TypeError:
            return mod.get_player_positions()
    return pd.DataFrame(columns=_POSITION_COLUMNS)


def get_defense_by_position(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Defense-vs-position rankings (NBA only; empty elsewhere — see basketball_ref)."""
    return _defense(sport_key)


def get_injury_report(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Injury report for the sport (ESPN)."""
    return _injuries(sport_key)
