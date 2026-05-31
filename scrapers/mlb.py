"""
MLB player stats & positions, sourced from ESPN.

Projections are live: ESPN's free baseball game-log endpoints back both batters
(hits, total bases, HR, RBI, runs, SB) and pitchers (strikeouts, hits allowed,
earned runs, outs). Batter and pitcher rows share one stats table; each row
carries only its side's columns (the other side's are 0), which the analysis
engine grades independently per prop type. See config.SPORT_STAT_CONFIGS[
"baseball_mlb"] for the stat→prop-line mapping.

These wrappers expose the same interface as scrapers/nba.py so the dispatcher in
scrapers/sources.py can treat them interchangeably. They're slate-scoped: only
the teams playing on the selected date (and only their prop players) are walked,
keeping the request count proportional to the slate rather than all 30 rosters.
"""

import datetime

import pandas as pd

from scrapers import espn

LEAGUE = "mlb"


def get_current_season_stats(team_codes=None, player_names=None) -> pd.DataFrame:
    """Per-game batter/pitcher stats for the slate's players (ESPN).

    Columns: name, team-code, opponent, gameday, minutes + the MLB stat columns
    (hits, total_bases, home_runs, rbis, runs, stolen_bases, strikeouts_pitcher,
    hits_allowed, earned_runs, outs). The MLB season is a single calendar year,
    so the current year is the ESPN season id.
    """
    return espn.get_slate_stats(
        LEAGUE, team_codes=team_codes, player_names=player_names,
        season=datetime.date.today().year,
    )


def get_player_positions(team_codes=None) -> pd.DataFrame:
    """MLB player positions for the slate teams (ESPN rosters).

    Columns: name, position (ESPN abbreviation: SP/RP/C/1B/.../DH), player_id,
    player_url.
    """
    return espn.get_player_positions(LEAGUE, team_codes=team_codes)
