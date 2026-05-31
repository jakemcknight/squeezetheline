"""
NFL player stats & positions, sourced from ESPN.

Projections are live: ESPN's free football game-log endpoints back
passing/rushing/receiving production for QB/RB/WR/TE. A single game-log feed
carries whichever categories a player accrues (a QB's passing + rushing, a WR's
receiving + rushing, etc.); the parser reads each stat by name, so one code path
covers every position. See config.SPORT_STAT_CONFIGS["americanfootball_nfl"] for
the stat→prop-line mapping.

These wrappers expose the same interface as scrapers/nba.py so the dispatcher in
scrapers/sources.py can treat them interchangeably. They're slate-scoped: only
the teams playing in the selected week (and only their prop players) are walked.

Football is weekly rather than nightly, so a "slate" is the games on the selected
date and recent-form windows use the last N games regardless of span.
"""

import datetime

import pandas as pd

from scrapers import espn

LEAGUE = "nfl"


def _season() -> int:
    """ESPN's NFL season id is the year the season *starts*; Jan–Jul dates still
    belong to the prior year's season (playoffs/offseason), so roll back."""
    today = datetime.date.today()
    return today.year if today.month >= 8 else today.year - 1


def get_current_season_stats(team_codes=None, player_names=None) -> pd.DataFrame:
    """Per-game player stats for the slate's players (ESPN).

    Columns: name, team-code, opponent, gameday, minutes + the football stat
    columns (pass_yards, pass_tds, completions, pass_attempts, interceptions,
    rush_yards, rush_attempts, rush_tds, receptions, receiving_yards,
    receiving_tds).
    """
    return espn.get_slate_stats(
        LEAGUE, team_codes=team_codes, player_names=player_names, season=_season(),
    )


def get_player_positions(team_codes=None) -> pd.DataFrame:
    """NFL player positions for the slate teams (ESPN rosters).

    Columns: name, position (QB/RB/WR/TE/...), player_id, player_url.
    """
    return espn.get_player_positions(LEAGUE, team_codes=team_codes)
