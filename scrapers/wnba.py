"""
WNBA player stats & positions, sourced from ESPN.

nba_api only covers the NBA, so WNBA data comes from ESPN's free endpoints
(see scrapers/espn.py). These thin wrappers expose the same interface as
scrapers/nba.py so the dispatcher in scrapers/sources.py can treat them
interchangeably.
"""

import datetime

import pandas as pd

from scrapers import espn

LEAGUE = "wnba"


def get_current_season_stats() -> pd.DataFrame:
    """All WNBA player game logs for the current season (ESPN).

    Same columns as scrapers.nba.get_current_season_stats: name, team-code,
    opponent, gameday, minutes, points, rebounds, assists, threes, steals,
    blocks, pra. The WNBA season is a single calendar year, so we use the
    current year as the ESPN season id.
    """
    return espn.get_season_stats(LEAGUE, season=datetime.date.today().year)


def get_player_positions() -> pd.DataFrame:
    """WNBA player positions (ESPN rosters).

    Columns: name, position (PG/SG/SF/PF/C), player_id, player_url.
    """
    return espn.get_player_positions(LEAGUE)
