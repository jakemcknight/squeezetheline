"""
Fetch NBA injury report from ESPN's public API.

Free, no auth required. Returns a DataFrame with player name, status,
and short comment for every currently listed injury.
"""

import pandas as pd

from config import SPORTS, DEFAULT_SPORT
from scrapers.espn import get_injuries, STATUS_SHORT  # noqa: F401 (STATUS_SHORT re-exported)

# ESPN league slug per Odds API sport key, derived from config.
_LEAGUE_BY_KEY = {
    cfg["key"]: cfg.get("espn_league")
    for cfg in SPORTS.values()
    if cfg.get("espn_league")
}
_DEFAULT_LEAGUE = SPORTS[DEFAULT_SPORT]["espn_league"]


def get_injury_report(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Fetch the injury report for a sport from ESPN.

    Returns a DataFrame with columns: name, team, status, status_short,
    comment, date. Defaults to the NBA so existing callers keep working.
    """
    league = _LEAGUE_BY_KEY.get(sport_key, _DEFAULT_LEAGUE)
    return get_injuries(league)
