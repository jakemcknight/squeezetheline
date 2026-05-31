"""
NCAA Football player stats & positions — NOT WIRED (projections stubbed).

Odds (scrapers/odds_api.py with sport_key="americanfootball_ncaaf") and injuries
(scrapers/espn.py / ESPN league "college-football") work today, so NCAA Football
is selectable and shows player-prop lines + injury context. Projections are off
for the same reason as the NFL (see scrapers/nfl.py): the projection engine is
basketball-specific.

Additional NCAA-only limitations:
  - Scale: ~130 FBS teams (plus FCS), far more players than the NFL. A
    teams -> rosters -> per-athlete gamelog walk would be a lot of requests;
    restrict any future scrape to the day's slate teams.
  - Team identity: there's no team-name map (config.py passes Odds API names
    through unchanged), and ESPN college logos are keyed by team id rather than
    abbreviation, so team logos don't resolve from a name alone.

What a stats source WOULD need: same football-aware analysis path described in
scrapers/nfl.py, scoped to slate teams to keep request counts sane.
"""

import pandas as pd

_STATS_COLUMNS = [
    "name", "team-code", "opponent", "gameday", "minutes",
    "points", "rebounds", "assists", "threes", "steals", "blocks", "pra",
]
_POSITION_COLUMNS = ["name", "position", "player_id", "player_url"]


def get_current_season_stats() -> pd.DataFrame:
    """STUB: no NCAA Football player season-stats source wired yet. Returns empty."""
    return pd.DataFrame(columns=_STATS_COLUMNS)


def get_player_positions() -> pd.DataFrame:
    """STUB: no NCAA Football player-positions source wired yet. Returns empty."""
    return pd.DataFrame(columns=_POSITION_COLUMNS)
