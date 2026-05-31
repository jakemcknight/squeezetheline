"""
NCAA Men's Basketball player stats & positions — NOT YET WIRED.

Odds (scrapers/odds_api.py with sport_key="basketball_ncaab") and injuries
(scrapers/injuries.py / ESPN league "mens-college-basketball") work today, so
the sport is selectable and shows lines + injury context. What's missing is a
*player season-stats* feed, which is what the Picks Board projection edges are
built on.

Why it's stubbed rather than implemented:
  - Scale: D1 men's basketball has ~360 teams and ~5,000+ players. The ESPN
    approach used for the WNBA (walk teams -> rosters -> per-athlete game logs)
    would be thousands of HTTP requests per refresh — impractical inline.
  - Season timing: the season runs Nov–Apr, so for much of the year there are
    no current props to analyze anyway.

TODO to enable projections for NCAA (then flip SPORTS["NCAA Men's Basketball"]
["projections"] = True and set stats_source = "ncaa" in config.py):
  1. Pick a bulk stats source — e.g. sports-reference.com/cbb season game logs,
     ESPN's core API, or stats.ncaa.org — that can return many players per call.
  2. Restrict scraping to teams on the day's slate (from get_todays_teams with
     sport_key="basketball_ncaab") rather than all of D1, to keep request
     counts sane.
  3. Emit the same columns as scrapers.nba.get_current_season_stats:
     name, team-code, opponent, gameday, minutes, points, rebounds, assists,
     threes, steals, blocks, pra.
  4. Add a positions source for the same players (ESPN rosters work but are
     per-team; scope to slate teams).
"""

import pandas as pd

_STATS_COLUMNS = [
    "name", "team-code", "opponent", "gameday", "minutes",
    "points", "rebounds", "assists", "threes", "steals", "blocks", "pra",
]
_POSITION_COLUMNS = ["name", "position", "player_id", "player_url"]


def get_current_season_stats() -> pd.DataFrame:
    """STUB: no NCAA player season-stats source wired yet. Returns empty.

    See the module docstring for the TODO needed to enable this.
    """
    return pd.DataFrame(columns=_STATS_COLUMNS)


def get_player_positions() -> pd.DataFrame:
    """STUB: no NCAA player-positions source wired yet. Returns empty."""
    return pd.DataFrame(columns=_POSITION_COLUMNS)
