"""
NFL player stats & positions — NOT WIRED (projections stubbed).

Odds (scrapers/odds_api.py with sport_key="americanfootball_nfl") and injuries
(scrapers/espn.py / ESPN league "nfl") work today, so the NFL is selectable and
shows player-prop lines + injury context. What's missing is *projections*.

Why it's stubbed rather than implemented:
  The projection engine (analysis.py / data.py) and the Picks Board UI assume
  basketball box-score columns (points, rebounds, assists, threes, steals,
  blocks, pra). Football stats are completely different (passing/rushing/
  receiving yards, attempts, completions, TDs, receptions) and split by
  position group (QB/RB/WR/TE), so the basketball pipeline can't project them.

What a stats source WOULD need (then set projections=True + a stats_source in
config.py AND add a football-aware analysis path):
  1. A per-game player game-log feed (ESPN football gamelog endpoints exist:
     site.web.api.espn.com/.../football/nfl/athletes/{id}/gamelog).
  2. Football-appropriate projection columns + line types (pass yds/TDs/
     completions, rush yds/att/TDs, receptions, receiving yds/TDs).
  3. A board/analysis variant that ranks edges on those columns.

Football is also weekly (one slate/week) rather than nightly, which the
date-driven slate model would need to account for.
"""

import pandas as pd

_STATS_COLUMNS = [
    "name", "team-code", "opponent", "gameday", "minutes",
    "points", "rebounds", "assists", "threes", "steals", "blocks", "pra",
]
_POSITION_COLUMNS = ["name", "position", "player_id", "player_url"]


def get_current_season_stats() -> pd.DataFrame:
    """STUB: no NFL player season-stats source wired yet. Returns empty.

    See the module docstring for what enabling projections would require.
    """
    return pd.DataFrame(columns=_STATS_COLUMNS)


def get_player_positions() -> pd.DataFrame:
    """STUB: no NFL player-positions source wired yet. Returns empty."""
    return pd.DataFrame(columns=_POSITION_COLUMNS)
