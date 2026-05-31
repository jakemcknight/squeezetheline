"""
MLB player stats & positions — NOT WIRED (projections stubbed).

Odds (scrapers/odds_api.py with sport_key="baseball_mlb") and injuries
(scrapers/espn.py / ESPN league "mlb") work today, so MLB is selectable and
shows player-prop lines + injury context. What's missing is *projections*.

Why it's stubbed rather than implemented:
  The projection engine (analysis.py / data.py) and the entire Picks Board UI
  are built around basketball box-score columns: points, rebounds, assists,
  threes, steals, blocks, pra. Baseball has an entirely different stat shape
  (hits, total bases, HR, RBIs, ERA, strikeouts; batters vs pitchers), so there
  is no meaningful way to feed MLB data through the basketball pipeline. Real
  MLB projections need a parallel analysis path, not just a stats scraper.

What a stats source WOULD need (then flip SPORTS["MLB"]["projections"]=True and
set stats_source accordingly in config.py, AND add a baseball-aware analysis
path):
  1. A per-game batter/pitcher game-log feed (ESPN baseball gamelog endpoints
     exist: site.web.api.espn.com/.../baseball/mlb/athletes/{id}/gamelog).
  2. Baseball-appropriate projection columns + line types (BA, HR, RBI, TB,
     SB for batters; K, ERA, outs, hits-allowed for pitchers).
  3. A board/analysis variant that ranks edges on those columns.
"""

import pandas as pd

# Same column contract as the basketball stats sources, so the dispatcher in
# scrapers/sources.py can treat a (future) MLB source interchangeably.
_STATS_COLUMNS = [
    "name", "team-code", "opponent", "gameday", "minutes",
    "points", "rebounds", "assists", "threes", "steals", "blocks", "pra",
]
_POSITION_COLUMNS = ["name", "position", "player_id", "player_url"]


def get_current_season_stats() -> pd.DataFrame:
    """STUB: no MLB player season-stats source wired yet. Returns empty.

    See the module docstring for what enabling projections would require.
    """
    return pd.DataFrame(columns=_STATS_COLUMNS)


def get_player_positions() -> pd.DataFrame:
    """STUB: no MLB player-positions source wired yet. Returns empty."""
    return pd.DataFrame(columns=_POSITION_COLUMNS)
