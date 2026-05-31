"""
NCAA football player stats & positions, sourced from ESPN.

Same code path as scrapers/nfl.py (ESPN football game logs), pointed at the
"college-football" league. The projectable prop menu is narrower than the NFL's
(no completions/INT props) — see config.SPORT_STAT_CONFIGS["americanfootball_ncaaf"].

Two college-specific caveats:
  - Team matching: the Odds API has no short-code map for the ~130+ FBS teams
    (config.TEAM_NAME_TO_CODE_BY_SPORT has no NCAAF entry), so slate "team codes"
    are full display names. espn._slate_teams matches those against ESPN team
    display names (best-effort), which can miss teams whose Odds-API naming
    differs from ESPN's.
  - Scale: even slate-scoped, a big Saturday slate spans many teams, so the
    refresh is the slowest of any sport. It's admin-triggered and cached.
"""

import datetime

import pandas as pd

from scrapers import espn

LEAGUE = "college-football"


def _season() -> int:
    today = datetime.date.today()
    return today.year if today.month >= 8 else today.year - 1


def get_current_season_stats(team_codes=None, player_names=None) -> pd.DataFrame:
    """Per-game player stats for the slate's players (ESPN). Same columns as
    scrapers.nfl.get_current_season_stats."""
    return espn.get_slate_stats(
        LEAGUE, team_codes=team_codes, player_names=player_names, season=_season(),
    )


def get_player_positions(team_codes=None) -> pd.DataFrame:
    """NCAAF player positions for the slate teams (ESPN rosters)."""
    return espn.get_player_positions(LEAGUE, team_codes=team_codes)
