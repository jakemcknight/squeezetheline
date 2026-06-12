"""
FIFA World Cup player stats & positions, sourced from ESPN.

Projections are live, but soccer is sourced differently from the US sports.
ESPN has no league-wide soccer game-log endpoint, and the generic
/athletes/{id}/gamelog feed 404s for soccer. Instead each player's recent
*club* match form (goals, shots, shots on target, assists, cards) comes from the
athlete "overview" feed, which scrapers/espn.py:get_soccer_gamelog turns into
per-match rate rows. Recent club form is the strongest public per-player signal
for an international tournament, where national-team samples are tiny — see that
function for the (documented) approximations involved.

These wrappers expose the same interface as scrapers/nba.py so the dispatcher in
scrapers/sources.py can treat them interchangeably. They're slate-scoped: only
the national teams playing on the selected date (and only their prop players)
are walked, keeping the request count proportional to the slate.

The opponent in each stat row is the player's *club* opponent (not the World Cup
opponent); the tournament opponent-strength adjustment is applied downstream by
soccer_model.py, which keys off the slate's national-team matchup. See
config.SPORT_STAT_CONFIGS["soccer_fifa_world_cup"] for the stat→prop mapping.
"""

import pandas as pd

from scrapers import espn

# ESPN's World Cup league slug. The 48-team field lives under soccer/fifa.world.
LEAGUE = "fifa.world"


def get_current_season_stats(team_codes=None, player_names=None) -> pd.DataFrame:
    """Per-match player stats for the slate's players (ESPN overview feed).

    Columns: name, team-code, opponent, gameday, minutes + the soccer stat
    columns (started, goals, assists, shots, shots_on_target, fouls,
    yellow_cards, red_cards, cards). Soccer ignores the season id (the overview
    feed is always current form), so it isn't passed through.
    """
    return espn.get_slate_stats(
        LEAGUE, team_codes=team_codes, player_names=player_names,
    )


def get_player_positions(team_codes=None) -> pd.DataFrame:
    """World Cup player positions for the slate teams (ESPN rosters).

    Columns: name, position (G/D/M/F), player_id, player_url.
    """
    return espn.get_player_positions(LEAGUE, team_codes=team_codes)
