"""Tests for the soccer wiring in config.py and the odds layer.

Covers the sport/stat config, the country→code map, logo URLs, the Odds-API
market map, and the Yes/No → synthetic-0.5-line rewrite used for the
anytime-goalscorer and to-receive-a-card markets.
"""
import config
from scrapers import odds_api


SOCCER = "soccer_fifa_world_cup"


# --- config -----------------------------------------------------------------

def test_soccer_sport_registered_and_active():
    assert "FIFA World Cup" in config.SPORTS
    cfg = config.SPORTS["FIFA World Cup"]
    assert cfg["key"] == SOCCER
    assert cfg["active"] is True
    assert cfg["projections"] is True
    assert cfg["espn_league"] == "fifa.world"
    assert "FIFA World Cup" in config.active_sports()


def test_soccer_stat_config():
    cfg = config.stat_configs_for(SOCCER)
    stat_keys = [k for k, _pt, _l in cfg]
    assert stat_keys == ["goals", "shots", "shots_on_target", "assists", "cards"]
    # prop_types must match what odds_api.MARKET_MAP emits so lines join to stats.
    prop_to_stat = config.prop_to_stat_for(SOCCER)
    assert prop_to_stat["Anytime Goalscorer"] == "goals"
    assert prop_to_stat["To Receive a Card"] == "cards"
    assert prop_to_stat["Shots on Target"] == "shots_on_target"
    # assists reuses the existing "Total Assists" prop_type (shared Odds-API key).
    assert prop_to_stat["Total Assists"] == "assists"


def test_country_name_to_code():
    assert config.team_name_to_code("Argentina", SOCCER) == "ARG"
    assert config.team_name_to_code("United States", SOCCER) == "USA"
    assert config.team_name_to_code("USA", SOCCER) == "USA"          # variant
    assert config.team_name_to_code("South Korea", SOCCER) == "KOR"
    assert config.team_name_to_code("Korea Republic", SOCCER) == "KOR"  # variant
    # Unknown names pass through unchanged (graceful, like NCAAF).
    assert config.team_name_to_code("Atlantis", SOCCER) == "Atlantis"


def test_soccer_team_logo_url_uses_countries_set():
    url = config.team_logo_url("ARG", SOCCER)
    assert url == "https://a.espncdn.com/i/teamlogos/countries/500/arg.png"
    assert config.team_logo_url("", SOCCER) == ""


def test_soccer_headshot_url_empty():
    # ESPN has no stable soccer headshot asset by athlete id, so we skip them.
    assert config.player_photo_url("277206", SOCCER) == ""


def test_soccer_markets_listed():
    markets = config.SPORTS["FIFA World Cup"]["markets"]
    assert "player_goal_scorer_anytime" in markets
    assert "player_shots" in markets
    assert "player_to_receive_card" in markets


# --- odds_api ---------------------------------------------------------------

def test_market_map_has_soccer_keys():
    assert odds_api.MARKET_MAP["player_goal_scorer_anytime"] == "Anytime Goalscorer"
    assert odds_api.MARKET_MAP["player_shots"] == "Shots"
    assert odds_api.MARKET_MAP["player_shots_on_target"] == "Shots on Target"
    assert odds_api.MARKET_MAP["player_to_receive_card"] == "To Receive a Card"
    # assists shares the basketball key, mapped to "Total Assists".
    assert odds_api.MARKET_MAP["player_assists"] == "Total Assists"


def test_unsupported_markets_not_mapped():
    # first/last goalscorer and red-card markets are intentionally unsupported.
    for key in ("player_first_goal_scorer", "player_last_goal_scorer",
                "player_to_receive_red_card"):
        assert key not in odds_api.MARKET_MAP


def test_yesno_outcome_synthesizes_half_line():
    # Anytime-goalscorer "Yes" -> Over 0.5 on goals.
    parsed = odds_api._outcome_line(
        "player_goal_scorer_anytime",
        {"name": "Yes", "description": "Lionel Messi", "price": 120},
    )
    assert parsed == ("Lionel Messi", 0.5)
    # "No" outcomes are dropped (we only track the scorer side).
    assert odds_api._outcome_line(
        "player_goal_scorer_anytime", {"name": "No", "description": "Lionel Messi"}
    ) is None


def test_over_under_outcome_uses_point():
    parsed = odds_api._outcome_line(
        "player_shots",
        {"name": "Over", "description": "Kylian Mbappé", "point": 2.5},
    )
    assert parsed == ("Kylian Mbappé", 2.5)
    # The "Under" side is not tracked (the pipeline keys off the Over line).
    assert odds_api._outcome_line(
        "player_shots", {"name": "Under", "description": "Kylian Mbappé", "point": 2.5}
    ) is None


def test_markets_for_soccer():
    s = odds_api.markets_for(SOCCER)
    assert "player_goal_scorer_anytime" in s
    assert "player_shots_on_target" in s
