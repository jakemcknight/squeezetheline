import os
import datetime

# --- NatStat API (player stats, backfill) ---
API_BASE = "https://api3.natst.at/6843-c01eff"

# Current NBA season identifier used in the NatStat API
SEASON_TAG = "season_2026"
SEASON_YEAR = 2026

# Rate limiting for backfill
BACKFILL_REQUEST_LIMIT = 480
BACKFILL_COOLDOWN_SECONDS = 3600

# Data directory (relative to project root)
DATA_DIR = "data"

# --- Sport configuration ---
# The Odds API supports many sports. Players, line types, and data sources
# differ per sport, so each sport declares which sources back it. Per-sport
# fields:
#   key          — The Odds API sport key (used for odds/props)
#   active        — whether the sport shows in the sport selector
#   projections   — whether we can produce stat projections (needs a player
#                   season-stats source). Sports with projections=False still
#                   show odds + injuries but no Picks-Board edge analysis.
#   stats_source  — which scraper backs season stats / positions
#                   ("nba_api" | "espn" | None)
#   espn_league   — ESPN league slug for injuries/rosters (None if unsupported)
#   ml_models     — whether trained XGBoost models exist for this sport
#                   (only NBA, which has the historical box-score dataset)
#   markets       — Odds API player-prop markets to request
#
# Coverage today:
#   NBA  — fully wired (nba_api stats, HashtagBasketball defense, ML models).
#   WNBA — odds + ESPN injuries + ESPN player stats/positions. No
#          defense-vs-position source (HashtagBasketball has no WNBA page) and
#          no ML models (no historical dataset) — analysis degrades gracefully.
#   NCAA — odds + ESPN injuries wired. No player season-stats source yet
#          (~360 D1 teams; see scrapers/ncaa.py), so projections are off.
SPORTS = {
    "NBA": {
        "key": "basketball_nba",
        "active": True,
        "projections": True,
        "stats_source": "nba_api",
        "espn_league": "nba",
        "ml_models": True,
        "markets": [
            "player_points", "player_rebounds", "player_assists",
            "player_points_rebounds_assists", "player_threes",
            "player_steals", "player_blocks",
        ],
    },
    "WNBA": {
        "key": "basketball_wnba",
        "active": True,
        "projections": True,
        "stats_source": "espn",
        "espn_league": "wnba",
        "ml_models": False,
        "markets": [
            "player_points", "player_rebounds", "player_assists",
            "player_points_rebounds_assists", "player_threes",
            "player_steals", "player_blocks",
        ],
    },
    "NCAA Men's Basketball": {
        "key": "basketball_ncaab",
        "active": True,
        # No scalable player season-stats source wired yet, so we can pull
        # odds + injuries but can't compute projection edges. See scrapers/ncaa.py.
        "projections": False,
        "stats_source": None,
        "espn_league": "mens-college-basketball",
        "ml_models": False,
        "markets": ["player_points", "player_rebounds", "player_assists"],
    },
    "NFL": {
        "key": "americanfootball_nfl",
        "active": False,  # stub — different sport entirely, out of scope here
        "projections": False,
        "stats_source": None,
        "espn_league": "nfl",
        "ml_models": False,
        "markets": [
            "player_pass_yds", "player_rush_yds", "player_receptions",
            "player_anytime_td",
        ],
    },
}

DEFAULT_SPORT = "NBA"


def active_sports() -> dict:
    """Return the subset of SPORTS that should appear in the sport selector."""
    return {name: cfg for name, cfg in SPORTS.items() if cfg.get("active")}


def sport_config(name: str) -> dict:
    """Look up a sport's config by display name, falling back to the default."""
    return SPORTS.get(name, SPORTS[DEFAULT_SPORT])


def sport_key_for(name: str) -> str:
    """Return the Odds API sport key for a sport display name."""
    return sport_config(name).get("key", SPORTS[DEFAULT_SPORT]["key"])


# --- The Odds API (player props) ---
# Sign up for a free key at https://the-odds-api.com
# Set ODDS_API_KEY as an environment variable, or in .streamlit/secrets.toml when deployed
def get_odds_api_key() -> str:
    """Read the Odds API key lazily so Streamlit secrets are available at call time."""
    key = os.environ.get("ODDS_API_KEY")
    if key:
        return key
    try:
        import streamlit as st
        return st.secrets["ODDS_API_KEY"]
    except Exception:
        return ""


ODDS_API_BASE = "https://api.the-odds-api.com/v4"
PREFERRED_BOOKMAKER = "draftkings"  # fallback to first available if not found

# HashtagBasketball defense abbreviations → the 3-letter codes we use elsewhere
# (NatStat-style codes for BRK/CHH, standard codes everywhere else).
TEAM_CODE_MAP = {
    "GS": "GSW",
    "BKN": "BRK",
    "SA": "SAS",
    "NO": "NOP",
    "NY": "NYK",
    "CHA": "CHH",
    "PHO": "PHX",
}

# nba_api team abbreviations → NatStat-style codes (for historical data consistency)
NBA_API_TEAM_CODE_MAP = {
    "BKN": "BRK",
    "CHA": "CHH",
}

# NBA team IDs — used to construct cdn.nba.com logo URLs.
# These IDs are stable; map our 3-letter codes (NatStat-style: BRK/CHH).
NBA_TEAM_IDS = {
    "ATL": 1610612737, "BOS": 1610612738, "BRK": 1610612751,
    "CHH": 1610612766, "CHI": 1610612741, "CLE": 1610612739,
    "DAL": 1610612742, "DEN": 1610612743, "DET": 1610612765,
    "GSW": 1610612744, "HOU": 1610612745, "IND": 1610612754,
    "LAC": 1610612746, "LAL": 1610612747, "MEM": 1610612763,
    "MIA": 1610612748, "MIL": 1610612749, "MIN": 1610612750,
    "NOP": 1610612740, "NYK": 1610612752, "OKC": 1610612760,
    "ORL": 1610612753, "PHI": 1610612755, "PHX": 1610612756,
    "POR": 1610612757, "SAC": 1610612758, "SAS": 1610612759,
    "TOR": 1610612761, "UTA": 1610612762, "WAS": 1610612764,
}


# ESPN league slug per Odds API sport key — used to build ESPN logo/headshot
# URLs for sports whose stats come from ESPN (e.g. WNBA).
ESPN_LEAGUE_BY_KEY = {cfg["key"]: cfg.get("espn_league") for cfg in SPORTS.values()}


def team_logo_url(team_code: str, sport_key: str = "basketball_nba") -> str:
    """Return a team-logo URL for a team code, or '' if unknown.

    NBA logos come from cdn.nba.com (by numeric team id); other ESPN-backed
    sports use ESPN's logo CDN (by lowercase abbreviation).
    """
    if sport_key == "basketball_nba":
        team_id = NBA_TEAM_IDS.get(team_code)
        if not team_id:
            return ""
        return f"https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"
    league = ESPN_LEAGUE_BY_KEY.get(sport_key)
    if not league or not team_code:
        return ""
    return f"https://a.espncdn.com/i/teamlogos/{league}/500/{team_code.lower()}.png"


def player_photo_url(player_id: int | str, sport_key: str = "basketball_nba") -> str:
    """Return a player-headshot URL for a player id, or '' if unknown.

    NBA ids come from nba_api (cdn.nba.com headshots); ESPN-backed sports use
    ESPN athlete ids and ESPN's headshot CDN.
    """
    if not player_id:
        return ""
    if sport_key == "basketball_nba":
        return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"
    league = ESPN_LEAGUE_BY_KEY.get(sport_key)
    if not league:
        return ""
    return f"https://a.espncdn.com/i/headshots/{league}/players/full/{player_id}.png"


# The Odds API full team names → 3-letter codes
TEAM_NAME_TO_CODE = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BRK",
    "Charlotte Hornets": "CHH",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

# WNBA: The Odds API full team names → ESPN team abbreviations (so odds-derived
# team codes match the codes ESPN stats/positions use, keeping opponent and
# game-status mapping consistent). Codes verified against ESPN's WNBA teams
# endpoint. Includes the 2025–26 expansion clubs (Golden State, Toronto,
# Portland) so the map stays valid as the league grows.
# TODO: verify the exact Odds API display strings once a WNBA slate is live
# (these follow ESPN's "City Nickname" naming, which the Odds API also uses).
WNBA_TEAM_NAME_TO_CODE = {
    "Atlanta Dream": "ATL",
    "Chicago Sky": "CHI",
    "Connecticut Sun": "CON",
    "Dallas Wings": "DAL",
    "Golden State Valkyries": "GS",
    "Indiana Fever": "IND",
    "Las Vegas Aces": "LV",
    "Los Angeles Sparks": "LA",
    "Minnesota Lynx": "MIN",
    "New York Liberty": "NY",
    "Phoenix Mercury": "PHX",
    "Portland Fire": "POR",
    "Seattle Storm": "SEA",
    "Toronto Tempo": "TOR",
    "Washington Mystics": "WSH",
}

# The Odds API team-name → code map per sport key. Sports without a map (e.g.
# NCAA, with hundreds of teams) fall back to passing the name through unchanged.
TEAM_NAME_TO_CODE_BY_SPORT = {
    "basketball_nba": TEAM_NAME_TO_CODE,
    "basketball_wnba": WNBA_TEAM_NAME_TO_CODE,
}


def team_name_to_code(full_name: str, sport_key: str = "basketball_nba") -> str:
    """Convert an Odds API full team name to its short code for the sport.

    Falls back to the original name when the sport has no map or the name is
    unrecognized (so downstream code still has a usable, if verbose, key).
    """
    mapping = TEAM_NAME_TO_CODE_BY_SPORT.get(sport_key, {})
    return mapping.get(full_name, full_name)


def format_date(date: datetime.date) -> str:
    """Format a date as YYYY-MM-DD with zero-padded month/day."""
    return date.strftime("%Y-%m-%d")


def flatten_json(y):
    """Recursively flatten a nested dict/list structure into a single-level dict."""
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out
