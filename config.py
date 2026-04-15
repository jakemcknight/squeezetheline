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


def team_logo_url(team_code: str) -> str:
    """Return the NBA.com primary logo URL for a 3-letter team code, or '' if unknown."""
    team_id = NBA_TEAM_IDS.get(team_code)
    if not team_id:
        return ""
    return f"https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"


def player_photo_url(player_id: int | str) -> str:
    """Return the NBA.com headshot URL (1040x760) for a player ID."""
    if not player_id:
        return ""
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"


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
