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
def _get_odds_api_key() -> str:
    key = os.environ.get("ODDS_API_KEY")
    if key:
        return key
    # Try Streamlit secrets (only available when running under streamlit)
    try:
        import streamlit as st
        return st.secrets["ODDS_API_KEY"]
    except Exception:
        return ""


ODDS_API_KEY = _get_odds_api_key()
ODDS_API_BASE = "https://api.the-odds-api.com/v4"
PREFERRED_BOOKMAKER = "draftkings"  # fallback to first available if not found

# Team code mappings — defense data sources use different abbreviations
TEAM_CODE_MAP = {
    "GS": "GSW",
    "BKN": "BRK",
    "SA": "SAS",
    "NO": "NOP",
    "NY": "NYK",
    "CHA": "CHH",
}

# nba_api team abbreviations → NatStat-style codes (for historical data consistency)
NBA_API_TEAM_CODE_MAP = {
    "BKN": "BRK",
    "CHA": "CHH",
}

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
