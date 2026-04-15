"""
Fetch NBA player prop lines from The Odds API.

Replaces the Selenium-based Bovada scraper with a fast, stable REST API.
Free tier: 500 credits/month at https://the-odds-api.com

Usage:
    Set your API key in config.py (ODDS_API_KEY), then call get_all_props().
"""

import datetime

import requests
import pandas as pd
from zoneinfo import ZoneInfo

from config import ODDS_API_BASE, PREFERRED_BOOKMAKER, TEAM_NAME_TO_CODE, get_odds_api_key

# Map The Odds API market keys to the prop type names used in the analysis
MARKET_MAP = {
    "player_points": "Total Points",
    "player_rebounds": "Total Rebounds",
    "player_assists": "Total Assists",
    "player_points_rebounds_assists": "Total PRA",
    "player_threes": "Total 3PM",
    "player_steals": "Total Steals",
    "player_blocks": "Total Blocks",
}

# All markets we want, comma-separated for a single API call per event
MARKETS = ",".join(MARKET_MAP.keys())

# Timezone for determining game dates (NBA schedule uses Eastern)
EASTERN = ZoneInfo("America/New_York")


def get_nba_events() -> list[dict]:
    """
    Fetch all upcoming NBA events (games). This endpoint is free — no credit cost.

    Returns a list of dicts with keys: id, home_team, away_team, commence_time.
    """
    url = f"{ODDS_API_BASE}/sports/basketball_nba/events"
    resp = requests.get(url, params={"apiKey": get_odds_api_key()})
    resp.raise_for_status()
    return resp.json()


def get_events_for_date(date: datetime.date = None) -> list[dict]:
    """
    Return only the events whose game time falls on the given date (Eastern time).
    Defaults to today.
    """
    if date is None:
        date = datetime.date.today()
    all_events = get_nba_events()
    filtered = []
    for event in all_events:
        utc_time = datetime.datetime.fromisoformat(event["commence_time"].replace("Z", "+00:00"))
        eastern_date = utc_time.astimezone(EASTERN).date()
        if eastern_date == date:
            filtered.append(event)
    return filtered


def get_event_props(event_id: str, all_books: bool = False) -> list[dict]:
    """
    Fetch player prop lines for a single event.

    By default returns only the PREFERRED_BOOKMAKER's lines (one entry per
    player/stat). If `all_books=True`, returns one entry per book per
    player/stat with a `book` field for line shopping.

    Returns a list of dicts: {type, player, spread, book?}.
    """
    url = f"{ODDS_API_BASE}/sports/basketball_nba/events/{event_id}/odds"
    resp = requests.get(url, params={
        "apiKey": get_odds_api_key(),
        "regions": "us",
        "markets": MARKETS,
        "oddsFormat": "american",
    })
    resp.raise_for_status()
    data = resp.json()

    bookmakers = data.get("bookmakers", [])
    if not bookmakers:
        return []

    if all_books:
        # Return outcomes from every bookmaker
        props = []
        for book in bookmakers:
            book_key = book.get("key", "")
            for market in book.get("markets", []):
                prop_type = MARKET_MAP.get(market["key"])
                if prop_type is None:
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome["name"] == "Over":
                        props.append({
                            "type": prop_type,
                            "player": outcome["description"],
                            "spread": outcome["point"],
                            "price": outcome.get("price"),
                            "book": book_key,
                        })
        return props

    # Single-book mode: prefer DraftKings, fall back to whatever is available
    book = None
    for b in bookmakers:
        if b["key"] == PREFERRED_BOOKMAKER:
            book = b
            break
    if book is None:
        book = bookmakers[0]

    props = []
    for market in book.get("markets", []):
        prop_type = MARKET_MAP.get(market["key"])
        if prop_type is None:
            continue
        for outcome in market.get("outcomes", []):
            if outcome["name"] == "Over":
                props.append({
                    "type": prop_type,
                    "player": outcome["description"],
                    "spread": outcome["point"],
                })
    return props


def _team_code(full_name: str) -> str:
    """Convert a full team name to its 3-letter code."""
    return TEAM_NAME_TO_CODE.get(full_name, full_name)


def get_todays_teams(date: datetime.date = None) -> list[str]:
    """Return a list of team codes playing on the given date."""
    events = get_events_for_date(date)
    teams = []
    for event in events:
        teams.append(_team_code(event["away_team"]))
        teams.append(_team_code(event["home_team"]))
    return teams


def get_todays_games(date: datetime.date = None) -> dict[str, str]:
    """Return a dict mapping each team code to its opponent for the given date."""
    events = get_events_for_date(date)
    games = {}
    for event in events:
        away = _team_code(event["away_team"])
        home = _team_code(event["home_team"])
        games[away] = home
        games[home] = away
    return games


def get_all_props(date: datetime.date = None, all_books: bool = False) -> pd.DataFrame:
    """
    Fetch player prop lines for all games on the given date.

    By default returns one row per player/stat (preferred book only).
    With `all_books=True` returns one row per book per player/stat
    (use for line shopping).

    Returns a DataFrame with columns: type, player, spread (and book, price
    when `all_books=True`).
    """
    events = get_events_for_date(date)
    print(f"  Found {len(events)} games on {date or datetime.date.today()}")

    all_props = []
    for event in events:
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        print(f"  Fetching props: {away} @ {home}...")
        event_props = get_event_props(event["id"], all_books=all_books)
        all_props.extend(event_props)

    print(f"  {len(all_props)} total prop lines fetched")
    if all_props:
        return pd.DataFrame(all_props)
    return pd.DataFrame(columns=["type", "player", "spread"])
