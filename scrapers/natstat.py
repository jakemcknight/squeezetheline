import re
import datetime

import pandas as pd
import requests

from config import API_BASE, SEASON_TAG, format_date, flatten_json


def get_todays_teams(date: datetime.date = None) -> list[str]:
    """Return a list of team codes playing on the given date (defaults to today)."""
    if date is None:
        date = datetime.date.today()
    url = f"{API_BASE}/games/NBA/{format_date(date)}"
    games = requests.get(url).json()
    teams = []
    for value in games["games"].values():
        teams.append(value["visitor-code"])
        teams.append(value["home-code"])
    return teams


def get_todays_games(date: datetime.date = None) -> dict[str, str]:
    """Return a dict mapping each team code to its opponent for the given date."""
    if date is None:
        date = datetime.date.today()
    url = f"{API_BASE}/games/NBA/{format_date(date)}"
    games = requests.get(url).json()
    teams = {}
    for value in games["games"].values():
        teams[value["visitor-code"]] = value["home-code"]
        teams[value["home-code"]] = value["visitor-code"]
    return teams


def get_teams() -> pd.DataFrame:
    """Fetch all NBA teams for the current season."""
    url = f"{API_BASE}/teams/NBA/{SEASON_TAG.replace('season_', '')}"
    teams = requests.get(url).json()
    output = []
    for key, value in teams["teams"].items():
        output.append({"name": value["name"], "team-code": value["code"], "key": key})
    return pd.DataFrame(output)


def get_players(team_code: str) -> pd.DataFrame:
    """Fetch the roster for a single team."""
    url = f"{API_BASE}/players/NBA/{team_code}"
    players = requests.get(url).json()
    output = []
    for player in players["players"].values():
        output.append(player)
    return pd.DataFrame(output)


def get_all_players(teams: pd.DataFrame) -> pd.DataFrame:
    """Fetch rosters for all teams and combine into one DataFrame."""
    players = pd.DataFrame()
    for code in teams["team-code"]:
        players = pd.concat([players, get_players(code)])
    return players


def get_player_stats(code: str) -> pd.DataFrame:
    """Fetch current-season game logs for a single player by player code."""
    try:
        url = f"{API_BASE}/players/NBA/{code}"
        stats = requests.get(url).json()
        player_key = list(stats["players"].keys())[0]
        season_data = stats["players"][player_key]["seasons"][SEASON_TAG]
        s_tag = list(season_data.keys())[1]
        output = []
        for game in season_data[s_tag]["playerperfs"].values():
            statline = game.get("statline", "")
            match_minutes = re.search(r"(\d+)m", statline)
            game["minutes"] = match_minutes.group(1) if match_minutes else 0
            match_points = re.search(r"(\d+)p", statline)
            game["points"] = match_points.group(1) if match_points else 0
            match_reb = re.search(r"(\d+)r", statline)
            game["rebounds"] = match_reb.group(1) if match_reb else 0
            match_assists = re.search(r"(\d+)a", statline)
            game["assists"] = match_assists.group(1) if match_assists else 0
            game["code"] = code
            output.append(game)
        return pd.DataFrame(output)
    except Exception:
        print(f"Request failed for player code: {code}")
        return pd.DataFrame()


def get_all_player_stats(players: pd.DataFrame) -> pd.DataFrame:
    """Fetch stats for all players and combine into one DataFrame."""
    stats = pd.DataFrame()
    for code in players["code"].unique():
        stats = pd.concat([stats, get_player_stats(code)])
    stats["minutes"] = stats["minutes"].fillna(0)
    stats["points"] = stats["points"].fillna(0)
    stats["rebounds"] = stats["rebounds"].fillna(0)
    stats["assists"] = stats["assists"].fillna(0)
    return stats


def get_player_performances(date_string: str) -> list[dict]:
    """Fetch all player performances for a given date, handling pagination."""
    url = f"{API_BASE}/playerperfs/nba/{date_string}"
    lines = requests.get(url).json()
    output_data = []
    if "performances" not in lines:
        return output_data
    for perf in lines["performances"].values():
        output_data.append(flatten_json(perf))
    while lines["meta"]["page"] != lines["meta"]["pages-total"]:
        if "page-next" not in lines["meta"]:
            break
        lines = requests.get(lines["meta"]["page-next"]).json()
        for perf in lines["performances"].values():
            output_data.append(flatten_json(perf))
    return output_data
