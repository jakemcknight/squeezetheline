"""
Fetch NBA injury report from ESPN's public API.

Free, no auth required. Returns a DataFrame with player name, status,
and short comment for every currently listed injury.
"""

import pandas as pd
import requests
from unidecode import unidecode

ESPN_INJURY_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"

# Normalize status strings so the main table and filters are consistent
STATUS_SHORT = {
    "Day-To-Day": "DTD",
    "Out": "OUT",
    "Doubtful": "DBT",
    "Questionable": "Q",
    "Probable": "PROB",
    "Active": "ACT",
    "Suspended": "SUS",
}


def get_injury_report() -> pd.DataFrame:
    """
    Returns a DataFrame with columns: name, team, status, status_short, comment, date.
    """
    try:
        resp = requests.get(ESPN_INJURY_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch ESPN injuries: {e}")
        return pd.DataFrame(columns=["name", "team", "status", "status_short", "comment", "date"])

    rows = []
    for team_block in data.get("injuries", []):
        team_name = team_block.get("displayName", "")
        for inj in team_block.get("injuries", []):
            athlete = inj.get("athlete") or {}
            name = athlete.get("displayName", "").strip()
            if not name:
                continue
            status = inj.get("status", "")
            rows.append({
                "name": unidecode(name),
                "team": team_name,
                "status": status,
                "status_short": STATUS_SHORT.get(status, status),
                "comment": inj.get("shortComment", ""),
                "date": inj.get("date", ""),
            })
    return pd.DataFrame(rows)
