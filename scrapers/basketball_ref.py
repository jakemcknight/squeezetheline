import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import SEASON_YEAR


def get_player_positions() -> pd.DataFrame:
    """Scrape player position data from Basketball Reference play-by-play page."""
    url = f"https://www.basketball-reference.com/leagues/NBA_{SEASON_YEAR}_play-by-play.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    table = soup.find_all("table", id="pbp_stats")
    rows = table[0].find_all("tr")

    positions = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) > 0:
            positions.append({"name": cells[0].text, "position": cells[3].text})
    return pd.DataFrame(positions)


DEFENSE_COLUMNS = ["position", "team", "team_rank", "stat", "value", "rank"]


def get_defense_by_position(sport_key: str = "basketball_nba") -> pd.DataFrame:
    """Scrape defense-vs-position rankings from HashtagBasketball (NBA only).

    HashtagBasketball only publishes an NBA defense-vs-position page
    (https://hashtagbasketball.com/wnba-defense-vs-position 404s, and there's
    no college equivalent). For non-NBA sports we return an empty frame with
    the expected columns; analysis.analyze_stat treats a missing defense rank
    as neutral (the per-row score guards on pd.notna(rank)), so projections
    still work — they just don't get the matchup bonus.

    TODO: find a defense-vs-position source for WNBA/NCAA (would likely mean
    computing it ourselves from opponent box scores once a stats feed exists).
    """
    if sport_key != "basketball_nba":
        return pd.DataFrame(columns=DEFENSE_COLUMNS)
    response = requests.get("https://hashtagbasketball.com/nba-defense-vs-position")
    soup = BeautifulSoup(response.text, "lxml")
    table = soup.find_all(
        "table", class_="table table-sm table-bordered table-striped table--statistics"
    )
    rows = table[2].find_all("tr")

    headers = [
        "", "", "points", "fg%", "ft%", "3pm",
        "rebounds", "assists", "steals", "blocks", "turnovers",
    ]
    defense_data = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) > 0:
            for i in range(2, 10):
                try:
                    defense_data.append({
                        "position": cells[0].text,
                        "team": cells[1].text.split()[0],
                        "team_rank": cells[1].text.split()[1],
                        "stat": headers[i],
                        "value": cells[i].text.split()[0],
                        "rank": cells[i].text.split()[1],
                    })
                except Exception:
                    pass
    df = pd.DataFrame(defense_data)
    df["value"] = df["value"].astype(float)
    df["rank"] = df["rank"].astype(int)
    df["team_rank"] = df["team_rank"].astype(int)
    return df
