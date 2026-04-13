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


def get_defense_by_position() -> pd.DataFrame:
    """Scrape defense-vs-position rankings from HashtagBasketball."""
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
