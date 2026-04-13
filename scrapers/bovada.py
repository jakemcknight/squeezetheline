import re
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver


def get_game_links() -> list[str]:
    """Scrape Bovada's NBA page for today's game links using Selenium."""
    options = webdriver.ChromeOptions()
    options.add_argument("javascript.enabled")
    driver = webdriver.Chrome(options=options)
    driver.get("https://www.bovada.lv/sports/basketball/nba")
    time.sleep(5)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "lxml")
    links = []
    for tag in soup.findAll("a", attrs={"class": "game-view-cta"}):
        links.append("https://www.bovada.lv" + tag.get("href"))
    return links


def get_player_props(link: str) -> pd.DataFrame:
    """Scrape player prop lines from a single Bovada game page."""
    options = webdriver.ChromeOptions()
    options.add_argument("javascript.enabled")
    driver = webdriver.Chrome(options=options)
    driver.get(link)
    time.sleep(10)
    html = driver.page_source
    driver.quit()

    soup = BeautifulSoup(html, "lxml")
    props = []
    for tag in soup.findAll("sp-single-market"):
        header = tag.find("h3")
        pattern = r"(.+)\s+-\s+(.+)\s+\((\S{3})\)"
        matches = re.findall(pattern, header.text)
        if matches:
            spread = tag.find("ul", class_="spread-header")
            if spread:
                props.append({
                    "type": matches[0][0].strip(),
                    "player": matches[0][1],
                    "team": matches[0][2],
                    "spread": spread.text,
                })
    return pd.DataFrame(props)


def get_all_props(game_links: list[str]) -> pd.DataFrame:
    """Scrape props for all of today's games and combine."""
    props = pd.DataFrame()
    for link in game_links:
        props = pd.concat([props, get_player_props(link)])
    return props
