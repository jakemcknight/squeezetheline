"""
Backfill historical player performance data using nba_api.

Pulls box scores from NBA.com via the nba_api package — completely free,
no API key needed. Uses LeagueGameLog to fetch an entire season of player
stats in a single request, so a full 10+ year backfill only needs ~12 calls.

Usage:
    python backfill.py
    python backfill.py --seasons 2024-25 2023-24   # specific seasons only
"""

import sys
import time

import pandas as pd
from nba_api.stats.endpoints import LeagueGameLog

from data import load_historical_data, save_historical_data

# nba_api team codes that differ from NatStat codes in existing data
TEAM_CODE_MAP = {
    "BKN": "BRK",
    "CHA": "CHH",
}

# Delay between API calls — NBA.com can be aggressive about rate limiting.
# 5 seconds is conservative and safe for the small number of calls we make.
REQUEST_DELAY = 5


def map_team_code(code: str) -> str:
    """Map nba_api team abbreviations to match existing historical data codes."""
    return TEAM_CODE_MAP.get(code, code)


def extract_opponent(matchup: str, team_abbr: str) -> str:
    """Extract opponent team code from MATCHUP string like 'LAL vs. BOS'."""
    matchup = matchup.replace("vs.", "|").replace("@", "|")
    for part in matchup.split("|"):
        code = part.strip()
        if code and code != team_abbr:
            return code
    return ""


def pull_season(season: str) -> pd.DataFrame:
    """
    Pull all player game logs for a single NBA season.

    Returns a DataFrame with columns matching the existing historical_data.csv
    format so old NatStat data and new nba_api data can coexist.
    """
    log = LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        player_or_team_abbreviation="P",
    )
    raw = log.get_data_frames()[0]

    df = pd.DataFrame()
    df["player"] = raw["PLAYER_NAME"]
    df["team_code"] = raw["TEAM_ABBREVIATION"].apply(map_team_code)
    df["team_name"] = raw["TEAM_NAME"]
    df["opponent_code"] = raw.apply(
        lambda r: map_team_code(extract_opponent(r["MATCHUP"], r["TEAM_ABBREVIATION"])),
        axis=1,
    )
    df["game_gameday"] = raw["GAME_DATE"]
    df["game_loc"] = raw["MATCHUP"].apply(lambda x: "home" if "vs." in x else "away")
    df["min"] = raw["MIN"]
    df["pts"] = raw["PTS"]
    df["reb"] = raw["REB"]
    df["ast"] = raw["AST"]
    df["fgm"] = raw["FGM"]
    df["fga"] = raw["FGA"]
    df["threefm"] = raw["FG3M"]
    df["threefa"] = raw["FG3A"]
    df["ftm"] = raw["FTM"]
    df["fta"] = raw["FTA"]
    df["fgpct"] = raw["FG_PCT"]
    df["ftpct"] = raw["FT_PCT"]
    df["stl"] = raw["STL"]
    df["blk"] = raw["BLK"]
    df["oreb"] = raw["OREB"]
    df["dreb"] = raw["DREB"]
    df["to"] = raw["TOV"]
    df["pf"] = raw["PF"]
    df["date_string"] = raw["GAME_DATE"]
    return df


def build_season_list() -> list[str]:
    """Generate season strings from 2014-15 through 2025-26."""
    return [f"{y}-{str(y + 1)[-2:]}" for y in range(2014, 2026)]


def backfill(seasons: list[str] = None):
    if seasons is None:
        seasons = build_season_list()

    historical = load_historical_data()
    existing_dates = set()
    if not historical.empty and "date_string" in historical.columns:
        existing_dates = set(historical["date_string"].unique())

    total_new = 0

    for i, season in enumerate(seasons):
        print(f"[{i + 1}/{len(seasons)}] Season {season}...")

        try:
            df = pull_season(season)
        except Exception as e:
            print(f"  -> Error pulling {season}: {e}")
            time.sleep(REQUEST_DELAY)
            continue

        new_dates = set(df["date_string"].unique()) - existing_dates
        if not new_dates:
            print(f"  -> Already have all {len(df['date_string'].unique())} game dates")
        else:
            new_rows = df[df["date_string"].isin(new_dates)]
            historical = pd.concat([historical, new_rows], ignore_index=True)
            save_historical_data(historical)
            existing_dates.update(new_dates)
            total_new += len(new_rows)
            print(f"  -> Added {len(new_rows)} rows ({len(new_dates)} new game dates)")

        # Don't sleep after the last request
        if i < len(seasons) - 1:
            print(f"  Sleeping {REQUEST_DELAY}s...")
            time.sleep(REQUEST_DELAY)

    print(f"\nBackfill complete! {total_new} new rows added.")
    print(f"Total rows in historical data: {len(historical)}")


if __name__ == "__main__":
    # Allow passing specific seasons: python backfill.py --seasons 2024-25 2023-24
    if "--seasons" in sys.argv:
        idx = sys.argv.index("--seasons")
        seasons = sys.argv[idx + 1:]
        backfill(seasons)
    else:
        backfill()
