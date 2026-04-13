"""
Daily NBA prop analysis runner.

Fetches today's prop lines and current-season stats, then prints
the best over/under picks based on historical and recent performance.

Usage:
    python run_daily.py
"""

import datetime

import pandas as pd

from scrapers.odds_api import get_todays_teams, get_todays_games, get_all_props
from scrapers.nba import get_current_season_stats, get_player_positions
from scrapers.basketball_ref import get_defense_by_position
from data import (
    prepare_stats,
    prepare_props,
    save_stats,
    load_historical_props,
    save_historical_props,
)
from analysis import (
    analyze_stat,
    filter_strong_overs,
    filter_strong_unders,
    filter_trending_overs,
    filter_trending_unders,
)

pd.options.display.max_columns = 0
pd.options.display.max_rows = 100
pd.options.display.max_colwidth = 200


def main():
    today = datetime.date.today()
    print(f"=== NBA Prop Analysis for {today} ===\n")

    # --- Step 1: Get today's schedule ---
    print("Fetching today's games...")
    todays_teams = get_todays_teams()
    todays_games = get_todays_games()
    print(f"  {len(todays_teams)} teams playing today\n")

    # --- Step 2: Get current season stats + positions ---
    print("Fetching current season stats from NBA.com...")
    stats = get_current_season_stats()
    print(f"  {stats.name.nunique()} players, {len(stats)} game logs\n")

    print("Fetching player positions...")
    positions = get_player_positions()
    df = prepare_stats(stats, positions)
    save_stats(df)
    print(f"  {df.name.nunique()} players matched with positions\n")

    # --- Step 3: Fetch today's prop lines ---
    print("Fetching prop lines from The Odds API...")
    props = get_all_props()

    # Save to historical props
    props["date"] = str(today)
    old_props = load_historical_props()
    save_historical_props(pd.concat([props, old_props]))

    props = prepare_props(props)
    print(f"  {len(props)} prop lines\n")

    # --- Step 4: Get defense-vs-position data ---
    print("Fetching defense-vs-position rankings...")
    defense = get_defense_by_position()

    # --- Step 5: Run analysis ---
    stat_configs = [
        ("points", "Total Points"),
        ("rebounds", "Total Rebounds"),
        ("assists", "Total Assists"),
    ]

    for stat, prop_type in stat_configs:
        print(f"\n{'='*60}")
        print(f"  {stat.upper()}")
        print(f"{'='*60}")

        result = analyze_stat(stat, prop_type, df, props, todays_games, defense)

        strong_overs = filter_strong_overs(result)
        trending_overs = filter_trending_overs(result)
        strong_unders = filter_strong_unders(result)
        trending_unders = filter_trending_unders(result)

        if not strong_overs.empty:
            print(f"\n--- Strong Overs ({stat}) ---")
            print(strong_overs.to_string(index=False))

        if not trending_overs.empty:
            print(f"\n--- Trending Overs ({stat}) ---")
            print(trending_overs.to_string(index=False))

        if not strong_unders.empty:
            print(f"\n--- Strong Unders ({stat}) ---")
            print(strong_unders.to_string(index=False))

        if not trending_unders.empty:
            print(f"\n--- Trending Unders ({stat}) ---")
            print(trending_unders.to_string(index=False))


if __name__ == "__main__":
    main()
