import datetime

import pandas as pd

from config import TEAM_CODE_MAP
from data import load_historical_data


def compute_rest_days(df: pd.DataFrame, game_date: datetime.date) -> pd.DataFrame:
    """For each player, compute days of rest between their last game and `game_date`.

    Returns a DataFrame with columns: name, rest_days, last_game, b2b (bool).
    `b2b` (back-to-back) is True when the player played the calendar day before
    `game_date`.
    """
    if df.empty:
        return pd.DataFrame(columns=["name", "rest_days", "last_game", "b2b"])

    latest = df.sort_values("gameday", ascending=False).drop_duplicates("name")[["name", "gameday"]]
    latest = latest.rename(columns={"gameday": "last_game"})
    target = pd.Timestamp(game_date)
    latest["rest_days"] = (target - latest["last_game"]).dt.days
    latest["b2b"] = latest["rest_days"] == 1
    latest["last_game"] = latest["last_game"].dt.strftime("%Y-%m-%d")
    return latest


def analyze_stat(
    stat: str,
    prop_type: str,
    df: pd.DataFrame,
    props: pd.DataFrame,
    todays_games: dict[str, str],
    defense: pd.DataFrame,
    game_date: datetime.date | None = None,
) -> pd.DataFrame:
    """
    Core analysis for a single stat type (points, rebounds, assists).

    Compares a player's recent and historical performance against their current
    prop line, factoring in opponent defense-vs-position rankings.

    Returns a DataFrame with one row per player who has a prop line set, including:
      - Season average, last-5 average, last-10 average
      - Delta vs. the prop line for each window
      - Hit % (current season) and historical hit %
      - Opponent defense rank at the player's position
      - Standard deviation, stat-per-minute rate
    """
    stat_props = props[props["type"] == prop_type].copy()

    # --- Current-season hit rate ---
    played = df[df["minutes"] != 0].copy()
    spread_merge = played.merge(stat_props[["name", "spread"]], how="left")
    spread_merge = spread_merge[~spread_merge["spread"].isna()]
    hit_series = spread_merge.groupby("name").apply(
        lambda g: (g[stat] > g["spread"]).mean() * 100,
        include_groups=False,
    )
    hit_pct = hit_series.reset_index(name="hit%")
    hit_pct["hit%"] = hit_pct["hit%"].round(1)

    # --- Averages: full season, last 5, last 10 ---
    # Group by name only so traded players don't get split into multiple rows.
    # Their current team and position come from their most recent game.
    df["pra"] = df["points"] + df["rebounds"] + df["assists"]
    current_team = (
        df.sort_values("gameday", ascending=False)
        .drop_duplicates("name")[["name", "team-code", "position"]]
    )
    season_avg = df.groupby("name")[stat].mean().reset_index().merge(current_team, on="name")
    last5 = df[df["rank"] <= 5].groupby("name")[stat].mean().reset_index().merge(current_team, on="name")
    last5 = last5.rename(columns={stat: f"{stat}_5g"})
    last10 = df[df["rank"] <= 10].groupby("name")[stat].mean().reset_index().merge(current_team, on="name")
    last10 = last10.rename(columns={stat: f"{stat}_10g"})

    # --- Season min ---
    stat_min = played.groupby("name")[[stat]].min().reset_index().rename(columns={stat: f"{stat}_min"})

    # --- Merge everything onto stat_props ---
    stat_props = (
        stat_props
        .merge(season_avg, how="left")
        .merge(last5, how="left")
        .merge(last10, how="left")
        .merge(hit_pct, how="left")
        .merge(stat_min, how="left")
    )

    # --- Deltas ---
    stat_props["delta"] = stat_props[stat] - stat_props["spread"]
    stat_props["delta_5g"] = stat_props[f"{stat}_5g"] - stat_props["spread"]
    stat_props["delta_10g"] = stat_props[f"{stat}_10g"] - stat_props["spread"]
    stat_props = stat_props[~stat_props["delta"].isna()]

    # --- Opponent matchup ---
    stat_props["opponent"] = stat_props["team-code"].apply(
        lambda x: todays_games.get(x, "")
    )

    # --- Defense-vs-position rank ---
    defense = defense.copy()
    defense["team"] = defense["team"].apply(lambda x: TEAM_CODE_MAP.get(x, x))
    defense_stat = defense[defense["stat"] == stat][["position", "team", "rank"]]
    defense_stat = defense_stat.rename(columns={"team": "opponent"})
    stat_props = stat_props.merge(defense_stat, how="left")

    # --- Volatility metrics ---
    std_dev = df.groupby("name")[stat].std().reset_index().rename(columns={stat: "std_dev"})
    stat_props = stat_props.merge(std_dev, how="left")
    stat_props["std%"] = stat_props["std_dev"] / stat_props[stat]

    df["spm"] = df[stat] / df["minutes"]
    spm = df.groupby("name")["spm"].mean().reset_index()
    stat_props = stat_props.merge(spm, how="left")

    # --- Historical hit rate ---
    history = load_historical_data()
    if not history.empty:
        history = history.rename(columns={
            "player": "name", "team_code": "team", "opponent_code": "opponent",
            "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
            "threefm": "threes", "stl": "steals", "blk": "blocks",
        })
        # Derive PRA if not already present
        for col in ("points", "rebounds", "assists"):
            history[col] = pd.to_numeric(history[col], errors="coerce").fillna(0)
        history["pra"] = history["points"] + history["rebounds"] + history["assists"]
        for col in ("threes", "steals", "blocks"):
            if col in history.columns:
                history[col] = pd.to_numeric(history[col], errors="coerce").fillna(0)
        hist_spread = history[history["minutes"] != 0].merge(
            stat_props[["name", "spread"]], how="left"
        )
        hist_spread = hist_spread[~hist_spread["spread"].isna()]
        hist_series = hist_spread.groupby("name").apply(
            lambda g: (g[stat] > g["spread"]).mean() * 100,
            include_groups=False,
        )
        hist_hit = hist_series.reset_index(name="history_hit%")
        hist_hit["history_hit%"] = hist_hit["history_hit%"].round(1)
        stat_props = stat_props.merge(hist_hit, how="left")
    else:
        stat_props["history_hit%"] = None

    # --- Rest days / back-to-back ---
    if game_date is not None:
        rest = compute_rest_days(df, game_date)
        stat_props = stat_props.merge(rest, on="name", how="left")

    # --- Reorder columns so history_hit% is right after hit% ---
    columns = list(stat_props.columns)
    if "hit%" in columns and "history_hit%" in columns:
        after_index = columns.index("hit%")
        columns.remove("history_hit%")
        columns.insert(after_index + 1, "history_hit%")
        stat_props = stat_props[columns]

    return stat_props


def filter_strong_overs(result: pd.DataFrame) -> pd.DataFrame:
    """Players where all deltas are positive and both hit rates exceed 50%."""
    return result[
        (result["delta"] > 0)
        & (result["delta_5g"] > 0)
        & (result["delta_10g"] > 0)
        & (result["hit%"] > 50)
        & (result["history_hit%"] > 50)
    ]


def filter_strong_unders(result: pd.DataFrame) -> pd.DataFrame:
    """Players where all deltas are negative and both hit rates are below 50%."""
    return result[
        (result["delta"] < 0)
        & (result["delta_5g"] < 0)
        & (result["delta_10g"] < 0)
        & (result["hit%"] < 50)
        & (result["history_hit%"] < 50)
    ]


def filter_trending_overs(result: pd.DataFrame) -> pd.DataFrame:
    """Players where all deltas are positive (ignoring hit rate thresholds)."""
    return result[
        (result["delta"] > 0)
        & (result["delta_5g"] > 0)
        & (result["delta_10g"] > 0)
    ]


def filter_trending_unders(result: pd.DataFrame) -> pd.DataFrame:
    """Players where all deltas are negative (ignoring hit rate thresholds)."""
    return result[
        (result["delta"] < 0)
        & (result["delta_5g"] < 0)
        & (result["delta_10g"] < 0)
    ]


def build_player_summaries(
    player_names: list[str],
    current_stats: pd.DataFrame,
    props: pd.DataFrame,
    todays_games: dict[str, str] | None = None,
) -> dict:
    """
    Build a summary for each player who has a prop line today.

    Returns a dict keyed by player name. Each value contains:
      - team, position
      - today_lines: {points, rebounds, assists}
      - season_avg: {points, rebounds, assists, minutes, games}
      - career_avg: {points, rebounds, assists, minutes, games}
      - last_20: list of game dicts (most recent first)
    """
    history = load_historical_data()
    history_renamed = pd.DataFrame()
    if not history.empty:
        history_renamed = history.rename(columns={
            "player": "name", "team_code": "team", "opponent_code": "opponent",
            "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
            "threefm": "threes", "stl": "steals", "blk": "blocks",
        })
        # Coerce types
        for col in ["points", "rebounds", "assists", "minutes"]:
            history_renamed[col] = pd.to_numeric(history_renamed[col], errors="coerce").fillna(0)
        for col in ("threes", "steals", "blocks"):
            if col in history_renamed.columns:
                history_renamed[col] = pd.to_numeric(history_renamed[col], errors="coerce").fillna(0)
        history_renamed["pra"] = history_renamed["points"] + history_renamed["rebounds"] + history_renamed["assists"]
        history_renamed["gameday"] = pd.to_datetime(history_renamed["game_gameday"], errors="coerce")

    # Map prop type names to the stat column keys used elsewhere
    prop_to_stat = {
        "Total Points": "points",
        "Total Rebounds": "rebounds",
        "Total Assists": "assists",
        "Total PRA": "pra",
        "Total 3PM": "threes",
        "Total Steals": "steals",
        "Total Blocks": "blocks",
    }

    summaries = {}
    for name in player_names:
        # Today's lines (keyed by stat name)
        player_props = props[props["name"] == name]
        today_lines = {}
        for _, row in player_props.iterrows():
            stat_key = prop_to_stat.get(row["type"])
            if stat_key:
                today_lines[stat_key] = row["spread"]

        # Season averages from current_stats (filter to games with minutes)
        season_games = current_stats[
            (current_stats["name"] == name) & (current_stats["minutes"] != 0)
        ]

        def _avg(df: pd.DataFrame, col: str) -> float:
            return float(df[col].mean()) if col in df.columns and len(df) else 0.0

        season_avg = {
            "games": int(len(season_games)),
            "minutes": _avg(season_games, "minutes"),
            "points": _avg(season_games, "points"),
            "rebounds": _avg(season_games, "rebounds"),
            "assists": _avg(season_games, "assists"),
            "pra": _avg(season_games, "pra"),
            "threes": _avg(season_games, "threes"),
            "steals": _avg(season_games, "steals"),
            "blocks": _avg(season_games, "blocks"),
        }

        # Use the most recent game for current team / position (handles traded players)
        if len(season_games):
            most_recent = season_games.sort_values("gameday", ascending=False).iloc[0]
            team = most_recent["team-code"]
            position = most_recent["position"] if "position" in season_games.columns else ""
        else:
            team = ""
            position = ""

        # Career averages from history (filter to games with minutes)
        career_avg = {
            "games": 0, "minutes": 0.0,
            "points": 0.0, "rebounds": 0.0, "assists": 0.0,
            "pra": 0.0, "threes": 0.0, "steals": 0.0, "blocks": 0.0,
        }
        last_20 = []
        vs_opponent = []
        if not history_renamed.empty:
            career_games = history_renamed[
                (history_renamed["name"] == name) & (history_renamed["minutes"] != 0)
            ]
            if len(career_games):
                career_avg = {
                    "games": int(len(career_games)),
                    "minutes": _avg(career_games, "minutes"),
                    "points": _avg(career_games, "points"),
                    "rebounds": _avg(career_games, "rebounds"),
                    "assists": _avg(career_games, "assists"),
                    "pra": _avg(career_games, "pra"),
                    "threes": _avg(career_games, "threes"),
                    "steals": _avg(career_games, "steals"),
                    "blocks": _avg(career_games, "blocks"),
                }
                # Last 20 games (sorted most recent first)
                last_20_df = career_games.sort_values("gameday", ascending=False).head(20)
                for _, g in last_20_df.iterrows():
                    last_20.append({
                        "date": str(g["gameday"].date()) if pd.notna(g["gameday"]) else "",
                        "opponent": g.get("opponent", ""),
                        "min": float(g["minutes"]),
                        "pts": float(g["points"]),
                        "reb": float(g["rebounds"]),
                        "ast": float(g["assists"]),
                        "pra": float(g.get("pra", 0)),
                        "threes": float(g.get("threes", 0)),
                        "steals": float(g.get("steals", 0)),
                        "blocks": float(g.get("blocks", 0)),
                    })

        # --- Historical performance vs tonight's opponent ---
        vs_opponent = []
        vs_opponent_avg = None
        if todays_games and team and not history_renamed.empty:
            opp = todays_games.get(team, "")
            if opp:
                vs_games = history_renamed[
                    (history_renamed["name"] == name)
                    & (history_renamed["opponent"] == opp)
                    & (history_renamed["minutes"] != 0)
                ].sort_values("gameday", ascending=False)
                for _, g in vs_games.head(10).iterrows():
                    vs_opponent.append({
                        "date": str(g["gameday"].date()) if pd.notna(g["gameday"]) else "",
                        "opponent": g.get("opponent", ""),
                        "min": float(g["minutes"]),
                        "pts": float(g["points"]),
                        "reb": float(g["rebounds"]),
                        "ast": float(g["assists"]),
                        "pra": float(g.get("pra", 0)),
                        "threes": float(g.get("threes", 0)),
                        "steals": float(g.get("steals", 0)),
                        "blocks": float(g.get("blocks", 0)),
                    })
                if len(vs_games):
                    vs_opponent_avg = {
                        "games": int(len(vs_games)),
                        "opponent": opp,
                        "minutes": _avg(vs_games, "minutes"),
                        "points": _avg(vs_games, "points"),
                        "rebounds": _avg(vs_games, "rebounds"),
                        "assists": _avg(vs_games, "assists"),
                        "pra": _avg(vs_games, "pra"),
                        "threes": _avg(vs_games, "threes"),
                        "steals": _avg(vs_games, "steals"),
                        "blocks": _avg(vs_games, "blocks"),
                    }

        summaries[name] = {
            "team": team,
            "position": position,
            "today_lines": today_lines,
            "season_avg": season_avg,
            "career_avg": career_avg,
            "last_20": last_20,
            "vs_opponent": vs_opponent,
            "vs_opponent_avg": vs_opponent_avg,
        }

    return summaries
