import datetime

import pandas as pd

from config import TEAM_CODE_MAP
from data import load_historical_data


def compute_starters(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Flag each player as a likely starter.

    Heuristic: average minutes in the last `window` games, and if they're in
    the top 5 on their current team by that average, they're marked a starter.

    Returns: DataFrame with columns [name, starter (bool), mpg_recent].
    """
    if df.empty:
        return pd.DataFrame(columns=["name", "starter", "mpg_recent"])

    recent = df[df["rank"] <= window] if "rank" in df.columns else df

    # Current team per player (already handled by "most recent game" elsewhere,
    # but recompute here in case caller passes raw df).
    current_team = (
        df.sort_values("gameday", ascending=False)
        .drop_duplicates("name")[["name", "team-code"]]
    )

    mpg = recent.groupby("name")["minutes"].mean().reset_index(name="mpg_recent")
    mpg = mpg.merge(current_team, on="name", how="left")

    # Rank within team by recent mpg, top 5 = starters
    mpg["team_rank"] = mpg.groupby("team-code")["mpg_recent"].rank(
        method="min", ascending=False
    )
    mpg["starter"] = mpg["team_rank"] <= 5
    return mpg[["name", "starter", "mpg_recent"]]


def compute_team_last_games(df: pd.DataFrame) -> pd.DataFrame:
    """Return each team's most recent game date based on the current-season player logs."""
    if df.empty or "team-code" not in df.columns:
        return pd.DataFrame(columns=["team-code", "team_last_game"])
    return (
        df.sort_values("gameday", ascending=False)
        .drop_duplicates("team-code")[["team-code", "gameday"]]
        .rename(columns={"gameday": "team_last_game"})
    )


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
    hit_pct = (
        spread_merge.assign(_hit=(spread_merge[stat] > spread_merge["spread"]).astype(float))
        .groupby("name")["_hit"]
        .mean()
        .mul(100)
        .round(1)
        .reset_index(name="hit%")
    )

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
        hist_hit = (
            hist_spread.assign(_hit=(hist_spread[stat] > hist_spread["spread"]).astype(float))
            .groupby("name")["_hit"]
            .mean()
            .mul(100)
            .round(1)
            .reset_index(name="history_hit%")
        )
        stat_props = stat_props.merge(hist_hit, how="left")
    else:
        stat_props["history_hit%"] = None

    # --- Last-10 sparkline data: list of stat values most recent → oldest ---
    # We sort ascending by rank so most-recent is first; the inline bar chart
    # in the table reads it most-recent → oldest.
    last10_lookup = (
        df[df["rank"] <= 10]
        .sort_values(["name", "rank"])
        .groupby("name")[stat]
        .apply(list)
        .to_dict()
    )
    stat_props["last10"] = stat_props["name"].map(last10_lookup)

    # --- Last-10 hit/miss visual: colored squares for each game vs tonight's line ---
    def _hit_squares(row):
        values = last10_lookup.get(row["name"]) or []
        line = row.get("spread")
        if line is None or pd.isna(line):
            return ""
        # Reverse so oldest is on left, most recent on right (chronological)
        squares = []
        for v in reversed(values):
            if pd.isna(v):
                continue
            if v > line:
                squares.append("\U0001f7e9")  # green square
            elif v < line:
                squares.append("\U0001f7e5")  # red square
            else:
                squares.append("\u26ab")  # black circle (push)
        return "".join(squares)

    stat_props["last10_hits"] = stat_props.apply(_hit_squares, axis=1)

    # --- Composite confidence score (0-100) per player ---
    # Combines: avg delta strength, hit-rate edge from 50%, history hit-rate
    # edge, sample size, and (when present) defensive matchup quality.
    def _score(row) -> float:
        d = abs(row.get("delta", 0) or 0)
        d5 = abs(row.get(f"{stat}_5g", 0) - row.get("spread", 0)) if pd.notna(row.get(f"{stat}_5g")) else 0
        d10 = abs(row.get(f"{stat}_10g", 0) - row.get("spread", 0)) if pd.notna(row.get(f"{stat}_10g")) else 0
        # Avg deltas — bigger gap from line is stronger signal
        delta_pts = min(40, (d + d5 + d10) * 4)  # 40 pt cap
        # Hit rate edge from 50% (current season)
        hit = row.get("hit%") or 50
        hit_pts = min(25, abs(hit - 50) * 0.5)
        # Historical hit rate edge
        hist = row.get("history_hit%") or 50
        hist_pts = min(20, abs(hist - 50) * 0.4)
        # Defense rank bonus — higher rank = weaker defense, helps overs
        rank = row.get("rank")
        def_pts = 0
        if pd.notna(rank):
            # Rank 1 = best D (favors under), 30 = worst (favors over)
            d_signed = (row.get("delta", 0) or 0)
            if d_signed > 0:  # over leaning, want high rank
                def_pts = min(15, max(0, (rank - 15)) * 0.5)
            elif d_signed < 0:  # under leaning, want low rank
                def_pts = min(15, max(0, (15 - rank)) * 0.5)
        return round(min(100.0, delta_pts + hit_pts + hist_pts + def_pts), 1)

    stat_props["confidence"] = stat_props.apply(_score, axis=1)

    # --- Trend indicator: is the last-5 avg above or below the last-10 avg? ---
    stat_props["trend"] = stat_props.apply(
        lambda r: (
            "↑" if pd.notna(r.get(f"{stat}_5g")) and pd.notna(r.get(f"{stat}_10g"))
                   and r[f"{stat}_5g"] > r[f"{stat}_10g"]
            else "↓" if pd.notna(r.get(f"{stat}_5g")) and pd.notna(r.get(f"{stat}_10g"))
                       and r[f"{stat}_5g"] < r[f"{stat}_10g"]
            else "→"
        ),
        axis=1,
    )

    # --- Performance vs tonight's opponent (season + career) ---
    def _vs_opp_str(row, source_df, has_opponent_col):
        if not has_opponent_col or pd.isna(row.get("opponent")) or not row.get("opponent"):
            return ""
        opp = row["opponent"]
        games = source_df[
            (source_df["name"] == row["name"])
            & (source_df["opponent"] == opp)
            & (source_df["minutes"] != 0)
        ]
        if games.empty:
            return "0/0"
        hits = int((games[stat] > row["spread"]).sum())
        return f"{hits}/{len(games)}"

    # Season vs this opponent (current_stats, i.e. df)
    if "opponent" in df.columns:
        stat_props["vs_opp_season"] = stat_props.apply(
            lambda r: _vs_opp_str(r, df, True), axis=1
        )
    else:
        stat_props["vs_opp_season"] = ""

    # Career vs this opponent (all historical data)
    if not history.empty and "opponent" in history.columns:
        stat_props["vs_opp_career"] = stat_props.apply(
            lambda r: _vs_opp_str(r, history, True), axis=1
        )
    else:
        stat_props["vs_opp_career"] = ""

    # --- Rest days / back-to-back ---
    if game_date is not None:
        rest = compute_rest_days(df, game_date)
        stat_props = stat_props.merge(rest, on="name", how="left")

        # Opponent's back-to-back: did their opponent also play yesterday?
        team_last = compute_team_last_games(df)
        target = pd.Timestamp(game_date)
        team_last["team_rest_days"] = (target - team_last["team_last_game"]).dt.days
        # Map opponent_code -> opponent's rest days
        opp_rest = team_last.rename(columns={"team-code": "opponent", "team_rest_days": "opp_rest"})
        stat_props = stat_props.merge(opp_rest[["opponent", "opp_rest"]], on="opponent", how="left")
        stat_props["opp_b2b"] = stat_props["opp_rest"] == 1

    # --- Starter flag (top 5 mpg on team in last 10 games) ---
    starters = compute_starters(df, window=10)
    stat_props = stat_props.merge(starters, on="name", how="left")
    # Default missing starter value to False instead of NaN
    stat_props["starter"] = stat_props["starter"].fillna(False).astype(bool)

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

        # Normalize home/away into a single 'is_home' boolean
        # NatStat era used 'H'/'V', nba_api era uses 'home'/'away'
        if "game_loc" in history_renamed.columns:
            loc = history_renamed["game_loc"].astype(str).str.lower().str.strip()
            home_marks = {"h", "home"}
            history_renamed["is_home"] = loc.isin(home_marks)
            # Where game_loc is missing, derive from team_code vs game_home-code
            if "game_home-code" in history_renamed.columns:
                missing = history_renamed["game_loc"].isna()
                history_renamed.loc[missing, "is_home"] = (
                    history_renamed.loc[missing, "team"]
                    == history_renamed.loc[missing, "game_home-code"]
                )

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

        # --- Home/Away splits (career) ---
        home_avg = None
        away_avg = None
        if not history_renamed.empty and "is_home" in history_renamed.columns:
            player_career = history_renamed[
                (history_renamed["name"] == name) & (history_renamed["minutes"] != 0)
            ]
            home_games = player_career[player_career["is_home"] == True]  # noqa: E712
            away_games = player_career[player_career["is_home"] == False]  # noqa: E712
            if len(home_games):
                home_avg = {
                    "games": int(len(home_games)),
                    "minutes": _avg(home_games, "minutes"),
                    "points": _avg(home_games, "points"),
                    "rebounds": _avg(home_games, "rebounds"),
                    "assists": _avg(home_games, "assists"),
                    "pra": _avg(home_games, "pra"),
                    "threes": _avg(home_games, "threes"),
                    "steals": _avg(home_games, "steals"),
                    "blocks": _avg(home_games, "blocks"),
                }
            if len(away_games):
                away_avg = {
                    "games": int(len(away_games)),
                    "minutes": _avg(away_games, "minutes"),
                    "points": _avg(away_games, "points"),
                    "rebounds": _avg(away_games, "rebounds"),
                    "assists": _avg(away_games, "assists"),
                    "pra": _avg(away_games, "pra"),
                    "threes": _avg(away_games, "threes"),
                    "steals": _avg(away_games, "steals"),
                    "blocks": _avg(away_games, "blocks"),
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
            "home_avg": home_avg,
            "away_avg": away_avg,
        }

    return summaries
