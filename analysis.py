from __future__ import annotations

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
    sport_key: str = "basketball_nba",
) -> pd.DataFrame:
    """
    Core analysis for a single stat type (e.g. points, hits, passing yards).

    Compares a player's recent and historical performance against their current
    prop line, factoring in opponent defense-vs-position rankings.

    Sport-aware: works for any stat column the sport's scraper emits. The
    career/history layer (historical hit%, vs-opponent career, home/away) is
    only available for the NBA, which has the committed historical dataset;
    other sports degrade gracefully (those columns come back empty/None). The
    defense-vs-position rank is likewise NBA-only and blank elsewhere.

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
    # PRA is a derived basketball stat; only synthesize it when its inputs exist
    # and the scraper didn't already provide it (other sports have no PRA).
    if stat == "pra" and "pra" not in df.columns and {"points", "rebounds", "assists"}.issubset(df.columns):
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

    # --- Defense-vs-position rank (NBA only; empty/no-op for other sports) ---
    if not defense.empty and {"stat", "team", "position", "rank"}.issubset(defense.columns):
        defense = defense.copy()
        defense["team"] = defense["team"].apply(lambda x: TEAM_CODE_MAP.get(x, x))
        defense_stat = defense[defense["stat"] == stat][["position", "team", "rank"]]
        defense_stat = defense_stat.rename(columns={"team": "opponent"})
        stat_props = stat_props.merge(defense_stat, how="left")
    else:
        stat_props["rank"] = pd.NA

    # --- Volatility metrics ---
    std_dev = df.groupby("name")[stat].std().reset_index().rename(columns={stat: "std_dev"})
    stat_props = stat_props.merge(std_dev, how="left")
    stat_props["std%"] = stat_props["std_dev"] / stat_props[stat]

    df["spm"] = df[stat] / df["minutes"]
    spm = df.groupby("name")["spm"].mean().reset_index()
    stat_props = stat_props.merge(spm, how="left")

    # --- Historical hit rate ---
    # The committed historical dataset is NBA-only (basketball box-score schema),
    # so career-based metrics are computed for the NBA and skipped elsewhere.
    history = load_historical_data() if sport_key == "basketball_nba" else pd.DataFrame()
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
        # No career dataset for this sport: fall back to the season hit rate so
        # the "history" column and the Strong Overs/Unders filters (which require
        # a history-hit edge) stay meaningful rather than excluding every row.
        stat_props["history_hit%"] = stat_props["hit%"]

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
    sport_key: str = "basketball_nba",
) -> dict:
    """
    Build a summary for each player who has a prop line today.

    Sport-aware: every average/game-row carries the sport's own stat columns
    (config.SPORT_STAT_CONFIGS), keyed by stat_key (e.g. "points" for the NBA,
    "hits"/"total_bases" for MLB, "pass_yards" for football).

    Returns a dict keyed by player name. Each value contains:
      - team, position
      - today_lines: {stat_key: line}
      - season_avg / career_avg: {games, minutes, <stat_key>: avg, ...}
      - last_20: list of game dicts (most recent first), each with
        {date, opponent, min, <stat_key>: value, ...}
      - vs_opponent / vs_opponent_avg / home_avg / away_avg

    The career layer (career_avg, last_20, vs-opponent, home/away) is sourced
    from the committed historical dataset for the NBA. Other sports have no such
    dataset, so that layer falls back to the current season's game logs (still
    useful: recent form, vs-opponent this season) and home/away splits are
    omitted (the ESPN game log doesn't carry a reliable home/away flag).
    """
    from config import stat_keys_for, prop_to_stat_for

    stat_keys = stat_keys_for(sport_key)
    prop_to_stat = prop_to_stat_for(sport_key)

    # --- Career/history source (NBA only) -----------------------------------
    history_renamed = pd.DataFrame()
    if sport_key == "basketball_nba":
        history = load_historical_data()
        if not history.empty:
            history_renamed = history.rename(columns={
                "player": "name", "team_code": "team", "opponent_code": "opponent",
                "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
                "threefm": "threes", "stl": "steals", "blk": "blocks",
            })
            for col in ["points", "rebounds", "assists", "minutes"]:
                history_renamed[col] = pd.to_numeric(history_renamed[col], errors="coerce").fillna(0)
            for col in ("threes", "steals", "blocks"):
                if col in history_renamed.columns:
                    history_renamed[col] = pd.to_numeric(history_renamed[col], errors="coerce").fillna(0)
            history_renamed["pra"] = history_renamed["points"] + history_renamed["rebounds"] + history_renamed["assists"]
            history_renamed["gameday"] = pd.to_datetime(history_renamed["game_gameday"], errors="coerce")
            # Normalize home/away into a single 'is_home' boolean
            if "game_loc" in history_renamed.columns:
                loc = history_renamed["game_loc"].astype(str).str.lower().str.strip()
                history_renamed["is_home"] = loc.isin({"h", "home"})
                if "game_home-code" in history_renamed.columns:
                    missing = history_renamed["game_loc"].isna()
                    history_renamed.loc[missing, "is_home"] = (
                        history_renamed.loc[missing, "team"]
                        == history_renamed.loc[missing, "game_home-code"]
                    )

    # When there's no career dataset, the current season's game logs stand in as
    # the "career" source so the detail page still shows recent form / vs-opp.
    has_real_history = not history_renamed.empty
    career_source = history_renamed if has_real_history else current_stats

    def _avg(df: pd.DataFrame, col: str) -> float:
        return float(df[col].mean()) if col in df.columns and len(df) else 0.0

    def _avg_dict(g: pd.DataFrame, extra: dict | None = None) -> dict:
        d = {"games": int(len(g)), "minutes": _avg(g, "minutes")}
        for k in stat_keys:
            d[k] = _avg(g, k)
        if extra:
            d.update(extra)
        return d

    def _game_rows(g: pd.DataFrame, n: int) -> list[dict]:
        rows = []
        for _, r in g.sort_values("gameday", ascending=False).head(n).iterrows():
            row = {
                "date": str(r["gameday"].date()) if pd.notna(r.get("gameday")) else "",
                "opponent": r.get("opponent", ""),
                "min": float(r.get("minutes", 0) or 0),
            }
            for k in stat_keys:
                row[k] = float(r.get(k, 0) or 0)
            rows.append(row)
        return rows

    summaries = {}
    for name in player_names:
        # Today's lines (keyed by stat_key)
        player_props = props[props["name"] == name]
        today_lines = {}
        for _, row in player_props.iterrows():
            stat_key = prop_to_stat.get(row["type"])
            if stat_key:
                today_lines[stat_key] = row["spread"]

        # Season averages from current_stats (games where the player appeared)
        season_games = current_stats[
            (current_stats["name"] == name) & (current_stats.get("minutes", 0) != 0)
        ] if "minutes" in current_stats.columns else current_stats[current_stats["name"] == name]
        season_avg = _avg_dict(season_games)

        # Current team / position from the most recent game (handles trades)
        if len(season_games):
            most_recent = season_games.sort_values("gameday", ascending=False).iloc[0]
            team = most_recent["team-code"]
            position = most_recent["position"] if "position" in season_games.columns else ""
        else:
            team = ""
            position = ""

        # Career averages + last-20 + vs-opponent from the career source
        career_avg = _avg_dict(career_source.iloc[0:0])  # zeroed template
        last_20 = []
        vs_opponent = []
        vs_opponent_avg = None
        if "name" in career_source.columns:
            career_games = career_source[career_source["name"] == name]
            if "minutes" in career_games.columns:
                career_games = career_games[career_games["minutes"] != 0]
            if len(career_games):
                career_avg = _avg_dict(career_games)
                last_20 = _game_rows(career_games, 20)

            # Performance vs tonight's opponent
            if todays_games and team and "opponent" in career_games.columns:
                opp = todays_games.get(team, "")
                if opp:
                    vs_games = career_games[career_games["opponent"] == opp].sort_values(
                        "gameday", ascending=False
                    )
                    vs_opponent = _game_rows(vs_games, 10)
                    if len(vs_games):
                        vs_opponent_avg = _avg_dict(vs_games, {"opponent": opp})

        # Home/Away splits — career dataset only (no reliable flag elsewhere)
        home_avg = None
        away_avg = None
        if has_real_history and "is_home" in history_renamed.columns:
            player_career = history_renamed[
                (history_renamed["name"] == name) & (history_renamed["minutes"] != 0)
            ]
            home_games = player_career[player_career["is_home"] == True]  # noqa: E712
            away_games = player_career[player_career["is_home"] == False]  # noqa: E712
            if len(home_games):
                home_avg = _avg_dict(home_games)
            if len(away_games):
                away_avg = _avg_dict(away_games)

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
            "has_career": bool(has_real_history),
        }

    return summaries
