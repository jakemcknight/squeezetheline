"""Data pipeline orchestration for Squeeze the Line.

This module is the Streamlit-free heart of the app: it hits the scraping/odds
sources, runs the projection analysis, and reads/writes the on-disk daily cache.
``app.py`` (the Streamlit UI) imports ``fetch_fresh_data`` here, and the FastAPI
backend's ``LiveProvider`` re-implements the same flow (see
``backend/providers/live.py``) — keep the two in sync when the pipeline changes.

Nothing here imports ``streamlit``, so this module can be imported and exercised
without a running Streamlit session.
"""

import os
import json
import datetime
from io import StringIO

import pandas as pd

from scrapers.odds_api import get_todays_games, get_all_props, get_events_for_date, get_game_times
from scrapers.sources import (
    get_season_stats,
    get_positions,
    get_defense_by_position,
    get_injury_report,
)
from config import stat_configs_for
from data import prepare_stats, prepare_props, DATA_DIR
from analysis import analyze_stat, build_player_summaries


CACHE_DIR = os.path.join(DATA_DIR, "daily_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(date: datetime.date, sport_key: str = "basketball_nba") -> str:
    # NBA keeps the original bare-date filename for backward compatibility with
    # already-cached slates; other sports get a sport-suffixed file so they
    # don't collide on the same date.
    if sport_key == "basketball_nba":
        return os.path.join(CACHE_DIR, f"{date}.json")
    return os.path.join(CACHE_DIR, f"{date}__{sport_key}.json")


def save_daily_results(events, results, summaries, date: datetime.date,
                       sport_key: str = "basketball_nba"):
    """Save fetched results to disk."""
    payload = {
        "date": str(date),
        "sport_key": sport_key,
        "events": events,
        "results": {stat: df.to_json() for stat, df in results.items()},
        "summaries": summaries,
    }
    with open(_cache_path(date, sport_key), "w") as f:
        json.dump(payload, f)


def load_daily_results(date: datetime.date, sport_key: str = "basketball_nba"):
    """Load cached results from disk. Returns (events, results, summaries) or None."""
    path = _cache_path(date, sport_key)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        payload = json.load(f)
    events = payload["events"]
    results = {stat: pd.read_json(StringIO(df_json)) for stat, df_json in payload["results"].items()}
    summaries = payload.get("summaries", {})
    return events, results, summaries


def fetch_fresh_data(date: datetime.date, all_books: bool = False,
                     sport_key: str = "basketball_nba"):
    """Hit all APIs and run the full analysis pipeline for the given date.

    When `all_books=True`, also pull props from every US bookmaker so the
    player detail page can show line shopping comparisons.

    `sport_key` selects the Odds API sport and the matching stats/positions/
    defense/injury sources (see scrapers/sources.py). Sports without a stats
    source (e.g. NCAA) produce an empty `df`, which yields empty analysis
    results — the caller shows odds/injuries but no projection edges.
    """
    events = get_events_for_date(date, sport_key)
    todays_games = get_todays_games(date, sport_key)
    # Fetch the prop lines first so the stats fetch can be scoped to just the
    # players/teams on the slate. ESPN-backed sports (MLB/NFL/NCAAF) walk only
    # the slate teams' rosters and pull game logs only for prop players, keeping
    # the request count proportional to the slate; the NBA source ignores the
    # scoping (its single LeagueGameLog call already covers the whole league).
    raw_props = get_all_props(date, all_books=all_books, sport_key=sport_key)
    slate_teams = list(todays_games.keys())
    prop_players = (
        sorted(raw_props["player"].dropna().unique().tolist())
        if not raw_props.empty and "player" in raw_props.columns else []
    )
    stats = get_season_stats(sport_key, team_codes=slate_teams, player_names=prop_players)
    positions = get_positions(sport_key, team_codes=slate_teams)
    # prepare_stats is safe on an empty frame (it returns an empty frame with
    # the expected columns), but a sport with no stats source has nothing to
    # analyze, so we skip the projection pipeline below for it.
    df = prepare_stats(stats, positions)
    has_stats = not df.empty
    # For the main analysis, dedupe to one line per player/stat (best/median).
    # Save the raw multi-book table separately for the detail view.
    if all_books and "book" in raw_props.columns:
        # Use the median line per player+market for the analysis (stable across books)
        analysis_props = (
            raw_props.groupby(["type", "player"])["spread"]
            .median().reset_index()
        )
    else:
        analysis_props = raw_props.copy()
    props = prepare_props(analysis_props)
    defense = get_defense_by_position()

    player_meta = positions[["name", "player_url", "player_id"]].drop_duplicates(subset="name")
    # Keep this name for backward-compat; downstream code merges on it
    player_urls = player_meta[["name", "player_url"]]
    player_id_map = dict(zip(player_meta["name"], player_meta["player_id"]))

    # Injury report from ESPN
    injuries = get_injury_report(sport_key)
    injury_join = (
        injuries[["name", "status_short", "comment"]].drop_duplicates(subset="name")
        if not injuries.empty
        else pd.DataFrame(columns=["name", "status_short", "comment"])
    )

    # --- Injury impact lookup: per team, which OUT players are typical starters? ---
    # We use the players' average minutes (last 10 games) to rank "significance".
    # For each team, build a list of OUT players sorted by mpg, plus a count.
    team_out_impact: dict[str, dict] = {}
    if not injuries.empty:
        # Map each injured player to their current team via current season stats
        recent_mins = (
            df[df["rank"] <= 10]
            .groupby(["name", "team-code"])["minutes"].mean()
            .reset_index()
            .rename(columns={"minutes": "mpg"})
        )
        out_set = set(injuries[injuries["status_short"].isin(["OUT", "DBT"])]["name"].dropna())
        for team_code in recent_mins["team-code"].dropna().unique():
            team_out = recent_mins[
                (recent_mins["team-code"] == team_code) & (recent_mins["name"].isin(out_set))
            ].sort_values("mpg", ascending=False)
            if team_out.empty:
                continue
            starters_out = team_out[team_out["mpg"] >= 25.0]  # ~starter threshold
            team_out_impact[team_code] = {
                "count": int(len(team_out)),
                "starters_out_count": int(len(starters_out)),
                "starters_out_names": ", ".join(starters_out["name"].tolist()[:3]),
                "all_out_names": ", ".join(team_out["name"].tolist()[:5]),
            }

    # Game tipoff times per team (so we can flag in-progress / completed games)
    game_times = get_game_times(date, sport_key)
    now_utc = pd.Timestamp.now(tz="UTC")

    def _classify(team_code: str) -> dict:
        commence = game_times.get(team_code)
        if not commence:
            return {"game_status": "unknown", "tipoff": ""}
        try:
            tipoff = pd.Timestamp(commence)
            if tipoff.tzinfo is None:
                tipoff = tipoff.tz_localize("UTC")
            if now_utc < tipoff:
                status = "pregame"
            elif now_utc < tipoff + pd.Timedelta(hours=3):
                status = "live"
            else:
                status = "completed"
            return {"game_status": status, "tipoff": tipoff.isoformat()}
        except Exception:
            return {"game_status": "unknown", "tipoff": commence}

    results = {}
    if not has_stats:
        # No player stats source for this sport (NCAAB/MLB/NFL/NCAAF): we still
        # fetched odds + injuries, but there's nothing to project against.
        # Return empty per-stat frames so the projection board stays empty, and
        # stash the raw lines + injuries under a sentinel summary key so the UI
        # can render a sport-appropriate odds + injuries view instead.
        for stat, _, _ in stat_configs_for(sport_key):
            results[stat] = pd.DataFrame()
        odds_records = (
            analysis_props[["type", "player", "spread"]]
            .dropna(subset=["player"])
            .sort_values(["type", "player"])
            .to_dict("records")
            if not analysis_props.empty else []
        )
        inj_cols = [c for c in ("name", "team", "status_short", "comment") if c in injuries.columns]
        inj_records = injuries[inj_cols].to_dict("records") if not injuries.empty else []
        odds_only = {"__odds_only__": {"props": odds_records, "injuries": inj_records}}
        return events, results, odds_only

    for stat, prop_type, _label in stat_configs_for(sport_key):
        result = analyze_stat(stat, prop_type, df, props, todays_games, defense,
                              game_date=date, sport_key=sport_key)
        result = result.merge(player_urls, on="name", how="left")
        if not injury_join.empty:
            result = result.merge(injury_join, on="name", how="left")
        # Replace NaN in injury columns with empty strings so healthy players
        # show a clean blank cell instead of "None"
        for col in ("status_short", "comment"):
            if col in result.columns:
                result[col] = result[col].fillna("")

        # Attach teammate injury impact: number of starters/OUT teammates
        def _impact_short(team):
            info = team_out_impact.get(team)
            if not info:
                return ""
            n = info["starters_out_count"]
            if n == 0:
                return f"{info['count']} bench out"
            return f"{n} starter{'s' if n > 1 else ''} out"

        def _impact_full(team):
            info = team_out_impact.get(team)
            if not info:
                return ""
            return info.get("starters_out_names") or info.get("all_out_names") or ""

        result["teammates_out"] = result["team-code"].apply(_impact_short)
        result["teammates_out_names"] = result["team-code"].apply(_impact_full)
        # Tag each row with its game's status (pregame / live / completed).
        # Classify once per team then map onto the column, which is pandas 3.x safe.
        classifications = {t: _classify(t) for t in result["team-code"].unique()}
        result["game_status"] = result["team-code"].map(lambda t: classifications.get(t, {}).get("game_status", "unknown"))
        result["tipoff"] = result["team-code"].map(lambda t: classifications.get(t, {}).get("tipoff", ""))
        results[stat] = result

    # Build per-player summaries for the detail view
    all_players = sorted(set(props["name"].dropna().unique()))
    summaries = build_player_summaries(all_players, df, props, todays_games=todays_games,
                                       sport_key=sport_key)

    # Attach the NBA player_id (used to build the headshot URL)
    for name, summary in summaries.items():
        pid = player_id_map.get(name)
        if pid is not None:
            summary["player_id"] = int(pid)

    # Attach injury info (if any) onto each summary
    if not injuries.empty:
        inj_lookup = injuries.drop_duplicates("name").set_index("name").to_dict("index")
        for name, summary in summaries.items():
            if name in inj_lookup:
                row = inj_lookup[name]
                summary["injury"] = {
                    "status": row.get("status", ""),
                    "status_short": row.get("status_short", ""),
                    "comment": row.get("comment", ""),
                }

    # If we pulled multi-book data, attach per-player line-shopping table
    if all_books and "book" in raw_props.columns:
        for name, summary in summaries.items():
            player_books = raw_props[raw_props["player"] == name]
            if not player_books.empty:
                summary["all_books"] = player_books.to_dict("records")

    return events, results, summaries
