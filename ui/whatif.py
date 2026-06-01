"""What-If simulator: how a player performs when a specific teammate is out."""

import pandas as pd

import streamlit as st

from ui.charts import make_last_n_chart


def render_whatif(results: dict, sport_key: str):
    """Render the What-If (player-out impact) view, then stop."""
    st.title("What-If: Player out impact")
    st.caption(
        "How does a player perform when a specific teammate is out? "
        "Filters the evaluated player's games to only those where the "
        "selected 'out' player didn't play (0 minutes or absent from box score)."
    )

    from data import load_historical_data
    history = load_historical_data()
    if history.empty:
        st.error("No historical data available.")
        st.stop()

    # Normalize columns once
    hist = history.rename(columns={
        "player": "name", "team_code": "team", "opponent_code": "opponent",
        "pts": "points", "reb": "rebounds", "ast": "assists", "min": "minutes",
        "threefm": "threes", "stl": "steals", "blk": "blocks",
    })
    for col in ("points", "rebounds", "assists", "minutes", "threes", "steals", "blocks"):
        if col in hist.columns:
            hist[col] = pd.to_numeric(hist[col], errors="coerce").fillna(0)
    hist["pra"] = hist["points"] + hist["rebounds"] + hist["assists"]
    hist["gameday"] = pd.to_datetime(hist.get("game_gameday", hist.get("date_string")), errors="coerce")
    hist = hist[hist["name"].notna()]

    # Build team rosters from current-season data so selectors only show
    # active players (not retired guys who happen to be in the historical CSV).
    season_only = hist[hist["gameday"] >= pd.Timestamp("2024-10-01")]
    rosters = (
        season_only.groupby("team")["name"]
        .agg(lambda s: sorted(set(s.dropna())))
        .to_dict()
    )
    teams_list = sorted(rosters.keys())

    if not teams_list:
        st.warning("No team data available.")
        st.stop()

    # --- Selectors ---
    sel_team = st.selectbox("Team", options=teams_list, index=0, key="whatif_team")
    roster = rosters.get(sel_team, [])
    if len(roster) < 2:
        st.warning(f"Need at least 2 players on {sel_team}.")
        st.stop()

    # Pull the injury report so we can label the OUT-player dropdown.
    # Cache for 5 minutes so flipping selectors doesn't re-hit ESPN.
    @st.cache_data(ttl=300)
    def _whatif_injuries(sk: str):
        from scrapers.injuries import get_injury_report
        inj = get_injury_report(sk)
        return dict(zip(inj["name"], inj["status_short"])) if not inj.empty else {}

    injury_status = _whatif_injuries(sport_key)

    c1, c2 = st.columns(2)
    with c1:
        eval_player = st.selectbox("Player to evaluate", options=roster, key="whatif_eval")
    with c2:
        out_options = [p for p in roster if p != eval_player]
        # Sort: currently-injured players first (most relevant for "out"),
        # then everyone else alphabetically.
        out_options.sort(key=lambda n: (not bool(injury_status.get(n)), n))

        def _format_out_player(name: str) -> str:
            status = injury_status.get(name, "")
            return f"{name} — {status}" if status else name

        out_player = st.selectbox(
            "Player who is OUT",
            options=out_options,
            format_func=_format_out_player,
            key="whatif_out",
        )

    period = st.radio(
        "Period",
        ["This season only", "Career (all years)"],
        horizontal=True,
        key="whatif_period",
    )

    # --- Compute ---
    if period == "This season only":
        eval_games_all = hist[(hist["name"] == eval_player) & (hist["team"] == sel_team) & (hist["gameday"] >= pd.Timestamp("2024-10-01"))]
        out_games_all = hist[(hist["name"] == out_player) & (hist["gameday"] >= pd.Timestamp("2024-10-01"))]
    else:
        eval_games_all = hist[(hist["name"] == eval_player) & (hist["team"] == sel_team)]
        out_games_all = hist[(hist["name"] == out_player)]

    # Dates the OUT player actually played (positive minutes)
    out_player_played_dates = set(
        out_games_all[out_games_all["minutes"] > 0]["gameday"].dropna()
    )

    # Eval player's games where the out player did NOT play
    eval_games_with_out_player_absent = eval_games_all[
        ~eval_games_all["gameday"].isin(out_player_played_dates) & (eval_games_all["minutes"] > 0)
    ].sort_values("gameday", ascending=False)

    eval_games_played = eval_games_all[eval_games_all["minutes"] > 0]

    # --- Render results ---
    st.divider()

    n_total = len(eval_games_played)
    n_out = len(eval_games_with_out_player_absent)
    if n_total == 0:
        st.warning(f"{eval_player} has no games on {sel_team} in this period.")
        st.stop()

    sample_pct = (n_out / n_total) * 100 if n_total else 0
    sm1, sm2 = st.columns(2)
    sm1.metric(f"Games played by {eval_player}", n_total)
    sm2.metric(f"Of those, with {out_player} OUT", f"{n_out}", delta=f"{sample_pct:.0f}% of sample")

    if n_out == 0:
        st.info(f"No games found where {eval_player} played but {out_player} didn't. Try Career view or a different teammate.")
        st.stop()

    # Side-by-side averages
    def _avg_row(label, df):
        if df.empty:
            return None
        return {
            "Sample": label,
            "Games": int(len(df)),
            "MIN": float(df["minutes"].mean()),
            "PTS": float(df["points"].mean()),
            "REB": float(df["rebounds"].mean()),
            "AST": float(df["assists"].mean()),
            "PRA": float(df["pra"].mean()),
            "3PM": float(df["threes"].mean()) if "threes" in df.columns else 0,
            "STL": float(df["steals"].mean()) if "steals" in df.columns else 0,
            "BLK": float(df["blocks"].mean()) if "blocks" in df.columns else 0,
        }

    # --- Tonight's lines vs the with-teammate-out averages ---
    # Look up tonight's lines for the eval player from the cached daily results.
    LINE_STATS = [
        ("points", "Points", "Total Points"),
        ("rebounds", "Rebounds", "Total Rebounds"),
        ("assists", "Assists", "Total Assists"),
        ("pra", "PRA", "Total PRA"),
        ("threes", "3PM", "Total 3PM"),
        ("steals", "Steals", "Total Steals"),
        ("blocks", "Blocks", "Total Blocks"),
    ]
    tonight_lines = {}
    for stat_key, _, _ in LINE_STATS:
        if stat_key in results:
            row = results[stat_key][results[stat_key]["name"] == eval_player]
            if not row.empty:
                tonight_lines[stat_key] = float(row.iloc[0]["spread"])

    if tonight_lines:
        st.subheader(f"Tonight's lines vs. with-{out_player}-out average")
        active = [(k, lbl) for k, lbl, _ in LINE_STATS if k in tonight_lines]
        n_cols = min(len(active), 4)
        cols = st.columns(n_cols)
        for i, (stat_key, label) in enumerate(active):
            with cols[i % n_cols]:
                line = tonight_lines[stat_key]
                # Average for this stat in the with-out subset
                if stat_key in eval_games_with_out_player_absent.columns:
                    out_avg = float(eval_games_with_out_player_absent[stat_key].mean())
                    delta = out_avg - line
                    # Hit rate over the filtered games
                    hits = int((eval_games_with_out_player_absent[stat_key] > line).sum())
                    hit_pct = (hits / n_out) * 100
                    st.metric(
                        label,
                        f"Line: {line:.1f}",
                        delta=f"{delta:+.1f} vs avg ({out_avg:.1f})",
                    )
                    st.caption(f"Hit {hits}/{n_out} ({hit_pct:.0f}%)")
                else:
                    st.metric(label, f"Line: {line:.1f}", delta="no data")
    else:
        st.caption(f"No prop lines for {eval_player} on the current slate (or data not refreshed).")

    # --- Charts: per-stat bar charts of the filtered games ---
    if tonight_lines:
        st.subheader(f"Last 10 games with {out_player} out")
        # Build the chart data: dicts of {date, opponent, pts, reb, ast, ...}
        recent = eval_games_with_out_player_absent.head(10).copy()
        chart_records = []
        for _, g in recent.iterrows():
            chart_records.append({
                "date": g["gameday"].strftime("%Y-%m-%d") if pd.notna(g["gameday"]) else "",
                "opponent": g.get("opponent", ""),
                "pts": float(g.get("points", 0)),
                "reb": float(g.get("rebounds", 0)),
                "ast": float(g.get("assists", 0)),
                "pra": float(g.get("pra", 0)),
                "threes": float(g.get("threes", 0)),
                "steals": float(g.get("steals", 0)),
                "blocks": float(g.get("blocks", 0)),
            })

        chart_stats = [
            ("points", "Points", "pts"),
            ("rebounds", "Rebounds", "reb"),
            ("assists", "Assists", "ast"),
            ("pra", "PRA", "pra"),
            ("threes", "3PM", "threes"),
            ("steals", "Steals", "steals"),
            ("blocks", "Blocks", "blocks"),
        ]
        active_charts = [s for s in chart_stats if s[0] in tonight_lines]
        n_chart_cols = min(len(active_charts), 3)
        if n_chart_cols > 0:
            chart_cols = st.columns(n_chart_cols)
            for i, (full_stat, label, game_key) in enumerate(active_charts):
                with chart_cols[i % n_chart_cols]:
                    chart = make_last_n_chart(
                        chart_records, game_key, label,
                        tonight_lines.get(full_stat),
                        n=10,
                    )
                    if chart is not None:
                        st.altair_chart(chart, use_container_width=True)

    rows = [
        _avg_row("All games (baseline)", eval_games_played),
        _avg_row(f"With {out_player} OUT", eval_games_with_out_player_absent),
    ]
    avg_df = pd.DataFrame([r for r in rows if r is not None])

    st.subheader("Averages comparison")
    st.dataframe(avg_df, use_container_width=True, hide_index=True, column_config={
        "MIN": st.column_config.NumberColumn(format="%.1f"),
        "PTS": st.column_config.NumberColumn(format="%.1f"),
        "REB": st.column_config.NumberColumn(format="%.1f"),
        "AST": st.column_config.NumberColumn(format="%.1f"),
        "PRA": st.column_config.NumberColumn(format="%.1f"),
        "3PM": st.column_config.NumberColumn(format="%.1f"),
        "STL": st.column_config.NumberColumn(format="%.1f"),
        "BLK": st.column_config.NumberColumn(format="%.1f"),
    })

    # Specific game log
    st.subheader(f"Games where {out_player} was out ({n_out} most recent first)")
    log_df = eval_games_with_out_player_absent.head(30).copy()
    log_df["date"] = log_df["gameday"].dt.strftime("%Y-%m-%d")
    log_cols = ["date", "opponent", "minutes", "points", "rebounds", "assists", "threes", "steals", "blocks"]
    log_cols = [c for c in log_cols if c in log_df.columns]
    log_df = log_df[log_cols].rename(columns={
        "date": "Date", "opponent": "Opp", "minutes": "MIN",
        "points": "PTS", "rebounds": "REB", "assists": "AST",
        "threes": "3PM", "steals": "STL", "blocks": "BLK",
    })
    st.dataframe(log_df, use_container_width=True, hide_index=True, column_config={
        "MIN": st.column_config.NumberColumn(format="%.0f"),
        "PTS": st.column_config.NumberColumn(format="%.0f"),
        "REB": st.column_config.NumberColumn(format="%.0f"),
        "AST": st.column_config.NumberColumn(format="%.0f"),
        "3PM": st.column_config.NumberColumn(format="%.0f"),
        "STL": st.column_config.NumberColumn(format="%.0f"),
        "BLK": st.column_config.NumberColumn(format="%.0f"),
    })

    st.stop()
