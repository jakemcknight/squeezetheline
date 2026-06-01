"""Player comparison view for the Streamlit UI."""

import pandas as pd

import streamlit as st

from config import stat_configs_for


def _is_basketball(sport_key: str) -> bool:
    return str(sport_key).startswith("basketball")


def render_compare(summaries: dict, results: dict, sport_key: str):
    """Render the side-by-side player comparison view, then stop."""
    st.title("Compare Players")
    st.caption("Side-by-side season averages, hit rates, and tonight's matchup.")

    all_player_names = sorted(summaries.keys()) if summaries else []
    if not all_player_names:
        st.info("No players with props yet. Click Fetch / Refresh Data first.")
        st.stop()

    cmp_cols = st.columns(3)
    selected = []
    for i, col in enumerate(cmp_cols):
        with col:
            pick = st.selectbox(
                f"Player {i+1}",
                options=[""] + all_player_names,
                key=f"compare_player_{i}",
            )
            if pick:
                selected.append(pick)

    if len(selected) < 2:
        st.info("Pick at least 2 players to compare.")
        st.stop()

    # Build a comparison DataFrame — metric rows are sport-aware (the sport's
    # stat set, with averages from build_player_summaries).
    _cmp_defs = stat_configs_for(sport_key)
    _cmp_basketball = _is_basketball(sport_key)
    _career_label = "Career" if _cmp_basketball else "Recent"

    def _avg_getter(section: str, key: str):
        return lambda s, _r: round(float(s.get(section, {}).get(key, 0) or 0), 1)

    METRIC_ROWS = [
        ("Team", lambda s, _r: s.get("team", "")),
        ("Position", lambda s, _r: s.get("position", "")),
        ("Tonight's Opp", lambda _s, r: (r.iloc[0].get("opponent", "") if r is not None and not r.empty else "")),
        ("Inj status", lambda s, _r: (s.get("injury") or {}).get("status_short") or "-"),
        ("Season games", lambda s, _r: s.get("season_avg", {}).get("games", 0)),
    ]
    if _cmp_basketball:
        METRIC_ROWS.append(("Season MIN", _avg_getter("season_avg", "minutes")))
    for _key, _pt, _label in _cmp_defs:
        METRIC_ROWS.append((f"Season {_label}", _avg_getter("season_avg", _key)))
    for _key, _pt, _label in _cmp_defs:
        METRIC_ROWS.append((f"{_career_label} {_label}", _avg_getter("career_avg", _key)))
    # Home/away splits exist for the NBA only (career dataset); show them for the
    # leading stat so the table stays compact.
    if _cmp_basketball and _cmp_defs:
        _lead = _cmp_defs[0][0]
        METRIC_ROWS.append((f"Home {_cmp_defs[0][2]}",
                            lambda s, _r, k=_lead: round((s.get("home_avg") or {}).get(k, 0), 1) if s.get("home_avg") else "-"))
        METRIC_ROWS.append((f"Away {_cmp_defs[0][2]}",
                            lambda s, _r, k=_lead: round((s.get("away_avg") or {}).get(k, 0), 1) if s.get("away_avg") else "-"))

    # For each selected player, build a column
    compare_data = {"Metric": [row[0] for row in METRIC_ROWS]}
    _opp_stat = _cmp_defs[0][0] if _cmp_defs else None
    for player in selected:
        summary = summaries.get(player, {})
        # Find any result row for tonight (just to read the opponent)
        result_lead = results.get(_opp_stat)
        player_row = None
        if result_lead is not None and not result_lead.empty:
            match = result_lead[result_lead["name"] == player]
            if not match.empty:
                player_row = match
        compare_data[player] = [getter(summary, player_row) for _, getter in METRIC_ROWS]

    compare_df = pd.DataFrame(compare_data)
    st.dataframe(compare_df, use_container_width=True, hide_index=True)

    # --- Tonight's lines side-by-side ---
    st.subheader("Tonight's lines")
    line_rows = []
    STAT_DISPLAY_CMP = [(key, label) for key, _pt, label in _cmp_defs]
    for stat_key, label in STAT_DISPLAY_CMP:
        line_data = {"Stat": label}
        any_line = False
        for player in selected:
            line = (summaries.get(player, {}).get("today_lines") or {}).get(stat_key)
            if line is not None:
                any_line = True
            # Also pull this season's hit% from results
            result_df = results.get(stat_key)
            hit = None
            if result_df is not None and not result_df.empty:
                match = result_df[result_df["name"] == player]
                if not match.empty:
                    hit = match.iloc[0].get("hit%")
            cell = ""
            if line is not None:
                cell = f"{line:.1f}"
                if hit is not None and not pd.isna(hit):
                    cell += f" ({hit:.0f}%)"
            line_data[player] = cell
        if any_line:
            line_rows.append(line_data)

    if line_rows:
        st.dataframe(pd.DataFrame(line_rows), use_container_width=True, hide_index=True)
    else:
        st.caption("None of the selected players have prop lines on the current slate.")

    st.stop()
