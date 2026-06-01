"""Odds + injuries view for sports without a player-stats / projection source.

Sports like NCAAB/MLB/NFL/NCAAF have game odds and injury reports but no
projection edges. The pipeline stashes the raw lines + injuries under the
``__odds_only__`` summary sentinel; this view renders them instead of the
(empty) projection board.
"""

import pandas as pd

import streamlit as st

from config import sport_config


def render_odds_only(selected_sport: str, events, summaries):
    """Render the lines + injuries page for a non-projection sport, then stop."""
    _cfg = sport_config(selected_sport)
    st.title(f"{selected_sport} — Lines & Injuries")
    _cats = _cfg.get("stat_categories") or []
    if _cats:
        st.caption("Player-prop markets on the slate: " + ", ".join(_cats))
    st.info(
        f"**{selected_sport}** has limited support: game odds and injury reports "
        "are available, but projection edges (averages, hit rates, confidence) "
        "require a player-stats source wired into the basketball-specific "
        "projection engine, which isn't done for this sport yet. See "
        "`scrapers/` for the per-sport stub explaining what's needed."
    )

    _odds_only = (summaries or {}).get("__odds_only__", {})
    _props = _odds_only.get("props", [])
    _inj = _odds_only.get("injuries", [])

    lines_tab, inj_tab = st.tabs(["Player Prop Lines", f"Injuries ({len(_inj)})"])
    with lines_tab:
        if not events:
            st.warning("No games found on this date for this sport.")
        elif not _props:
            st.warning(
                "No player-prop lines were returned for this slate. The Odds API "
                "may not yet list props for these games, or the data predates the "
                "sport being wired up — try **Fetch / Refresh Data** for a date "
                "with an active slate."
            )
        else:
            props_df = pd.DataFrame(_props).rename(
                columns={"type": "Prop", "player": "Player", "spread": "Line"}
            )
            prop_types = sorted(props_df["Prop"].dropna().unique().tolist())
            sel = st.multiselect("Filter by prop", prop_types, default=prop_types)
            view = props_df[props_df["Prop"].isin(sel)] if sel else props_df
            st.caption(f"{len(view)} lines across {len(prop_types)} prop types.")
            st.dataframe(
                view[["Player", "Prop", "Line"]],
                use_container_width=True, hide_index=True,
                column_config={"Line": st.column_config.NumberColumn("Line", format="%.1f")},
            )
    with inj_tab:
        if not _inj:
            st.info("No injuries currently listed by ESPN for this sport.")
        else:
            inj_df = pd.DataFrame(_inj).rename(columns={
                "name": "Player", "team": "Team",
                "status_short": "Status", "comment": "Note",
            })
            show_cols = [c for c in ("Player", "Team", "Status", "Note") if c in inj_df.columns]
            st.dataframe(inj_df[show_cols], use_container_width=True, hide_index=True)
