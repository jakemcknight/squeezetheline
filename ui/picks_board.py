"""Picks board rendering: the results table, card layout, and column config."""

import pandas as pd

import streamlit as st


COLUMN_CONFIG = {
    "name": st.column_config.TextColumn("Player"),
    "confidence": st.column_config.ProgressColumn("Conf", format="%.0f", min_value=0, max_value=100, help="Composite confidence score (0-100): combines delta strength, hit% edge, history hit% edge, and defensive matchup."),
    "trend": st.column_config.TextColumn("Trend", help="↑ last-5 avg > last-10 avg (trending up), ↓ trending down, → flat"),
    "last10": st.column_config.BarChartColumn("Last 10", help="Stat values across the player's last 10 games (most recent on right)"),
    "last10_hits": st.column_config.TextColumn("Hit/Miss", help="Each square = one of the last 10 games vs tonight's line. Green = beat the line, red = missed."),
    "game_status": st.column_config.TextColumn("Game", help="pregame / live / completed — live games show in-game lines that aren't pre-game lines"),
    "status_short": st.column_config.TextColumn("Inj", help="Injury status (OUT/DBT/Q/DTD/PROB)"),
    "teammates_out": st.column_config.TextColumn("Inj Impact", help="How many of this player's teammates are OUT (starters specifically). More teammates out usually = more minutes/usage available for this player."),
    "starter": st.column_config.CheckboxColumn("Starter", help="Top 5 mpg on team in last 10 games"),
    "player_url": st.column_config.LinkColumn("Profile", display_text="Profile"),
    "team-code": st.column_config.TextColumn("Team"),
    "opponent": st.column_config.TextColumn("Opp"),
    "position": st.column_config.TextColumn("Pos"),
    "spread": st.column_config.NumberColumn("Line", format="%.1f"),
    "delta": st.column_config.NumberColumn("Delta", format="%+.1f"),
    "delta_5g": st.column_config.NumberColumn("Delta 5G", format="%+.1f"),
    "delta_10g": st.column_config.NumberColumn("Delta 10G", format="%+.1f"),
    "hit%": st.column_config.NumberColumn("Hit %", format="%.0f%%", width="medium"),
    "history_hit%": st.column_config.NumberColumn("Hist Hit %", format="%.0f%%", width="medium"),
    "vs_opp_season": st.column_config.TextColumn("vs Opp (Szn)", help="Games this season beat tonight's line / games vs this opponent this season"),
    "vs_opp_career": st.column_config.TextColumn("vs Opp (Career)", help="Career games beat tonight's line / total career games vs this opponent"),
    "rank": st.column_config.NumberColumn("Def Rank", format="%.0f"),
    "rest_days": st.column_config.NumberColumn("Rest", format="%.0f", help="Days since last game"),
    "b2b": st.column_config.CheckboxColumn("B2B", help="Back-to-back (player played yesterday)"),
    "opp_rest": st.column_config.NumberColumn("Opp Rest", format="%.0f", help="Days of rest for opponent"),
    "opp_b2b": st.column_config.CheckboxColumn("Opp B2B", help="Opponent played last night (easier matchup)"),
    "std_dev": st.column_config.NumberColumn("Std Dev", format="%.1f"),
    "spm": st.column_config.NumberColumn("SPM", format="%.2f"),
}


def _hit_bar_style(val):
    """Paint the cell with a green or red bar based on the value (0-100)."""
    if pd.isna(val):
        return ""
    # Use rgba so the bar has a visible fill without hiding text
    if val >= 50:
        color = "rgba(34, 197, 94, 0.45)"
    else:
        color = "rgba(239, 68, 68, 0.45)"
    width = max(0.0, min(100.0, float(val)))
    return (
        f"background-image: linear-gradient(90deg, {color} {width}%, transparent {width}%);"
        "background-repeat: no-repeat;"
        "font-weight: 600;"
    )


def show_table(df: pd.DataFrame, key: str):
    """Display a results table with row selection — selecting a row opens the player detail.

    Hit% and Hist Hit% cells get a colored bar (green ≥ 50%, red < 50%).
    We strip column_config for those columns so Streamlit doesn't override
    the Styler background with its own cell renderer.
    """
    hit_cols = [c for c in ("hit%", "history_hit%") if c in df.columns]

    # Build column_config excluding the hit% columns so Styler backgrounds render
    col_cfg = {k: v for k, v in COLUMN_CONFIG.items() if k not in hit_cols}

    if hit_cols:
        styled = df.style.map(_hit_bar_style, subset=hit_cols).format(
            {c: "{:.0f}%" for c in hit_cols}
        )
    else:
        styled = df

    left, mid, right = st.columns([1, 12, 1])
    with mid:
        event = st.dataframe(
            styled,
            column_config=col_cfg,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=key,
        )
    if event.selection.rows:
        idx = event.selection.rows[0]
        player_name = df.iloc[idx]["name"]
        st.session_state["selected_player"] = player_name
        try:
            from activity import log, ACTION_PLAYER_VIEW
            log(ACTION_PLAYER_VIEW, {"player": player_name})
        except Exception:
            pass
        st.rerun()


def show_cards(df: pd.DataFrame, key: str):
    """Compact card-style list for mobile.

    Each pick is a card with a colored left edge (green for over leans, red
    for under leans), player + status badges, line/hit, big delta indicator,
    and a tap-friendly View button.
    """
    INJ_BG = {
        "OUT": "#ef4444", "DBT": "#f97316", "Q": "#f59e0b",
        "DTD": "#eab308", "PROB": "#84cc16",
    }
    for idx, row in df.reset_index(drop=True).iterrows():
        name = row["name"]
        delta = row.get("delta", 0) or 0
        edge_color = "#22c55e" if delta > 0 else "#ef4444" if delta < 0 else "#6b7280"
        arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
        team = row.get("team-code", "")
        opp = row.get("opponent", "")
        line = row.get("spread", 0)
        hit = row.get("hit%", 0) or 0
        rest = row.get("rest_days")
        b2b = bool(row.get("b2b", False))
        opp_b2b = bool(row.get("opp_b2b", False))
        starter = bool(row.get("starter", False))
        trend = row.get("trend", "")
        status = row.get("status_short", "") if isinstance(row.get("status_short", ""), str) else ""

        # Inline badges
        badges = []
        if status:
            color = INJ_BG.get(status, "#8b92a5")
            badges.append(
                f'<span style="background:{color}22;color:{color};padding:2px 6px;'
                f'border-radius:4px;font-size:0.7rem;font-weight:700;">{status}</span>'
            )
        if starter:
            badges.append(
                '<span style="background:#22c55e22;color:#22c55e;padding:2px 6px;'
                'border-radius:4px;font-size:0.7rem;font-weight:700;">STARTER</span>'
            )
        if b2b:
            badges.append(
                '<span style="background:#f9731622;color:#f97316;padding:2px 6px;'
                'border-radius:4px;font-size:0.7rem;font-weight:700;">B2B</span>'
            )
        if opp_b2b:
            badges.append(
                '<span style="background:#84cc1622;color:#84cc16;padding:2px 6px;'
                'border-radius:4px;font-size:0.7rem;font-weight:700;">OPP B2B</span>'
            )
        badges_html = " ".join(badges)
        rest_html = f" · {int(rest)}d rest" if rest is not None and not pd.isna(rest) else ""
        trend_html = f" {trend}" if trend in ("↑", "↓") else ""

        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(
                    f"""
                    <div style="border-left: 4px solid {edge_color}; padding-left: 10px; margin: -8px 0;">
                        <div style="font-weight:700;font-size:1.05rem;">{name}{trend_html}</div>
                        <div style="margin-top:4px;">{badges_html}</div>
                        <div style="color:#8b92a5;font-size:0.85rem;margin-top:6px;">
                            {team} vs {opp}{rest_html} · Line <strong style="color:#e6edf3;">{line:.1f}</strong> · Hit <strong style="color:#e6edf3;">{hit:.0f}%</strong>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            with c2:
                st.markdown(
                    f"""
                    <div style="text-align:right;color:{edge_color};font-size:1.4rem;
                                font-weight:700;line-height:1;padding-top:6px;">
                        {arrow} {abs(delta):.1f}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            if st.button("View detail", key=f"{key}_card_{idx}", use_container_width=True):
                st.session_state["selected_player"] = name
                try:
                    from activity import log, ACTION_PLAYER_VIEW
                    log(ACTION_PLAYER_VIEW, {"player": name, "via": "card"})
                except Exception:
                    pass
                st.rerun()


def show_results(df: pd.DataFrame, key: str):
    """Render results as cards (compact view) or table, based on the toggle."""
    if st.session_state.get("compact_view"):
        show_cards(df, key=key)
    else:
        show_table(df, key=key)
