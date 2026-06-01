"""Player detail page rendering for the Streamlit UI."""

import os
import datetime

import pandas as pd

import streamlit as st

from config import DEFAULT_SPORT, sport_config
from picks import add_pick
from auth import is_admin
from ui.charts import make_last_n_chart


def _is_basketball(sport_key: str) -> bool:
    return str(sport_key).startswith("basketball")


def _fmt_stat_delta(val, line):
    """Format a per-game stat cell as 'value  ↑/↓ delta vs line'."""
    if pd.isna(val):
        return ""
    base = f"{val:.0f}"
    if line is None:
        return base
    d = val - line
    if d > 0:
        return f"{base}  ↑ {d:.1f}"
    if d < 0:
        return f"{base}  ↓ {abs(d):.1f}"
    return base


def _render_avg_table(window_rows, stat_defs, show_volume):
    """Render an averages table (one row per window) over the sport's stats.

    window_rows: list of (window_label, avg_dict_or_None).
    stat_defs:   list of (stat_key, prop_type, label) from config.
    """
    cfg = {}
    if show_volume:
        cfg["MIN"] = st.column_config.NumberColumn(format="%.1f")
    data = []
    for label, src in window_rows:
        if not src:
            continue
        row = {"Window": label}
        if show_volume:
            row["MIN"] = round(float(src.get("minutes", 0) or 0), 1)
        for key, _pt, slabel in stat_defs:
            row[slabel] = round(float(src.get(key, 0) or 0), 1)
            cfg[slabel] = st.column_config.NumberColumn(format="%.1f")
        data.append(row)
    if not data:
        return
    _, mid, _ = st.columns([1, 6, 1])
    with mid:
        st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True, column_config=cfg)


def _render_gamelog_table(games, stat_defs, lines, show_volume):
    """Render a game-by-game table (Date/Opp + sport stats), colored vs lines."""
    df = pd.DataFrame(games).rename(columns={"date": "Date", "opponent": "Opp", "min": "MIN"})
    present = [(key, slabel) for key, _pt, slabel in stat_defs if key in df.columns]
    df = df.rename(columns={key: slabel for key, slabel in present})
    display_cols = ["Date", "Opp"] + (["MIN"] if show_volume else []) + [slabel for _, slabel in present]
    df = df[[c for c in display_cols if c in df.columns]]

    line_by_label = {slabel: lines.get(key) for key, slabel in present}
    fmt = {}
    if show_volume and "MIN" in df.columns:
        fmt["MIN"] = "{:.0f}"
    for slabel in line_by_label:
        fmt[slabel] = (lambda lbl: (lambda v: _fmt_stat_delta(v, line_by_label[lbl])))(slabel)

    def _color(row):
        styles = ["" for _ in row]
        for slabel, ln in line_by_label.items():
            if slabel not in row.index or ln is None:
                continue
            val = row[slabel]
            if pd.isna(val):
                continue
            idx = row.index.get_loc(slabel)
            d = val - ln
            if d > 0:
                styles[idx] = "color: #22c55e; font-weight: 600;"
            elif d < 0:
                styles[idx] = "color: #ef4444; font-weight: 600;"
        return styles

    styled = df.style.format(fmt).apply(_color, axis=1)
    _, mid, _ = st.columns([1, 6, 1])
    with mid:
        st.dataframe(styled, use_container_width=True, hide_index=True)


def render_player_detail(name: str, summaries: dict, results: dict):
    """Render a detailed view for a single player."""
    summary = summaries.get(name)
    if summary is None:
        st.warning(f"No summary data for {name}.")
        return

    # The picks board stores the active sport/date in session_state; read them
    # here so this view doesn't depend on app.py module globals.
    sport_key = st.session_state.get("sport_key", "basketball_nba")
    selected_date = st.session_state.get("selected_date", datetime.date.today())

    # Sport-aware stat set for this player's tables (config-driven).
    from config import stat_configs_for
    _sport_key = st.session_state.get("sport_key", "basketball_nba")
    _stat_defs = stat_configs_for(_sport_key)          # [(stat_key, prop_type, label)]
    _show_volume = _is_basketball(_sport_key)           # MIN column only makes sense for hoops

    if st.button("Back to picks", type="secondary"):
        st.session_state.pop("selected_player", None)
        st.rerun()

    team = summary.get("team", "")
    pos = summary.get("position", "")
    player_id = summary.get("player_id")

    # Find tonight's opponent for this player from any results df
    opponent_code = ""
    for result_df in results.values():
        if not result_df.empty:
            row = result_df[result_df["name"] == name]
            if not row.empty:
                opponent_code = row.iloc[0].get("opponent", "") or ""
                break

    from config import team_logo_url, player_photo_url
    _sk = st.session_state.get("sport_key", "basketball_nba")
    photo = player_photo_url(player_id, _sk) if player_id else ""
    team_logo = team_logo_url(team, _sk)
    opp_logo = team_logo_url(opponent_code, _sk) if opponent_code else ""

    # Hero header: player photo on left, name + team-vs-opp on right
    hero_l, hero_r = st.columns([1, 3], gap="medium")
    with hero_l:
        if photo:
            st.image(photo, width=180)
    with hero_r:
        st.title(name)
        # Team vs opponent row with logos
        matchup_html = "<div style='display:flex;align-items:center;gap:10px;margin-top:-6px;'>"
        if team_logo:
            matchup_html += f"<img src='{team_logo}' style='height:36px;width:36px;'>"
        matchup_html += f"<span style='font-weight:600;font-size:1.05rem;'>{team}</span>"
        if opponent_code:
            matchup_html += "<span style='color:#8b92a5;margin:0 4px;'>vs</span>"
            if opp_logo:
                matchup_html += f"<img src='{opp_logo}' style='height:36px;width:36px;'>"
            matchup_html += f"<span style='font-weight:600;font-size:1.05rem;'>{opponent_code}</span>"
        matchup_html += f"<span style='color:#8b92a5;margin-left:14px;'>· {pos}</span>"
        matchup_html += "</div>"
        st.markdown(matchup_html, unsafe_allow_html=True)

    # Game status banner (pregame / live / completed) for this player
    # Pull from the first available results dataframe
    for result_df in results.values():
        if not result_df.empty and "game_status" in result_df.columns:
            row = result_df[result_df["name"] == name]
            if not row.empty:
                gs = row.iloc[0]["game_status"]
                tipoff = row.iloc[0].get("tipoff", "")
                tipoff_str = ""
                if tipoff:
                    try:
                        t = pd.Timestamp(tipoff).tz_convert("America/New_York")
                        tipoff_str = t.strftime("%-I:%M %p ET") if os.name != "nt" else t.strftime("%#I:%M %p ET")
                    except Exception:
                        tipoff_str = tipoff
                if gs == "live":
                    st.error(f"LIVE — game is in progress (tipped off at {tipoff_str}). Lines may be live, not pre-game.")
                elif gs == "completed":
                    st.warning(f"Game has finished (tipped off at {tipoff_str}).")
                elif gs == "pregame" and tipoff_str:
                    st.caption(f"Tipoff: {tipoff_str}")
            break

    injury = summary.get("injury")
    if injury:
        status = injury.get("status", "")
        comment = injury.get("comment", "")
        # Color the banner by severity
        color = {
            "Out": "#ef4444", "Doubtful": "#f97316", "Questionable": "#f59e0b",
            "Day-To-Day": "#eab308", "Probable": "#84cc16",
        }.get(status, "#8b92a5")
        st.markdown(
            f"""
            <div style="background-color: {color}22; border-left: 4px solid {color};
                        padding: 10px 14px; border-radius: 6px; margin-bottom: 14px;">
                <div style="color: {color}; font-weight: 700; font-size: 0.9rem;
                            text-transform: uppercase; letter-spacing: 0.05em;">
                    {status}
                </div>
                <div style="color: #e6edf3; font-size: 0.95rem; margin-top: 2px;">
                    {comment}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- Live game tracker (only when this player's game is in progress) ---
    is_player_live = False
    for result_df in results.values():
        if result_df is not None and not result_df.empty:
            row = result_df[result_df["name"] == name]
            if not row.empty and row.iloc[0].get("game_status") == "live":
                is_player_live = True
                break
    # Live box scores come from nba_api's live scoreboard, which is NBA-only.
    if is_player_live and st.session_state.get("sport_key", "basketball_nba") == "basketball_nba":
        try:
            from scrapers.nba import get_live_box_score
            live = get_live_box_score(name)
        except Exception:
            live = None
        if live:
            lines_dict = summary.get("today_lines", {}) or {}
            cards_html = []
            stat_map = [
                ("points", "Points", "pts"),
                ("rebounds", "Rebounds", "reb"),
                ("assists", "Assists", "ast"),
                ("threes", "3PM", "threes"),
                ("steals", "Steals", "steals"),
                ("blocks", "Blocks", "blocks"),
            ]
            for stat_key, label, live_key in stat_map:
                line = lines_dict.get(stat_key)
                if line is None:
                    continue
                current = live.get(live_key, 0)
                pacing_color = "#22c55e" if current >= line else ("#ef4444" if current < line * 0.6 else "#f59e0b")
                cards_html.append(
                    f"<td style='padding:8px 14px;border-right:1px solid #2a2f3a;'>"
                    f"<div style='color:#8b92a5;font-size:0.7rem;text-transform:uppercase;'>{label}</div>"
                    f"<div style='font-size:1.4rem;font-weight:700;color:{pacing_color};'>{current} / {line:.1f}</div>"
                    f"</td>"
                )
            period = live.get("period", "?")
            clock = live.get("time_remaining", "")
            st.markdown(
                f"""
                <div style="background:#1a1d24;border:2px solid #ef4444;border-radius:8px;
                            padding:10px 16px;margin:10px 0;">
                    <div style="color:#ef4444;font-weight:800;font-size:0.85rem;
                                text-transform:uppercase;letter-spacing:0.05em;">
                        \U0001f534 LIVE &middot; Q{period} {clock}
                    </div>
                    <table cellpadding="0" cellspacing="0" style="margin-top:8px;width:100%;">
                        <tr>{''.join(cards_html) or '<td style="color:#8b92a5;">No lines on the slate.</td>'}</tr>
                    </table>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # --- Quick-jump nav (anchor links to major sections below) ---
    st.markdown(
        """
        <style>
          .stl-jump a {
            display: inline-block; padding: 4px 10px; margin-right: 6px;
            background: #1a1d24; color: #e6edf3; border-radius: 14px;
            border: 1px solid #2a2f3a; text-decoration: none;
            font-size: 0.78rem; font-weight: 600;
          }
          .stl-jump a:hover { background: #22c55e; color: #0f1115; }
        </style>
        <div class="stl-jump" style="margin: 8px 0 12px;">
          <a href="#today-s-lines">Lines</a>
          <a href="#line-movement-today">Movement</a>
          <a href="#last-10-games">Charts</a>
          <a href="#averages">Averages</a>
          <a href="#last-20-games">Last 20</a>
          <a href="#career-vs">Vs Opp</a>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Teammate injury impact context ---
    # Look up the precomputed teammates_out info from any result row
    teammates_out_label = ""
    teammates_out_names = ""
    for result_df in results.values():
        if result_df is not None and not result_df.empty:
            row = result_df[result_df["name"] == name]
            if not row.empty:
                teammates_out_label = row.iloc[0].get("teammates_out", "") or ""
                teammates_out_names = row.iloc[0].get("teammates_out_names", "") or ""
                break
    if teammates_out_label and teammates_out_names:
        st.markdown(
            f"""
            <div style="background-color: #f9731622; border-left: 4px solid #f97316;
                        padding: 10px 14px; border-radius: 6px; margin-bottom: 14px;">
                <div style="color: #f97316; font-weight: 700; font-size: 0.8rem;
                            text-transform: uppercase; letter-spacing: 0.05em;">
                    Teammate Impact &middot; {teammates_out_label}
                </div>
                <div style="color: #e6edf3; font-size: 0.95rem; margin-top: 2px;">
                    Out: {teammates_out_names}
                </div>
                <div style="color: #8b92a5; font-size: 0.8rem; margin-top: 4px;">
                    Use the <strong>What-If</strong> tab to see this player's stats specifically
                    in games where one of these teammates didn't play.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- Today's lines vs averages ---
    st.subheader("Today's Lines")
    lines = summary.get("today_lines", {})
    season_avg = summary.get("season_avg", {})
    career_avg = summary.get("career_avg", {})

    # For the averages / game-log tables, show only the stats relevant to this
    # player — the ones they have a line for, or a non-zero season average. This
    # keeps e.g. an MLB batter's page from showing empty pitcher columns (and a
    # pitcher's page from showing empty batting columns). Falls back to all stats
    # when nothing qualifies (so the tables are never empty).
    _table_defs = [
        d for d in _stat_defs
        if (d[0] in lines) or (float(season_avg.get(d[0], 0) or 0) > 0)
    ] or _stat_defs

    # Only show stats the player has a line for (so the layout stays clean
    # when a player doesn't have an obscure prop). Driven by the sport's config:
    # (stat_key, label, game_row_key) — the game-row key equals the stat key.
    STAT_DISPLAY = [(key, label, key) for key, _pt, label in _stat_defs]
    active_stats = [s for s in STAT_DISPLAY if lines.get(s[0]) is not None]
    if not active_stats:
        active_stats = STAT_DISPLAY[:3]  # default to PTS/REB/AST if no lines

    n_cols = min(len(active_stats), 4)
    cols = st.columns(n_cols)
    pick_tracking_on = st.session_state.get("pick_tracking", False)
    pick_date = st.session_state.get("selected_date", datetime.date.today())
    for i, (stat_key, label, _) in enumerate(active_stats):
        with cols[i % n_cols]:
            line = lines.get(stat_key)
            s_avg = season_avg.get(stat_key, 0)
            c_avg = career_avg.get(stat_key, 0)
            if line is None:
                st.metric(label, "—", help="No line for this prop")
            else:
                delta = s_avg - line
                st.metric(label, f"Line: {line}", delta=f"{delta:+.1f} vs season avg")
                st.caption(f"Season: {s_avg:.1f}  |  Career: {c_avg:.1f}")
                # Always-available "Add to parlay" buttons (separate from manual pick saving)
                parlay_cols = st.columns(2)
                with parlay_cols[0]:
                    if st.button("➕ Parlay Over", key=f"parlay_over_{stat_key}", use_container_width=True):
                        leg = {
                            "player": name, "stat": stat_key, "side": "over",
                            "line": float(line),
                            "team": summary.get("team", ""), "opponent": opponent_code,
                            "hit_pct": float(player_rows.iloc[0].get("hit%", 50.0)) if 'player_rows' in dir() and player_rows is not None and not player_rows.empty else 50.0,
                            "confidence": None,
                            "line_odds": -110,
                        }
                        # Pull confidence from the relevant result df
                        rdf = results.get(stat_key)
                        if rdf is not None and not rdf.empty:
                            m = rdf[rdf["name"] == name]
                            if not m.empty:
                                leg["hit_pct"] = float(m.iloc[0].get("hit%") or 50.0)
                                leg["confidence"] = float(m.iloc[0].get("confidence") or 0.0)
                        builder = st.session_state.setdefault("_parlay_builder", [])
                        builder.append(leg)
                        st.toast(f"Added to parlay: {name} OVER {line} {label} ({len(builder)} legs)")
                with parlay_cols[1]:
                    if st.button("➕ Parlay Under", key=f"parlay_under_{stat_key}", use_container_width=True):
                        leg = {
                            "player": name, "stat": stat_key, "side": "under",
                            "line": float(line),
                            "team": summary.get("team", ""), "opponent": opponent_code,
                            "hit_pct": 50.0,
                            "confidence": None,
                            "line_odds": -110,
                        }
                        rdf = results.get(stat_key)
                        if rdf is not None and not rdf.empty:
                            m = rdf[rdf["name"] == name]
                            if not m.empty:
                                # For unders, the "hit pct" is 100 - hit% (probability of going under)
                                leg["hit_pct"] = 100 - float(m.iloc[0].get("hit%") or 50.0)
                                leg["confidence"] = float(m.iloc[0].get("confidence") or 0.0)
                        builder = st.session_state.setdefault("_parlay_builder", [])
                        builder.append(leg)
                        st.toast(f"Added to parlay: {name} UNDER {line} {label} ({len(builder)} legs)")

                if pick_tracking_on:
                    btn_cols = st.columns(2)
                    with btn_cols[0]:
                        if st.button(f"Over", key=f"pick_over_{stat_key}", use_container_width=True):
                            add_pick(
                                date=pick_date, player=name, stat=stat_key,
                                line=line, side="over",
                                team=summary.get("team", ""),
                                opponent=summary.get("opponent", "") if isinstance(summary.get("opponent", ""), str) else "",
                            )
                            try:
                                from activity import log, ACTION_SAVE_PICK
                                log(ACTION_SAVE_PICK, {"player": name, "stat": stat_key, "side": "over", "line": line})
                            except Exception:
                                pass
                            st.toast(f"Saved: {name} OVER {line} {label}")
                    with btn_cols[1]:
                        if st.button(f"Under", key=f"pick_under_{stat_key}", use_container_width=True):
                            add_pick(
                                date=pick_date, player=name, stat=stat_key,
                                line=line, side="under",
                                team=summary.get("team", ""),
                                opponent=summary.get("opponent", "") if isinstance(summary.get("opponent", ""), str) else "",
                            )
                            try:
                                from activity import log, ACTION_SAVE_PICK
                                log(ACTION_SAVE_PICK, {"player": name, "stat": stat_key, "side": "under", "line": line})
                            except Exception:
                                pass
                            st.toast(f"Saved: {name} UNDER {line} {label}")

    # --- Tracked book-line history (only after we've accumulated some) ---
    try:
        from prop_history import get_player_line_history
        history_rows = []
        for stat_key, label, _ in active_stats:
            line = lines.get(stat_key)
            if line is None:
                continue
            hist = get_player_line_history(name, stat_key, near_line=float(line))
            if hist.get("available") and hist.get("all_games", 0) > 0:
                all_n = hist["all_games"]
                all_o = hist["all_overs"]
                all_pct = (all_o / all_n * 100) if all_n else 0
                near_n = hist["near_games"]
                near_o = hist["near_overs"]
                near_pct = (near_o / near_n * 100) if near_n else 0
                history_rows.append({
                    "Stat": label,
                    "Tonight's Line": float(line),
                    "Beat all-time": f"{all_o}/{all_n} ({all_pct:.0f}%)",
                    f"Beat near {line:g} (±1)": f"{near_o}/{near_n} ({near_pct:.0f}%)" if near_n else "—",
                })
        if history_rows:
            st.subheader("Tracked book-line history")
            st.caption(
                "How often this player has actually beat their book line in past games "
                "(based on lines we've snapshotted since launch — grows over time)."
            )
            st.dataframe(
                pd.DataFrame(history_rows),
                use_container_width=True,
                hide_index=True,
            )
    except Exception:
        pass

    # --- ML model predictions (if models are trained) ---
    # XGBoost models are trained only on the NBA historical dataset, so skip
    # them for other sports (their players aren't in the model's encodings).
    _ml_supported = sport_config(st.session_state.get("sport", DEFAULT_SPORT)).get("ml_models")
    try:
        if not _ml_supported:
            raise RuntimeError("ML models not available for this sport")
        from model import predict_player_stat, load_model
        ml_rows = []
        for stat_key, label, _ in active_stats:
            line = lines.get(stat_key)
            if line is None:
                continue
            if load_model(stat_key) is None:
                continue
            recent = {
                "avg_5": float(result_df.loc[result_df["name"] == name, f"{stat_key}_5g"].iloc[0])
                    if (result_df := results.get(stat_key)) is not None and not result_df.empty and
                    not result_df[result_df["name"] == name].empty and
                    f"{stat_key}_5g" in result_df.columns else 0.0,
            }
            # Fill the other expected averages from what we have in season_avg
            recent["avg_10"] = season_avg.get(stat_key, recent.get("avg_5", 0.0))
            recent["avg_25"] = career_avg.get(stat_key, recent.get("avg_10", 0.0))
            recent["min_avg_10"] = season_avg.get("minutes", 28.0)

            pred = predict_player_stat(
                player=name,
                stat=stat_key,
                opponent=opponent_code or "",
                team=team or "",
                home=True,  # default; don't know without game_loc for tonight
                rest_days=int(summary.get("injury", {}).get("rest_days", 2) or 2),
                recent_averages=recent,
            )
            if pred is not None:
                delta = pred - line
                ml_rows.append({
                    "Stat": label, "Line": line, "Model prediction": round(pred, 1),
                    "Delta": round(delta, 1),
                    "Lean": "OVER" if delta > 0.5 else "UNDER" if delta < -0.5 else "NEUTRAL",
                })
        if ml_rows:
            st.subheader("ML model prediction")
            st.caption(
                "XGBoost trained on 320k historical box scores. Takes player, "
                "opponent, recent rolling averages, rest days, and home/away."
            )
            st.dataframe(pd.DataFrame(ml_rows), use_container_width=True, hide_index=True)
    except Exception:
        pass

    # --- Line movement today (if we've snapshotted multiple times) ---
    try:
        from prop_history import get_line_movement
        movement_entries = {}
        for stat_key, label, _ in active_stats:
            snaps = get_line_movement(name, stat_key, datetime.date.today())
            if len(snaps) >= 2:
                movement_entries[stat_key] = (label, snaps)
        if movement_entries:
            st.subheader("Line movement today")
            st.caption("How the book's line has shifted since first snapshot today.")
            mv_cols = st.columns(min(len(movement_entries), 3))
            for i, (stat_key, (label, snaps)) in enumerate(movement_entries.items()):
                with mv_cols[i % len(mv_cols)]:
                    open_line = snaps[0]["line"]
                    current = snaps[-1]["line"]
                    delta = current - open_line
                    arrow = "↑" if delta > 0 else "↓" if delta < 0 else "→"
                    color = "#22c55e" if delta > 0 else "#ef4444" if delta < 0 else "#8b92a5"
                    st.metric(
                        label,
                        f"Now {current:.1f}",
                        delta=f"{arrow} {abs(delta):.1f} from {open_line:.1f}",
                    )
                    # Mini sparkline
                    mv_df = pd.DataFrame([
                        {"t": pd.to_datetime(s["snapshot_at"]), "line": float(s["line"])}
                        for s in snaps
                    ])
                    import altair as alt
                    chart = alt.Chart(mv_df).mark_line(point=True, color=color).encode(
                        x=alt.X("t:T", title=None, axis=alt.Axis(format="%-I%p") if os.name != "nt" else alt.Axis(format="%#I%p")),
                        y=alt.Y("line:Q", title=None),
                    ).properties(height=80)
                    st.altair_chart(chart, use_container_width=True)
    except Exception:
        pass

    # --- Alternate lines lookup (admin-only, costs API credits) ---
    if is_admin():
        with st.expander("Alternate lines (SGP-friendly)"):
            st.caption(
                "Live lookup of every alt line + odds offered for this player. "
                "Costs 1 Odds API credit per market — use sparingly."
            )
            from scrapers.odds_api import get_event_alt_props, ALT_MARKET_MAP, get_events_for_date
            alt_stat_choices = list(ALT_MARKET_MAP.items())
            alt_label = st.selectbox(
                "Stat",
                options=[v for _, v in alt_stat_choices],
                key=f"alt_stat_{name}",
            )
            if st.button("Fetch alt lines", key=f"alt_fetch_{name}"):
                alt_market = next((k for k, v in alt_stat_choices if v == alt_label), None)
                # Find this player's event ID
                events_today = get_events_for_date(selected_date, sport_key)
                player_team = summary.get("team", "")
                alt_event = None
                # Iterate events and find the one whose home/away team matches
                from config import team_name_to_code
                for ev in events_today:
                    home = team_name_to_code(ev["home_team"], sport_key)
                    away = team_name_to_code(ev["away_team"], sport_key)
                    if player_team in (home, away):
                        alt_event = ev
                        break
                if alt_event is None:
                    st.error(f"Couldn't find tonight's event for {player_team}.")
                elif alt_market:
                    with st.spinner("Fetching alt lines..."):
                        alts = get_event_alt_props(alt_event["id"], name, alt_market, sport_key=sport_key)
                    if not alts:
                        st.info("No alt lines returned (may not be offered yet).")
                    else:
                        alt_df = pd.DataFrame(alts)
                        # Aggregate: best price per line across books
                        best = alt_df.loc[alt_df.groupby("line")["price"].idxmax()].sort_values("line")
                        st.dataframe(
                            best.rename(columns={"line": "Line", "price": "Best Odds", "book": "Best Book"}),
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Line": st.column_config.NumberColumn(format="%.1f"),
                                "Best Odds": st.column_config.NumberColumn(format="%+d"),
                            },
                        )

    # --- EV + Kelly calculator ---
    st.subheader("EV + Kelly calculator")
    st.caption(
        "Enter the actual book odds for each prop to see expected value per $1 "
        "staked and a suggested bet size (quarter-Kelly on a $1,000 bankroll). "
        "Uses this season's hit% as the estimated win probability."
    )
    from performance import ev_and_kelly
    bankroll = st.number_input(
        "Bankroll", min_value=10.0, value=1000.0, step=100.0, key="ev_bankroll_input",
    )

    ev_rows = []
    # Get each player's per-stat hit% from the results dict
    for stat_key, label, _ in active_stats:
        line = lines.get(stat_key)
        if line is None:
            continue
        result_df = results.get(stat_key)
        if result_df is None or result_df.empty:
            continue
        player_rows = result_df[result_df["name"] == name]
        if player_rows.empty:
            continue
        hit_pct = player_rows.iloc[0].get("hit%")
        if hit_pct is None or pd.isna(hit_pct):
            continue
        # Collect the odds input per stat
        odds_key = f"ev_odds_{name}_{stat_key}"
        odds = st.session_state.get(odds_key, -110)
        ev_rows.append({
            "stat_key": stat_key, "label": label, "line": line,
            "hit_pct": float(hit_pct), "odds": int(odds),
        })

    if ev_rows:
        ev_cols = st.columns(min(len(ev_rows), 3))
        for i, row in enumerate(ev_rows):
            with ev_cols[i % len(ev_cols)]:
                st.markdown(f"**{row['label']}** — Line {row['line']:.1f}")
                odds_val = st.number_input(
                    "Book odds (American)", value=row["odds"], step=5,
                    key=f"ev_odds_{name}_{row['stat_key']}",
                )
                result = ev_and_kelly(row["hit_pct"], int(odds_val), bankroll=bankroll)
                if not result:
                    continue
                ev_color = "#22c55e" if result["ev_per_dollar"] > 0 else "#ef4444"
                st.markdown(
                    f"""
                    <div style='background:#1a1d24;border-left:3px solid {ev_color};
                                padding:8px 12px;border-radius:4px;margin-top:4px;'>
                      <div style='font-size:0.85rem;color:#8b92a5;'>Hit {result['hit_pct']}% vs implied {result['implied_pct']}%</div>
                      <div style='font-size:0.9rem;color:{ev_color};font-weight:700;'>
                        EV: {result['ev_per_dollar']:+.3f} / $1 &nbsp;·&nbsp; Edge: {result['edge_pct']:+.1f}%
                      </div>
                      <div style='font-size:0.85rem;color:#8b92a5;margin-top:4px;'>
                        Kelly ¼: {result['kelly_quarter_pct']:.2f}% &nbsp;·&nbsp; Stake: ${result['suggested_stake']:.2f}
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # --- Sharp-book divergence (when multi-book data is present) ---
    all_books = summary.get("all_books")
    if all_books:
        # Pinnacle isn't in The Odds API's US region, but we can approximate
        # "sharp signal" by flagging props where the spread of lines across
        # books is wide — sharps tend to disagree with public books.
        st.subheader("Cross-book divergence")
        st.caption(
            "Wide spreads across books usually mean the market hasn't settled — "
            "often a sign sharps are moving lines on one side."
        )
        ab_df = pd.DataFrame(all_books)
        div_rows = []
        for prop_type, group in ab_df.groupby("type"):
            if len(group) < 2:
                continue
            spread = group["spread"].max() - group["spread"].min()
            if spread >= 0.5:
                min_book = group.loc[group["spread"].idxmin(), "book"]
                max_book = group.loc[group["spread"].idxmax(), "book"]
                div_rows.append({
                    "Stat": prop_type,
                    "Range": f"{group['spread'].min():.1f} – {group['spread'].max():.1f}",
                    "Spread": f"{spread:.1f}",
                    "Lowest": f"{min_book} ({group['spread'].min():.1f})",
                    "Highest": f"{max_book} ({group['spread'].max():.1f})",
                    "Books": len(group),
                })
        if div_rows:
            _, dv_mid, _ = st.columns([1, 4, 1])
            with dv_mid:
                st.dataframe(pd.DataFrame(div_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("No significant cross-book divergence on this slate.")

    # --- Line shopping (when multi-book data is present) ---
    if all_books:
        st.subheader("Line Shopping")
        st.caption("Best line per stat across every available US sportsbook.")
        BOOK_LABELS = {
            "draftkings": "DraftKings", "fanduel": "FanDuel", "betmgm": "BetMGM",
            "caesars": "Caesars", "betrivers": "BetRivers", "pointsbetus": "PointsBet",
            "wynnbet": "WynnBet", "unibet_us": "Unibet", "barstool": "Barstool",
        }
        books_df = pd.DataFrame(all_books)
        # Pivot so each prop type is a row and each book is a column
        for prop_type, group in books_df.groupby("type"):
            st.markdown(f"**{prop_type}**")
            display = group[["book", "spread", "price"]].rename(
                columns={"book": "Book", "spread": "Line", "price": "Odds"}
            )
            display["Book"] = display["Book"].map(lambda b: BOOK_LABELS.get(b, b))
            display = display.sort_values("Line", ascending=False).reset_index(drop=True)
            _, ls_mid, _ = st.columns([1, 4, 1])
            with ls_mid:
                st.dataframe(display, use_container_width=True, hide_index=True, column_config={
                    "Line": st.column_config.NumberColumn(format="%.1f"),
                    "Odds": st.column_config.NumberColumn(format="%+d"),
                })

    # --- Last 10 games charts ---
    last_20 = summary.get("last_20", [])
    if last_20:
        st.subheader("Last 10 Games")
        chart_stats = [s for s in active_stats if s[2] in last_20[0]]
        n_chart_cols = min(len(chart_stats), 3)
        chart_cols = st.columns(n_chart_cols) if n_chart_cols > 0 else []
        for i, (full_stat, label, game_key) in enumerate(chart_stats):
            with chart_cols[i % n_chart_cols]:
                chart = make_last_n_chart(last_20, game_key, label, lines.get(full_stat), n=10)
                if chart is not None:
                    st.altair_chart(chart, use_container_width=True)

    # --- Averages summary (season, career, home, away) ---
    # "Career" is the committed historical dataset for the NBA; for other sports
    # it's the current-season game logs (labeled accordingly) — see
    # analysis.build_player_summaries.
    st.subheader("Averages")
    home_avg = summary.get("home_avg")
    away_avg = summary.get("away_avg")
    _career_label = "Career" if summary.get("has_career") else "Recent"
    window_rows = [
        (f"This Season ({season_avg.get('games', 0)} games)", season_avg),
        (f"{_career_label} ({career_avg.get('games', 0)} games)", career_avg),
        (f"Home ({home_avg['games'] if home_avg else 0} games)", home_avg),
        (f"Away ({away_avg['games'] if away_avg else 0} games)", away_avg),
    ]
    _render_avg_table(window_rows, _table_defs, _show_volume)

    # --- Last 20 games ---
    st.subheader("Last 20 Games")
    last_20 = summary.get("last_20", [])
    if not last_20:
        st.info("No game history available for this player.")
    else:
        _render_gamelog_table(last_20, _table_defs, lines, _show_volume)

        # Hit rate over last 20 vs current line
        st.subheader("Hit Rate vs Today's Lines (Last 20)")
        hit_stats = [(label, key, key) for key, _pt, label in _stat_defs]
        active = [s for s in hit_stats if lines.get(s[1]) is not None]
        if active:
            n_cols = min(len(active), 4)
            cols = st.columns(n_cols)
            for i, (label, line_key, game_key) in enumerate(active):
                with cols[i % n_cols]:
                    line = lines.get(line_key)
                    hits = sum(1 for g in last_20 if g.get(game_key, 0) > line)
                    pct = (hits / len(last_20)) * 100
                    st.metric(label, f"{hits}/{len(last_20)}", delta=f"{pct:.0f}%")

    # --- History vs tonight's opponent ---
    vs_opp = summary.get("vs_opponent", [])
    vs_opp_avg = summary.get("vs_opponent_avg")
    if vs_opp:
        opp_code = vs_opp_avg.get("opponent") if vs_opp_avg else "opponent"
        _vs_label = "Career" if summary.get("has_career") else "This season"
        st.subheader(
            f"{_vs_label} vs {opp_code} "
            f"({len(vs_opp)} most recent · {vs_opp_avg['games'] if vs_opp_avg else 0} total)"
        )
        if vs_opp_avg:
            _render_avg_table([(f"vs {opp_code}", vs_opp_avg)], _table_defs, _show_volume)
        _render_gamelog_table(vs_opp, _table_defs, lines, _show_volume)
