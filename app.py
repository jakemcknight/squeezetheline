import os
import json
import datetime
from io import StringIO

import streamlit as st
import pandas as pd
import altair as alt

from scrapers.odds_api import get_todays_games, get_all_props, get_events_for_date
from scrapers.nba import get_current_season_stats, get_player_positions
from scrapers.basketball_ref import get_defense_by_position
from data import prepare_stats, prepare_props, DATA_DIR
from analysis import (
    analyze_stat,
    filter_strong_overs,
    filter_strong_unders,
    filter_trending_overs,
    filter_trending_unders,
    build_player_summaries,
)

LOGO_PATH = os.path.join(os.path.dirname(__file__), "assets", "logo.png")

st.set_page_config(
    page_title="Squeeze the Line",
    page_icon=LOGO_PATH if os.path.exists(LOGO_PATH) else "\U0001f3c0",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Custom CSS for branding ---
st.markdown(
    """
    <style>
        /* Import a nicer font */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        /* Header / title styling */
        h1 {
            font-weight: 800 !important;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        h2, h3 {
            font-weight: 700 !important;
            letter-spacing: -0.01em;
        }

        /* Card-like containers for metrics */
        [data-testid="stMetric"] {
            background: #1a1d24;
            border: 1px solid #2a2f3a;
            border-radius: 10px;
            padding: 16px 18px;
        }

        /* DataFrame styling */
        [data-testid="stDataFrame"] {
            border: 1px solid #2a2f3a;
            border-radius: 8px;
            overflow: hidden;
        }

        /* Sidebar polish */
        [data-testid="stSidebar"] {
            background: #141720;
            border-right: 1px solid #2a2f3a;
        }

        /* Buttons */
        .stButton > button {
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.15s ease;
        }
        .stButton > button:hover {
            transform: translateY(-1px);
        }

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
        }
        .stTabs [data-baseweb="tab"] {
            padding: 10px 18px;
            font-weight: 600;
        }

        /* Hide Streamlit branding */
        #MainMenu { visibility: hidden; }
        footer { visibility: hidden; }
        [data-testid="stDecoration"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)


# --- Password gate ---
def check_password() -> bool:
    """Returns True once the user enters the correct password."""
    expected = os.environ.get("APP_PASSWORD")
    if not expected:
        # Fall back to Streamlit secrets when running on Streamlit Cloud
        try:
            expected = st.secrets["APP_PASSWORD"]
        except Exception:
            expected = "juice"  # local development default

    if st.session_state.get("authenticated"):
        return True

    # Center the password gate
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.write("")
        st.write("")
        if os.path.exists(LOGO_PATH):
            # st.image centers better when wrapped in an inner column trio
            _, logo_mid, _ = st.columns([1, 2, 1])
            with logo_mid:
                st.image(LOGO_PATH, use_container_width=True)
        st.markdown(
            """
            <p style="text-align: center; color: #8b92a5; margin-top: 4px; margin-bottom: 32px;">
                NBA player props · data-driven picks
            </p>
            """,
            unsafe_allow_html=True,
        )
        pwd = st.text_input(
            "Password",
            type="password",
            label_visibility="collapsed",
            placeholder="Password",
        )
        if pwd:
            if pwd == expected:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Wrong password.")
    return False


if not check_password():
    st.stop()

STAT_CONFIGS = [
    ("points", "Total Points"),
    ("rebounds", "Total Rebounds"),
    ("assists", "Total Assists"),
]

DISPLAY_COLS = [
    "name", "player_url", "team-code", "opponent", "position", "spread",
    "delta", "delta_5g", "delta_10g",
    "hit%", "history_hit%",
    "rank", "std_dev", "spm",
]

CACHE_DIR = os.path.join(DATA_DIR, "daily_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(date: datetime.date) -> str:
    return os.path.join(CACHE_DIR, f"{date}.json")


def save_daily_results(events, results, summaries, date: datetime.date):
    """Save fetched results to disk."""
    payload = {
        "date": str(date),
        "events": events,
        "results": {stat: df.to_json() for stat, df in results.items()},
        "summaries": summaries,
    }
    with open(_cache_path(date), "w") as f:
        json.dump(payload, f)


def load_daily_results(date: datetime.date):
    """Load cached results from disk. Returns (events, results, summaries) or None."""
    path = _cache_path(date)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        payload = json.load(f)
    events = payload["events"]
    results = {stat: pd.read_json(StringIO(df_json)) for stat, df_json in payload["results"].items()}
    summaries = payload.get("summaries", {})
    return events, results, summaries


def fetch_fresh_data(date: datetime.date):
    """Hit all APIs and run the full analysis pipeline for the given date."""
    events = get_events_for_date(date)
    todays_games = get_todays_games(date)
    stats = get_current_season_stats()
    positions = get_player_positions()
    df = prepare_stats(stats, positions)
    props = get_all_props(date)
    props = prepare_props(props)
    defense = get_defense_by_position()

    player_urls = positions[["name", "player_url"]].drop_duplicates(subset="name")

    results = {}
    for stat, prop_type in STAT_CONFIGS:
        result = analyze_stat(stat, prop_type, df, props, todays_games, defense)
        result = result.merge(player_urls, on="name", how="left")
        results[stat] = result

    # Build per-player summaries for the detail view
    all_players = sorted(set(props["name"].dropna().unique()))
    summaries = build_player_summaries(all_players, df, props)

    return events, results, summaries


COLUMN_CONFIG = {
    "name": st.column_config.TextColumn("Player"),
    "player_url": st.column_config.LinkColumn("Profile", display_text="NBA.com"),
    "team-code": st.column_config.TextColumn("Team"),
    "opponent": st.column_config.TextColumn("Opp"),
    "position": st.column_config.TextColumn("Pos"),
    "spread": st.column_config.NumberColumn("Line", format="%.1f"),
    "delta": st.column_config.NumberColumn("Delta", format="%+.1f"),
    "delta_5g": st.column_config.NumberColumn("Delta 5G", format="%+.1f"),
    "delta_10g": st.column_config.NumberColumn("Delta 10G", format="%+.1f"),
    "hit%": st.column_config.ProgressColumn("Hit %", min_value=0, max_value=100, format="%.0f%%", width="medium"),
    "history_hit%": st.column_config.ProgressColumn("Hist Hit %", min_value=0, max_value=100, format="%.0f%%", width="medium"),
    "rank": st.column_config.NumberColumn("Def Rank", format="%.0f"),
    "std_dev": st.column_config.NumberColumn("Std Dev", format="%.1f"),
    "spm": st.column_config.NumberColumn("SPM", format="%.2f"),
}


def show_table(df: pd.DataFrame, key: str):
    """Display a results table with row selection — selecting a row opens the player detail.

    The table auto-sizes each column to its content and is centered on the page.
    """
    left, mid, right = st.columns([1, 12, 1])
    with mid:
        event = st.dataframe(
            df,
            column_config=COLUMN_CONFIG,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key=key,
        )
    if event.selection.rows:
        idx = event.selection.rows[0]
        st.session_state["selected_player"] = df.iloc[idx]["name"]
        st.rerun()


def make_last_n_chart(last_games: list[dict], stat_key: str, stat_label: str, line: float | None, n: int = 10):
    """Build a bar chart of a player's last N games for one stat, with a prop line overlay.

    Bars are green if the stat > line, blue if <= line.
    """
    if not last_games:
        return None
    # last_games is most-recent-first; take N most recent then reverse for chronological order
    recent = list(reversed(last_games[:n]))
    df = pd.DataFrame(recent)
    df["game_num"] = range(1, len(df) + 1)
    df["label"] = df.apply(lambda r: f"{r['date']}\nvs {r['opponent']}", axis=1)
    if line is not None:
        df["hit"] = df[stat_key] > line

    bars = alt.Chart(df).mark_bar(size=28).encode(
        x=alt.X("game_num:O", title=None, axis=alt.Axis(labels=False, ticks=False)),
        y=alt.Y(f"{stat_key}:Q", title=stat_label),
        color=(
            alt.Color(
                "hit:N",
                scale=alt.Scale(domain=[True, False], range=["#22c55e", "#ef4444"]),
                legend=None,
            )
            if line is not None
            else alt.value("#3b82f6")
        ),
        tooltip=[
            alt.Tooltip("date:N", title="Date"),
            alt.Tooltip("opponent:N", title="Opp"),
            alt.Tooltip(f"{stat_key}:Q", title=stat_label),
        ],
    )

    layers = [bars]
    if line is not None:
        line_df = pd.DataFrame({"line": [line]})
        rule = alt.Chart(line_df).mark_rule(
            color="white", strokeDash=[6, 4], size=2
        ).encode(y="line:Q")
        label = alt.Chart(line_df).mark_text(
            align="left", baseline="middle", dx=5, color="white"
        ).encode(y="line:Q", text=alt.value(f"Line: {line}"))
        layers.extend([rule, label])

    title = f"{stat_label} — Last {len(recent)}"
    if line is not None:
        title += f"  (Line: {line})"
    return alt.layer(*layers).properties(height=220, title=title)


def render_player_detail(name: str, summaries: dict, results: dict):
    """Render a detailed view for a single player."""
    summary = summaries.get(name)
    if summary is None:
        st.warning(f"No summary data for {name}.")
        return

    if st.button("Back to picks", type="secondary"):
        st.session_state.pop("selected_player", None)
        st.rerun()

    team = summary.get("team", "")
    pos = summary.get("position", "")
    st.title(name)
    st.caption(f"{team}  |  {pos}")

    # --- Today's lines vs averages ---
    st.subheader("Today's Lines")
    lines = summary.get("today_lines", {})
    season_avg = summary.get("season_avg", {})
    career_avg = summary.get("career_avg", {})

    cols = st.columns(3)
    for i, stat in enumerate(["points", "rebounds", "assists"]):
        with cols[i]:
            line = lines.get(stat)
            s_avg = season_avg.get(stat, 0)
            c_avg = career_avg.get(stat, 0)
            if line is None:
                st.metric(stat.capitalize(), "—", help="No line for this prop")
            else:
                delta = s_avg - line
                st.metric(
                    stat.capitalize(),
                    f"Line: {line}",
                    delta=f"{delta:+.1f} vs season avg",
                )
                st.caption(f"Season: {s_avg:.1f}  |  Career: {c_avg:.1f}")

    # --- Last 10 games charts ---
    last_20 = summary.get("last_20", [])
    if last_20:
        st.subheader("Last 10 Games")
        chart_cols = st.columns(3)
        for i, (stat_key, stat_label, full_stat) in enumerate([
            ("pts", "Points", "points"),
            ("reb", "Rebounds", "rebounds"),
            ("ast", "Assists", "assists"),
        ]):
            with chart_cols[i]:
                chart = make_last_n_chart(last_20, stat_key, stat_label, lines.get(full_stat), n=10)
                if chart is not None:
                    st.altair_chart(chart, use_container_width=True)

    # --- Averages summary ---
    st.subheader("Averages")
    avg_df = pd.DataFrame([
        {
            "Window": f"This Season ({season_avg.get('games', 0)} games)",
            "MIN": season_avg.get("minutes", 0),
            "PTS": season_avg.get("points", 0),
            "REB": season_avg.get("rebounds", 0),
            "AST": season_avg.get("assists", 0),
        },
        {
            "Window": f"Career ({career_avg.get('games', 0)} games)",
            "MIN": career_avg.get("minutes", 0),
            "PTS": career_avg.get("points", 0),
            "REB": career_avg.get("rebounds", 0),
            "AST": career_avg.get("assists", 0),
        },
    ])
    _, avg_mid, _ = st.columns([1, 6, 1])
    with avg_mid:
        st.dataframe(
            avg_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "MIN": st.column_config.NumberColumn(format="%.1f"),
                "PTS": st.column_config.NumberColumn(format="%.1f"),
                "REB": st.column_config.NumberColumn(format="%.1f"),
                "AST": st.column_config.NumberColumn(format="%.1f"),
            },
        )

    # --- Last 20 games ---
    st.subheader("Last 20 Games")
    last_20 = summary.get("last_20", [])
    if not last_20:
        st.info("No game history available for this player.")
    else:
        games_df = pd.DataFrame(last_20)
        games_df = games_df.rename(columns={
            "date": "Date", "opponent": "Opp", "min": "MIN",
            "pts": "PTS", "reb": "REB", "ast": "AST",
        })
        # Color hit/miss vs today's lines
        def highlight_hits(row):
            styles = [""] * len(row)
            for col, stat in [("PTS", "points"), ("REB", "rebounds"), ("AST", "assists")]:
                line = lines.get(stat)
                if line is not None and col in row.index:
                    val = row[col]
                    idx = row.index.get_loc(col)
                    if val > line:
                        styles[idx] = "background-color: rgba(0, 200, 0, 0.25)"
                    elif val < line:
                        styles[idx] = "background-color: rgba(200, 0, 0, 0.25)"
            return styles

        styled = games_df.style.apply(highlight_hits, axis=1).format({
            "MIN": "{:.0f}", "PTS": "{:.0f}", "REB": "{:.0f}", "AST": "{:.0f}",
        })
        _, games_mid, _ = st.columns([1, 6, 1])
        with games_mid:
            st.dataframe(styled, use_container_width=True, hide_index=True)

        # Hit rate over last 20 vs current line
        st.subheader("Hit Rate vs Today's Lines (Last 20)")
        cols = st.columns(3)
        for i, (label, stat, col) in enumerate([
            ("Points", "points", "PTS"),
            ("Rebounds", "rebounds", "REB"),
            ("Assists", "assists", "AST"),
        ]):
            with cols[i]:
                line = lines.get(stat)
                if line is None:
                    st.metric(label, "—")
                else:
                    hits = sum(1 for g in last_20 if g[col.lower().replace("pts","pts")] > line)
                    # quick correct mapping
                    key_map = {"PTS":"pts","REB":"reb","AST":"ast"}
                    hits = sum(1 for g in last_20 if g[key_map[col]] > line)
                    pct = (hits / len(last_20)) * 100
                    st.metric(label, f"{hits}/{len(last_20)}", delta=f"{pct:.0f}%")


# --- Header ---
header_col, date_col = st.columns([3, 1])
with header_col:
    logo_col, tag_col = st.columns([1, 2], gap="small")
    with logo_col:
        st.image(LOGO_PATH, width=160)
    with tag_col:
        st.markdown(
            """
            <div style="padding-top: 38px;">
                <p style="margin: 0; color: #8b92a5; font-size: 1rem; letter-spacing: 0.02em;">
                    NBA player props · data-driven picks
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

with st.sidebar:
    selected_date = st.date_input("Game Date", value=datetime.date.today())

with date_col:
    st.markdown(
        f"""
        <div style="text-align: right; padding-top: 38px; color: #8b92a5;">
            <div style="font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.1em;">Slate</div>
            <div style="color: #e6edf3; font-size: 1.1rem; font-weight: 600;">
                {selected_date.strftime("%a, %b %d")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.write("")  # small spacer

# --- Load cached data or prompt to fetch ---
cached = load_daily_results(selected_date)

with st.sidebar:
    if cached:
        st.success(f"Using saved data for {selected_date}.")
    else:
        st.warning(f"No data for {selected_date} yet.")

    if st.button("Fetch / Refresh Data", type="primary", use_container_width=True):
        with st.spinner("Fetching from NBA.com + The Odds API..."):
            events, results, summaries = fetch_fresh_data(selected_date)
            save_daily_results(events, results, summaries, selected_date)
            st.cache_data.clear()
            st.session_state.pop("selected_player", None)
        st.rerun()

    # Show a backfill prompt only when no historical data (compressed or raw) exists
    from data import HISTORICAL_DATA_PATH, HISTORICAL_DATA_GZ_PATH
    if not os.path.exists(HISTORICAL_DATA_PATH) and not os.path.exists(HISTORICAL_DATA_GZ_PATH):
        st.divider()
        st.warning("Historical data is missing. Career averages and historical hit% will be unavailable until backfilled.")
        if st.button("Run Historical Backfill", use_container_width=True):
            from backfill import backfill
            with st.spinner("Backfilling 2014-15 to current season... ~1 minute."):
                backfill()
            st.success("Backfill complete.")
            st.rerun()

if cached is None:
    st.info("No data for this date. Click **Fetch / Refresh Data** in the sidebar.")
    st.stop()

events, results, summaries = cached

# --- Player Detail View ---
if "selected_player" in st.session_state and st.session_state["selected_player"]:
    render_player_detail(st.session_state["selected_player"], summaries, results)
    st.stop()

# --- Today's games ---
with st.expander("Today's Games", expanded=False):
    cols = st.columns(min(len(events), 4) if events else 1)
    for i, event in enumerate(events):
        with cols[i % len(cols)]:
            st.markdown(f"**{event['away_team']}**  \n@ {event['home_team']}")

st.divider()

# --- Stat selector ---
stat_tab = st.radio(
    "Stat", ["Points", "Rebounds", "Assists"],
    horizontal=True, label_visibility="collapsed",
)
stat = stat_tab.lower()
result = results[stat]

# --- Sidebar filters ---
with st.sidebar:
    st.header("Filters")

    teams = sorted(result["team-code"].dropna().unique())
    selected_teams = st.multiselect("Team", teams, default=[])

    opponents = sorted(result["opponent"].dropna().unique())
    selected_opponents = st.multiselect("Opponent", opponents, default=[])

    min_hit = st.slider("Min current hit %", 0, 100, 0)
    max_hit = st.slider("Max current hit %", 0, 100, 100)

    min_spread = st.number_input("Min spread", value=0.0, step=0.5)

# --- Apply filters ---
filtered = result.copy()
if selected_teams:
    filtered = filtered[filtered["team-code"].isin(selected_teams)]
if selected_opponents:
    filtered = filtered[filtered["opponent"].isin(selected_opponents)]
filtered = filtered[
    (filtered["hit%"] >= min_hit)
    & (filtered["hit%"] <= max_hit)
    & (filtered["spread"] >= min_spread)
]

# --- Display columns (only show what exists) ---
show_cols = [c for c in DISPLAY_COLS if c in filtered.columns]

st.caption("Click a row to see player details.")

# --- Tabs for different views ---
tab_strong_o, tab_trend_o, tab_strong_u, tab_trend_u, tab_all = st.tabs([
    "Strong Overs", "Trending Overs", "Strong Unders", "Trending Unders", "All Players",
])

with tab_strong_o:
    df_view = filter_strong_overs(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No strong overs found with current filters.")
    else:
        st.caption(f"All deltas positive + both hit rates > 50% ({len(df_view)} players)")
        show_table(df_view, key=f"strong_o_{stat}")

with tab_trend_o:
    df_view = filter_trending_overs(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No trending overs found with current filters.")
    else:
        st.caption(f"All deltas positive ({len(df_view)} players)")
        show_table(df_view, key=f"trend_o_{stat}")

with tab_strong_u:
    df_view = filter_strong_unders(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No strong unders found with current filters.")
    else:
        st.caption(f"All deltas negative + both hit rates < 50% ({len(df_view)} players)")
        show_table(df_view, key=f"strong_u_{stat}")

with tab_trend_u:
    df_view = filter_trending_unders(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No trending unders found with current filters.")
    else:
        st.caption(f"All deltas negative ({len(df_view)} players)")
        show_table(df_view, key=f"trend_u_{stat}")

with tab_all:
    df_view = filtered[show_cols].sort_values("hit%", ascending=False).reset_index(drop=True)
    st.caption(f"{len(df_view)} players")
    show_table(df_view, key=f"all_{stat}")
