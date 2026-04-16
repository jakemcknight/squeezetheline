import os
import json
import datetime
from io import StringIO

import streamlit as st
import pandas as pd
import altair as alt

from scrapers.odds_api import get_todays_games, get_all_props, get_events_for_date, get_game_times, OddsAPIQuotaError
from scrapers.nba import get_current_season_stats, get_player_positions
from scrapers.basketball_ref import get_defense_by_position
from scrapers.injuries import get_injury_report
from picks import (
    add_pick,
    remove_pick,
    load_picks,
    grade_picks,
    picks_summary,
)
from auto_picks import fetch_auto_picks, summarize_picks as auto_summarize_picks
from auto_runner import run_daily_jobs
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

        /* ==========================
           Mobile (< 768px) overrides
           ========================== */
        @media (max-width: 768px) {
            /* Reduce main container padding to maximize screen usage */
            .main .block-container {
                padding: 1rem 0.75rem !important;
                max-width: 100% !important;
            }

            /* Smaller heading sizes */
            h1 { font-size: 1.6rem !important; }
            h2 { font-size: 1.25rem !important; }
            h3 { font-size: 1.05rem !important; }

            /* Full-width buttons on mobile for easier tap targets */
            .stButton > button {
                width: 100% !important;
                padding: 12px !important;
            }

            /* Tabs: smaller padding so all 5 fit */
            .stTabs [data-baseweb="tab"] {
                padding: 8px 10px !important;
                font-size: 0.85rem !important;
            }
            .stTabs [data-baseweb="tab-list"] {
                gap: 2px !important;
                overflow-x: auto;
            }

            /* Metric cards: tighter padding */
            [data-testid="stMetric"] {
                padding: 10px 12px !important;
            }
            [data-testid="stMetric"] label {
                font-size: 0.8rem !important;
            }
            [data-testid="stMetricValue"] {
                font-size: 1.1rem !important;
            }

            /* DataFrames: shrink font so more fits + proper horizontal scroll */
            [data-testid="stDataFrame"] {
                font-size: 0.8rem !important;
            }

            /* Tighten altair chart padding */
            .vega-embed {
                padding: 0 !important;
            }

            /* Stack all horizontal column layouts vertically on mobile so each
               column gets full width. This makes 3-across metric cards, chart
               rows, and centering wrappers work sensibly on narrow screens. */
            [data-testid="stHorizontalBlock"] {
                flex-direction: column !important;
                gap: 0.5rem !important;
            }
            [data-testid="stHorizontalBlock"] > div {
                width: 100% !important;
                min-width: 0 !important;
                flex: 1 1 100% !important;
            }
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# --- Auth gate (Supabase) ---
from auth import (
    sign_in as auth_sign_in,
    sign_up as auth_sign_up,
    is_authenticated,
    is_admin,
    current_user,
    sign_out as auth_sign_out,
    get_supabase,
    get_supabase_diagnostic,
)


def render_auth_gate() -> bool:
    """Returns True once the user is signed in. Otherwise renders the
    sign-in / sign-up form and returns False."""
    if is_authenticated():
        return True

    if get_supabase() is None:
        st.error(
            "Auth is not configured. The site admin needs to set "
            "`SUPABASE_URL` and `SUPABASE_ANON_KEY` in Streamlit secrets."
        )
        with st.expander("Diagnostic"):
            st.json(get_supabase_diagnostic())
        st.stop()

    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.write("")
        st.write("")
        if os.path.exists(LOGO_PATH):
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

        # Show a success banner if the user just signed up
        just_signed_up = st.session_state.pop("just_signed_up", None)
        if just_signed_up:
            st.success(
                f"Account created for **{just_signed_up}**! "
                "Check your email and click the confirmation link, then sign in below."
            )

        # Pick the active tab without writing to a widget's session_state key
        # (Streamlit forbids modifying a widget's state after it's instantiated.)
        # The "_force_signin_tab" flag from a successful signup wins this turn,
        # otherwise we honor whatever the user had selected last.
        force_signin = st.session_state.pop("_force_signin_tab", False)
        default_idx = 0  # "Sign in" by default
        if not force_signin:
            last_mode = st.session_state.get("_last_auth_mode", "Sign in")
            default_idx = 0 if last_mode == "Sign in" else 1

        mode = st.radio(
            "auth_mode_radio",
            ["Sign in", "Sign up"],
            horizontal=True,
            label_visibility="collapsed",
            index=default_idx,
            key="auth_mode_widget",
        )
        st.session_state["_last_auth_mode"] = mode

        if mode == "Sign in":
            with st.form("signin_form", clear_on_submit=False):
                default_email = just_signed_up or st.session_state.get("signin_email", "")
                email = st.text_input("Email", value=default_email, key="signin_email")
                pwd = st.text_input("Password", type="password", key="signin_pwd")
                submitted = st.form_submit_button("Sign in", use_container_width=True, type="primary")
            if submitted:
                if not email or not pwd:
                    st.error("Email and password required.")
                else:
                    ok, msg = auth_sign_in(email.strip(), pwd)
                    if ok:
                        st.rerun()
                    else:
                        st.error(msg)
        else:  # Sign up
            with st.form("signup_form", clear_on_submit=False):
                email = st.text_input("Email", key="signup_email")
                pwd = st.text_input("Password (8+ chars)", type="password", key="signup_pwd")
                pwd2 = st.text_input("Confirm password", type="password", key="signup_pwd2")
                submitted = st.form_submit_button("Create account", use_container_width=True, type="primary")
            if submitted:
                if not email or not pwd:
                    st.error("Email and password required.")
                elif len(pwd) < 8:
                    st.error("Password must be at least 8 characters.")
                elif pwd != pwd2:
                    st.error("Passwords don't match.")
                else:
                    ok, msg = auth_sign_up(email.strip(), pwd)
                    if ok:
                        # Use a non-widget key as the signal so we can flip
                        # the radio's default index on the next render
                        st.session_state["just_signed_up"] = email.strip()
                        st.session_state["_force_signin_tab"] = True
                        st.rerun()
                    else:
                        st.error(msg)
    return False


if not render_auth_gate():
    st.stop()


# --- Run daily auto-jobs (refresh + grade) — admin-only ---
# The auto-pipeline takes 30-60 seconds per step because it hits NBA.com.
# Only admins trigger it; regular users just see whatever is already in
# Supabase so their login is instant. Only runs once per session.
if is_admin() and not st.session_state.get("_daily_jobs_attempted"):
    st.session_state["_daily_jobs_attempted"] = True
    # Quick checks first (just Supabase reads) so we can skip the full
    # pipeline load spinner when nothing actually needs to run.
    from auto_runner import maybe_auto_refresh, maybe_auto_grade
    with st.spinner("Checking for daily auto-picks..."):
        refresh_status = maybe_auto_refresh()
    if refresh_status.get("action") == "ran":
        st.toast(f"Auto-generated {refresh_status.get('saved', 0)} picks for today.")
    with st.spinner("Checking for pending picks to grade..."):
        grade_status = maybe_auto_grade()
    if grade_status.get("action") == "ran":
        st.toast(f"Graded {grade_status.get('graded', 0)} pending picks.")
    st.session_state["_last_job_status"] = {"refresh": refresh_status, "grade": grade_status}

STAT_CONFIGS = [
    ("points", "Total Points"),
    ("rebounds", "Total Rebounds"),
    ("assists", "Total Assists"),
    ("pra", "Total PRA"),
    ("threes", "Total 3PM"),
    ("steals", "Total Steals"),
    ("blocks", "Total Blocks"),
]

DISPLAY_COLS = [
    "name", "trend", "last10", "last10_hits", "game_status", "status_short", "starter", "player_url", "team-code", "opponent", "position", "spread",
    "delta", "delta_5g", "delta_10g",
    "hit%", "history_hit%",
    "vs_opp_season", "vs_opp_career",
    "rank", "rest_days", "b2b", "opp_rest", "opp_b2b", "std_dev", "spm",
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


def fetch_fresh_data(date: datetime.date, all_books: bool = False):
    """Hit all APIs and run the full analysis pipeline for the given date.

    When `all_books=True`, also pull props from every US bookmaker so the
    player detail page can show line shopping comparisons.
    """
    events = get_events_for_date(date)
    todays_games = get_todays_games(date)
    stats = get_current_season_stats()
    positions = get_player_positions()
    df = prepare_stats(stats, positions)
    raw_props = get_all_props(date, all_books=all_books)
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
    injuries = get_injury_report()
    injury_join = (
        injuries[["name", "status_short", "comment"]].drop_duplicates(subset="name")
        if not injuries.empty
        else pd.DataFrame(columns=["name", "status_short", "comment"])
    )

    # Game tipoff times per team (so we can flag in-progress / completed games)
    game_times = get_game_times(date)
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
    for stat, prop_type in STAT_CONFIGS:
        result = analyze_stat(stat, prop_type, df, props, todays_games, defense, game_date=date)
        result = result.merge(player_urls, on="name", how="left")
        if not injury_join.empty:
            result = result.merge(injury_join, on="name", how="left")
        # Replace NaN in injury columns with empty strings so healthy players
        # show a clean blank cell instead of "None"
        for col in ("status_short", "comment"):
            if col in result.columns:
                result[col] = result[col].fillna("")
        # Tag each row with its game's status (pregame / live / completed).
        # Classify once per team then map onto the column, which is pandas 3.x safe.
        classifications = {t: _classify(t) for t in result["team-code"].unique()}
        result["game_status"] = result["team-code"].map(lambda t: classifications.get(t, {}).get("game_status", "unknown"))
        result["tipoff"] = result["team-code"].map(lambda t: classifications.get(t, {}).get("tipoff", ""))
        results[stat] = result

    # Build per-player summaries for the detail view
    all_players = sorted(set(props["name"].dropna().unique()))
    summaries = build_player_summaries(all_players, df, props, todays_games=todays_games)

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


COLUMN_CONFIG = {
    "name": st.column_config.TextColumn("Player"),
    "trend": st.column_config.TextColumn("Trend", help="↑ last-5 avg > last-10 avg (trending up), ↓ trending down, → flat"),
    "last10": st.column_config.BarChartColumn("Last 10", help="Stat values across the player's last 10 games (most recent on right)"),
    "last10_hits": st.column_config.TextColumn("Hit/Miss", help="Each square = one of the last 10 games vs tonight's line. Green = beat the line, red = missed."),
    "game_status": st.column_config.TextColumn("Game", help="pregame / live / completed — live games show in-game lines that aren't pre-game lines"),
    "status_short": st.column_config.TextColumn("Inj", help="Injury status (OUT/DBT/Q/DTD/PROB)"),
    "starter": st.column_config.CheckboxColumn("Starter", help="Top 5 mpg on team in last 10 games"),
    "player_url": st.column_config.LinkColumn("Profile", display_text="NBA.com"),
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
        st.session_state["selected_player"] = df.iloc[idx]["name"]
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
    # Short date label like "4/13" for the x-axis
    df["date_short"] = pd.to_datetime(df["date"]).dt.strftime("%-m/%-d") if os.name != "nt" else pd.to_datetime(df["date"]).dt.strftime("%#m/%#d")
    df["label"] = df.apply(lambda r: f"{r['date']}\nvs {r['opponent']}", axis=1)
    if line is not None:
        df["hit"] = df[stat_key] > line

    bars = alt.Chart(df).mark_bar(size=28).encode(
        x=alt.X("date_short:N", title=None, sort=list(df["date_short"]), axis=alt.Axis(labelAngle=0)),
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
    photo = player_photo_url(player_id) if player_id else ""
    team_logo = team_logo_url(team)
    opp_logo = team_logo_url(opponent_code) if opponent_code else ""

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

    # --- Today's lines vs averages ---
    st.subheader("Today's Lines")
    lines = summary.get("today_lines", {})
    season_avg = summary.get("season_avg", {})
    career_avg = summary.get("career_avg", {})

    # Only show stats the player has a line for (so the layout stays clean
    # when a player doesn't have an obscure prop like blocks or 3PM).
    STAT_DISPLAY = [
        ("points", "Points", "pts"),
        ("rebounds", "Rebounds", "reb"),
        ("assists", "Assists", "ast"),
        ("pra", "PRA", "pra"),
        ("threes", "3PM", "threes"),
        ("steals", "Steals", "steals"),
        ("blocks", "Blocks", "blocks"),
    ]
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
                            st.toast(f"Saved: {name} OVER {line} {label}")
                    with btn_cols[1]:
                        if st.button(f"Under", key=f"pick_under_{stat_key}", use_container_width=True):
                            add_pick(
                                date=pick_date, player=name, stat=stat_key,
                                line=line, side="under",
                                team=summary.get("team", ""),
                                opponent=summary.get("opponent", "") if isinstance(summary.get("opponent", ""), str) else "",
                            )
                            st.toast(f"Saved: {name} UNDER {line} {label}")

    # --- Line shopping (when multi-book data is present) ---
    all_books = summary.get("all_books")
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
    st.subheader("Averages")
    home_avg = summary.get("home_avg")
    away_avg = summary.get("away_avg")

    def _avg_row(label: str, src: dict | None):
        if not src:
            return None
        return {
            "Window": label,
            "MIN": src.get("minutes", 0),
            "PTS": src.get("points", 0),
            "REB": src.get("rebounds", 0),
            "AST": src.get("assists", 0),
            "PRA": src.get("pra", 0),
            "3PM": src.get("threes", 0),
            "STL": src.get("steals", 0),
            "BLK": src.get("blocks", 0),
        }

    avg_rows = [
        _avg_row(f"This Season ({season_avg.get('games', 0)} games)", season_avg),
        _avg_row(f"Career ({career_avg.get('games', 0)} games)", career_avg),
        _avg_row(f"Home ({home_avg['games'] if home_avg else 0} games)", home_avg),
        _avg_row(f"Away ({away_avg['games'] if away_avg else 0} games)", away_avg),
    ]
    avg_df = pd.DataFrame([r for r in avg_rows if r is not None])
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
                "PRA": st.column_config.NumberColumn(format="%.1f"),
                "3PM": st.column_config.NumberColumn(format="%.1f"),
                "STL": st.column_config.NumberColumn(format="%.1f"),
                "BLK": st.column_config.NumberColumn(format="%.1f"),
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

        # Format each stat cell as "value  ↑/↓ delta"
        def format_stat(val, line):
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

        # Color each stat cell based on whether it beat the line
        def color_stat(row):
            styles = ["" for _ in row]
            for stat_col, line_key in [("PTS", "points"), ("REB", "rebounds"), ("AST", "assists")]:
                if stat_col not in row.index:
                    continue
                val = row[stat_col]
                line = lines.get(line_key)
                if line is None or pd.isna(val):
                    continue
                idx = row.index.get_loc(stat_col)
                d = val - line
                if d > 0:
                    styles[idx] = "color: #22c55e; font-weight: 600;"
                elif d < 0:
                    styles[idx] = "color: #ef4444; font-weight: 600;"
            return styles

        styled = (
            games_df.style
            .format({
                "MIN": "{:.0f}",
                "PTS": lambda v: format_stat(v, lines.get("points")),
                "REB": lambda v: format_stat(v, lines.get("rebounds")),
                "AST": lambda v: format_stat(v, lines.get("assists")),
            })
            .apply(color_stat, axis=1)
        )
        _, games_mid, _ = st.columns([1, 6, 1])
        with games_mid:
            st.dataframe(styled, use_container_width=True, hide_index=True)

        # Hit rate over last 20 vs current line
        st.subheader("Hit Rate vs Today's Lines (Last 20)")
        hit_stats = [
            ("Points", "points", "pts"),
            ("Rebounds", "rebounds", "reb"),
            ("Assists", "assists", "ast"),
            ("PRA", "pra", "pra"),
            ("3PM", "threes", "threes"),
            ("Steals", "steals", "steals"),
            ("Blocks", "blocks", "blocks"),
        ]
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
        st.subheader(f"Career vs {opp_code} ({len(vs_opp)} most recent · {vs_opp_avg['games'] if vs_opp_avg else 0} total)")

        # Summary averages row
        if vs_opp_avg:
            avg_row = pd.DataFrame([{
                "Window": f"Career vs {opp_code}",
                "MIN": vs_opp_avg.get("minutes", 0),
                "PTS": vs_opp_avg.get("points", 0),
                "REB": vs_opp_avg.get("rebounds", 0),
                "AST": vs_opp_avg.get("assists", 0),
                "PRA": vs_opp_avg.get("pra", 0),
                "3PM": vs_opp_avg.get("threes", 0),
                "STL": vs_opp_avg.get("steals", 0),
                "BLK": vs_opp_avg.get("blocks", 0),
            }])
            _, opp_avg_mid, _ = st.columns([1, 6, 1])
            with opp_avg_mid:
                st.dataframe(avg_row, use_container_width=True, hide_index=True, column_config={
                    "MIN": st.column_config.NumberColumn(format="%.1f"),
                    "PTS": st.column_config.NumberColumn(format="%.1f"),
                    "REB": st.column_config.NumberColumn(format="%.1f"),
                    "AST": st.column_config.NumberColumn(format="%.1f"),
                    "PRA": st.column_config.NumberColumn(format="%.1f"),
                    "3PM": st.column_config.NumberColumn(format="%.1f"),
                    "STL": st.column_config.NumberColumn(format="%.1f"),
                    "BLK": st.column_config.NumberColumn(format="%.1f"),
                })

        # Recent matchups table with the same coloring treatment as Last 20
        opp_df = pd.DataFrame(vs_opp)
        opp_df = opp_df.rename(columns={
            "date": "Date", "opponent": "Opp", "min": "MIN",
            "pts": "PTS", "reb": "REB", "ast": "AST",
        })

        def format_stat_opp(val, line):
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

        def color_opp(row):
            styles = ["" for _ in row]
            for stat_col, line_key in [("PTS", "points"), ("REB", "rebounds"), ("AST", "assists")]:
                if stat_col not in row.index:
                    continue
                val = row[stat_col]
                line = lines.get(line_key)
                if line is None or pd.isna(val):
                    continue
                idx = row.index.get_loc(stat_col)
                d = val - line
                if d > 0:
                    styles[idx] = "color: #22c55e; font-weight: 600;"
                elif d < 0:
                    styles[idx] = "color: #ef4444; font-weight: 600;"
            return styles

        styled_opp = (
            opp_df.style
            .format({
                "MIN": "{:.0f}",
                "PTS": lambda v: format_stat_opp(v, lines.get("points")),
                "REB": lambda v: format_stat_opp(v, lines.get("rebounds")),
                "AST": lambda v: format_stat_opp(v, lines.get("assists")),
            })
            .apply(color_opp, axis=1)
        )
        _, opp_mid, _ = st.columns([1, 6, 1])
        with opp_mid:
            st.dataframe(styled_opp, use_container_width=True, hide_index=True)


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
    # --- User info + sign out ---
    user = current_user()
    if user:
        admin_badge = "  · ADMIN" if is_admin() else ""
        st.markdown(
            f"<div style='color:#8b92a5;font-size:0.8rem;margin-bottom:4px;'>"
            f"Signed in as<br/><strong style='color:#e6edf3;font-size:0.9rem;'>{user['email']}</strong>"
            f"<span style='color:#22c55e;font-weight:700;'>{admin_badge}</span></div>",
            unsafe_allow_html=True,
        )
        if st.button("Sign out", use_container_width=True):
            auth_sign_out()
            st.rerun()
        st.divider()

    selected_date = st.date_input("Game Date", value=datetime.date.today())
    st.session_state["selected_date"] = selected_date

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

    if is_admin():
        if st.button("Fetch / Refresh Data", type="primary", use_container_width=True):
            try:
                with st.spinner("Fetching from NBA.com + The Odds API..."):
                    shop = st.session_state.get("line_shopping", False)
                    events, results, summaries = fetch_fresh_data(selected_date, all_books=shop)
                    save_daily_results(events, results, summaries, selected_date)
                    st.cache_data.clear()
                    st.session_state.pop("selected_player", None)
                st.rerun()
            except OddsAPIQuotaError as e:
                st.error(str(e))
                st.info(
                    "Tip: every refresh costs ~7 credits per game (1 per market). "
                    "Trim the markets list in scrapers/odds_api.py (MARKET_MAP) to "
                    "save credits, or upgrade at https://the-odds-api.com — paid "
                    "tier starts at $30/mo for 20k credits."
                )
    else:
        st.caption("Only admins can refresh data. Reach out to the site owner to be granted access.")

    # Optional line shopping (multi-book) — admin only since it costs API credits
    if is_admin():
        line_shopping = st.checkbox(
            "Enable line shopping (all books)",
            value=st.session_state.get("line_shopping", False),
            help="Pulls lines from every US sportsbook (DK, FD, MGM, Caesars, etc.) instead "
                 "of just DraftKings. Player detail page will show the best line per book. "
                 "Uses ~5x more Odds API credits per refresh.",
        )
        st.session_state["line_shopping"] = line_shopping

    # Optional pick tracking
    pick_tracking = st.checkbox(
        "Enable pick tracking",
        value=st.session_state.get("pick_tracking", False),
        help="Save picks you make and auto-grade them after games finish. "
             "Picks are stored on disk (data/picks.json).",
    )
    st.session_state["pick_tracking"] = pick_tracking

    # Navigation is now in the top nav bar, not the sidebar.

    # Show a backfill prompt only when no historical data (compressed or raw) exists
    from data import HISTORICAL_DATA_PATH, HISTORICAL_DATA_GZ_PATH
    if is_admin() and not os.path.exists(HISTORICAL_DATA_PATH) and not os.path.exists(HISTORICAL_DATA_GZ_PATH):
        st.divider()
        st.warning("Historical data is missing. Career averages and historical hit% will be unavailable until backfilled.")
        if st.button("Run Historical Backfill", use_container_width=True):
            from backfill import backfill
            with st.spinner("Backfilling 2014-15 to current season... ~1 minute."):
                backfill()
            st.success("Backfill complete.")
            st.rerun()

if cached is None:
    if is_admin():
        st.info("No data for this date. Click **Fetch / Refresh Data** in the sidebar.")
    else:
        st.info("No data for this date yet. Check back later — an admin needs to refresh first.")
    st.stop()

events, results, summaries = cached

# --- Top navigation ---
nav_options = ["Picks Board", "Auto Picks", "What-If"]
if is_admin():
    nav_options.append("AI Analysis")
if st.session_state.get("pick_tracking"):
    nav_options.append("My Picks")

# Default to Picks Board on first load
if "top_nav" not in st.session_state:
    st.session_state["top_nav"] = "Picks Board"
# Clamp to a valid option in case "My Picks" was hidden after being active
if st.session_state["top_nav"] not in nav_options:
    st.session_state["top_nav"] = "Picks Board"

nav_choice = st.radio(
    "nav",
    nav_options,
    horizontal=True,
    label_visibility="collapsed",
    key="top_nav",
)

# Clear any selected player / view flags when the user changes nav
if st.session_state.get("_last_nav") != nav_choice:
    st.session_state["_last_nav"] = nav_choice
    st.session_state.pop("selected_player", None)

st.divider()

# --- Game status banner ---
# Use the first stat's results for the count (game_status is per-row but consistent per team)
_first_result = next(iter(results.values()), pd.DataFrame())
if "game_status" in _first_result.columns and not _first_result.empty:
    status_counts = _first_result.drop_duplicates("team-code")["game_status"].value_counts().to_dict()
    pre = status_counts.get("pregame", 0)
    live = status_counts.get("live", 0)
    done = status_counts.get("completed", 0)
    parts = []
    if pre:
        parts.append(f"**{pre}** pregame")
    if live:
        parts.append(f"**{live}** :red[LIVE]")
    if done:
        parts.append(f"**{done}** completed")
    if parts:
        banner = "Game status: " + " · ".join(parts)
        if live or done:
            banner += "  ·  *(toggle 'Include live / completed games' in the sidebar to see them)*"
            st.warning(banner)
        else:
            st.info(banner)

# --- My Picks view ---
if nav_choice == "My Picks" and st.session_state.get("pick_tracking"):
    st.title("My Picks")

    # Auto-grade pending picks against the historical data we already have
    from data import load_historical_data
    if st.button("Auto-grade pending picks"):
        graded = grade_picks(load_historical_data())
        st.success(f"Graded {graded} picks.")
        st.rerun()

    summary = picks_summary()
    metric_cols = st.columns(5)
    metric_cols[0].metric("Total", summary["total"])
    metric_cols[1].metric("Pending", summary["pending"])
    metric_cols[2].metric("Won", summary["won"])
    metric_cols[3].metric("Lost", summary["lost"])
    metric_cols[4].metric("Win rate", f"{summary['win_rate']:.0f}%")

    picks = load_picks()
    if not picks:
        st.info("No picks saved yet. Open a player's detail page and click 'Save pick' to start.")
    else:
        picks_df = pd.DataFrame(picks).sort_values("created_at", ascending=False)
        # Show key columns
        cols_to_show = ["date", "player", "stat", "side", "line", "actual", "result", "team", "opponent"]
        cols_to_show = [c for c in cols_to_show if c in picks_df.columns]
        st.dataframe(picks_df[cols_to_show], use_container_width=True, hide_index=True)

        # Removal UI
        with st.expander("Remove a pick"):
            pick_options = {f"{p['player']} {p['side']} {p['line']} {p['stat']} ({p['date']})": p["id"]
                            for p in picks}
            choice = st.selectbox("Pick to remove", options=[""] + list(pick_options.keys()))
            if choice and st.button("Remove", type="secondary"):
                remove_pick(pick_options[choice])
                st.rerun()

    st.stop()


# --- Auto Picks view ---
if nav_choice == "What-If":
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
    def _whatif_injuries():
        from scrapers.injuries import get_injury_report
        inj = get_injury_report()
        return dict(zip(inj["name"], inj["status_short"])) if not inj.empty else {}

    injury_status = _whatif_injuries()

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


if nav_choice == "AI Analysis" and is_admin():
    st.title("AI Prop Analysis")
    st.caption(
        "Ask Claude to evaluate a specific prop. Uses all the data we have "
        "about the player, matchup, recent form, and tonight's line."
    )

    from ai_analysis import analyze_prop, STAT_LABEL

    players_with_props = sorted(summaries.keys()) if summaries else []
    if not players_with_props:
        st.info("No players with props yet. Click Fetch / Refresh Data first.")
        st.stop()

    ac1, ac2 = st.columns([2, 1])
    with ac1:
        ai_player = st.selectbox(
            "Player",
            options=players_with_props,
            index=0,
            key="ai_player",
        )

    # What stats does this player have a line on today?
    player_lines = (summaries.get(ai_player, {}) or {}).get("today_lines", {}) or {}
    if not player_lines:
        st.warning(f"{ai_player} doesn't have any prop lines on the current slate.")
        st.stop()

    with ac2:
        ai_stat = st.selectbox(
            "Stat",
            options=list(player_lines.keys()),
            format_func=lambda k: STAT_LABEL.get(k, k),
            key="ai_stat",
        )

    default_line = float(player_lines.get(ai_stat, 0.0))
    ac3, ac4, ac5 = st.columns([1, 1, 1])
    with ac3:
        ai_line = st.number_input(
            "Line", value=default_line, step=0.5, key="ai_line",
            help="Defaults to tonight's book line; override if you're evaluating a different number.",
        )
    with ac4:
        ai_side = st.radio("Side", ["Over", "Under"], horizontal=True, key="ai_side")
    with ac5:
        st.write("")
        st.write("")
        go = st.button("Ask Claude", type="primary", use_container_width=True)

    if go:
        # Look up the full result_row for this player + stat
        result_df = results.get(ai_stat)
        result_row = None
        if result_df is not None and not result_df.empty:
            rows = result_df[result_df["name"] == ai_player]
            if not rows.empty:
                result_row = rows.iloc[0].to_dict()

        with st.spinner("Claude is thinking..."):
            resp = analyze_prop(
                player=ai_player,
                stat=ai_stat,
                line=float(ai_line),
                side=ai_side.lower(),
                summary=summaries.get(ai_player, {}),
                result_row=result_row,
            )

        if "error" in resp:
            st.error(resp["error"])
        else:
            st.markdown(resp["text"])
            usage = resp.get("usage", {})
            if usage:
                st.caption(
                    f"Model: {resp.get('model', '?')} · "
                    f"Tokens in/out: {usage.get('input_tokens', 0)}/{usage.get('output_tokens', 0)}"
                )

    st.stop()


if nav_choice == "Auto Picks":
    st.title("Auto Picks")
    st.caption("Strong Overs and Strong Unders generated automatically every morning.")

    # Admin-only manual trigger + diagnostic
    if is_admin():
        with st.expander("Admin tools"):
            from auto_runner import maybe_auto_refresh, maybe_auto_grade
            from auto_picks import get_admin_client
            import os as _os
            srv = _os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY", "")
            st.write({
                "service_role_key_present": bool(srv),
                "last_job_status": st.session_state.get("_last_job_status", "not yet attempted this session"),
            })
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("Generate today's auto-picks now", use_container_width=True):
                    # Bypass time check + once-per-session guard
                    st.session_state.pop("_daily_jobs_attempted", None)
                    try:
                        from auto_picks import generate_and_save_picks
                        with st.spinner("Generating picks..."):
                            n = generate_and_save_picks(datetime.date.today())
                        st.success(f"Saved {n} picks.")
                    except Exception as e:
                        st.error(f"Failed: {e}")
            with col_b:
                if st.button("Grade pending picks now", use_container_width=True):
                    try:
                        from auto_picks import grade_pending_picks
                        with st.spinner("Grading..."):
                            n = grade_pending_picks(datetime.date.today())
                        st.success(f"Graded {n} picks.")
                    except Exception as e:
                        st.error(f"Failed: {e}")

    sub = st.radio(
        "auto_picks_subview",
        ["All Strong", "Top 5 Only"],
        horizontal=True,
        label_visibility="collapsed",
        key="auto_picks_subview_radio",
    )
    top_only = sub == "Top 5 Only"

    auto_picks = fetch_auto_picks(top_only=top_only)
    if not auto_picks:
        st.info(
            "No auto picks yet. The first batch will be saved at 10am ET tomorrow "
            "(or run the workflow manually from GitHub Actions)."
        )
        st.stop()

    summary = auto_summarize_picks(auto_picks)
    m = st.columns(5)
    m[0].metric("Total", summary["total"])
    m[1].metric("Pending", summary["pending"])
    m[2].metric("Won", summary["won"])
    m[3].metric("Lost", summary["lost"])
    m[4].metric("Win rate", f"{summary['win_rate']:.0f}%")

    df = pd.DataFrame(auto_picks)
    cols_to_show = [
        "date", "player", "stat", "side", "line", "actual", "result",
        "team", "opponent", "delta", "hit_pct", "history_hit_pct", "score", "is_top_pick",
    ]
    cols_to_show = [c for c in cols_to_show if c in df.columns]
    st.dataframe(
        df[cols_to_show],
        use_container_width=True,
        hide_index=True,
        column_config={
            "line": st.column_config.NumberColumn("Line", format="%.1f"),
            "actual": st.column_config.NumberColumn("Actual", format="%.0f"),
            "delta": st.column_config.NumberColumn("Delta", format="%+.1f"),
            "hit_pct": st.column_config.NumberColumn("Hit %", format="%.0f%%"),
            "history_hit_pct": st.column_config.NumberColumn("Hist Hit %", format="%.0f%%"),
            "score": st.column_config.NumberColumn("Score", format="%.1f"),
            "is_top_pick": st.column_config.CheckboxColumn("Top 5", help="In the top 5 of its side that day"),
        },
    )
    st.stop()


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


# --- Top Picks panel ---
def _composite_score(row, side: str) -> float:
    """Confidence score for ranking the strongest picks across all stats.

    Combines: avg delta across windows, current hit%, history hit%, and
    inverse volatility (lower std% gets a small boost).
    """
    d = row.get("delta", 0) or 0
    d5 = row.get("delta_5g", 0) or 0
    d10 = row.get("delta_10g", 0) or 0
    avg_delta = (abs(d) + abs(d5) + abs(d10)) / 3
    hit = row.get("hit%", 0) or 0
    hist = row.get("history_hit%", 0) or 0
    if side == "over":
        # Want positive deltas + hit rates above 50
        edge = (hit - 50) + (hist - 50)
    else:
        edge = (50 - hit) + (50 - hist)
    return avg_delta * (edge / 10 if edge > 0 else 0)


def _gather_top_picks(results: dict, side: str, limit: int = 5) -> pd.DataFrame:
    """Compose a unified ranking across all stats."""
    rows = []
    for stat_key, df in results.items():
        if df.empty:
            continue
        # Apply the strong filter for this side
        if side == "over":
            qualifying = df[
                (df["delta"] > 0) & (df["delta_5g"] > 0) & (df["delta_10g"] > 0)
                & (df["hit%"] > 50) & (df["history_hit%"] > 50)
            ]
        else:
            qualifying = df[
                (df["delta"] < 0) & (df["delta_5g"] < 0) & (df["delta_10g"] < 0)
                & (df["hit%"] < 50) & (df["history_hit%"] < 50)
            ]
        # Auto-exclude OUT/Doubtful from top picks regardless of toggle
        if "status_short" in qualifying.columns:
            qualifying = qualifying[~qualifying["status_short"].fillna("").isin({"OUT", "DBT"})]
        if qualifying.empty:
            continue
        labelled = qualifying.copy()
        labelled["stat"] = stat_key
        labelled["score"] = labelled.apply(lambda r: _composite_score(r, side), axis=1)
        rows.append(labelled)
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    return combined.sort_values("score", ascending=False).head(limit)


def _render_top_pick_row(row, side: str):
    arrow = "OVER" if side == "over" else "UNDER"
    color = "#22c55e" if side == "over" else "#ef4444"
    stat_label = {
        "points": "PTS", "rebounds": "REB", "assists": "AST", "pra": "PRA",
        "threes": "3PM", "steals": "STL", "blocks": "BLK",
    }.get(row["stat"], row["stat"].upper())
    delta = row.get("delta", 0)
    hit = row.get("hit%", 0)
    hist = row.get("history_hit%", 0)
    return (
        f"<div style='border-left:4px solid {color};padding:6px 10px;background:#1a1d24;"
        f"border-radius:6px;margin-bottom:6px;'>"
        f"<div style='font-weight:700;font-size:0.95rem;'>{row['name']} "
        f"<span style='color:{color};margin-left:4px;'>{arrow} {row['spread']:.1f} {stat_label}</span></div>"
        f"<div style='color:#8b92a5;font-size:0.78rem;margin-top:2px;'>"
        f"Δ {delta:+.1f} · Hit {hit:.0f}% · Hist {hist:.0f}% · vs {row.get('opponent', '')}"
        f"</div></div>"
    )


with st.container():
    top_overs = _gather_top_picks(results, "over", limit=5)
    top_unders = _gather_top_picks(results, "under", limit=5)
    if not top_overs.empty or not top_unders.empty:
        st.subheader("Today's Top Picks")
        tp_col1, tp_col2 = st.columns(2)
        with tp_col1:
            st.markdown("**Strongest Overs**")
            if top_overs.empty:
                st.caption("No strong overs found.")
            else:
                for _, r in top_overs.iterrows():
                    st.markdown(_render_top_pick_row(r, "over"), unsafe_allow_html=True)
        with tp_col2:
            st.markdown("**Strongest Unders**")
            if top_unders.empty:
                st.caption("No strong unders found.")
            else:
                for _, r in top_unders.iterrows():
                    st.markdown(_render_top_pick_row(r, "under"), unsafe_allow_html=True)

st.divider()

# --- Stat selector ---
STAT_LABELS = {
    "Points": "points",
    "Rebounds": "rebounds",
    "Assists": "assists",
    "PRA": "pra",
    "3PM": "threes",
    "Steals": "steals",
    "Blocks": "blocks",
}
stat_col, view_col = st.columns([4, 1])
with stat_col:
    stat_tab = st.radio(
        "Stat", list(STAT_LABELS.keys()),
        horizontal=True, label_visibility="collapsed",
    )
with view_col:
    compact = st.toggle("Compact", value=st.session_state.get("compact_view", False),
                        help="Card layout — better on mobile")
    st.session_state["compact_view"] = compact

# --- About / glossary expander ---
with st.expander("About & column reference"):
    st.markdown(
        """
### How it works
Squeeze the Line compares each player's historical stats to tonight's sportsbook lines,
then flags plays where the player is trending strongly above or below the line.

- **Prop lines** come from **The Odds API** (DraftKings feed)
- **Current season stats** come from **NBA.com** (`nba_api`)
- **Career history** goes back to the **2014-15 season** (~320k player-game rows)
- **Defense rankings** come from **HashtagBasketball**
- **Injury report** comes from **ESPN** (daily)

### Pick categories (tabs)
| Category | Criteria |
|---|---|
| **Strong Overs** | Season avg, last-5, and last-10 avg all **above** line **AND** current hit% > 50% **AND** history hit% > 50% |
| **Trending Overs** | Season avg, last-5, and last-10 avg all **above** line (no hit-rate check) |
| **Strong Unders** | All three averages **below** line **AND** both hit rates < 50% |
| **Trending Unders** | All three averages **below** line (no hit-rate check) |
| **All Players** | Everyone with a line, no filter |

### Column reference
| Column | Meaning |
|---|---|
| **Player** | Click the row to see a detailed player page |
| **Inj** | Injury status from ESPN — OUT, DBT (Doubtful), Q (Questionable), DTD (Day-to-Day), PROB (Probable). Blank means the player is healthy/not listed |
| **Starter** | ✓ if the player is in the **top 5 minutes-per-game on their team over the last 10 games**. Adapts to injuries and rotation changes automatically |
| **Profile** | Link to the player's NBA.com profile |
| **Team / Opp** | Player's current team and tonight's opponent |
| **Pos** | Position (PG/SG/SF/PF/C) from NBA.com |
| **Line** | Tonight's sportsbook over/under line for the selected stat |
| **Delta** | Season average minus the line. Positive = player averages above the line this season |
| **Delta 5G** | Same, but using the player's last 5 games only |
| **Delta 10G** | Same, but last 10 games |
| **Hit %** | % of this season's games where the player exceeded tonight's line. Green bar ≥ 50%, red bar < 50% |
| **Hist Hit %** | % of the player's entire career (2014-present) where they exceeded tonight's line |
| **vs Opp (Szn)** | Games this season where the player beat tonight's line / total games vs tonight's opponent this season (e.g. `2/3` = 2 of 3 games) |
| **vs Opp (Career)** | Same but across their full career (2014-present) |
| **Def Rank** | Opponent's defense-vs-position rank for this stat. **1 = toughest defense**, **30 = weakest**. Higher rank = better matchup for overs. Blank if the stat doesn't have defense data (e.g., PRA) |
| **Rest** | Days of rest since the player's last game |
| **B2B** | ✓ if the player also played yesterday (back-to-back) |
| **Std Dev** | How much this stat varies game-to-game. Higher = more volatile player |
| **SPM** | Stat per minute — the player's production rate when on the floor |

### Player detail page
Clicking any row (or selecting a player from the search) opens a detail page with:
- **Today's lines** with deltas vs. season average
- **Averages table** — this season and career, across all stats
- **Last 10 Games bar charts** — green bars beat tonight's line, red bars missed, with a dashed line marking the prop
- **Last 20 Games table** — colored text showing how much each stat beat or missed the line by
- **Hit Rate vs Today's Lines** — how often the player has beat tonight's line in their last 20 games
- **Career vs {tonight's opponent}** — their history specifically against this team (if any)
        """
    )

stat = STAT_LABELS[stat_tab]
if stat not in results:
    st.warning(
        f"No **{stat_tab}** data in the current cache. Click **Fetch / Refresh Data** "
        "in the sidebar to pull fresh data with all prop types."
    )
    st.stop()
result = results[stat]


def show_results(df: pd.DataFrame, key: str):
    """Render results as cards (compact view) or table, based on the toggle."""
    if st.session_state.get("compact_view"):
        show_cards(df, key=key)
    else:
        show_table(df, key=key)

# --- Sidebar filters ---
with st.sidebar:
    # --- Player search ---
    st.header("Player")
    all_player_names = sorted(summaries.keys()) if summaries else []
    picked = st.selectbox(
        "Search for a player",
        options=[""] + all_player_names,
        index=0,
        placeholder="Type a name...",
        label_visibility="collapsed",
    )
    if picked:
        st.session_state["selected_player"] = picked
        st.rerun()

    st.header("Filters")

    teams = sorted(result["team-code"].dropna().unique())
    selected_teams = st.multiselect("Team", teams, default=[])

    opponents = sorted(result["opponent"].dropna().unique())
    selected_opponents = st.multiselect("Opponent", opponents, default=[])

    min_hit = st.slider("Min current hit %", 0, 100, 0)
    max_hit = st.slider("Max current hit %", 0, 100, 100)

    min_spread = st.number_input("Min spread", value=0.0, step=0.5)

    include_inactive = st.checkbox(
        "Include OUT / Doubtful players",
        value=False,
        help="Ruled-out and doubtful players are hidden by default — they skew picks since they won't play.",
    )

    include_live = st.checkbox(
        "Include live / completed games",
        value=False,
        help="Games that have already tipped off return live (in-game) lines that don't reflect "
             "the pre-game line. These are hidden by default.",
    )

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

# Auto-hide OUT / Doubtful players unless the user opts in
if not include_inactive and "status_short" in filtered.columns:
    inactive_codes = {"OUT", "DBT"}
    filtered = filtered[~filtered["status_short"].fillna("").isin(inactive_codes)]

# Auto-hide live / completed games (lines aren't pre-game) unless the user opts in
if not include_live and "game_status" in filtered.columns:
    filtered = filtered[filtered["game_status"].isin(["pregame", "unknown"])]

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
        show_results(df_view, key=f"strong_o_{stat}")

with tab_trend_o:
    df_view = filter_trending_overs(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No trending overs found with current filters.")
    else:
        st.caption(f"All deltas positive ({len(df_view)} players)")
        show_results(df_view, key=f"trend_o_{stat}")

with tab_strong_u:
    df_view = filter_strong_unders(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No strong unders found with current filters.")
    else:
        st.caption(f"All deltas negative + both hit rates < 50% ({len(df_view)} players)")
        show_results(df_view, key=f"strong_u_{stat}")

with tab_trend_u:
    df_view = filter_trending_unders(filtered)[show_cols].reset_index(drop=True)
    if df_view.empty:
        st.info("No trending unders found with current filters.")
    else:
        st.caption(f"All deltas negative ({len(df_view)} players)")
        show_results(df_view, key=f"trend_u_{stat}")

with tab_all:
    df_view = filtered[show_cols].sort_values("hit%", ascending=False).reset_index(drop=True)
    st.caption(f"{len(df_view)} players")
    show_results(df_view, key=f"all_{stat}")
