"""Squeeze the Line — Streamlit entry point.

This is the thin UI shell. The heavy lifting lives in dedicated modules:

- ``pipeline``     — Streamlit-free data orchestration (``fetch_fresh_data`` +
                     the on-disk daily cache). The FastAPI backend re-implements
                     this same flow in ``backend/providers/live.py``.
- ``cron``         — the automated webhook pipeline (auto-picks/digest/alerts).
- ``ui.*``         — the rendering layer: picks board, player detail, charts,
                     filters, the compare / what-if / injuries views.

Everything below is module-level Streamlit script: it runs top-to-bottom on
every rerun. Keep the ordering — auth gate, sidebar, cached-data load, nav
dispatch — intact.
"""

import os
import datetime

import streamlit as st
import pandas as pd

from scrapers.odds_api import OddsAPIQuotaError
from config import (
    DEFAULT_SPORT, active_sports, sport_config,
    stat_labels_for,
)
from picks import (
    load_picks,
    remove_pick,
    grade_picks,
    picks_summary,
)
from auto_picks import fetch_auto_picks, summarize_picks as auto_summarize_picks
from data import DATA_DIR  # noqa: F401  (kept for tooling that imports app.DATA_DIR)
from analysis import (
    filter_strong_overs,
    filter_strong_unders,
    filter_trending_overs,
    filter_trending_unders,
)

from pipeline import fetch_fresh_data, save_daily_results, load_daily_results
from cron import handle_cron_webhook
from ui.charts import make_last_n_chart  # noqa: F401  (re-exported for back-compat)
from ui.filters import gather_top_picks, render_top_pick_row
from ui.picks_board import show_results
from ui.player_detail import render_player_detail
from ui.injuries import render_odds_only
from ui.compare import render_compare
from ui.whatif import render_whatif

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

        /* Sticky pills (stat selector) and st.tabs (Strong Overs / etc.) so
           they stay visible while scrolling the long results table. */
        [data-testid="stPills"] {
            position: sticky;
            top: 0;
            background: #0f1115;
            z-index: 50;
            padding: 6px 0;
        }
        .stTabs [data-baseweb="tab-list"] {
            position: sticky;
            top: 58px;
            background: #0f1115;
            z-index: 49;
        }

        /* ==========================
           Mobile (< 768px) overrides
           ========================== */
        @media (max-width: 768px) {
            /* Reduce main container padding to maximize screen usage */
            .main .block-container {
                padding: 0.75rem 0.5rem !important;
                max-width: 100% !important;
            }

            /* Force compact card view + bigger tap targets */
            [data-testid="stRadio"] label,
            [data-testid="stPills"] button {
                min-height: 44px !important;
                font-size: 0.9rem !important;
            }

            /* Hero photo smaller on mobile */
            [data-testid="stImage"] img {
                max-width: 90px !important;
                height: auto !important;
            }

            /* Sticky pills shouldn't overlap content as much */
            [data-testid="stPills"] {
                top: 0 !important;
                padding: 4px 0 !important;
            }
            .stTabs [data-baseweb="tab-list"] {
                top: 50px !important;
            }

            /* Quick-jump bar should wrap nicely on mobile */
            .stl-jump {
                display: flex !important;
                flex-wrap: wrap !important;
                gap: 6px !important;
            }
            .stl-jump a {
                flex: 1 1 calc(33% - 6px) !important;
                text-align: center !important;
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


# --- Webhook endpoint for automated cron (no login required) ---
# A free cron service pings /?refresh_token=YOUR_SECRET on a schedule; if the
# token matches we run the daily jobs in cron.py and stop without rendering the
# app. See cron.handle_cron_webhook for details.
handle_cron_webhook()


if not render_auth_gate():
    st.stop()


# Daily auto-jobs (refresh + grade) are now driven by the cron webhook —
# cron-job.org hits /?refresh_token=... twice a day and runs the pipeline
# in a background thread on Streamlit Cloud. Admins no longer trigger it
# on login, so logins are instant. The admin panel on Auto Picks still has
# manual trigger buttons if you ever need to force a run.

# Stat configs are now sport-aware — the projection pipeline reads
# config.stat_configs_for(sport_key) (which returns (stat_key, prop_type, label)
# per sport). This NBA-only list is kept for backward compatibility with any
# code/tooling that still imports it; live code paths use stat_configs_for().
STAT_CONFIGS = [
    ("points", "Total Points"),
    ("rebounds", "Total Rebounds"),
    ("assists", "Total Assists"),
    ("pra", "Total PRA"),
    ("threes", "Total 3PM"),
    ("steals", "Total Steals"),
    ("blocks", "Total Blocks"),
]

# Essential columns shown by default — the bare minimum to evaluate a pick.
DISPLAY_COLS_DEFAULT = [
    "name", "confidence", "trend", "last10_hits",
    "status_short", "teammates_out",
    "team-code", "opponent", "spread",
    "delta", "delta_10g", "hit%", "history_hit%",
    "rank",
]

# Full power-user view: everything we have.
DISPLAY_COLS_ALL = [
    "name", "confidence", "trend", "last10", "last10_hits", "game_status", "status_short", "teammates_out", "starter", "player_url", "team-code", "opponent", "position", "spread",
    "delta", "delta_5g", "delta_10g",
    "hit%", "history_hit%",
    "vs_opp_season", "vs_opp_career",
    "rank", "rest_days", "b2b", "opp_rest", "opp_b2b", "std_dev", "spm",
]

# Kept for backward compatibility with any code that imports DISPLAY_COLS
DISPLAY_COLS = DISPLAY_COLS_ALL


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
                    Player props · data-driven picks
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

    # --- Sport selector ---
    _sport_names = list(active_sports().keys())
    _default_sport_idx = _sport_names.index(DEFAULT_SPORT) if DEFAULT_SPORT in _sport_names else 0
    selected_sport = st.selectbox(
        "Sport",
        options=_sport_names,
        index=_default_sport_idx,
        help="NBA, WNBA, MLB, NFL, NCAA football and the FIFA World Cup have full "
             "projections (recent form, hit rates, confidence). NCAA basketball "
             "shows odds + injuries only — no player-stats source is wired for it yet.",
    )
    _sport_cfg = sport_config(selected_sport)
    sport_key = _sport_cfg["key"]
    st.session_state["sport"] = selected_sport
    st.session_state["sport_key"] = sport_key
    if not _sport_cfg.get("projections", True):
        st.caption("⚠️ Limited support: odds & injuries only — no projections for this sport yet.")

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
cached = load_daily_results(selected_date, sport_key)

with st.sidebar:
    if cached:
        st.success(f"Using saved data for {selected_date}.")
    else:
        st.warning(f"No data for {selected_date} yet.")

    if is_admin():
        if st.button("Fetch / Refresh Data", type="primary", use_container_width=True):
            try:
                with st.spinner(f"Fetching {selected_sport} data from The Odds API + stats sources..."):
                    shop = st.session_state.get("line_shopping", False)
                    events, results, summaries = fetch_fresh_data(
                        selected_date, all_books=shop, sport_key=sport_key
                    )
                    save_daily_results(events, results, summaries, selected_date, sport_key)
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

# Sports without a player-stats source (NCAAB/MLB/NFL/NCAAF) have odds +
# injuries but no projection edges. Render a focused odds + injuries page for
# them instead of the empty projection board, then stop.
if not sport_config(selected_sport).get("projections", True):
    render_odds_only(selected_sport, events, summaries)
    st.stop()


# Recompute game_status in every result DataFrame against the CURRENT time
# (the cached value is fixed at refresh time and would be stale).
def _now_status(tipoff_iso: str) -> str:
    if not tipoff_iso:
        return "unknown"
    try:
        tipoff = pd.Timestamp(tipoff_iso)
        if tipoff.tzinfo is None:
            tipoff = tipoff.tz_localize("UTC")
        now = pd.Timestamp.now(tz="UTC")
        if now < tipoff:
            return "pregame"
        if now < tipoff + pd.Timedelta(hours=3):
            return "live"
        return "completed"
    except Exception:
        return "unknown"


for _df in results.values():
    if "tipoff" in _df.columns:
        _df["game_status"] = _df["tipoff"].apply(_now_status)

# --- Top navigation ---
nav_options = ["Picks Board", "Auto Picks", "What-If", "Compare", "Parlays", "Leaderboard", "Performance"]
if is_admin():
    nav_options.append("AI Analysis")
    nav_options.append("Analytics")
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
    # Log the nav change for activity tracking
    try:
        from activity import log, ACTION_PAGE_VIEW
        log(ACTION_PAGE_VIEW, {"tab": nav_choice})
    except Exception:
        pass

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
            banner += "  ·  *(toggle 'Show live games' / 'Show completed games' in the sidebar to see them)*"
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


# --- What-If view ---
if nav_choice == "What-If":
    render_whatif(results, sport_key)


# --- Compare view ---
if nav_choice == "Compare":
    render_compare(summaries, results, sport_key)


if nav_choice == "Parlays":
    from parlays import (
        combined_odds, detect_correlations, estimated_hit_pct,
        save_parlay, fetch_user_parlays, delete_parlay,
    )

    st.title("Parlays")
    st.caption("Build multi-leg parlays from your picks, see combined odds + correlation warnings, and track them.")

    builder = st.session_state.get("_parlay_builder", [])

    # --- Active builder ---
    st.subheader(f"Builder ({len(builder)} legs)")
    if not builder:
        st.info("Open a player's detail page and click ➕ Parlay Over / Under to add legs here.")
    else:
        # Show each leg with a remove button
        for i, leg in enumerate(builder):
            cols = st.columns([4, 1])
            with cols[0]:
                arrow = "↑" if leg["side"] == "over" else "↓"
                color = "#22c55e" if leg["side"] == "over" else "#ef4444"
                conf_text = f" · conf {leg.get('confidence', 0):.0f}" if leg.get("confidence") is not None else ""
                st.markdown(
                    f"<div style='padding:6px 0;'>"
                    f"<span style='color:{color};font-weight:700;'>{arrow} {leg['side'].upper()}</span> "
                    f"<strong>{leg['player']}</strong> {leg['line']:.1f} {leg['stat'].upper()} "
                    f"<span style='color:#8b92a5;'>({leg['team']} vs {leg['opponent']}) · hit {leg.get('hit_pct', 0):.0f}%{conf_text}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with cols[1]:
                if st.button("Remove", key=f"parlay_remove_{i}", use_container_width=True):
                    builder.pop(i)
                    st.rerun()

        # Combined odds + EV
        odds = combined_odds(builder)
        est_hit = estimated_hit_pct(builder)
        m = st.columns(4)
        m[0].metric("Legs", len(builder))
        m[1].metric("Combined Odds", f"{odds['american']:+d}")
        m[2].metric("Implied %", f"{odds['implied_pct']:.1f}%")
        m[3].metric("Est. hit %", f"{est_hit:.1f}%", help="Naive product of leg hit rates. Doesn't account for correlation — see warnings.")

        # Correlation warnings
        warnings = detect_correlations(builder)
        if warnings:
            st.warning("⚠️ Correlation warnings:")
            for w in warnings:
                st.markdown(f"- {w}")

        # Save / clear
        save_cols = st.columns([2, 1, 1])
        with save_cols[0]:
            parlay_name = st.text_input(
                "Name this parlay",
                value=f"{datetime.date.today()} {len(builder)}-leg",
                key="parlay_name",
            )
            stake = st.number_input("Stake ($)", value=10.0, min_value=0.0, step=5.0, key="parlay_stake")
        with save_cols[1]:
            st.write("")
            st.write("")
            if st.button("💾 Save parlay", type="primary", use_container_width=True):
                user = current_user() or {}
                saved = save_parlay(user.get("email", "anonymous"), parlay_name, builder, stake=stake)
                if saved:
                    st.success("Parlay saved.")
                    st.session_state["_parlay_builder"] = []
                    st.rerun()
                else:
                    st.error("Could not save (Supabase parlays table may not exist).")
        with save_cols[2]:
            st.write("")
            st.write("")
            if st.button("🗑️ Clear", use_container_width=True):
                st.session_state["_parlay_builder"] = []
                st.rerun()

    # --- Saved parlays ---
    st.divider()
    st.subheader("My saved parlays")
    user = current_user() or {}
    saved_parlays = fetch_user_parlays(user.get("email", ""))
    if not saved_parlays:
        st.caption("None yet.")
    else:
        for p in saved_parlays:
            with st.container(border=True):
                top = st.columns([3, 1])
                with top[0]:
                    st.markdown(
                        f"**{p.get('name', 'Untitled')}** &middot; "
                        f"{len(p.get('legs', []))} legs &middot; "
                        f"{p.get('combined_odds_american', 0):+d} &middot; "
                        f"est {p.get('estimated_hit_pct', 0):.1f}% &middot; "
                        f"${p.get('stake', 0):.0f} stake"
                    )
                    st.caption(f"Saved {p.get('created_at', '')[:10]} · status: {p.get('status', '?')}")
                    for leg in p.get("legs", []):
                        side = (leg.get("side") or "").upper()
                        arrow = "↑" if side == "OVER" else "↓"
                        color = "#22c55e" if side == "OVER" else "#ef4444"
                        st.markdown(
                            f"<div style='padding-left:14px;color:#8b92a5;font-size:0.9rem;'>"
                            f"<span style='color:{color};font-weight:600;'>{arrow}</span> "
                            f"{leg.get('player')} {side} {leg.get('line', 0)} {leg.get('stat','').upper()}"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
                with top[1]:
                    if st.button("Delete", key=f"del_parlay_{p['id']}", use_container_width=True):
                        delete_parlay(p["id"])
                        st.rerun()

    st.stop()


if nav_choice == "Leaderboard":
    st.title("Leaderboard")
    st.caption("Whose saved picks have the best record. Tied to your manual pick log + saved parlays.")

    from auth import get_supabase
    sb_lb = get_supabase()
    if sb_lb is None:
        st.info("Leaderboard requires Supabase. Connect first.")
        st.stop()

    # Aggregate ALL graded parlays across users (anon can read by RLS policy)
    try:
        resp = sb_lb.table("parlays").select("*").execute()
        all_parlays = resp.data or []
    except Exception:
        all_parlays = []

    if not all_parlays:
        st.info("No saved parlays yet. Once people start building tickets, this will populate.")
        st.stop()

    # Build per-user stats
    rows = {}
    for p in all_parlays:
        email = p.get("user_email", "anonymous")
        if email not in rows:
            rows[email] = {"user": email.split("@")[0], "tickets": 0, "won": 0, "lost": 0, "open": 0, "profit": 0.0, "wagered": 0.0}
        rows[email]["tickets"] += 1
        stake = float(p.get("stake", 10.0))
        decimal = float(p.get("combined_odds_decimal", 1.0))
        rows[email]["wagered"] += stake
        status = p.get("status", "open")
        if status == "won":
            rows[email]["won"] += 1
            rows[email]["profit"] += stake * (decimal - 1)
        elif status == "lost":
            rows[email]["lost"] += 1
            rows[email]["profit"] -= stake
        elif status == "open":
            rows[email]["open"] += 1

    lb_df = pd.DataFrame(rows.values())
    if not lb_df.empty:
        lb_df["roi%"] = lb_df.apply(
            lambda r: (r["profit"] / r["wagered"] * 100) if r["wagered"] > 0 else 0.0,
            axis=1,
        ).round(1)
        lb_df = lb_df.sort_values("profit", ascending=False)
        st.dataframe(
            lb_df[["user", "tickets", "won", "lost", "open", "wagered", "profit", "roi%"]],
            use_container_width=True, hide_index=True,
            column_config={
                "wagered": st.column_config.NumberColumn("Wagered", format="$%.2f"),
                "profit": st.column_config.NumberColumn("Profit", format="$%+.2f"),
                "roi%": st.column_config.NumberColumn("ROI", format="%.1f%%"),
            },
        )
    st.stop()


if nav_choice == "Performance":
    st.title("Performance Analytics")
    st.caption(
        "How the auto-picks system is actually doing. "
        "ROI assumes standard -110 odds on every bet."
    )

    from performance import (
        fetch_auto_picks_graded, fetch_historical_props,
        summarize_picks, breakdown_by, summarize_historical_props,
    )

    range_opt = st.radio(
        "Date range",
        ["Last 7 days", "Last 30 days", "All time"],
        horizontal=True,
        key="perf_range",
    )
    date_from = None
    if range_opt == "Last 7 days":
        date_from = str(datetime.date.today() - datetime.timedelta(days=7))
    elif range_opt == "Last 30 days":
        date_from = str(datetime.date.today() - datetime.timedelta(days=30))

    odds_price = st.number_input(
        "Assumed price (American odds)", value=-110, step=5, key="perf_odds",
        help="Most player props juice at -110 to -120. Change to see ROI at different prices.",
    )

    picks_df = fetch_auto_picks_graded(date_from=date_from)
    props_df = fetch_historical_props(date_from=date_from)

    # --- Auto Picks overall ---
    st.subheader("Auto Picks overall")
    summary = summarize_picks(picks_df, odds=int(odds_price))
    if summary["bets"] == 0:
        st.info("No graded auto picks yet for this range. Wait for games to finish.")
    else:
        m = st.columns(6)
        m[0].metric("Graded bets", summary["bets"])
        m[1].metric("Won", summary["won"])
        m[2].metric("Lost", summary["lost"])
        m[3].metric("Push", summary["push"])
        m[4].metric("Win rate", f"{summary['win_rate']}%")
        roi_delta = f"{summary['profit']:+.2f} profit"
        m[5].metric("ROI", f"{summary['roi']}%", delta=roi_delta)
        if summary["roi"] > 0:
            st.success(f"Profitable at {int(odds_price)} odds.")
        else:
            st.warning(f"Not profitable at {int(odds_price)} odds.")

    # --- Breakdowns ---
    if not picks_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**By stat**")
            by_stat = breakdown_by(picks_df, "stat", odds=int(odds_price))
            if not by_stat.empty:
                st.dataframe(by_stat, use_container_width=True, hide_index=True,
                             column_config={"roi": st.column_config.NumberColumn(format="%.1f%%")})
        with c2:
            st.markdown("**By side (over/under)**")
            by_side = breakdown_by(picks_df, "side", odds=int(odds_price))
            if not by_side.empty:
                st.dataframe(by_side, use_container_width=True, hide_index=True,
                             column_config={"roi": st.column_config.NumberColumn(format="%.1f%%")})

        # Top-5 only breakdown
        st.markdown("**Top 5 picks only vs all strong picks**")
        top_only = picks_df[picks_df.get("is_top_pick", False) == True]  # noqa: E712
        rest = picks_df[picks_df.get("is_top_pick", False) != True]  # noqa: E712
        comp_rows = []
        if not top_only.empty:
            s = summarize_picks(top_only, odds=int(odds_price))
            comp_rows.append({"bucket": "Top 5 of day", **s})
        if not rest.empty:
            s = summarize_picks(rest, odds=int(odds_price))
            comp_rows.append({"bucket": "Rest of strong", **s})
        if comp_rows:
            st.dataframe(pd.DataFrame(comp_rows), use_container_width=True, hide_index=True)

        # Daily trend
        st.markdown("**Daily hit rate**")
        daily = picks_df.copy()
        daily["win"] = (daily["result"] == "won").astype(int)
        daily["bet"] = daily["result"].isin(["won", "lost"]).astype(int)
        daily_agg = daily.groupby("date").agg(won=("win", "sum"), bets=("bet", "sum")).reset_index()
        daily_agg = daily_agg[daily_agg["bets"] > 0]
        if not daily_agg.empty:
            daily_agg["win_rate"] = (daily_agg["won"] / daily_agg["bets"] * 100).round(1)
            import altair as alt
            chart = alt.Chart(daily_agg).mark_bar().encode(
                x=alt.X("date:N", title="Date"),
                y=alt.Y("win_rate:Q", title="Win rate (%)", scale=alt.Scale(domain=[0, 100])),
                color=alt.condition(
                    alt.datum.win_rate >= 52.4,  # break-even at -110
                    alt.value("#22c55e"),
                    alt.value("#ef4444"),
                ),
                tooltip=["date:N", "bets:Q", "won:Q", "win_rate:Q"],
            ).properties(height=200)
            breakeven = alt.Chart(pd.DataFrame({"y": [52.4]})).mark_rule(
                color="white", strokeDash=[4, 4]
            ).encode(y="y:Q")
            st.altair_chart(chart + breakeven, use_container_width=True)
            st.caption("Dashed line = break-even win rate at -110 odds (~52.4%).")

    # --- Backtest ---
    st.divider()
    st.subheader("Backtest — what would have happened")
    st.caption(
        "Simulates several strategies against every tracked prop line we've "
        "graded so far. Green ROI means that strategy was profitable at the price above."
    )
    if st.button("Run backtest", type="primary"):
        from backtest import run_all_strategies
        with st.spinner("Running backtest..."):
            bt = run_all_strategies(date_from=date_from, odds=int(odds_price))
        if bt.empty:
            st.info("Not enough graded data yet. Check back after a few days of auto-runs.")
        else:
            st.dataframe(
                bt,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "wagered": st.column_config.NumberColumn("Wagered", format="$%.2f"),
                    "profit": st.column_config.NumberColumn("Profit", format="$%+.2f"),
                    "roi": st.column_config.NumberColumn("ROI", format="%.1f%%"),
                },
            )

    # --- Strategy tuner ---
    st.divider()
    st.subheader("Strategy tuner")
    st.caption(
        "Sweep every combination of confidence/hit%/history% thresholds + side restriction "
        "against your graded auto-picks. The top rows tell you which filter combo would have "
        "produced the highest historical ROI."
    )
    if st.button("Run strategy sweep", key="run_strategy_sweep"):
        from strategy_tuner import sweep_strategies
        with st.spinner("Sweeping..."):
            sweep = sweep_strategies(picks_df, odds=int(odds_price))
        if sweep.empty:
            st.info("Not enough graded picks yet (need at least 20 per strategy). Wait a few weeks.")
        else:
            st.dataframe(
                sweep.head(50),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "roi": st.column_config.NumberColumn("ROI", format="%.1f%%"),
                    "win_rate": st.column_config.NumberColumn("Win %", format="%.1f%%"),
                    "profit": st.column_config.NumberColumn("Profit", format="$%+.2f"),
                },
            )

    # --- Historical props (tracked lines) ---
    st.divider()
    st.subheader("Tracked book-line history")
    st.caption("How often actual book lines resolved over vs. under across all tracked props.")
    hsum = summarize_historical_props(props_df)
    if hsum["total"] == 0:
        st.info("No graded tracked props yet for this range.")
    else:
        m = st.columns(5)
        m[0].metric("Graded lines", hsum["total"])
        m[1].metric("Over", hsum["over"])
        m[2].metric("Under", hsum["under"])
        m[3].metric("Push", hsum["push"])
        m[4].metric("Over rate", f"{hsum['over_rate']}%")

    st.stop()


if nav_choice == "Analytics" and is_admin():
    st.title("Analytics")
    st.caption("Who's using the platform, when, and what they're doing.")

    from activity import (
        fetch_activity_since, fetch_recent_activity,
        summarize, dau_series, per_user_summary,
    )

    range_opt = st.radio(
        "Range",
        ["Last 7 days", "Last 30 days", "Last 90 days"],
        horizontal=True,
        label_visibility="collapsed",
        key="analytics_range",
    )
    days = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}[range_opt]
    activity_df = fetch_activity_since(days=days)

    summary = summarize(activity_df)
    if summary["events"] == 0:
        st.info("No activity yet in this range. Events will start recording after the next sign-in / page load.")
        st.stop()

    m = st.columns(5)
    m[0].metric("Total events", summary["events"])
    m[1].metric("Unique users", summary["unique_users"])
    m[2].metric("Logins", summary["logins"])
    m[3].metric("Player views", summary["player_views"])
    m[4].metric("AI queries", summary["ai_queries"])

    # DAU chart
    st.subheader("Daily active users")
    dau = dau_series(activity_df)
    if not dau.empty:
        import altair as alt
        chart = alt.Chart(dau).mark_bar(color="#22c55e").encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("dau:Q", title="Distinct users"),
            tooltip=["date:T", "dau:Q"],
        ).properties(height=220)
        st.altair_chart(chart, use_container_width=True)

    # Per-user breakdown
    st.subheader("Per-user breakdown")
    per_user = per_user_summary(activity_df)
    if not per_user.empty:
        st.dataframe(
            per_user,
            use_container_width=True,
            hide_index=True,
            column_config={
                "user_email": "Email",
                "events": "Events",
                "active_days": "Active days",
                "last_seen": "Last seen",
                "first_seen": "First seen",
            },
        )

    # Recent activity feed
    st.subheader("Recent events")
    recent = fetch_recent_activity(limit=200)
    if not recent.empty:
        display = recent[["created_at", "user_email", "action", "details"]].copy()
        display["created_at"] = pd.to_datetime(display["created_at"]).dt.strftime("%Y-%m-%d %H:%M UTC")
        # Turn details JSON into a readable string
        display["details"] = display["details"].apply(
            lambda d: ", ".join(f"{k}={v}" for k, v in (d or {}).items()) if isinstance(d, dict) else str(d)
        )
        st.dataframe(display, use_container_width=True, hide_index=True, column_config={
            "created_at": "When",
            "user_email": "User",
            "action": "Action",
            "details": "Details",
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
                sport_label=selected_sport,
            )
            try:
                from activity import log, ACTION_AI_ANALYSIS
                log(ACTION_AI_ANALYSIS, {
                    "player": ai_player, "stat": ai_stat,
                    "line": float(ai_line), "side": ai_side.lower(),
                })
            except Exception:
                pass

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
            from auth import get_supabase
            import os as _os

            srv = _os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or st.secrets.get("SUPABASE_SERVICE_ROLE_KEY", "")
            refresh_tok = _os.environ.get("REFRESH_TOKEN") or st.secrets.get("REFRESH_TOKEN", "")
            odds_key = _os.environ.get("ODDS_API_KEY") or st.secrets.get("ODDS_API_KEY", "")

            # Count total auto_picks rows + most recent date
            latest_date = None
            total_picks = 0
            try:
                sb_read = get_supabase()
                if sb_read:
                    resp = sb_read.table("auto_picks").select("date", count="exact").order("date", desc=True).limit(1).execute()
                    total_picks = resp.count or 0
                    if resp.data:
                        latest_date = resp.data[0].get("date")
            except Exception as e:
                st.warning(f"Couldn't query auto_picks: {e}")

            st.write({
                "service_role_key_present": bool(srv),
                "refresh_token_present": bool(refresh_tok),
                "odds_api_key_present": bool(odds_key),
                "total_auto_picks_rows_in_supabase": total_picks,
                "most_recent_picks_date": latest_date,
                "last_job_status_this_session": st.session_state.get("_last_job_status", "not yet attempted this session"),
            })

            st.markdown(
                "**Test the webhook synchronously** (shows exactly what happens, step by step):"
            )
            if refresh_tok:
                debug_url = f"?refresh_token={refresh_tok}&debug=1"
                st.code(debug_url, language="text")
                st.caption("Append this to your app URL to run the pipeline synchronously and see every step's result.")
            else:
                st.warning("REFRESH_TOKEN not set — webhook won't work until you add it to secrets.")
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

# --- No-games day handling ---
# If the slate is empty, show a clear message instead of an empty board
if not events:
    st.markdown(
        f"""
        <div style='background:#1a1d24;border:1px solid #2a2f3a;border-radius:10px;
                    padding:32px;text-align:center;margin:24px 0;'>
          <div style='font-size:3rem;opacity:0.3;'>\U0001f3c0</div>
          <div style='color:#e6edf3;font-size:1.3rem;font-weight:700;margin-top:8px;'>
            No NBA games on {selected_date.strftime('%A, %b %-d') if os.name != 'nt' else selected_date.strftime('%A, %b %#d')}
          </div>
          <div style='color:#8b92a5;font-size:0.95rem;margin-top:8px;'>
            Try a different date from the sidebar, or explore <strong>Performance</strong>,
            <strong>What-If</strong>, or <strong>Auto Picks</strong> history in the top nav.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

# --- Today's games ---
with st.expander(f"Today's Games ({len(events)})", expanded=False):
    cols = st.columns(min(len(events), 4) if events else 1)
    for i, event in enumerate(events):
        with cols[i % len(cols)]:
            st.markdown(f"**{event['away_team']}**  \n@ {event['home_team']}")


# --- Today at a glance (digest card) ---
try:
    from digest import fetch_today_picks
    today_picks = fetch_today_picks(datetime.date.today())
    if today_picks:
        n_top = sum(1 for p in today_picks if p.get("is_top_pick"))
        with st.container():
            st.markdown(
                f"""
                <div style='background:#1a1d24;border:1px solid #2a2f3a;
                            border-radius:8px;padding:14px 18px;margin-bottom:16px;'>
                  <div style='color:#22c55e;font-weight:700;font-size:0.8rem;
                              text-transform:uppercase;letter-spacing:0.05em;'>
                    Today at a glance
                  </div>
                  <div style='color:#e6edf3;font-size:1rem;margin-top:6px;'>
                    <strong>{len(today_picks)}</strong> auto picks generated ·
                    <strong>{n_top}</strong> flagged as top 5 of each side
                  </div>
                  <div style='color:#8b92a5;font-size:0.85rem;margin-top:4px;'>
                    See them all under the <strong>Auto Picks</strong> tab, or
                    subscribe to Discord for a daily summary.
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
except Exception:
    pass

with st.container():
    top_overs = gather_top_picks(results, "over", limit=5)
    top_unders = gather_top_picks(results, "under", limit=5)
    if not top_overs.empty or not top_unders.empty:
        st.subheader("Today's Top Picks")
        tp_col1, tp_col2 = st.columns(2)
        with tp_col1:
            st.markdown("**Strongest Overs**")
            if top_overs.empty:
                st.caption("No strong overs found.")
            else:
                for _, r in top_overs.iterrows():
                    st.markdown(render_top_pick_row(r, "over"), unsafe_allow_html=True)
        with tp_col2:
            st.markdown("**Strongest Unders**")
            if top_unders.empty:
                st.caption("No strong unders found.")
            else:
                for _, r in top_unders.iterrows():
                    st.markdown(render_top_pick_row(r, "under"), unsafe_allow_html=True)

st.divider()

# --- Stat selector ---
# Stat selector pills are sport-aware: {display label: stat_key} for the
# selected sport (config.SPORT_STAT_CONFIGS).
STAT_LABELS = stat_labels_for(sport_key)
stat_col, view_col = st.columns([4, 1])
with stat_col:
    # st.pills is a newer, more compact selector. Falls back to radio on older
    # Streamlit versions.
    try:
        stat_tab = st.pills(
            "Stat", list(STAT_LABELS.keys()),
            default=list(STAT_LABELS.keys())[0],
            label_visibility="collapsed",
            selection_mode="single",
        )
        if stat_tab is None:
            stat_tab = list(STAT_LABELS.keys())[0]
    except AttributeError:
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
        try:
            from activity import log, ACTION_PLAYER_VIEW
            log(ACTION_PLAYER_VIEW, {"player": picked, "via": "search"})
        except Exception:
            pass
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

    st.markdown("**Game status filters**")
    show_live = st.checkbox(
        "Show live games",
        value=False,
        help="Games currently in progress. Live lines shift during the game and aren't pre-game lines.",
    )
    show_completed = st.checkbox(
        "Show completed games",
        value=False,
        help="Games that have already finished. Kept hidden by default since you can't bet them anymore.",
    )

    st.markdown("**View**")
    advanced_toggle = st.checkbox(
        "Show all columns (advanced)",
        value=st.session_state.get("show_advanced_columns", False),
        help="Default view shows ~14 essential columns. Toggle on to see all 26.",
    )
    st.session_state["show_advanced_columns"] = advanced_toggle

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

# Respect the per-status toggles
if "game_status" in filtered.columns:
    allowed = {"pregame", "unknown"}
    if show_live:
        allowed.add("live")
    if show_completed:
        allowed.add("completed")
    filtered = filtered[filtered["game_status"].isin(allowed)]

# --- Display columns: respect the 'Advanced columns' sidebar toggle ---
advanced = st.session_state.get("show_advanced_columns", False)
_cols_source = DISPLAY_COLS_ALL if advanced else DISPLAY_COLS_DEFAULT
show_cols = [c for c in _cols_source if c in filtered.columns]

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
