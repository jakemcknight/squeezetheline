"""
Backtesting — simulate what would have happened if we'd bet every graded
pick over a historical range. Uses the historical_props table (our own
snapshotted + graded prop data) as the ground truth.

Strategies we simulate:
- Blindly bet every over (or every under)
- Only bet when the player's season-avg beats the line by N points
- Only bet when this-season hit rate > threshold
- Only bet Strong Overs / Strong Unders (auto_picks data)
"""

import pandas as pd
import datetime
from typing import Optional

from performance import summarize_picks, _american_to_payout, _roi_at_price


def _anon():
    from auth import get_supabase
    return get_supabase()


def fetch_graded_props(date_from: Optional[str] = None, date_to: Optional[str] = None) -> pd.DataFrame:
    sb = _anon()
    if sb is None:
        return pd.DataFrame()
    try:
        q = sb.table("historical_props").select("*").eq("status", "graded").order("date", desc=True)
        if date_from:
            q = q.gte("date", date_from)
        if date_to:
            q = q.lte("date", date_to)
        resp = q.limit(10000).execute()
        return pd.DataFrame(resp.data or [])
    except Exception:
        return pd.DataFrame()


def simulate_blind(props: pd.DataFrame, side: str, odds: int = -110) -> dict:
    """Bet every prop on the chosen side. Returns ROI summary."""
    if props.empty:
        return _roi_at_price(0, 0, 0, odds)
    # Pretend every bet was on the given side
    won = int(((props["result"] == side)).sum())
    lost = int(((props["result"] != side) & (props["result"] != "push")).sum())
    push = int((props["result"] == "push").sum())
    return _roi_at_price(won, lost, push, odds)


def simulate_auto_picks(date_from: Optional[str] = None, odds: int = -110) -> dict:
    """Treat our auto_picks table as a strategy: all were bets, grade them.
    Returns ROI at the given price."""
    sb = _anon()
    if sb is None:
        return _roi_at_price(0, 0, 0, odds)
    try:
        q = sb.table("auto_picks").select("*").neq("result", "pending")
        if date_from:
            q = q.gte("date", date_from)
        resp = q.limit(10000).execute()
        df = pd.DataFrame(resp.data or [])
    except Exception:
        return _roi_at_price(0, 0, 0, odds)
    return summarize_picks(df, odds=odds)


def run_all_strategies(date_from: Optional[str] = None, date_to: Optional[str] = None,
                       odds: int = -110) -> pd.DataFrame:
    """Compare several blind strategies side by side."""
    props = fetch_graded_props(date_from, date_to)

    rows = []
    rows.append({"strategy": "Auto picks (our strong picks)", **simulate_auto_picks(date_from, odds)})
    rows.append({"strategy": "Bet every OVER", **simulate_blind(props, "over", odds)})
    rows.append({"strategy": "Bet every UNDER", **simulate_blind(props, "under", odds)})

    # Per-stat blind overs
    if not props.empty and "stat" in props.columns:
        for stat, group in props.groupby("stat"):
            rows.append({"strategy": f"Every OVER — {stat}", **simulate_blind(group, "over", odds)})
            rows.append({"strategy": f"Every UNDER — {stat}", **simulate_blind(group, "under", odds)})

    return pd.DataFrame(rows)
