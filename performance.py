"""
Performance analytics — aggregate stats over the auto_picks and
historical_props tables in Supabase.

Answers:
- What's my overall auto-pick hit rate? Trending over time?
- Which stat types have the best hit rate?
- Strong vs Trending, top 5 vs all — which buckets actually work?
- What ROI would I have earned at -110 odds?
- Does hit% > 50% on picks actually translate to profit?
"""

import datetime
from typing import Optional

import pandas as pd


def _anon():
    from auth import get_supabase
    return get_supabase()


def _american_to_payout(odds: int) -> float:
    """Convert American odds to profit per $1 staked on a winning bet."""
    if odds > 0:
        return odds / 100.0
    return 100.0 / abs(odds)


def _roi_at_price(won: int, lost: int, push: int, odds: int) -> dict:
    """Compute ROI at a given price. $1 stake on each bet."""
    bets = won + lost + push
    if bets == 0:
        return {"bets": 0, "wagered": 0.0, "profit": 0.0, "roi": 0.0}
    payout = _american_to_payout(odds)
    profit = (won * payout) - lost  # pushes = break even
    wagered = bets  # $1 each
    roi = (profit / wagered) * 100 if wagered else 0.0
    return {
        "bets": bets,
        "wagered": round(wagered, 2),
        "profit": round(profit, 2),
        "roi": round(roi, 2),
    }


def fetch_auto_picks_graded(date_from: Optional[str] = None) -> pd.DataFrame:
    sb = _anon()
    if sb is None:
        return pd.DataFrame()
    try:
        q = sb.table("auto_picks").select("*").neq("result", "pending").order("date", desc=True)
        if date_from:
            q = q.gte("date", date_from)
        resp = q.execute()
        return pd.DataFrame(resp.data or [])
    except Exception:
        return pd.DataFrame()


def fetch_historical_props(date_from: Optional[str] = None) -> pd.DataFrame:
    sb = _anon()
    if sb is None:
        return pd.DataFrame()
    try:
        q = sb.table("historical_props").select("*").eq("status", "graded").order("date", desc=True)
        if date_from:
            q = q.gte("date", date_from)
        resp = q.execute()
        return pd.DataFrame(resp.data or [])
    except Exception:
        return pd.DataFrame()


def summarize_picks(df: pd.DataFrame, odds: int = -110) -> dict:
    """Summary: total bets, won, lost, push, win rate, ROI at given odds."""
    if df.empty:
        return {"bets": 0, "won": 0, "lost": 0, "push": 0, "win_rate": 0.0, **_roi_at_price(0, 0, 0, odds)}
    won = int((df["result"] == "won").sum())
    lost = int((df["result"] == "lost").sum())
    push = int((df["result"] == "push").sum())
    win_rate = (won / max(won + lost, 1)) * 100
    return {
        "bets": won + lost + push,
        "won": won,
        "lost": lost,
        "push": push,
        "win_rate": round(win_rate, 1),
        **_roi_at_price(won, lost, push, odds),
    }


def breakdown_by(df: pd.DataFrame, group_col: str, odds: int = -110) -> pd.DataFrame:
    """Group by a column and compute summary per group."""
    if df.empty or group_col not in df.columns:
        return pd.DataFrame()
    rows = []
    for key, group in df.groupby(group_col):
        summary = summarize_picks(group, odds)
        rows.append({group_col: key, **summary})
    return pd.DataFrame(rows).sort_values("roi", ascending=False)


def summarize_historical_props(df: pd.DataFrame) -> dict:
    """For the historical_props table, the 'result' field is over/under/push
    rather than won/lost. This summary is about the raw over/under distribution,
    not bet outcomes — just how often the book was right."""
    if df.empty:
        return {"total": 0, "over": 0, "under": 0, "push": 0, "over_rate": 0.0}
    total = len(df)
    overs = int((df["result"] == "over").sum())
    unders = int((df["result"] == "under").sum())
    pushes = int((df["result"] == "push").sum())
    over_rate = (overs / max(overs + unders, 1)) * 100
    return {
        "total": total,
        "over": overs,
        "under": unders,
        "push": pushes,
        "over_rate": round(over_rate, 1),
    }


def ev_and_kelly(hit_pct: float, odds: int, bankroll: float = 1000.0, kelly_fraction: float = 0.25) -> dict:
    """Given our estimated hit % and the offered American odds, compute
    expected value per $1 staked and suggested bet size (quarter-Kelly)."""
    if hit_pct is None:
        return {}
    p = hit_pct / 100.0
    payout = _american_to_payout(odds)

    # Expected value per $1 staked
    ev = p * payout - (1 - p)

    # Kelly fraction: f = (bp - q) / b where b=payout, p=win prob, q=1-p
    b = payout
    q = 1 - p
    kelly_full = (b * p - q) / b if b > 0 else 0
    kelly_full = max(0.0, kelly_full)  # never negative
    kelly_scaled = kelly_full * kelly_fraction
    suggested = bankroll * kelly_scaled

    # Implied probability from odds (the "break-even" win rate at this price)
    if odds > 0:
        implied = 100.0 / (odds + 100.0)
    else:
        implied = abs(odds) / (abs(odds) + 100.0)

    return {
        "hit_pct": round(hit_pct, 1),
        "odds": odds,
        "implied_pct": round(implied * 100, 1),
        "edge_pct": round((p - implied) * 100, 1),
        "ev_per_dollar": round(ev, 3),
        "kelly_full_pct": round(kelly_full * 100, 1),
        "kelly_quarter_pct": round(kelly_scaled * 100, 2),
        "suggested_stake": round(suggested, 2),
    }
