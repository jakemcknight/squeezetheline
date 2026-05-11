"""
Strategy tuner — sweep parameter grids against accumulated historical_props
to find the most profitable filter combinations.

Reuses the same data the Performance tab does: graded auto_picks rows.
The tuner asks: 'if I had only bet picks where {confidence > X AND hit_pct > Y},
what ROI would I have gotten?'

Strategy variables we sweep:
- min_confidence (0, 30, 50, 70, 85)
- min_hit_pct (0, 50, 60, 70)
- min_history_hit_pct (0, 50, 60, 70)
- side restriction (any / over only / under only)
- top_pick_only (True / False)
"""

import itertools
import pandas as pd
from typing import Optional


def _american_payout(odds: int) -> float:
    return odds / 100.0 if odds > 0 else 100.0 / abs(odds)


def _summarize(df: pd.DataFrame, odds: int) -> dict:
    if df.empty:
        return {"bets": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "roi": 0.0, "profit": 0.0}
    wins = int((df["result"] == "won").sum())
    losses = int((df["result"] == "lost").sum())
    pushes = int((df["result"] == "push").sum())
    bets = wins + losses + pushes
    if bets == 0:
        return {"bets": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "roi": 0.0, "profit": 0.0}
    payout = _american_payout(odds)
    profit = wins * payout - losses
    win_rate = wins / max(wins + losses, 1) * 100
    roi = (profit / bets) * 100
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "roi": round(roi, 1),
        "profit": round(profit, 2),
    }


def _apply_filter(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    out = df.copy()
    if cfg.get("min_confidence", 0) > 0 and "score" in out.columns:
        out = out[out["score"].fillna(0) >= cfg["min_confidence"]]
    if cfg.get("min_hit_pct", 0) > 0 and "hit_pct" in out.columns:
        out = out[out["hit_pct"].fillna(0) >= cfg["min_hit_pct"]]
    if cfg.get("min_history_hit_pct", 0) > 0 and "history_hit_pct" in out.columns:
        out = out[out["history_hit_pct"].fillna(0) >= cfg["min_history_hit_pct"]]
    if cfg.get("side_only") in ("over", "under"):
        out = out[out["side"] == cfg["side_only"]]
    if cfg.get("top_pick_only") and "is_top_pick" in out.columns:
        out = out[out["is_top_pick"] == True]  # noqa: E712
    return out


def sweep_strategies(graded_picks: pd.DataFrame, odds: int = -110, min_bets: int = 20) -> pd.DataFrame:
    """Run a grid sweep. Returns a DataFrame of strategy → ROI/win rate."""
    if graded_picks.empty:
        return pd.DataFrame()

    sides = ["any", "over", "under"]
    confs = [0, 30, 50, 70, 85]
    hits = [0, 50, 60, 70]
    hists = [0, 50, 60, 70]
    top_onlys = [False, True]

    results = []
    for side, conf, hit, hist, top in itertools.product(sides, confs, hits, hists, top_onlys):
        cfg = {
            "side_only": side if side != "any" else None,
            "min_confidence": conf,
            "min_hit_pct": hit,
            "min_history_hit_pct": hist,
            "top_pick_only": top,
        }
        filtered = _apply_filter(graded_picks, cfg)
        summary = _summarize(filtered, odds)
        if summary["bets"] < min_bets:
            continue
        results.append({
            "side": side,
            "min_conf": conf,
            "min_hit%": hit,
            "min_hist%": hist,
            "top_only": top,
            **summary,
        })

    df = pd.DataFrame(results)
    if df.empty:
        return df
    return df.sort_values("roi", ascending=False)
