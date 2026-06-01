"""Top-pick ranking and filtering helpers for the picks board.

The strong/trending over/under filters themselves live in :mod:`analysis`; this
module holds the composite scoring used to rank the "Today's Top Picks" panel.
"""

import pandas as pd


def composite_score(row, side: str) -> float:
    """Smarter ranking: combines deltas, hit-rate edges, defensive matchup,
    rest/B2B context, line-movement direction, ML agreement, and pre-computed
    confidence score. Higher = stronger pick."""
    d = row.get("delta", 0) or 0
    d5 = row.get("delta_5g", 0) or 0
    d10 = row.get("delta_10g", 0) or 0
    avg_delta = (abs(d) + abs(d5) + abs(d10)) / 3

    hit = row.get("hit%", 0) or 0
    hist = row.get("history_hit%", 0) or 0
    if side == "over":
        edge = (hit - 50) + (hist - 50)
    else:
        edge = (50 - hit) + (50 - hist)

    base = avg_delta * (edge / 10 if edge > 0 else 0)

    # Boost for confidence column if present
    conf = row.get("confidence")
    if conf is not None and not pd.isna(conf):
        base *= (1.0 + float(conf) / 200)  # +50% at confidence=100

    # Defensive matchup bonus (high rank = weak D = good for overs)
    rank = row.get("rank")
    if rank is not None and not pd.isna(rank):
        if side == "over" and rank > 22:
            base *= 1.10
        elif side == "under" and rank < 8:
            base *= 1.10

    # Opponent on B2B = easier for player → boost overs
    if row.get("opp_b2b") and side == "over":
        base *= 1.05

    # Player on B2B = tired → boost unders
    if row.get("b2b") and side == "under":
        base *= 1.05

    return float(base)


def gather_top_picks(results: dict, side: str, limit: int = 5) -> pd.DataFrame:
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
        labelled["score"] = labelled.apply(lambda r: composite_score(r, side), axis=1)
        rows.append(labelled)
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    return combined.sort_values("score", ascending=False).head(limit)


def render_top_pick_row(row, side: str) -> str:
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
