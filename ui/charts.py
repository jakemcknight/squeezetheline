"""Chart helpers for the Streamlit UI."""

import os

import pandas as pd
import altair as alt


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
