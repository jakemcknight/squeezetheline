"""
Claude-powered prop bet analysis.

Admin-only feature: pick a player + stat + line + side (over/under),
and Claude gives a verdict (LEAN OVER / LEAN UNDER / PASS) with reasoning,
using all the data we already have about that player + matchup.

Requires ANTHROPIC_API_KEY in Streamlit secrets. Uses Claude Sonnet 4.6
by default (fast + cheap + very capable for this type of analysis).
"""

import os
import json
from typing import Optional

import streamlit as st
import pandas as pd


DEFAULT_MODEL = "claude-sonnet-4-6"

STAT_LABEL = {
    "points": "Points",
    "rebounds": "Rebounds",
    "assists": "Assists",
    "pra": "Points + Rebounds + Assists",
    "threes": "3-Pointers Made",
    "steals": "Steals",
    "blocks": "Blocks",
}


def _get_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    try:
        return st.secrets["ANTHROPIC_API_KEY"]
    except Exception:
        return ""


def _safe(val, fmt="{:.1f}") -> str:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "n/a"
        return fmt.format(val)
    except Exception:
        return str(val)


def build_context_block(player: str, stat: str, summary: dict, result_row: Optional[dict]) -> str:
    """Render a markdown block with everything we know about this prop."""
    stat_label = STAT_LABEL.get(stat, stat)
    lines = summary.get("today_lines", {}) or {}
    season_avg = summary.get("season_avg", {}) or {}
    career_avg = summary.get("career_avg", {}) or {}
    home_avg = summary.get("home_avg") or {}
    away_avg = summary.get("away_avg") or {}
    vs_opp_avg = summary.get("vs_opponent_avg") or {}
    vs_opp = summary.get("vs_opponent") or []
    last_20 = summary.get("last_20") or []
    injury = summary.get("injury") or {}
    team = summary.get("team", "")
    position = summary.get("position", "")

    # Fields from the per-stat results row (deltas, hit%, matchup, rest, etc.)
    r = result_row or {}

    def stat_key_in_game(game: dict) -> float:
        # last_20 game dicts use 'pts' / 'reb' / 'ast' etc.
        key_map = {
            "points": "pts", "rebounds": "reb", "assists": "ast",
            "pra": "pra", "threes": "threes", "steals": "steals", "blocks": "blocks",
        }
        return float(game.get(key_map.get(stat, stat), 0) or 0)

    last_10_line = ", ".join(
        f"{g.get('date','?')} vs {g.get('opponent','?')}: {stat_key_in_game(g):.0f}"
        for g in last_20[:10]
    )

    vs_opp_line = ", ".join(
        f"{g.get('date','?')}: {stat_key_in_game(g):.0f}"
        for g in vs_opp[:10]
    )

    # Pull tracked book-line history if we've accumulated any
    try:
        from prop_history import get_player_line_history
        line_for_lookup = lines.get(stat)
        line_hist = get_player_line_history(player, stat, near_line=float(line_for_lookup) if line_for_lookup else None)
    except Exception:
        line_hist = {"available": False}

    line_history_block = ""
    if line_hist.get("available") and line_hist.get("all_games", 0) > 0:
        all_n = line_hist["all_games"]
        all_o = line_hist["all_overs"]
        all_pct = (all_o / all_n * 100) if all_n else 0
        near_n = line_hist["near_games"]
        near_o = line_hist["near_overs"]
        near_pct = (near_o / near_n * 100) if near_n else 0
        line_history_block = f"""
### Tracked book-line history (real lines, not arbitrary thresholds)
- All graded historical lines: {all_o} OVERs / {all_n} games ({all_pct:.0f}%)
- Lines within ±1 of tonight's line: {near_o}/{near_n} ({near_pct:.0f}%)
"""

    ctx = f"""### Player: {player}
- Team: {team}
- Position: {position}
- Injury status: {injury.get('status', 'not listed') or 'not listed'}
  - Comment: {injury.get('comment', '—') or '—'}

### Stat under consideration: {stat_label}
- Tonight's line: {lines.get(stat, 'n/a')}
- Season average: {_safe(season_avg.get(stat))}  ({season_avg.get('games', 0)} games)
- Career average: {_safe(career_avg.get(stat))}  ({career_avg.get('games', 0)} games)
- Home average: {_safe(home_avg.get(stat))}  ({home_avg.get('games', 0)} games)
- Away average: {_safe(away_avg.get(stat))}  ({away_avg.get('games', 0)} games)
- Last-5 average: {_safe(r.get(f'{stat}_5g'))}
- Last-10 average: {_safe(r.get(f'{stat}_10g'))}
- Standard deviation: {_safe(r.get('std_dev'))}
- Stat per minute: {_safe(r.get('spm'), '{:.2f}')}

### Recent form (last 10 games, most recent first)
{last_10_line or 'no data'}

### Hit rate vs tonight's line
- This season: {_safe(r.get('hit%'), '{:.0f}%')}
- Career: {_safe(r.get('history_hit%'), '{:.0f}%')}

### Matchup
- Opponent: {r.get('opponent', 'n/a')}
- Opponent defensive rank vs {position}: {_safe(r.get('rank'), '{:.0f}')} (1 = toughest, 30 = softest)
- Rest days: {_safe(r.get('rest_days'), '{:.0f}')}
- Back-to-back for player: {'yes' if r.get('b2b') else 'no'}
- Opponent's rest days: {_safe(r.get('opp_rest'), '{:.0f}')}
- Opponent on back-to-back: {'yes' if r.get('opp_b2b') else 'no'}

### History vs this opponent
- Games: {vs_opp_avg.get('games', 0)}
- Career average vs opponent: {_safe(vs_opp_avg.get(stat))}
- Most recent matchups: {vs_opp_line or 'no data'}
- Hit this line vs this opponent (season): {r.get('vs_opp_season', 'n/a')}
- Hit this line vs this opponent (career): {r.get('vs_opp_career', 'n/a')}

### Trend indicator
- Direction: {r.get('trend', 'n/a')} (up / down / flat based on last-5 vs last-10)
{line_history_block}"""
    return ctx


def analyze_prop(player: str, stat: str, line: float, side: str,
                 summary: dict, result_row: Optional[dict],
                 model: str = DEFAULT_MODEL) -> dict:
    """Send the prop + context to Claude and return its analysis.

    Returns {'text': str, 'usage': {...}} on success, or {'error': str} on failure.
    """
    api_key = _get_api_key()
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY not configured in Streamlit secrets."}

    try:
        from anthropic import Anthropic
    except ImportError:
        return {"error": "anthropic package not installed. Check requirements.txt."}

    stat_label = STAT_LABEL.get(stat, stat)
    side_upper = side.upper()

    context = build_context_block(player, stat, summary, result_row)

    user_msg = f"""Evaluate this NBA player prop for me.

**Prop**: {player} {side_upper} {line:.1f} {stat_label}

Here's all the data I have:

{context}

Please respond with:

1. **Verdict**: one of `LEAN OVER`, `LEAN UNDER`, or `PASS`
2. **Confidence**: `LOW`, `MEDIUM`, or `HIGH`
3. **Agreement with my side ({side_upper})**: `Yes` / `No` / `Neutral`
4. **Top 3 factors supporting your verdict**
5. **Top 2 risks or reasons to be cautious**
6. **One-sentence bottom line**

Be decisive. I'd rather you say PASS on a thin spot than hedge on every prop.
Weight recent form and matchup heavily. Discount tiny sample sizes.
Call out any red flags (injury, DNP, blowout risk) clearly.
"""

    system = (
        "You are an expert NBA prop bettor. You analyze data cleanly, weigh "
        "sample sizes properly, and deliver crisp, actionable verdicts with "
        "clear reasoning. You are NOT giving financial advice — the user "
        "understands this is for entertainment."
    )

    try:
        client = Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=1200,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        # Extract text from content blocks
        text = ""
        for block in msg.content:
            if getattr(block, "type", None) == "text":
                text += block.text
        return {
            "text": text,
            "usage": {
                "input_tokens": msg.usage.input_tokens,
                "output_tokens": msg.usage.output_tokens,
            },
            "model": model,
        }
    except Exception as e:
        return {"error": f"Claude API error: {e}"}
