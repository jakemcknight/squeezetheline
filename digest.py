"""
Daily digest: build a readable summary of today's auto-picks + any
line movement, and (optionally) post it to a Discord channel via webhook.

Config (all optional, in Streamlit secrets):
- DISCORD_WEBHOOK_URL: where to post (omit to only render on-site)
"""

import os
import datetime
from typing import Optional

import requests
import pandas as pd


def _anon():
    from auth import get_supabase
    return get_supabase()


def _secret(name: str, default: str = "") -> str:
    val = os.environ.get(name)
    if val:
        return val
    try:
        import streamlit as st
        return st.secrets.get(name, default)
    except Exception:
        return default


STAT_LABEL = {
    "points": "PTS", "rebounds": "REB", "assists": "AST",
    "pra": "PRA", "threes": "3PM", "steals": "STL", "blocks": "BLK",
}


def fetch_today_picks(date: Optional[datetime.date] = None) -> list[dict]:
    """Return auto picks for the given date (defaults to today)."""
    sb = _anon()
    if sb is None:
        return []
    d = date or datetime.date.today()
    try:
        resp = (
            sb.table("auto_picks")
            .select("*")
            .eq("date", str(d))
            .order("is_top_pick", desc=True)
            .order("score", desc=True)
            .execute()
        )
        return resp.data or []
    except Exception:
        return []


def build_digest_text(picks: list[dict], date: Optional[datetime.date] = None) -> str:
    """Build a markdown-style digest string suitable for Discord or on-site."""
    d = date or datetime.date.today()
    if not picks:
        return f"**Squeeze the Line — {d.strftime('%a %b %-d')}**\nNo auto picks generated for today yet."

    top = [p for p in picks if p.get("is_top_pick")]
    rest = [p for p in picks if not p.get("is_top_pick")]

    lines = [f"**Squeeze the Line — {d.strftime('%a %b %-d')}**"]
    lines.append(f"_{len(picks)} total picks · {len(top)} flagged as top 5_")

    def _fmt_pick(p: dict) -> str:
        stat = STAT_LABEL.get(p["stat"], p["stat"].upper())
        side = p["side"].upper()
        arrow = "⬆️" if p["side"] == "over" else "⬇️"
        hit = p.get("hit_pct", 0) or 0
        hist = p.get("history_hit_pct", 0) or 0
        return (
            f"{arrow} **{p['player']}** {side} {p['line']:.1f} {stat} "
            f"({p.get('team','')} vs {p.get('opponent','')}) · "
            f"hit {hit:.0f}% / hist {hist:.0f}%"
        )

    if top:
        lines.append("")
        lines.append("**Top 5 of each side**")
        for p in top:
            lines.append(_fmt_pick(p))

    if rest:
        lines.append("")
        lines.append(f"**Other strong picks ({len(rest)})**")
        for p in rest[:10]:
            lines.append(_fmt_pick(p))
        if len(rest) > 10:
            lines.append(f"_… and {len(rest) - 10} more_")

    return "\n".join(lines)


def post_to_discord(text: str, webhook_url: Optional[str] = None) -> bool:
    """Post a message to a Discord channel via incoming webhook.

    Discord content limit is 2000 chars; split into chunks if needed.
    """
    url = webhook_url or _secret("DISCORD_WEBHOOK_URL")
    if not url:
        return False
    # Split on lines to avoid hitting 2000-char limit mid-sentence
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > 1900:
            chunks.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line
    if current:
        chunks.append(current)

    ok = True
    for chunk in chunks:
        try:
            resp = requests.post(url, json={"content": chunk}, timeout=10)
            if resp.status_code >= 300:
                ok = False
                print(f"Discord post failed: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            ok = False
            print(f"Discord post error: {e}")
    return ok


def send_daily_digest(date: Optional[datetime.date] = None) -> dict:
    """Top-level: build and send. Called from the webhook after refresh."""
    d = date or datetime.date.today()
    picks = fetch_today_picks(d)
    text = build_digest_text(picks, d)
    sent = post_to_discord(text) if _secret("DISCORD_WEBHOOK_URL") else False
    return {"picks": len(picks), "discord_sent": sent, "text": text}
