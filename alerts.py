"""
Priority alerts (separate from the daily digest).

Two kinds:
- Line-movement alerts: any prop where the line has moved >= 1.5 from open
- Big-edge alerts: any auto pick with EV > threshold at -110 odds

Sends a single email via the existing Resend integration. Designed to
fire from the cron webhook in addition to the daily digest.
"""

import datetime
import os
from typing import Optional


def _secret(name: str, default: str = "") -> str:
    val = os.environ.get(name)
    if val:
        return val
    try:
        import streamlit as st
        return st.secrets.get(name, default)
    except Exception:
        return default


def _anon():
    from auth import get_supabase
    return get_supabase()


STAT_LABEL = {
    "points": "PTS", "rebounds": "REB", "assists": "AST",
    "pra": "PRA", "threes": "3PM", "steals": "STL", "blocks": "BLK",
}

LOGO_URL = "https://raw.githubusercontent.com/jakemcknight/squeezetheline/main/assets/logo.png"


def fetch_high_ev_picks(game_date: datetime.date, ev_threshold: float = 0.05,
                       odds: int = -110) -> list[dict]:
    """Auto picks where estimated EV at the given price exceeds ev_threshold.

    EV per $1 = p*payout - (1-p). At -110 odds, payout = 100/110 ≈ 0.909.
    For an over: p = hit_pct / 100.
    For an under: p = (100 - hit_pct) / 100.
    """
    sb = _anon()
    if sb is None:
        return []
    try:
        resp = (
            sb.table("auto_picks")
            .select("*")
            .eq("date", str(game_date))
            .execute()
        )
        rows = resp.data or []
    except Exception:
        return []

    payout = (odds / 100.0) if odds > 0 else (100.0 / abs(odds))

    flagged = []
    for r in rows:
        hit = r.get("hit_pct")
        if hit is None:
            continue
        # For over picks, prob = hit_pct. For unders, prob = 100 - hit_pct.
        prob = float(hit) / 100.0 if r.get("side") == "over" else (100.0 - float(hit)) / 100.0
        ev = prob * payout - (1 - prob)
        if ev >= ev_threshold:
            r["_ev"] = round(ev, 3)
            r["_prob"] = round(prob, 3)
            flagged.append(r)

    flagged.sort(key=lambda x: x["_ev"], reverse=True)
    return flagged


def build_alert_email_html(
    moves: list[dict],
    high_ev: list[dict],
    game_date: datetime.date,
) -> str:
    date_str = game_date.strftime("%A, %b %-d, %Y") if os.name != "nt" else game_date.strftime("%A, %b %#d, %Y")

    moves_html = ""
    if moves:
        rows = []
        for m in moves[:20]:
            stat = STAT_LABEL.get(m["stat"], m["stat"].upper())
            d = m["delta"]
            color = "#22c55e" if d > 0 else "#ef4444"
            arrow = "↑" if d > 0 else "↓"
            rows.append(f"""
            <tr>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#e6edf3;font-weight:600;">{m['player']}</td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#e6edf3;">{stat}</td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#8b92a5;">{m['open_line']:.1f} → {m['current_line']:.1f}</td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:{color};font-weight:700;">{arrow} {abs(d):.1f}</td>
            </tr>""")
        moves_html = f"""
        <h2 style="color:#f59e0b;font-size:14px;text-transform:uppercase;
                   letter-spacing:0.08em;margin:24px 0 8px;">
          \U0001f4c8 Line movement alerts
        </h2>
        <table cellpadding="0" cellspacing="0" border="0" width="100%"
               style="border-collapse:collapse;background:#1a1d24;
                      border:1px solid #2a2f3a;border-radius:8px;overflow:hidden;">
          {''.join(rows)}
        </table>"""

    ev_html = ""
    if high_ev:
        rows = []
        for p in high_ev[:20]:
            stat = STAT_LABEL.get(p["stat"], p["stat"].upper())
            is_over = p["side"] == "over"
            side_color = "#22c55e" if is_over else "#ef4444"
            arrow = "↑" if is_over else "↓"
            rows.append(f"""
            <tr>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:{side_color};font-weight:700;white-space:nowrap;">
                {arrow} {p['side'].upper()}
              </td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#e6edf3;font-weight:600;">{p['player']}</td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#e6edf3;">{p['line']:.1f} {stat}</td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#22c55e;font-weight:700;">
                EV {p['_ev']:+.3f}
              </td>
              <td style="padding:6px 10px;border-bottom:1px solid #2a2f3a;
                         color:#8b92a5;">hit {p.get('hit_pct',0):.0f}% &middot; conf {p.get('score',0):.1f}</td>
            </tr>""")
        ev_html = f"""
        <h2 style="color:#22c55e;font-size:14px;text-transform:uppercase;
                   letter-spacing:0.08em;margin:24px 0 8px;">
          \U0001f3af High-EV alerts (EV ≥ +5%)
        </h2>
        <table cellpadding="0" cellspacing="0" border="0" width="100%"
               style="border-collapse:collapse;background:#1a1d24;
                      border:1px solid #2a2f3a;border-radius:8px;overflow:hidden;">
          {''.join(rows)}
        </table>"""

    body = moves_html + ev_html
    if not body:
        return ""

    return f"""<!doctype html>
<html>
  <body style="margin:0;padding:20px;background:#0f1115;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <div style="max-width:680px;margin:0 auto;">
      <table cellpadding="0" cellspacing="0" border="0" width="100%">
        <tr>
          <td style="vertical-align:middle;width:80px;">
            <img src="{LOGO_URL}" width="64" height="64" alt="logo" style="display:block;border:0;">
          </td>
          <td style="vertical-align:middle;padding-left:14px;">
            <div style="color:#22c55e;font-size:22px;font-weight:800;">
              Priority Alerts
            </div>
            <div style="color:#8b92a5;font-size:13px;">{date_str}</div>
          </td>
        </tr>
      </table>
      {body}
      <div style="color:#8b92a5;font-size:11px;margin-top:32px;border-top:1px solid #2a2f3a;padding-top:12px;">
        Line moves of 1.5+ from opening &middot; EV computed at -110 odds &middot; not financial advice.
      </div>
    </div>
  </body>
</html>"""


def build_alert_email_text(moves, high_ev, game_date) -> str:
    lines = [f"Squeeze the Line — Priority Alerts ({game_date})"]
    if moves:
        lines.append("\nLine movement (≥ 1.5):")
        for m in moves[:20]:
            stat = STAT_LABEL.get(m["stat"], m["stat"].upper())
            lines.append(
                f"  {m['player']} {stat}: {m['open_line']:.1f} → {m['current_line']:.1f} ({m['delta']:+.1f})"
            )
    if high_ev:
        lines.append("\nHigh-EV picks:")
        for p in high_ev[:20]:
            stat = STAT_LABEL.get(p["stat"], p["stat"].upper())
            lines.append(
                f"  {p['side'].upper()} {p['player']} {p['line']:.1f} {stat} · EV {p['_ev']:+.3f} · hit {p.get('hit_pct',0):.0f}%"
            )
    return "\n".join(lines)


def send_priority_alerts(game_date: Optional[datetime.date] = None,
                         min_move: float = 1.5,
                         ev_threshold: float = 0.05) -> dict:
    """Build and send the priority alerts email. Returns a status dict."""
    d = game_date or datetime.date.today()
    from prop_history import get_significant_line_moves
    moves = get_significant_line_moves(d, min_move=min_move)
    high_ev = fetch_high_ev_picks(d, ev_threshold=ev_threshold)

    if not moves and not high_ev:
        return {"sent": False, "reason": "no alerts to send", "moves": 0, "high_ev": 0}

    html = build_alert_email_html(moves, high_ev, d)
    text = build_alert_email_text(moves, high_ev, d)
    subject = f"[PRIORITY] Squeeze the Line — {len(moves)} moves, {len(high_ev)} high-EV"

    from digest import send_email_via_resend
    sent = send_email_via_resend(subject, html, text)
    return {"sent": sent, "moves": len(moves), "high_ev": len(high_ev)}
