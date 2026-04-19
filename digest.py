"""
Daily digest: build a readable summary of today's auto-picks and send it
to your inbox via Resend. Called from the webhook after the refresh runs.

Config (in Streamlit secrets):
- RESEND_API_KEY: get one at https://resend.com (3k emails/mo free)
- DIGEST_FROM: sender email (e.g. 'digest@yourdomain.com' or
  'onboarding@resend.dev' to use Resend's shared sender)
- DIGEST_RECIPIENTS: comma-separated list of recipient emails
"""

import os
import datetime
from typing import Optional

import requests


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


LOGO_URL = "https://raw.githubusercontent.com/jakemcknight/squeezetheline/main/assets/logo.png"


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


def fetch_today_events(date: Optional[datetime.date] = None) -> list[dict]:
    """Pull today's NBA events (commence_time per game) for the banner."""
    try:
        from scrapers.odds_api import get_events_for_date
        return get_events_for_date(date)
    except Exception:
        return []


def build_game_status_banner(events: list[dict], date: datetime.date) -> str:
    """Produce a human banner like '8 games tonight, tipoffs 7:00–10:30 PM ET'."""
    if not events:
        return "No NBA games on the slate."
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    times = []
    for e in events:
        commence = e.get("commence_time")
        if not commence:
            continue
        try:
            t = datetime.datetime.fromisoformat(commence.replace("Z", "+00:00")).astimezone(et)
            times.append(t)
        except Exception:
            pass
    n = len(events)
    plural = "games" if n != 1 else "game"
    if not times:
        return f"{n} {plural} on the slate."
    earliest = min(times).strftime("%-I:%M %p") if os.name != "nt" else min(times).strftime("%#I:%M %p")
    latest = max(times).strftime("%-I:%M %p") if os.name != "nt" else max(times).strftime("%#I:%M %p")
    if earliest == latest:
        return f"{n} {plural} tonight, tipoff {earliest} ET."
    return f"{n} {plural} tonight, tipoffs {earliest}–{latest} ET."


def _fmt_pick_line(p: dict) -> str:
    stat = STAT_LABEL.get(p["stat"], p["stat"].upper())
    side = p["side"].upper()
    arrow = "↑" if p["side"] == "over" else "↓"
    hit = p.get("hit_pct", 0) or 0
    hist = p.get("history_hit_pct", 0) or 0
    score = p.get("score") or 0
    extras = []
    if p.get("delta_10g") is not None:
        extras.append(f"Δ10 {p['delta_10g']:+.1f}")
    if p.get("def_rank") is not None:
        extras.append(f"def #{p['def_rank']}")
    if p.get("vs_opp_career"):
        extras.append(f"vs opp {p['vs_opp_career']}")
    if p.get("ml_prediction") is not None:
        extras.append(f"ML {p['ml_prediction']:.1f}")
    extras_str = " · ".join(extras)
    return (
        f"{arrow} {p['player']} {side} {p['line']:.1f} {stat} "
        f"({p.get('team','')} vs {p.get('opponent','')}) · "
        f"hit {hit:.0f}% / hist {hist:.0f}% · conf {score:.1f}"
        + (f" · {extras_str}" if extras_str else "")
    )


def build_digest_text(picks: list[dict], date: Optional[datetime.date] = None) -> str:
    """Plain-text version for logs / fallback."""
    d = date or datetime.date.today()
    if not picks:
        return f"Squeeze the Line — {d.strftime('%a %b %-d')}\nNo auto picks generated for today yet."

    top = [p for p in picks if p.get("is_top_pick")]
    rest = [p for p in picks if not p.get("is_top_pick")]

    lines = [f"Squeeze the Line — {d.strftime('%a %b %-d')}"]
    lines.append(f"{len(picks)} total picks · {len(top)} flagged as top 5")

    if top:
        lines.append("")
        lines.append("Top 5 of each side:")
        for p in top:
            lines.append(_fmt_pick_line(p))
    if rest:
        lines.append("")
        lines.append(f"Other strong picks ({len(rest)}):")
        for p in rest[:10]:
            lines.append(_fmt_pick_line(p))
        if len(rest) > 10:
            lines.append(f"… and {len(rest) - 10} more")

    return "\n".join(lines)


def build_digest_html(picks: list[dict], date: Optional[datetime.date] = None,
                      events: Optional[list[dict]] = None) -> str:
    """Nicely formatted HTML email matching the site's dark-green theme."""
    d = date or datetime.date.today()
    date_str = d.strftime("%A, %b %-d, %Y") if os.name != "nt" else d.strftime("%A, %b %#d, %Y")
    banner = build_game_status_banner(events or [], d)

    if not picks:
        body_html = "<p style='color:#e6edf3;'>No auto picks generated for today.</p>"
    else:
        top = [p for p in picks if p.get("is_top_pick")]
        rest = [p for p in picks if not p.get("is_top_pick")]

        def _row(p: dict) -> str:
            stat = STAT_LABEL.get(p["stat"], p["stat"].upper())
            is_over = p["side"] == "over"
            side_color = "#22c55e" if is_over else "#ef4444"
            arrow = "&uarr;" if is_over else "&darr;"
            hit = p.get("hit_pct", 0) or 0
            hist = p.get("history_hit_pct", 0) or 0
            score = p.get("score") or 0

            # Extras row — only render fields we have data for
            chips = []
            if p.get("delta_10g") is not None:
                d10 = p["delta_10g"]
                d10_color = "#22c55e" if d10 > 0 else "#ef4444" if d10 < 0 else "#8b92a5"
                chips.append(
                    f'<span style="color:{d10_color};">Δ10 {d10:+.1f}</span>'
                )
            if p.get("def_rank") is not None:
                # Higher rank = weaker defense (good for overs)
                rank = int(p["def_rank"])
                rank_color = "#22c55e" if (rank > 20 and is_over) or (rank < 10 and not is_over) else "#8b92a5"
                chips.append(f'<span style="color:{rank_color};">def #{rank}</span>')
            if p.get("vs_opp_career"):
                chips.append(f'<span>vs opp {p["vs_opp_career"]}</span>')
            if p.get("ml_prediction") is not None:
                ml = float(p["ml_prediction"])
                ml_delta = ml - float(p["line"])
                ml_color = "#22c55e" if (ml_delta > 0 and is_over) or (ml_delta < 0 and not is_over) else "#ef4444"
                chips.append(
                    f'<span style="color:{ml_color};">ML {ml:.1f} ({ml_delta:+.1f})</span>'
                )

            extras_html = (
                f'<div style="color:#8b92a5;font-size:11px;margin-top:4px;">'
                + " &middot; ".join(chips) + "</div>"
            ) if chips else ""

            return f"""
            <tr>
              <td style="padding:8px 10px;border-bottom:1px solid #2a2f3a;
                         color:{side_color};font-weight:700;white-space:nowrap;
                         font-size:13px;vertical-align:top;">
                {arrow} {p['side'].upper()}
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #2a2f3a;
                         vertical-align:top;">
                <div style="color:#e6edf3;font-weight:600;font-size:14px;">
                  {p['player']}
                </div>
                <div style="color:#8b92a5;font-size:11px;margin-top:2px;">
                  {p.get('team','')} vs {p.get('opponent','')}
                </div>
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #2a2f3a;
                         color:#e6edf3;white-space:nowrap;font-size:13px;
                         vertical-align:top;">
                {p['line']:.1f} {stat}
              </td>
              <td style="padding:8px 10px;border-bottom:1px solid #2a2f3a;
                         color:#8b92a5;white-space:nowrap;font-size:12px;
                         vertical-align:top;">
                <div>hit {hit:.0f}% / hist {hist:.0f}%</div>
                <div style="margin-top:2px;color:#22c55e;font-weight:600;">
                  conf {score:.1f}
                </div>
                {extras_html}
              </td>
            </tr>
            """

        sections = []
        if top:
            sections.append(f"""
            <h2 style="color:#22c55e;font-size:14px;text-transform:uppercase;
                       letter-spacing:0.08em;margin:24px 0 8px;">
              Top 5 of each side
            </h2>
            <table role="presentation" cellpadding="0" cellspacing="0" border="0"
                   width="100%" style="border-collapse:collapse;background:#1a1d24;
                   border:1px solid #2a2f3a;border-radius:8px;overflow:hidden;">
              {''.join(_row(p) for p in top)}
            </table>
            """)
        if rest:
            shown = rest[:15]
            more = len(rest) - len(shown)
            sections.append(f"""
            <h2 style="color:#8b92a5;font-size:14px;text-transform:uppercase;
                       letter-spacing:0.08em;margin:24px 0 8px;">
              Other strong picks ({len(rest)})
            </h2>
            <table role="presentation" cellpadding="0" cellspacing="0" border="0"
                   width="100%" style="border-collapse:collapse;background:#1a1d24;
                   border:1px solid #2a2f3a;border-radius:8px;overflow:hidden;">
              {''.join(_row(p) for p in shown)}
            </table>
            """)
            if more > 0:
                sections.append(
                    f'<p style="color:#8b92a5;font-size:13px;margin-top:8px;">'
                    f'… and {more} more — view them all on the site</p>'
                )

        body_html = "\n".join(sections)

    return f"""<!doctype html>
<html>
  <body style="margin:0;padding:20px;background:#0f1115;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
    <div style="max-width:680px;margin:0 auto;">
      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
        <tr>
          <td style="vertical-align:middle;width:80px;">
            <img src="{LOGO_URL}" alt="Squeeze the Line" width="72" height="72"
                 style="display:block;border:0;">
          </td>
          <td style="vertical-align:middle;padding-left:14px;">
            <div style="color:#22c55e;font-size:24px;font-weight:800;
                        letter-spacing:-0.02em;">
              Squeeze the Line
            </div>
            <div style="color:#8b92a5;font-size:13px;margin-top:2px;">
              {date_str} &middot; {len(picks)} auto picks
            </div>
          </td>
        </tr>
      </table>
      <div style="background:#1a1d24;border:1px solid #2a2f3a;border-radius:8px;
                  padding:10px 14px;margin-top:16px;color:#e6edf3;font-size:13px;">
        \U0001f3c0 {banner}
      </div>
      {body_html}
      <div style="color:#8b92a5;font-size:11px;margin-top:32px;border-top:1px solid #2a2f3a;padding-top:12px;">
        Conf = composite confidence score (higher is stronger). Δ10 = avg over last 10 minus the line.
        def #N = opponent's defensive rank vs this position (higher = weaker D = good for overs).
        ML = the trained XGBoost model's prediction; in parens is its delta vs the line.
        vs opp = career hit rate against tonight's opponent.
        <br/><br/>
        Automated digest from squeezetheline.com. For entertainment only; not financial advice.
      </div>
    </div>
  </body>
</html>"""


def send_email_via_resend(subject: str, html: str, text: str) -> bool:
    """Send via Resend (https://resend.com). Returns True on success."""
    api_key = _secret("RESEND_API_KEY")
    from_addr = _secret("DIGEST_FROM")
    recipients_raw = _secret("DIGEST_RECIPIENTS")

    if not api_key or not from_addr or not recipients_raw:
        return False

    to_list = [e.strip() for e in recipients_raw.split(",") if e.strip()]
    if not to_list:
        return False

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": from_addr,
                "to": to_list,
                "subject": subject,
                "html": html,
                "text": text,
            },
            timeout=15,
        )
        if resp.status_code >= 300:
            print(f"Resend send failed: {resp.status_code} {resp.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"Resend send error: {e}")
        return False


def send_daily_digest(date: Optional[datetime.date] = None) -> dict:
    """Top-level: build and send the digest.

    If there are no picks for the day (no NBA games, or no strong picks
    found), we skip the email by default — no point spamming the inbox
    on off-days. Set `DIGEST_SEND_ON_EMPTY=true` in secrets if you want
    a daily email regardless.
    """
    d = date or datetime.date.today()
    picks = fetch_today_picks(d)

    if not picks:
        send_on_empty = _secret("DIGEST_SEND_ON_EMPTY", "").lower() in ("true", "1", "yes")
        if not send_on_empty:
            print(f"[digest] No picks for {d} — skipping email (set DIGEST_SEND_ON_EMPTY=true to override).")
            return {"picks": 0, "email_sent": False, "skipped_reason": "no picks"}

    events = fetch_today_events(d)
    subject = f"Squeeze the Line — {d.strftime('%a %b %-d')} · {len(picks)} picks"
    html = build_digest_html(picks, d, events=events)
    text = build_digest_text(picks, d)

    sent = send_email_via_resend(subject, html, text)
    return {"picks": len(picks), "email_sent": sent, "text": text}
