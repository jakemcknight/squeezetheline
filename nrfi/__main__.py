"""Entry point: `python -m nrfi` — prints today's NRFI/YRFI board (model
probabilities + sportsbook edge when lines are available) and writes CSV +
markdown copies under nrfi/out/.

Flags:
  --no-odds         model-only board, skip the sportsbook layer entirely
  --odds-csv FILE   manual odds instead of The Odds API; header row
                    away,home,book,nrfi_odds,yrfi_odds (American odds)
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

from . import data, model, odds

OUT_DIR = Path(__file__).resolve().parent / "out"
DISCLAIMER = (
    "Model estimates only — not guaranteed outcomes. Betting involves risk; "
    "wager responsibly."
)
EDGE_DISCLAIMER = (
    "Edges are model probability vs no-vig (vig-removed) market probability — "
    "model estimates, not guarantees; bet at your own risk."
)


def build_board(today: date) -> List[model.BoardRow]:
    slate = data.fetch_slate(today)
    if not slate:
        return []

    season = today.year
    first_innings = data.fetch_season_first_innings(season, today)
    team_rates = model.build_team_rates(first_innings)
    league = model.league_half_rate(first_innings)

    pitcher_ids = [
        p["id"]
        for g in slate
        for p in (g["away_pitcher"], g["home_pitcher"])
        if p
    ]
    pitcher_logs = data.fetch_pitcher_starts(pitcher_ids, season, today)

    rows: List[model.BoardRow] = []
    for g in slate:
        away_off = team_rates.get(g["away_id"])
        home_off = team_rates.get(g["home_id"])

        def pitcher_rate(p: Optional[dict]) -> Optional[model.PitcherRates]:
            if not p:
                return None
            return model.build_pitcher_rates(pitcher_logs.get(p["id"], []), first_innings)

        away_pit = pitcher_rate(g["away_pitcher"])   # away starter pitches bottom 1st
        home_pit = pitcher_rate(g["home_pitcher"])   # home starter pitches top 1st

        p_top = model.half_inning_prob(away_off, home_pit, league)
        p_bottom = model.half_inning_prob(home_off, away_pit, league)
        p_yrfi = 1.0 - (1.0 - p_top) * (1.0 - p_bottom)

        row = model.BoardRow(
            matchup=f"{g['away_name']} @ {g['home_name']}",
            away_pitcher=(g["away_pitcher"] or {}).get("fullName", "TBD"),
            home_pitcher=(g["home_pitcher"] or {}).get("fullName", "TBD"),
            p_yrfi=p_yrfi,
            p_top=p_top,
            p_bottom=p_bottom,
            lean=model.lean_for(p_yrfi),
            confidence=model.confidence_for(away_off, home_off, away_pit, home_pit),
            status=g["status"],
            detail={
                "league_half_rate": league,
                "away_team_1st_rate": away_off.rate if away_off else float("nan"),
                "home_team_1st_rate": home_off.rate if home_off else float("nan"),
                "away_pitcher_starts": away_pit.starts if away_pit else 0,
                "home_pitcher_starts": home_pit.starts if home_pit else 0,
            },
        )
        row.detail["_away_name"] = g["away_name"]  # for odds-event matching
        row.detail["_home_name"] = g["home_name"]
        rows.append(row)

    rows.sort(key=lambda r: r.p_yrfi, reverse=True)
    return rows


def attach_market_edges(
    rows: List[model.BoardRow], today: date, odds_csv: Optional[Path]
) -> Optional[str]:
    """Attach a MarketEdge to each row we can price. Returns a status note
    for the header (None means the API path worked silently)."""
    if odds_csv is not None:
        events, err = odds.load_manual_odds(odds_csv)
        if err:
            return f"Manual odds not used: {err}"
        note = f"Odds source: manual CSV ({odds_csv.name}), {len(events)} games priced."
    else:
        api_key = odds.resolve_api_key()
        if not api_key:
            return (
                "No ODDS_API_KEY configured — model-only board. "
                "Use --odds-csv FILE for manual lines."
            )
        events, err = odds.fetch_market_odds(today, api_key)
        if err:
            return f"Odds unavailable ({err}) — model-only board."
        if not events:
            return (
                "The Odds API returned no totals_1st_1_innings prices today — "
                "model-only board. Use --odds-csv FILE for manual lines."
            )
        note = None

    for row in rows:
        game = {
            "away_name": row.detail["_away_name"],
            "home_name": row.detail["_home_name"],
        }
        ev = odds.match_event(game, events)
        if ev:
            row.market = odds.compute_edge(row.p_nrfi, ev, row.confidence)

    # +EV plays first (by EV), then the rest by model YRFI as before.
    rows.sort(
        key=lambda r: (
            1 if (r.market and r.market.bet) else 0,
            r.market.ev_per_dollar if (r.market and r.market.bet) else r.p_yrfi,
        ),
        reverse=True,
    )
    return note


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_edge(m: Optional[odds.MarketEdge], attr: str) -> str:
    if m is None:
        return "—"
    if attr == "book":
        return m.side_book if m.bet else f"{m.n_books} bks"
    if attr == "nrfi_odds":
        return odds.format_american(m.best_nrfi.nrfi_american)
    if attr == "yrfi_odds":
        return odds.format_american(m.best_yrfi.yrfi_american)
    if attr == "implied":
        return f"{m.novig_nrfi:.1%}"
    if attr == "edge":
        return f"{m.edge_nrfi:+.1%}"
    if attr == "ev":
        return f"{m.ev_per_dollar:+.1%}"
    if attr == "bet":
        return m.side if m.bet else "—"
    if attr == "kelly":
        return f"{m.kelly_pct:.1%}" if m.bet else "—"
    return "—"


def _table(rows: List[model.BoardRow], with_market: bool) -> List[List[str]]:
    header = ["#", "Matchup", "Away SP", "Home SP", "YRFI%", "NRFI%", "Lean", "Conf"]
    if with_market:
        header += ["Book", "NRFI", "YRFI", "ImpNRFI%", "Edge%", "EV%", "BET", "Kelly%"]
    header.append("Status")
    body = []
    for i, r in enumerate(rows, 1):
        cells = [
            str(i),
            r.matchup,
            r.away_pitcher,
            r.home_pitcher,
            f"{r.p_yrfi:.1%}",
            f"{r.p_nrfi:.1%}",
            r.lean,
            r.confidence,
        ]
        if with_market:
            m = r.market
            cells += [
                _fmt_edge(m, "book"),
                _fmt_edge(m, "nrfi_odds"),
                _fmt_edge(m, "yrfi_odds"),
                _fmt_edge(m, "implied"),
                _fmt_edge(m, "edge"),
                _fmt_edge(m, "ev"),
                _fmt_edge(m, "bet"),
                _fmt_edge(m, "kelly"),
            ]
        cells.append(r.status)
        body.append(cells)
    return [header] + body


def print_board(rows: List[model.BoardRow], today: date, note: Optional[str]) -> None:
    with_market = any(r.market for r in rows)
    print(f"\nNRFI / YRFI board — {today.strftime('%A, %B %d, %Y')}")
    print(DISCLAIMER)
    if with_market:
        print(EDGE_DISCLAIMER)
    if note:
        print(f"NOTE: {note}")
    if not rows:
        print("\nNo MLB games today.")
        return
    league = rows[0].detail.get("league_half_rate")
    if league is not None:
        print(f"League YRFI base rate this season: {1 - (1 - league) ** 2:.1%}")
    if with_market:
        bets = [r for r in rows if r.market and r.market.bet]
        print(
            f"+EV plays (edge ≥ {odds.EDGE_THRESHOLD:.0%}, conf not Low): "
            f"{len(bets)}"
        )
    print()
    table = _table(rows, with_market)
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for n, row in enumerate(table):
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)))
        if n == 0:
            print("  ".join("-" * w for w in widths))


def write_outputs(rows: List[model.BoardRow], today: date, note: Optional[str]) -> List[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = today.isoformat()
    with_market = any(r.market for r in rows)

    csv_path = OUT_DIR / f"nrfi_board_{stamp}.csv"
    with csv_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            ["date", "matchup", "away_sp", "home_sp", "p_yrfi", "p_nrfi",
             "p_top1", "p_bottom1", "lean", "confidence",
             "best_nrfi_book", "best_nrfi_odds", "best_yrfi_book",
             "best_yrfi_odds", "n_books", "implied_nrfi_novig",
             "edge_nrfi", "ev_per_dollar", "bet_side", "bet_book",
             "kelly_stake_pct", "status"]
        )
        for r in rows:
            m = r.market
            w.writerow(
                [stamp, r.matchup, r.away_pitcher, r.home_pitcher,
                 f"{r.p_yrfi:.4f}", f"{r.p_nrfi:.4f}", f"{r.p_top:.4f}",
                 f"{r.p_bottom:.4f}", r.lean, r.confidence,
                 m.best_nrfi.book if m else "",
                 m.best_nrfi.nrfi_american if m else "",
                 m.best_yrfi.book if m else "",
                 m.best_yrfi.yrfi_american if m else "",
                 m.n_books if m else 0,
                 f"{m.novig_nrfi:.4f}" if m else "",
                 f"{m.edge_nrfi:+.4f}" if m else "",
                 f"{m.ev_per_dollar:+.4f}" if m else "",
                 m.side if (m and m.bet) else "",
                 m.side_book if (m and m.bet) else "",
                 f"{m.kelly_pct:.4f}" if (m and m.bet) else "",
                 r.status]
            )

    md_path = OUT_DIR / f"nrfi_board_{stamp}.md"
    lines = [
        f"# NRFI / YRFI board — {today.strftime('%A, %B %d, %Y')}",
        "",
        f"> {DISCLAIMER}",
    ]
    if with_market:
        lines.append(f"> {EDGE_DISCLAIMER}")
    if note:
        lines += ["", f"_NOTE: {note}_"]
    lines.append("")
    if rows:
        table = _table(rows, with_market)
        lines.append("| " + " | ".join(table[0]) + " |")
        lines.append("|" + "|".join("---" for _ in table[0]) + "|")
        for row in table[1:]:
            lines.append("| " + " | ".join(row) + " |")
    else:
        lines.append("No MLB games today.")
    md_path.write_text("\n".join(lines) + "\n")

    return [csv_path, md_path]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nrfi", description=__doc__)
    parser.add_argument("--no-odds", action="store_true",
                        help="skip the sportsbook edge layer")
    parser.add_argument("--odds-csv", type=Path, metavar="FILE",
                        help="manual odds CSV: away,home,book,nrfi_odds,yrfi_odds")
    args = parser.parse_args(argv)

    today = date.today()
    try:
        rows = build_board(today)
    except RuntimeError as err:
        print(f"NRFI board unavailable: {err}", file=sys.stderr)
        return 1

    note: Optional[str] = None
    if rows and not args.no_odds:
        note = attach_market_edges(rows, today, args.odds_csv)

    print_board(rows, today, note)
    if rows:
        for path in write_outputs(rows, today, note):
            rel = path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path
            print(f"Saved {rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
