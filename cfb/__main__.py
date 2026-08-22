"""Entry point: `python -m cfb` — prints this week's FBS board (model
spread / total / win prob vs the market, with de-vig edges and optional
quarter-Kelly stakes) and writes CSV + markdown copies under cfb/out/.

Flags:
  --no-odds     model-only board, skip the sportsbook layer
  --year N      season year (default: inferred from today)
  --week N      CFBD week number (default: the next upcoming week)
  --stub        force the bundled demo ratings even if a CFBD key exists

Spread columns are quoted from the HOME team's perspective (negative =
home favored), the standard book convention.
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import data, model, odds

OUT_DIR = Path(__file__).resolve().parent / "out"
DISCLAIMER = (
    "Model estimates only — not guaranteed outcomes. CFB sides/totals are a "
    "sharp, efficient market; treat edges skeptically. Bet responsibly."
)
EDGE_DISCLAIMER = (
    "Edges compare the model to no-vig (vig-removed) market probabilities; "
    "Kelly stakes are optional sizing guidance, not directives."
)
CFBD_KEY_MSG = (
    "No CFBD_API_KEY configured — using bundled DEMO stub ratings (NOT real "
    "SP+; confidence forced Low, so no BETs can flag). Get a free key in ~1 "
    "minute at https://collegefootballdata.com/key, then `export "
    "CFBD_API_KEY=...` or add it to backend/.env."
)


def season_year(today: date) -> int:
    return today.year if today.month >= 6 else today.year - 1


# ---------------------------------------------------------------------------
# Board assembly
# ---------------------------------------------------------------------------

def _lookup_fuzzy(ratings: Dict[str, model.TeamRating], name: str,
                  avg: float) -> model.TeamRating:
    """Stub mode: slate names are Odds API style ('TCU Horned Frogs'),
    ratings keyed by school — reuse the odds matcher's scoring."""
    if name in ratings:
        return ratings[name]
    best, best_score = None, 0.0
    for school in ratings:
        s = odds._score(school, name)
        if s > best_score:
            best, best_score = school, s
    if best is not None and best_score >= 0.85:
        return ratings[best]
    return model.placeholder_unrated(name, avg)


def build_board(args, today: date,
                odds_key: str) -> Tuple[List[model.BoardRow], List[str], Optional[int]]:
    """Returns (rows, header_notes, week_number)."""
    notes: List[str] = []
    cfbd_key = "" if args.stub else data.resolve_cfbd_key()
    year = args.year or season_year(today)

    if cfbd_key:
        calendar = data.fetch_calendar(year, cfbd_key)
        week_num = args.week
        if week_num is None:
            wk = data.pick_current_week(calendar, today)
            if wk is None:
                raise RuntimeError(f"no upcoming regular-season week in the {year} calendar")
            week_num = data._g(wk, "week")
        games = data.fetch_week_games(year, week_num, cfbd_key)
        sp, sp_label = data.fetch_sp_ratings(year, cfbd_key)
        returning = data.fetch_returning_production(year, cfbd_key)
        ratings = model.build_ratings(sp, returning)
        avg = model.league_avg_pts(ratings)
        applied = model.apply_results(ratings, data.fetch_completed_games(year, cfbd_key), avg)
        notes.append(f"Ratings: {sp_label}, updated with {applied} completed "
                     f"{year} games (prior worth {model.PRIOR_GAMES} games).")
        lookup = lambda name: ratings.get(name) or model.placeholder_unrated(name, avg)
    else:
        notes.append(CFBD_KEY_MSG)
        if not odds_key:
            raise RuntimeError(
                "neither CFBD_API_KEY nor ODDS_API_KEY is available — "
                "need at least one to build a slate"
            )
        events, err = odds.fetch_event_list(odds_key)
        if err:
            raise RuntimeError(err)
        games = data.slate_from_odds_events(events, today)
        week_num = None
        sp, sp_label = data.load_stub_ratings()
        ratings = model.build_ratings(sp, {})
        avg = model.league_avg_pts(ratings)
        notes.append(f"Ratings: {sp_label}.")
        lookup = lambda name: _lookup_fuzzy(ratings, name, avg)

    rows: List[model.BoardRow] = []
    for g in games:
        home_r, away_r = lookup(g["home"]), lookup(g["away"])
        proj = model.project(home_r, away_r, g["neutral"], avg)
        conf_week = week_num if cfbd_key else 0    # stub => treat as week 0
        conf, why = model.confidence_for(conf_week, home_r, away_r)
        rows.append(model.BoardRow(away=g["away"], home=g["home"],
                                   neutral=g["neutral"], start=g["start"],
                                   proj=proj, confidence=conf, conf_why=why))
    return rows, notes, week_num


def attach_market(rows: List[model.BoardRow], today: date,
                  odds_key: str) -> Optional[str]:
    if not odds_key:
        return "No ODDS_API_KEY configured — model-only board."
    events, err = odds.fetch_market_odds(today, odds_key)
    if err:
        return f"Odds unavailable ({err}) — model-only board."
    if not events:
        return "The Odds API returned no NCAAF prices — model-only board."
    matched = 0
    for row in rows:
        ev = odds.match_event(row.away, row.home, events)
        if ev is None:
            continue
        row.market = odds.compute_market(
            ev, row.home, row.away, row.proj.margin, row.proj.total,
            row.proj.p_home, row.confidence)
        matched += 1
    return f"Market lines matched for {matched}/{len(rows)} games."


def sort_rows(rows: List[model.BoardRow]) -> None:
    def key(r: model.BoardRow):
        m = r.market
        if m is None:
            return (0, 0.0, r.start)
        # edge in "threshold units" so spread/total/ML edges are comparable
        units = 0.0
        if m.spread:
            units = max(units, abs(m.spread.edge) / odds.SPREAD_EDGE_PTS)
        if m.total:
            units = max(units, abs(m.total.edge) / odds.TOTAL_EDGE_PTS)
        if m.ml:
            units = max(units, abs(m.ml.edge) / odds.ML_EDGE_PROB)
        best = m.best()
        has_bet = best is not None and best.bet
        return (2 if has_bet else 1,
                best.ev_per_dollar if has_bet else units, r.start)
    rows.sort(key=key, reverse=True)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_start(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone()
        return dt.strftime("%a %m/%d %H:%M")
    except ValueError:
        return iso[:10]


def _line_str(v: float) -> str:
    return "PK" if abs(v) < 0.05 else f"{-v:+.1f}"   # home-margin -> book quote


def _best_play(m: Optional[odds.GameMarket]) -> str:
    best = m.best() if m else None
    if best is None:
        return "—"
    return f"{best.side} {odds.format_american(best.price)} ({best.book})"


def _bets_str(m: Optional[odds.GameMarket]) -> str:
    if m is None:
        return "—"
    kinds = [v.kind for v in (m.spread, m.total, m.ml) if v and v.bet]
    return ",".join(kinds) if kinds else "—"


def _table(rows: List[model.BoardRow], with_market: bool) -> List[List[str]]:
    header = ["#", "Matchup", "Kick", "mSprd", "mkSprd", "EdgP",
              "mTot", "mkTot", "EdgT", "mHome%", "Imp%", "EdgML"]
    if with_market:
        header += ["Best play", "BET", "Kelly%"]
    header += ["Conf"]
    body = []
    for i, r in enumerate(rows, 1):
        m = r.market
        sp, tot, ml = (m.spread, m.total, m.ml) if m else (None, None, None)
        cells = [
            str(i), r.matchup, _fmt_start(r.start),
            _line_str(r.proj.margin),
            _line_str(-sp.market_value) if sp else "—",   # market_value is book quote already
            f"{sp.edge:+.1f}" if sp else "—",
            f"{r.proj.total:.1f}",
            f"{tot.market_value:g}" if tot else "—",
            f"{tot.edge:+.1f}" if tot else "—",
            f"{r.proj.p_home:.0%}",
            f"{ml.market_value:.0%}" if ml else "—",
            f"{ml.edge:+.1%}" if ml else "—",
        ]
        if with_market:
            best = m.best() if m else None
            cells += [
                _best_play(m),
                _bets_str(m),
                f"{best.kelly_pct:.1%}" if best and best.bet else "—",
            ]
        cells.append(r.confidence)
        body.append(cells)
    return [header] + body


def print_board(rows: List[model.BoardRow], today: date, week: Optional[int],
                notes: List[str]) -> None:
    with_market = any(r.market for r in rows)
    wk = f"Week {week}" if week is not None else "upcoming slate"
    print(f"\nCFB board — {wk} — generated {today.strftime('%A, %B %d, %Y')}")
    print(DISCLAIMER)
    if with_market:
        print(EDGE_DISCLAIMER)
    for n in notes:
        print(f"NOTE: {n}")
    if not rows:
        print("\nNo FBS games found for this week.")
        return
    print(f"Spreads quoted for the HOME team (negative = home favored); "
          f"HFA {model.HFA:g} pts, margin SD {model.MARGIN_SD:g}.")
    if with_market:
        bets = [r for r in rows if r.market and any(
            v and v.bet for v in (r.market.spread, r.market.total, r.market.ml))]
        print(f"+EV plays (spread ≥{odds.SPREAD_EDGE_PTS:g} pts / total "
              f"≥{odds.TOTAL_EDGE_PTS:g} pts / ML ≥{odds.ML_EDGE_PROB:.0%}, "
              f"conf not Low): {len(bets)}")
    print()
    table = _table(rows, with_market)
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for n, row in enumerate(table):
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)))
        if n == 0:
            print("  ".join("-" * w for w in widths))


def write_outputs(rows: List[model.BoardRow], today: date, week: Optional[int],
                  notes: List[str]) -> List[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = today.isoformat()
    with_market = any(r.market for r in rows)

    csv_path = OUT_DIR / f"cfb_board_{stamp}.csv"
    with csv_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            ["date", "week", "kick", "away", "home", "neutral",
             "model_home_pts", "model_away_pts", "model_margin", "model_total",
             "model_home_winpct",
             "mkt_spread_home", "spread_edge_pts", "spread_side", "spread_price",
             "spread_book", "spread_p_model", "spread_p_novig", "spread_ev",
             "mkt_total", "total_edge_pts", "total_side", "total_price",
             "total_book", "total_p_model", "total_p_novig", "total_ev",
             "ml_novig_home", "ml_edge", "ml_side", "ml_price", "ml_book", "ml_ev",
             "n_books", "bets", "best_kelly_pct", "confidence", "conf_why"]
        )
        for r in rows:
            m = r.market
            sp, tot, ml = (m.spread, m.total, m.ml) if m else (None, None, None)
            best = m.best() if m else None
            w.writerow(
                [stamp, week if week is not None else "", r.start, r.away,
                 r.home, int(r.neutral),
                 f"{r.proj.home_pts:.2f}", f"{r.proj.away_pts:.2f}",
                 f"{r.proj.margin:+.2f}", f"{r.proj.total:.2f}",
                 f"{r.proj.p_home:.4f}",
                 f"{sp.market_value:+.1f}" if sp else "",
                 f"{sp.edge:+.2f}" if sp else "",
                 sp.side if sp else "", sp.price if sp else "",
                 sp.book if sp else "",
                 f"{sp.p_model:.4f}" if sp else "",
                 f"{sp.p_novig:.4f}" if sp else "",
                 f"{sp.ev_per_dollar:+.4f}" if sp else "",
                 f"{tot.market_value:g}" if tot else "",
                 f"{tot.edge:+.2f}" if tot else "",
                 tot.side if tot else "", tot.price if tot else "",
                 tot.book if tot else "",
                 f"{tot.p_model:.4f}" if tot else "",
                 f"{tot.p_novig:.4f}" if tot else "",
                 f"{tot.ev_per_dollar:+.4f}" if tot else "",
                 f"{ml.market_value:.4f}" if ml else "",
                 f"{ml.edge:+.4f}" if ml else "",
                 ml.side if ml else "", ml.price if ml else "",
                 ml.book if ml else "",
                 f"{ml.ev_per_dollar:+.4f}" if ml else "",
                 m.n_books if m else 0, _bets_str(m),
                 f"{best.kelly_pct:.4f}" if best and best.bet else "",
                 r.confidence, r.conf_why]
            )

    md_path = OUT_DIR / f"cfb_board_{stamp}.md"
    wk = f"Week {week}" if week is not None else "upcoming slate"
    lines = [f"# CFB board — {wk} — {today.strftime('%A, %B %d, %Y')}", "",
             f"> {DISCLAIMER}"]
    if with_market:
        lines.append(f"> {EDGE_DISCLAIMER}")
    for n in notes:
        lines += ["", f"_NOTE: {n}_"]
    lines.append("")
    if rows:
        table = _table(rows, with_market)
        lines.append("| " + " | ".join(table[0]) + " |")
        lines.append("|" + "|".join("---" for _ in table[0]) + "|")
        for row in table[1:]:
            lines.append("| " + " | ".join(row) + " |")
    else:
        lines.append("No FBS games found for this week.")
    md_path.write_text("\n".join(lines) + "\n")

    return [csv_path, md_path]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m cfb", description=__doc__)
    parser.add_argument("--no-odds", action="store_true",
                        help="skip the sportsbook edge layer")
    parser.add_argument("--year", type=int, help="season year")
    parser.add_argument("--week", type=int, help="CFBD week number")
    parser.add_argument("--stub", action="store_true",
                        help="force bundled demo ratings (never real SP+)")
    args = parser.parse_args(argv)

    today = date.today()
    odds_key = odds.resolve_api_key()
    try:
        rows, notes, week = build_board(args, today, odds_key)
    except RuntimeError as err:
        print(f"CFB board unavailable: {err}", file=sys.stderr)
        return 1

    if rows and not args.no_odds:
        note = attach_market(rows, today, odds_key)
        if note:
            notes.append(note)
    sort_rows(rows)

    print_board(rows, today, week, notes)
    if rows:
        for path in write_outputs(rows, today, week, notes):
            rel = path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path
            print(f"Saved {rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
