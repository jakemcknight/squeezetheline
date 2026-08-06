"""Entry point: `python -m nrfi` — prints today's NRFI/YRFI board and writes
CSV + markdown copies under nrfi/out/."""
from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

from . import data, model

OUT_DIR = Path(__file__).resolve().parent / "out"
DISCLAIMER = (
    "Model estimates only — not guaranteed outcomes. Betting involves risk; "
    "wager responsibly."
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

        rows.append(
            model.BoardRow(
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
        )

    rows.sort(key=lambda r: r.p_yrfi, reverse=True)
    return rows


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _table(rows: List[model.BoardRow]) -> List[List[str]]:
    header = ["#", "Matchup", "Away SP", "Home SP", "YRFI%", "NRFI%", "Lean", "Conf", "Status"]
    body = [
        [
            str(i),
            r.matchup,
            r.away_pitcher,
            r.home_pitcher,
            f"{r.p_yrfi:.1%}",
            f"{r.p_nrfi:.1%}",
            r.lean,
            r.confidence,
            r.status,
        ]
        for i, r in enumerate(rows, 1)
    ]
    return [header] + body


def print_board(rows: List[model.BoardRow], today: date) -> None:
    print(f"\nNRFI / YRFI board — {today.strftime('%A, %B %d, %Y')}")
    print(DISCLAIMER)
    if not rows:
        print("\nNo MLB games today.")
        return
    league = rows[0].detail.get("league_half_rate")
    if league is not None:
        print(f"League YRFI base rate this season: {1 - (1 - league) ** 2:.1%}\n")
    table = _table(rows)
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for n, row in enumerate(table):
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)))
        if n == 0:
            print("  ".join("-" * w for w in widths))


def write_outputs(rows: List[model.BoardRow], today: date) -> List[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = today.isoformat()

    csv_path = OUT_DIR / f"nrfi_board_{stamp}.csv"
    with csv_path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(
            ["date", "matchup", "away_sp", "home_sp", "p_yrfi", "p_nrfi",
             "p_top1", "p_bottom1", "lean", "confidence", "status"]
        )
        for r in rows:
            w.writerow(
                [stamp, r.matchup, r.away_pitcher, r.home_pitcher,
                 f"{r.p_yrfi:.4f}", f"{r.p_nrfi:.4f}", f"{r.p_top:.4f}",
                 f"{r.p_bottom:.4f}", r.lean, r.confidence, r.status]
            )

    md_path = OUT_DIR / f"nrfi_board_{stamp}.md"
    lines = [
        f"# NRFI / YRFI board — {today.strftime('%A, %B %d, %Y')}",
        "",
        f"> {DISCLAIMER}",
        "",
    ]
    if rows:
        table = _table(rows)
        lines.append("| " + " | ".join(table[0]) + " |")
        lines.append("|" + "|".join("---" for _ in table[0]) + "|")
        for row in table[1:]:
            lines.append("| " + " | ".join(row) + " |")
    else:
        lines.append("No MLB games today.")
    md_path.write_text("\n".join(lines) + "\n")

    return [csv_path, md_path]


def main() -> int:
    today = date.today()
    try:
        rows = build_board(today)
    except RuntimeError as err:
        print(f"NRFI board unavailable: {err}", file=sys.stderr)
        return 1
    print_board(rows, today)
    if rows:
        for path in write_outputs(rows, today):
            print(f"\nSaved {path.relative_to(Path.cwd()) if path.is_relative_to(Path.cwd()) else path}", end="")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
