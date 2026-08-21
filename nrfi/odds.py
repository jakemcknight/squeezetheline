"""Edge-vs-line layer: NRFI/YRFI sportsbook prices + de-vig + EV math.

Market source: The Odds API `totals_1st_1_innings` market (Over/Under 0.5
runs in the 1st inning — Over 0.5 == YRFI, Under 0.5 == NRFI), fetched per
event via /v4/sports/baseball_mlb/events/{id}/odds. Costs 1 credit per
event per region; responses are cached for ODDS_TTL_MINUTES so repeated
runs don't burn credits.

The API key is resolved the same way the rest of the repo does it:
ODDS_API_KEY env var, then config.get_odds_api_key() (Streamlit secrets),
then backend/.env — never hardcoded here.

Manual fallback: if no key is configured (or you want to price a book the
API doesn't carry), pass --odds-csv FILE where FILE has a header row
    away,home,book,nrfi_odds,yrfi_odds
with American odds (e.g. -196, +152) and team names matching the board
(full names, fuzzy-matched the same way as API events).

Edge math per game:
  * American -> decimal; implied prob = 1/decimal.
  * De-vig within each book's two-way market: p_novig = imp / (imp_n + imp_y).
  * Model edge (NRFI-signed): model_nrfi - novig_nrfi. Positive favors the
    NRFI side, negative favors YRFI.
  * Bet side = the side with positive EV at the BEST available price across
    books (line shopping); EV per $1 = p_model*(dec-1) - (1-p_model).
  * Suggested stake = quarter-Kelly, capped at KELLY_CAP of bankroll —
    optional sizing guidance, not a directive.
"""
from __future__ import annotations

import csv
import difflib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

ODDS_BASE = "https://api.the-odds-api.com/v4"
MARKET_KEY = "totals_1st_1_innings"
REGIONS = "us"
ODDS_TTL_MINUTES = 10
EDGE_THRESHOLD = 0.04       # min |edge| to flag a BET
KELLY_FRACTION = 0.25       # quarter-Kelly
KELLY_CAP = 0.03            # never suggest more than 3% of bankroll
CACHE_DIR = Path(__file__).resolve().parent / "cache"

_REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_api_key() -> str:
    key = os.environ.get("ODDS_API_KEY", "")
    if key:
        return key
    try:
        sys.path.insert(0, str(_REPO_ROOT))
        from config import get_odds_api_key  # same resolution the app uses
        key = get_odds_api_key()
        if key:
            return key
    except Exception:
        pass
    env_file = _REPO_ROOT / "backend" / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            m = re.match(r"\s*ODDS_API_KEY\s*=\s*(\S+)", line)
            if m:
                return m.group(1).strip().strip('"').strip("'")
    return ""


# ---------------------------------------------------------------------------
# Price containers
# ---------------------------------------------------------------------------

@dataclass
class BookPrice:
    book: str
    nrfi_american: int
    yrfi_american: int

    @property
    def nrfi_decimal(self) -> float:
        return american_to_decimal(self.nrfi_american)

    @property
    def yrfi_decimal(self) -> float:
        return american_to_decimal(self.yrfi_american)

    @property
    def novig_nrfi(self) -> float:
        """No-vig NRFI probability from this book's two-way prices."""
        imp_n = 1.0 / self.nrfi_decimal
        imp_y = 1.0 / self.yrfi_decimal
        return imp_n / (imp_n + imp_y)


@dataclass
class MarketEdge:
    """Everything the board needs to display market context for one game."""
    best_nrfi: BookPrice            # book with the best NRFI price
    best_yrfi: BookPrice            # book with the best YRFI price
    novig_nrfi: float               # average no-vig NRFI prob across books
    n_books: int
    side: str                       # "NRFI" | "YRFI" | "—"
    side_book: str
    side_american: int
    edge_nrfi: float                # model_nrfi - novig_nrfi (signed)
    ev_per_dollar: float            # EV of $1 on the chosen side at best price
    kelly_pct: float                # suggested quarter-Kelly stake (0 if no bet)
    bet: bool


def american_to_decimal(a: int) -> float:
    return 1.0 + (a / 100.0 if a > 0 else 100.0 / abs(a))


def format_american(a: int) -> str:
    return f"+{a}" if a > 0 else str(a)


# ---------------------------------------------------------------------------
# Fetching (The Odds API)
# ---------------------------------------------------------------------------

def _cache_path(day: date) -> Path:
    return CACHE_DIR / f"odds_{day.isoformat()}.json"


def fetch_market_odds(day: date, api_key: str) -> Tuple[List[dict], Optional[str]]:
    """Return (events_with_prices, error). Each event: {away, home,
    commence_time, books: [{book, nrfi, yrfi}, ...]}. Cached for
    ODDS_TTL_MINUTES to avoid burning credits on repeated runs."""
    cache_file = _cache_path(day)
    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            if time.time() - cached.get("ts", 0) < ODDS_TTL_MINUTES * 60:
                return cached["events"], None
        except (json.JSONDecodeError, OSError):
            pass

    sess = requests.Session()
    try:
        resp = sess.get(
            f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": api_key},
            timeout=30,
        )
        resp.raise_for_status()
        events = resp.json()
    except Exception as err:  # noqa: BLE001
        return [], f"Odds API events request failed: {err}"

    out: List[dict] = []
    for ev in events:
        try:
            resp = sess.get(
                f"{ODDS_BASE}/sports/baseball_mlb/events/{ev['id']}/odds",
                params={
                    "apiKey": api_key,
                    "regions": REGIONS,
                    "markets": MARKET_KEY,
                    "oddsFormat": "american",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:  # noqa: BLE001 — skip single-event hiccups
            continue
        books = []
        for bk in data.get("bookmakers", []):
            for mkt in bk.get("markets", []):
                if mkt.get("key") != MARKET_KEY:
                    continue
                over = under = None
                for oc in mkt.get("outcomes", []):
                    if oc.get("point") != 0.5:
                        continue  # only the 0.5-run line is NRFI/YRFI
                    if oc["name"] == "Over":
                        over = oc["price"]
                    elif oc["name"] == "Under":
                        under = oc["price"]
                if over is not None and under is not None:
                    books.append({"book": bk["title"], "nrfi": under, "yrfi": over})
        if books:
            out.append(
                {
                    "away": ev["away_team"],
                    "home": ev["home_team"],
                    "commence_time": ev.get("commence_time", ""),
                    "books": books,
                }
            )

    remaining = resp.headers.get("x-requests-remaining") if out else None
    if remaining is not None:
        print(f"[odds] The Odds API credits remaining: {remaining}", file=sys.stderr)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps({"ts": time.time(), "events": out}))
    return out, None


# ---------------------------------------------------------------------------
# Manual CSV fallback
# ---------------------------------------------------------------------------

def load_manual_odds(path: Path) -> Tuple[List[dict], Optional[str]]:
    """Read away,home,book,nrfi_odds,yrfi_odds rows into the same event shape
    fetch_market_odds produces. Multiple rows per game = multiple books."""
    try:
        rows = list(csv.DictReader(path.open()))
    except OSError as err:
        return [], f"could not read {path}: {err}"
    events: Dict[Tuple[str, str], dict] = {}
    for i, row in enumerate(rows, 2):
        try:
            away, home = row["away"].strip(), row["home"].strip()
            book = (row.get("book") or "manual").strip()
            nrfi, yrfi = int(row["nrfi_odds"]), int(row["yrfi_odds"])
        except (KeyError, TypeError, ValueError):
            return [], (
                f"bad row {i} in {path} — expected header "
                "away,home,book,nrfi_odds,yrfi_odds with American odds"
            )
        ev = events.setdefault((away, home), {"away": away, "home": home,
                                              "commence_time": "", "books": []})
        ev["books"].append({"book": book, "nrfi": nrfi, "yrfi": yrfi})
    return list(events.values()), None


# ---------------------------------------------------------------------------
# Matching odds events to statsapi games
# ---------------------------------------------------------------------------

def _norm(name: str) -> str:
    return re.sub(r"[^a-z ]", "", name.lower()).strip()


def _team_match(a: str, b: str) -> float:
    a, b = _norm(a), _norm(b)
    if a == b:
        return 1.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def match_event(game: dict, events: List[dict]) -> Optional[dict]:
    """Best odds event for a statsapi game (team-name fuzzy match; on
    doubleheaders the same event prices both, which is the best we can do
    without per-game start times from the odds side)."""
    best, best_score = None, 0.0
    for ev in events:
        score = _team_match(game["away_name"], ev["away"]) + _team_match(
            game["home_name"], ev["home"]
        )
        if score > best_score:
            best, best_score = ev, score
    return best if best_score >= 1.6 else None  # ~0.8 avg similarity floor


# ---------------------------------------------------------------------------
# Edge computation
# ---------------------------------------------------------------------------

def compute_edge(model_nrfi: float, event: dict, confidence: str) -> MarketEdge:
    prices = [BookPrice(b["book"], b["nrfi"], b["yrfi"]) for b in event["books"]]
    best_nrfi = max(prices, key=lambda p: p.nrfi_decimal)
    best_yrfi = max(prices, key=lambda p: p.yrfi_decimal)
    novig = sum(p.novig_nrfi for p in prices) / len(prices)

    model_yrfi = 1.0 - model_nrfi
    ev_nrfi = model_nrfi * (best_nrfi.nrfi_decimal - 1) - model_yrfi
    ev_yrfi = model_yrfi * (best_yrfi.yrfi_decimal - 1) - model_nrfi
    edge_nrfi = model_nrfi - novig

    if ev_nrfi >= ev_yrfi:
        side, book_price, dec, american, p, ev_best = (
            "NRFI", best_nrfi, best_nrfi.nrfi_decimal, best_nrfi.nrfi_american,
            model_nrfi, ev_nrfi,
        )
    else:
        side, book_price, dec, american, p, ev_best = (
            "YRFI", best_yrfi, best_yrfi.yrfi_decimal, best_yrfi.yrfi_american,
            model_yrfi, ev_yrfi,
        )

    bet = (
        ev_best > 0
        and abs(edge_nrfi) >= EDGE_THRESHOLD
        and confidence != "Low"
    )
    kelly = 0.0
    if bet:
        full_kelly = (dec * p - 1.0) / (dec - 1.0)
        kelly = min(max(full_kelly * KELLY_FRACTION, 0.0), KELLY_CAP)
    if not bet:
        side = "—"

    return MarketEdge(
        best_nrfi=best_nrfi,
        best_yrfi=best_yrfi,
        novig_nrfi=novig,
        n_books=len(prices),
        side=side,
        side_book=book_price.book if bet else "",
        side_american=american,
        edge_nrfi=edge_nrfi,
        ev_per_dollar=ev_best,
        kelly_pct=kelly,
        bet=bet,
    )
