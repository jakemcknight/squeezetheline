"""Edge-vs-line layer: NCAAF market prices + de-vig + EV math.

Market source: The Odds API bulk endpoint
    /v4/sports/americanfootball_ncaaf/odds?markets=h2h,spreads,totals
(one call = 3 credits: markets x regions). Responses cached for
ODDS_TTL_MINUTES so repeated runs don't burn credits. The API key resolves
the same way the rest of the repo does it: ODDS_API_KEY env var, then
config.get_odds_api_key(), then backend/.env — never hardcoded.

Edge math per game and market (mirrors nrfi/odds.py):
  * American -> decimal; implied prob = 1/decimal.
  * De-vig each book's two-way market: p_novig = imp_a / (imp_a + imp_b);
    the market's no-vig probability is the average across books.
  * SPREAD — consensus line = median home spread across books. Model
    P(home covers) = Phi((model_margin - consensus_margin) / MARGIN_SD).
    Point edge = model_margin - consensus_margin.
  * TOTAL — consensus O/U = median total. P(over) =
    Phi((model_total - consensus_total) / TOTAL_SD). Point edge likewise.
  * MONEYLINE — model home win prob vs average no-vig implied.
  * Line shopping: for the model-favored side, best book = most favorable
    point first (more points caught for the dog side, lower Over / higher
    Under), then best price at that point.
  * BET flags only when EV > 0 at the best price, the edge clears the
    market's threshold, AND model confidence isn't Low. Stake suggestion =
    quarter-Kelly capped at KELLY_CAP — optional sizing, not a directive.

Team-name matching: CFBD uses school names ("TCU"), The Odds API appends
mascots ("TCU Horned Frogs") — normalized prefix match + alias table +
fuzzy fallback.
"""
from __future__ import annotations

import difflib
import json
import os
import re
import statistics
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from .model import MARGIN_SD, TOTAL_SD, normal_cdf

ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT_KEY = "americanfootball_ncaaf"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_TTL_MINUTES = 15
SPREAD_EDGE_PTS = 2.0       # min |model - market| points to flag a spread BET
TOTAL_EDGE_PTS = 3.0        # ... total BET
ML_EDGE_PROB = 0.05         # min |model - no-vig| win-prob edge for ML BET
KELLY_FRACTION = 0.25       # quarter-Kelly
KELLY_CAP = 0.02            # never suggest more than 2% of bankroll
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


def american_to_decimal(a: float) -> float:
    return 1.0 + (a / 100.0 if a > 0 else 100.0 / abs(a))


def format_american(a: float) -> str:
    a = int(round(a))
    return f"+{a}" if a > 0 else str(a)


# ---------------------------------------------------------------------------
# Fetching (one bulk call for the whole slate)
# ---------------------------------------------------------------------------

def fetch_market_odds(day: date, api_key: str) -> Tuple[List[dict], Optional[str]]:
    """Return (events, error). Each event: {home, away, commence_time,
    books: {book_title: {h2h: {home, away}, spread: {home_point,
    home_price, away_price}, total: {point, over, under}}}}."""
    cache_file = CACHE_DIR / f"odds_{day.isoformat()}.json"
    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text())
            if time.time() - cached.get("ts", 0) < ODDS_TTL_MINUTES * 60:
                return cached["events"], None
        except (json.JSONDecodeError, OSError):
            pass

    try:
        resp = requests.get(
            f"{ODDS_BASE}/sports/{SPORT_KEY}/odds",
            params={"apiKey": api_key, "regions": REGIONS, "markets": MARKETS,
                    "oddsFormat": "american"},
            timeout=45,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as err:  # noqa: BLE001
        return [], f"Odds API request failed: {err}"

    remaining = resp.headers.get("x-requests-remaining")
    if remaining is not None:
        print(f"[odds] The Odds API credits remaining: {remaining}", file=sys.stderr)

    events: List[dict] = []
    for ev in raw:
        home, away = ev.get("home_team", ""), ev.get("away_team", "")
        books: Dict[str, dict] = {}
        for bk in ev.get("bookmakers", []):
            entry: dict = {}
            for mkt in bk.get("markets", []):
                ocs = mkt.get("outcomes", [])
                if mkt["key"] == "h2h":
                    prices = {o["name"]: o["price"] for o in ocs}
                    if home in prices and away in prices:
                        entry["h2h"] = {"home": prices[home], "away": prices[away]}
                elif mkt["key"] == "spreads":
                    by = {o["name"]: o for o in ocs}
                    if home in by and away in by:
                        entry["spread"] = {
                            "home_point": by[home].get("point"),
                            "home_price": by[home]["price"],
                            "away_price": by[away]["price"],
                        }
                elif mkt["key"] == "totals":
                    over = next((o for o in ocs if o["name"] == "Over"), None)
                    under = next((o for o in ocs if o["name"] == "Under"), None)
                    if over and under and over.get("point") is not None:
                        entry["total"] = {"point": over["point"],
                                          "over": over["price"],
                                          "under": under["price"]}
            if entry:
                books[bk["title"]] = entry
        if books:
            events.append({"home": home, "away": away,
                           "commence_time": ev.get("commence_time", ""),
                           "books": books})

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps({"ts": time.time(), "events": events}))
    return events, None


def fetch_event_list(api_key: str) -> Tuple[List[dict], Optional[str]]:
    """Bare upcoming events (no prices) — used only for the no-CFBD slate
    fallback. Free: the /events endpoint doesn't cost credits."""
    try:
        resp = requests.get(f"{ODDS_BASE}/sports/{SPORT_KEY}/events",
                            params={"apiKey": api_key}, timeout=30)
        resp.raise_for_status()
        return [{"home": e["home_team"], "away": e["away_team"],
                 "commence_time": e.get("commence_time", "")}
                for e in resp.json()], None
    except Exception as err:  # noqa: BLE001
        return [], f"Odds API events request failed: {err}"


# ---------------------------------------------------------------------------
# CFBD school name  <->  Odds API "School Mascot" matching
# ---------------------------------------------------------------------------

ALIASES = {  # CFBD school name -> distinctive fragment of the Odds API name
    "App State": "appalachian state",
    "UT San Antonio": "utsa",
    "Louisiana Monroe": "ul monroe",
    "UL Monroe": "ul monroe",
    "Southern Miss": "southern mississippi",
    "Sam Houston": "sam houston",
    "Massachusetts": "umass",
    "Connecticut": "uconn",
    "Miami": "miami hurricanes",
    "Miami (OH)": "miami (oh)",
    "San José State": "san jose state",
    "Hawai'i": "hawaii",
    "USF": "south florida",
    "UTEP": "utep",
    "Ole Miss": "ole miss",
}


def _norm(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = s.lower().replace("&", "and").replace("'", "")
    s = re.sub(r"\bst\.\b", "state", s)
    return re.sub(r"[^a-z0-9() ]", " ", s).strip()


def _score(school: str, odds_name: str) -> float:
    """How well a CFBD school name matches an Odds API 'School Mascot'."""
    a = _norm(ALIASES.get(school, school))
    b = _norm(odds_name)
    if a == b:
        return 1.0
    if b.startswith(a + " ") or (" " + a + " ") in (" " + b + " "):
        return 0.95  # school is a clean word-boundary prefix/substring
    # fuzzy vs the odds name with trailing mascot words progressively removed
    words = b.split()
    best = difflib.SequenceMatcher(None, a, b).ratio()
    for cut in (1, 2):
        if len(words) > cut:
            best = max(best, difflib.SequenceMatcher(
                None, a, " ".join(words[:-cut])).ratio())
    return best


def match_event(away: str, home: str, events: List[dict]) -> Optional[dict]:
    best, best_score = None, 0.0
    for ev in events:
        score = _score(away, ev["away"]) + _score(home, ev["home"])
        if score > best_score:
            best, best_score = ev, score
    return best if best_score >= 1.7 else None


# ---------------------------------------------------------------------------
# Edge computation
# ---------------------------------------------------------------------------

@dataclass
class MarketView:
    """One market (spread / total / moneyline) for one game."""
    kind: str                 # "SPREAD" | "TOTAL" | "ML"
    market_value: float       # consensus home spread / total / no-vig home %
    model_value: float        # model margin / total / home win %
    edge: float               # point edge (spread/total) or prob edge (ML)
    p_model: float            # model prob of the chosen side
    p_novig: float            # market no-vig prob of the chosen side
    side: str                 # e.g. "TCU -6.5", "Over 51.5", "UNC ML"
    book: str
    price: float              # American odds at the best book
    point: Optional[float]    # the best book's own line (spread/total)
    ev_per_dollar: float
    kelly_pct: float
    bet: bool


@dataclass
class GameMarket:
    n_books: int
    spread: Optional[MarketView]
    total: Optional[MarketView]
    ml: Optional[MarketView]

    def best(self) -> Optional[MarketView]:
        views = [v for v in (self.spread, self.total, self.ml) if v]
        bets = [v for v in views if v.bet]
        pool = bets or views
        return max(pool, key=lambda v: v.ev_per_dollar) if pool else None


def _novig(price_a: float, price_b: float) -> float:
    ia = 1.0 / american_to_decimal(price_a)
    ib = 1.0 / american_to_decimal(price_b)
    return ia / (ia + ib)


def _finish(kind: str, market_value: float, model_value: float, edge: float,
            p_side: float, novig_side: float, side: str, book: str,
            price: float, point: Optional[float], threshold_ok: bool,
            confidence: str) -> MarketView:
    dec = american_to_decimal(price)
    ev = p_side * (dec - 1.0) - (1.0 - p_side)
    bet = ev > 0 and threshold_ok and confidence != "Low"
    kelly = 0.0
    if bet:
        full = (dec * p_side - 1.0) / (dec - 1.0)
        kelly = min(max(full * KELLY_FRACTION, 0.0), KELLY_CAP)
    return MarketView(kind=kind, market_value=market_value,
                      model_value=model_value, edge=edge, p_model=p_side,
                      p_novig=novig_side, side=side, book=book, price=price,
                      point=point, ev_per_dollar=ev, kelly_pct=kelly, bet=bet)


def _spread_view(event: dict, home: str, away: str, margin: float,
                 confidence: str) -> Optional[MarketView]:
    rows = [(b, e["spread"]) for b, e in event["books"].items()
            if "spread" in e and e["spread"]["home_point"] is not None]
    if not rows:
        return None
    consensus = statistics.median(s["home_point"] for _, s in rows)
    market_margin = -consensus                      # home -6.5 => margin +6.5
    p_home_cover = normal_cdf(margin - market_margin, MARGIN_SD)
    novig_home = statistics.fmean(
        _novig(s["home_price"], s["away_price"]) for _, s in rows)
    edge_pts = margin - market_margin

    home_side = edge_pts >= 0
    # line shop: most favorable point for our side, then best price there
    if home_side:
        book, s = max(rows, key=lambda r: (r[1]["home_point"],
                                           american_to_decimal(r[1]["home_price"])))
        point, price = s["home_point"], s["home_price"]
        p_side = normal_cdf(margin + point, MARGIN_SD)   # cover at book's line
        novig_side, team = novig_home, home
    else:
        book, s = max(rows, key=lambda r: (-r[1]["home_point"],
                                           american_to_decimal(r[1]["away_price"])))
        point, price = -s["home_point"], s["away_price"]
        p_side = 1.0 - normal_cdf(margin - point, MARGIN_SD)
        novig_side, team = 1.0 - novig_home, away
    side = f"{team} {point:+g}"
    return _finish("SPREAD", consensus, margin, edge_pts, p_side, novig_side,
                   side, book, price, point,
                   abs(edge_pts) >= SPREAD_EDGE_PTS, confidence)


def _total_view(event: dict, total: float, confidence: str) -> Optional[MarketView]:
    rows = [(b, e["total"]) for b, e in event["books"].items() if "total" in e]
    if not rows:
        return None
    consensus = statistics.median(t["point"] for _, t in rows)
    edge_pts = total - consensus
    novig_over = statistics.fmean(_novig(t["over"], t["under"]) for _, t in rows)

    over_side = edge_pts >= 0
    if over_side:
        book, t = max(rows, key=lambda r: (-r[1]["point"],
                                           american_to_decimal(r[1]["over"])))
        point, price = t["point"], t["over"]
        p_side = normal_cdf(total - point, TOTAL_SD)
        novig_side, label = novig_over, "Over"
    else:
        book, t = max(rows, key=lambda r: (r[1]["point"],
                                           american_to_decimal(r[1]["under"])))
        point, price = t["point"], t["under"]
        p_side = 1.0 - normal_cdf(total - point, TOTAL_SD)
        novig_side, label = 1.0 - novig_over, "Under"
    return _finish("TOTAL", consensus, total, edge_pts, p_side, novig_side,
                   f"{label} {point:g}", book, price, point,
                   abs(edge_pts) >= TOTAL_EDGE_PTS, confidence)


def _ml_view(event: dict, home: str, away: str, p_home: float,
             confidence: str) -> Optional[MarketView]:
    rows = [(b, e["h2h"]) for b, e in event["books"].items() if "h2h" in e]
    if not rows:
        return None
    novig_home = statistics.fmean(_novig(h["home"], h["away"]) for _, h in rows)
    edge = p_home - novig_home

    home_side = edge >= 0
    if home_side:
        book, h = max(rows, key=lambda r: american_to_decimal(r[1]["home"]))
        price, p_side, novig_side, team = h["home"], p_home, novig_home, home
    else:
        book, h = max(rows, key=lambda r: american_to_decimal(r[1]["away"]))
        price, p_side, novig_side, team = (h["away"], 1.0 - p_home,
                                           1.0 - novig_home, away)
    return _finish("ML", novig_home, p_home, edge, p_side, novig_side,
                   f"{team} ML", book, price, None,
                   abs(edge) >= ML_EDGE_PROB, confidence)


def compute_market(event: dict, home: str, away: str, margin: float,
                   total: float, p_home: float, confidence: str) -> GameMarket:
    return GameMarket(
        n_books=len(event["books"]),
        spread=_spread_view(event, home, away, margin, confidence),
        total=_total_view(event, total, confidence),
        ml=_ml_view(event, home, away, p_home, confidence),
    )
