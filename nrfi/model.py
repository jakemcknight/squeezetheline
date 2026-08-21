"""NRFI/YRFI model — simple, transparent, documented.

For each game we estimate two half-inning probabilities and combine them:

    P(YRFI) = 1 - (1 - p_top) * (1 - p_bottom)

where p_top is P(away team scores >=1 run in the top of the 1st vs the home
starter) and p_bottom is the mirror image.

Each half-inning probability comes from a log5 (odds-ratio) blend of:

  * the OFFENSE's season rate of scoring >=1 run in the 1st inning, and
  * the STARTER's season rate of allowing >=1 run in the 1st inning of his
    starts (a true first-inning split — the starter always pitches the 1st,
    so opponent 1st-inning runs in his starts are his),

both shrunk toward the league half-inning base rate before combining, so
small samples (early season, rookie starters) regress toward league average
instead of producing wild numbers:

    shrunk = (n * rate + K * league) / (n + K)

K_TEAM = 20 pseudo-games, K_PITCHER = 8 pseudo-starts. A pitcher with no
tracked starts (TBD / season debut) is treated as exactly league average.

log5 combination (Bill James), with L the league half-inning rate:

    p = (t*q/L) / (t*q/L + (1-t)*(1-q)/(1-L))

v1 limitations / improvement path (documented, not built):
  * No lineup-specific handedness or day-of lineup data — team 1st-inning
    rate stands in for the actual 1-through-3 hitters due up.
  * No park factors or weather. Park could be added by scaling L per venue.
  * Pitcher rate ignores which bullpen-game "opener" pitched the 1st on
    teams that use openers; gamesStarted==1 is taken at face value.
  * Market-edge layer lives in odds.py (The Odds API totals_1st_1_innings,
    de-vig + EV + fractional Kelly) and attaches to BoardRow.market.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

K_TEAM = 20      # pseudo-games of league-average shrinkage for team offense
K_PITCHER = 8    # pseudo-starts of shrinkage for starter suppression
HALF_CLAMP = (0.05, 0.70)   # sanity clamp on any single half-inning estimate
YES_THRESHOLD = 0.56        # "clearly above 55%"
NO_THRESHOLD = 0.44         # "clearly below 45%"


@dataclass
class TeamRates:
    games: int = 0
    scored_first: int = 0     # games with >=1 run scored in the 1st

    @property
    def rate(self) -> float:
        return self.scored_first / self.games if self.games else 0.0


@dataclass
class PitcherRates:
    starts: int = 0
    allowed_first: int = 0    # starts where the opponent scored in the 1st

    @property
    def rate(self) -> float:
        return self.allowed_first / self.starts if self.starts else 0.0


@dataclass
class BoardRow:
    matchup: str
    away_pitcher: str
    home_pitcher: str
    p_yrfi: float
    p_top: float
    p_bottom: float
    lean: str
    confidence: str
    status: str
    detail: Dict[str, float] = field(default_factory=dict)
    market: Optional[object] = None  # odds.MarketEdge when a line was found

    @property
    def p_nrfi(self) -> float:
        return 1.0 - self.p_yrfi


def build_team_rates(first_innings: Dict[int, dict]) -> Dict[int, TeamRates]:
    teams: Dict[int, TeamRates] = {}
    for g in first_innings.values():
        for side, runs_key in (("away_id", "away_first"), ("home_id", "home_first")):
            t = teams.setdefault(g[side], TeamRates())
            t.games += 1
            if g[runs_key] > 0:
                t.scored_first += 1
    return teams


def league_half_rate(first_innings: Dict[int, dict]) -> float:
    halves = scored = 0
    for g in first_innings.values():
        halves += 2
        scored += (g["away_first"] > 0) + (g["home_first"] > 0)
    return scored / halves if halves else 0.27


def build_pitcher_rates(
    starts: List[dict], first_innings: Dict[int, dict]
) -> PitcherRates:
    pr = PitcherRates()
    for s in starts:
        g = first_innings.get(s["gamePk"])
        if g is None:
            continue  # start outside our final-game sample (e.g. suspended)
        opp_first = g["away_first"] if s["isHome"] else g["home_first"]
        pr.starts += 1
        if opp_first > 0:
            pr.allowed_first += 1
    return pr


def shrink(rate: float, n: int, k: int, league: float) -> float:
    return (n * rate + k * league) / (n + k) if (n + k) else league


def log5(t: float, q: float, league: float) -> float:
    """Combine offense rate t and pitcher-allowed rate q against base rate L."""
    num = t * q / league
    den = num + (1 - t) * (1 - q) / (1 - league)
    p = num / den if den else league
    return min(max(p, HALF_CLAMP[0]), HALF_CLAMP[1])


def half_inning_prob(
    offense: Optional[TeamRates],
    pitcher: Optional[PitcherRates],
    league: float,
) -> float:
    t = shrink(offense.rate, offense.games, K_TEAM, league) if offense else league
    q = shrink(pitcher.rate, pitcher.starts, K_PITCHER, league) if pitcher else league
    return log5(t, q, league)


def lean_for(p_yrfi: float) -> str:
    if p_yrfi > YES_THRESHOLD:
        return "YES"
    if p_yrfi < NO_THRESHOLD:
        return "NO"
    return "PASS"


def confidence_for(
    away: Optional[TeamRates],
    home: Optional[TeamRates],
    away_p: Optional[PitcherRates],
    home_p: Optional[PitcherRates],
) -> str:
    team_games = min(away.games if away else 0, home.games if home else 0)
    pitcher_starts = min(
        away_p.starts if away_p else 0, home_p.starts if home_p else 0
    )
    if team_games >= 40 and pitcher_starts >= 10:
        return "High"
    if team_games < 20 or pitcher_starts < 5:
        return "Low"
    return "Med"
