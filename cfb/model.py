"""CFB power-rating model — simple, transparent, documented.

Each FBS team carries two point-denominated ratings seeded from SP+:

  * OFF — adjusted points scored per game vs an average FBS defense
  * DEF — adjusted points allowed per game vs an average FBS offense
          (lower is better)

plus SP+ special teams (ST, small additive term). Projections:

  expected_home_pts = AVG_PTS + (home OFF - AVG_PTS) + (away DEF - AVG_PTS)
                    = home OFF + away DEF - AVG_PTS          (+ HFA/2)
  expected_away_pts = away OFF + home DEF - AVG_PTS          (- HFA/2)

  margin = expected_home - expected_away + (home ST - away ST)
  total  = expected_home + expected_away

AVG_PTS is the league mean of the rating set, so an average-vs-average
matchup projects to exactly the league-average score. HFA = 2.5 points
(CFB consensus ~2.3-2.8), zero for neutral-site games.

Win probability: margin -> Normal CDF with SD = 16 points, the classic
CFB score-margin dispersion. Cover probability for a spread uses the same
SD around the market line; total over/under uses SD = 17 points.

IN-SEASON UPDATES (dormant until games finish): each completed game yields
opponent-adjusted samples
    off_sample = points_scored  - opp DEF seed + AVG_PTS
    def_sample = points_allowed - opp OFF seed + AVG_PTS
capped at +/-21 from the pre-game expectation to tame blowouts, then
    OFF_new = (PRIOR_GAMES * OFF_seed + sum(off_samples)) / (PRIOR_GAMES + n)
with PRIOR_GAMES = 8 — i.e. the preseason SP+ seed is worth eight games of
evidence, so early results nudge rather than whipsaw the ratings.

EARLY-SEASON HONESTY: through week 3 every game is confidence Low (ratings
are still mostly preseason projection). Unrated opponents (FCS) get a flat
placeholder rating and force Low. Teams returning <45% of production are
extra-uncertain and cap confidence at Med. Low confidence blocks BET flags
in the edge layer — the board still shows edges, it just won't tell you to
bet on preseason guesswork.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

HFA = 2.5                 # home-field advantage, points (0 on neutral sites)
MARGIN_SD = 16.0          # SD of (actual - true) margin, CFB
TOTAL_SD = 17.0           # SD around the true total, CFB
PRIOR_GAMES = 8           # preseason seed worth this many games of results
UPDATE_CAP = 21.0         # cap per-game sample deviation from expectation
FCS_OFF_PENALTY = 11.0    # unrated (FCS) team: OFF this far below FBS avg
FCS_DEF_PENALTY = 11.0    # ... and DEF this far above (worse than) FBS avg
LOW_RETURNING = 0.45      # returning production below this caps conf at Med
SCORE_CLAMP = (3.0, 70.0)


@dataclass
class TeamRating:
    name: str
    off: float
    def_: float
    st: float = 0.0
    seed_off: float = 0.0
    seed_def: float = 0.0
    games: int = 0
    returning: Optional[float] = None
    unrated: bool = False           # FCS / not in the ratings set
    off_samples: List[float] = field(default_factory=list)
    def_samples: List[float] = field(default_factory=list)


@dataclass
class Projection:
    home_pts: float
    away_pts: float
    margin: float       # home - away, positive = home favored
    total: float
    p_home: float       # model home win probability


@dataclass
class BoardRow:
    away: str
    home: str
    neutral: bool
    start: str
    proj: Projection
    confidence: str
    conf_why: str
    market: Optional[object] = None    # odds.GameMarket when lines matched

    @property
    def matchup(self) -> str:
        return f"{self.away} @ {self.home}" + (" (N)" if self.neutral else "")

    def model_line(self) -> str:
        """Model's spread quoted like a book quotes the home line."""
        m = self.proj.margin
        if abs(m) < 0.05:
            return "PK"
        return f"{'-' if m > 0 else '+'}{abs(m):.1f}"


def normal_cdf(x: float, sd: float) -> float:
    return 0.5 * (1.0 + math.erf(x / (sd * math.sqrt(2.0))))


def build_ratings(sp: Dict[str, dict],
                  returning: Dict[str, float]) -> Dict[str, TeamRating]:
    ratings: Dict[str, TeamRating] = {}
    for team, r in sp.items():
        ratings[team] = TeamRating(
            name=team, off=r["off"], def_=r["def"], st=r.get("st", 0.0),
            seed_off=r["off"], seed_def=r["def"],
            returning=returning.get(team),
        )
    return ratings


def league_avg_pts(ratings: Dict[str, TeamRating]) -> float:
    rated = [t for t in ratings.values() if not t.unrated]
    if not rated:
        return 27.0
    return (sum(t.off for t in rated) + sum(t.def_ for t in rated)) / (2 * len(rated))


def placeholder_unrated(name: str, avg: float) -> TeamRating:
    """FCS / unknown opponent: a flat below-FBS-average placeholder. Any
    game touching one of these is confidence Low, so it can never BET."""
    return TeamRating(name=name, off=avg - FCS_OFF_PENALTY,
                      def_=avg + FCS_DEF_PENALTY,
                      seed_off=avg - FCS_OFF_PENALTY,
                      seed_def=avg + FCS_DEF_PENALTY, unrated=True)


# ---------------------------------------------------------------------------
# In-season updates (opponent-adjusted, prior-weighted — see module doc)
# ---------------------------------------------------------------------------

def apply_results(ratings: Dict[str, TeamRating], completed: List[dict],
                  avg: float) -> int:
    """Fold completed games into OFF/DEF. Returns games applied."""
    applied = 0
    for g in completed:
        home, away = ratings.get(g["home"]), ratings.get(g["away"])
        if home is None or away is None:
            continue  # non-FBS results don't move FBS ratings
        hp, ap = float(g["home_points"]), float(g["away_points"])
        hfa = 0.0 if g["neutral"] else HFA
        for team, opp, pts_for, pts_against, adj in (
            (home, away, hp, ap, -hfa / 2.0),
            (away, home, ap, hp, +hfa / 2.0),
        ):
            exp_for = team.seed_off + opp.seed_def - avg - adj
            exp_against = opp.seed_off + team.seed_def - avg + adj
            off_s = pts_for + adj - opp.seed_def + avg
            def_s = pts_against - adj - opp.seed_off + avg
            off_s = max(exp_for - UPDATE_CAP, min(exp_for + UPDATE_CAP, off_s))
            def_s = max(exp_against - UPDATE_CAP, min(exp_against + UPDATE_CAP, def_s))
            team.off_samples.append(off_s)
            team.def_samples.append(def_s)
            team.games += 1
        applied += 1

    for t in ratings.values():
        if t.off_samples:
            n = len(t.off_samples)
            t.off = (PRIOR_GAMES * t.seed_off + sum(t.off_samples)) / (PRIOR_GAMES + n)
            t.def_ = (PRIOR_GAMES * t.seed_def + sum(t.def_samples)) / (PRIOR_GAMES + n)
    return applied


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------

def project(home: TeamRating, away: TeamRating, neutral: bool,
            avg: float) -> Projection:
    hfa = 0.0 if neutral else HFA
    home_pts = home.off + away.def_ - avg + hfa / 2.0
    away_pts = away.off + home.def_ - avg - hfa / 2.0
    home_pts = max(SCORE_CLAMP[0], min(SCORE_CLAMP[1], home_pts))
    away_pts = max(SCORE_CLAMP[0], min(SCORE_CLAMP[1], away_pts))
    margin = home_pts - away_pts + (home.st - away.st)
    total = home_pts + away_pts
    return Projection(
        home_pts=home_pts, away_pts=away_pts, margin=margin, total=total,
        p_home=normal_cdf(margin, MARGIN_SD),
    )


def confidence_for(week: Optional[int], home: TeamRating,
                   away: TeamRating) -> tuple[str, str]:
    """(level, reason). Low blocks BET flags downstream."""
    if home.unrated or away.unrated:
        return "Low", "unrated (FCS) opponent"
    if week is not None and week <= 3:
        return "Low", "weeks 0-3: ratings still mostly preseason projection"
    games = min(home.games, away.games)
    if games < 4:
        return "Low", "fewer than 4 games of results"
    level = "High" if games >= 7 else "Med"
    for t in (home, away):
        if t.returning is not None and t.returning < LOW_RETURNING and level == "High":
            level = "Med"
            return level, f"{t.name} returns <{LOW_RETURNING:.0%} of production"
    return level, ""
