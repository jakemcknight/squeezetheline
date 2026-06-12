"""
Soccer (FIFA World Cup) player-prop projection model.

This is soccer's analogue to model.py (the NBA XGBoost model): the sport's own
projection engine, kept separate because soccer props behave nothing like the US
sports'. It is pure, dependency-free math (just the stdlib), so it's fully unit
testable and adds no new package requirements.

Why a bespoke model instead of the linear last-5/last-10/season blend the US
sports use in providers/live.py:

  1. **Low counts → Poisson, not Gaussian.** A player takes ~0–6 shots and
     scores ~0–2 goals a match. Those are small non-negative counts, well modeled
     by a Poisson distribution and badly modeled by a normal one. We project an
     expected rate (lambda) and read prop probabilities straight off the Poisson:
       P(over a line)            = 1 - CDF(floor(line))
       P(anytime goalscorer)     = P(at least 1 goal) = 1 - e^(-lambda)

  2. **Tiny, noisy samples → shrink toward a position prior.** The public ESPN
     feed gives only the last ~5 club matches, so a striker's quiet week or a
     defender's fluke goal would wreck a raw average. We shrink the empirical
     per-match rate toward a position-based prior with a pseudo-count, which is
     where "forwards vs defenders" enters the model: a forward's goal prior is
     ~9x a defender's, so identical 0-goal samples still yield very different
     projections.

  3. **Opponent strength.** lambda is scaled by a national-team defensive factor
     derived from a documented strength table (FIFA-ranking tiers, June 2026):
     scoring stats are nudged up against weak defenses and down against elite
     ones, clamped to a sane band.

Everything below is deliberately transparent and tunable rather than a black box,
because the inputs are thin and a bettor needs to understand the assumptions.
"""

from __future__ import annotations

import math
from typing import Optional

# --- Poisson primitives -----------------------------------------------------


def poisson_pmf(k: int, lam: float) -> float:
    """P(X = k) for X ~ Poisson(lam)."""
    if lam < 0 or k < 0:
        return 0.0
    if lam == 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * lam ** k / math.factorial(k)


def poisson_cdf(k: int, lam: float) -> float:
    """P(X <= k) for X ~ Poisson(lam)."""
    if k < 0:
        return 0.0
    return sum(poisson_pmf(i, lam) for i in range(int(math.floor(k)) + 1))


def prob_over(line: float, lam: float) -> float:
    """P(X > line) for X ~ Poisson(lam).

    Prop lines are half-integers (0.5, 1.5, 2.5, ...), so "over" means strictly
    greater: Over 0.5 == X>=1, Over 1.5 == X>=2, etc. For a whole-number line we
    use strict > (X > 2 == X>=3), matching how a push would settle.
    """
    if lam <= 0:
        return 0.0
    return max(0.0, min(1.0, 1.0 - poisson_cdf(math.floor(line), lam)))


def anytime_scorer_prob(lam: float) -> float:
    """P(player scores at least one goal) = 1 - e^(-lambda)."""
    return prob_over(0.5, lam)


# --- Opponent strength ------------------------------------------------------

# National-team strength ratings (~0–100, higher = stronger/harder to score on),
# grouped into FIFA-ranking tiers as of June 2026. These are coarse, transparent
# tiers — not a live feed — used only to nudge scoring projections by opponent.
# Teams absent here resolve to a neutral factor of 1.0. Covers the full 48-team
# 2026 field (ESPN soccer/fifa.world abbreviations).
NATIONAL_TEAM_RATING = {
    # Elite (title contenders / top defenses)
    "ARG": 92, "FRA": 92, "ESP": 91, "ENG": 90, "BRA": 90, "POR": 88,
    "NED": 87, "GER": 86, "BEL": 84, "CRO": 83,
    # Strong
    "URU": 80, "COL": 79, "MAR": 79, "JPN": 77, "USA": 76, "MEX": 76,
    "SUI": 75, "SEN": 75, "ECU": 74, "KOR": 73, "AUT": 73, "NOR": 73,
    # Mid
    "AUS": 68, "SWE": 68, "CIV": 67, "EGY": 67, "IRN": 67, "CAN": 66,
    "SCO": 66, "PAR": 65, "TUR": 65, "CZE": 65, "PAN": 63, "QAT": 62,
    # Developing / weaker defenses
    "GHA": 61, "ALG": 61, "TUN": 60, "RSA": 60, "NZL": 58, "UZB": 58,
    "JOR": 57, "IRQ": 57, "KSA": 57, "BIH": 60, "CPV": 55, "CUW": 53,
    "COD": 59, "HAI": 53,
}

# Mean of the rated field — the reference "average" defense. Computed once so the
# factor is self-consistent if the table is edited.
_MEAN_RATING = sum(NATIONAL_TEAM_RATING.values()) / len(NATIONAL_TEAM_RATING)

# How far the opponent adjustment is allowed to swing the projection.
_OPP_FACTOR_FLOOR = 0.72
_OPP_FACTOR_CEIL = 1.32


def opponent_defensive_factor(opponent_code: Optional[str]) -> float:
    """Multiplier applied to a player's scoring rate given the opponent.

    >1 against weaker-than-average defenses, <1 against stronger ones, 1.0 when
    the opponent is unknown/unrated. Clamped to a sane band so a mismatch can't
    explode a projection.
    """
    if not opponent_code:
        return 1.0
    rating = NATIONAL_TEAM_RATING.get(opponent_code.upper())
    if not rating:
        return 1.0
    factor = _MEAN_RATING / rating
    return max(_OPP_FACTOR_FLOOR, min(_OPP_FACTOR_CEIL, factor))


# --- Position priors --------------------------------------------------------

# Per-match prior expectation by position group and stat. Used to shrink the
# (tiny) empirical sample toward a sensible position baseline. Values are
# rough per-90 rates for a regular starter at that position.
POSITION_PRIORS = {
    "goals":           {"F": 0.38, "M": 0.12, "D": 0.04, "G": 0.0},
    "shots":           {"F": 2.30, "M": 1.40, "D": 0.70, "G": 0.02},
    "shots_on_target": {"F": 0.85, "M": 0.48, "D": 0.22, "G": 0.0},
    "assists":         {"F": 0.18, "M": 0.18, "D": 0.09, "G": 0.0},
    "cards":           {"F": 0.11, "M": 0.18, "D": 0.22, "G": 0.08},
}
_DEFAULT_PRIOR = {"goals": 0.12, "shots": 1.4, "shots_on_target": 0.45,
                  "assists": 0.12, "cards": 0.16}

# Map ESPN position abbreviations to the four prior groups. ESPN's World Cup
# rosters report G/D/M/F, but we normalize richer abbreviations too for safety.
_POSITION_GROUP = {
    "G": "G", "GK": "G",
    "D": "D", "DF": "D", "CB": "D", "LB": "D", "RB": "D", "LWB": "D", "RWB": "D", "SW": "D",
    "M": "M", "MF": "M", "CM": "M", "DM": "M", "CDM": "M", "CAM": "M", "AM": "M",
    "LM": "M", "RM": "M",
    "F": "F", "FW": "F", "ST": "F", "CF": "F", "SS": "F", "LW": "F", "RW": "F", "W": "F",
}

# Bayesian pseudo-count: the prior is worth this many matches of evidence. With
# ~5 real matches, the empirical rate still dominates but isn't trusted blindly.
_PRIOR_STRENGTH = 3.0


def position_group(position: Optional[str]) -> str:
    """Normalize a roster position to one of G/D/M/F (defaults to M)."""
    if not position:
        return "M"
    return _POSITION_GROUP.get(str(position).upper().strip(), "M")


def position_prior(stat: str, position: Optional[str]) -> float:
    """Per-match prior rate for a stat at a position group."""
    grp = position_group(position)
    table = POSITION_PRIORS.get(stat)
    if table is None:
        return _DEFAULT_PRIOR.get(stat, 0.2)
    return table.get(grp, _DEFAULT_PRIOR.get(stat, 0.2))


# --- Projection -------------------------------------------------------------


def shrunk_rate(empirical_rate: float, n_games: int, stat: str,
                position: Optional[str]) -> float:
    """Shrink an empirical per-match rate toward the position prior.

    rate = (n * empirical + k * prior) / (n + k), where k = _PRIOR_STRENGTH.
    With no games (n=0) this returns the pure prior; with many games it returns
    the empirical rate. This is what keeps a 5-game sample honest.
    """
    n = max(0, int(n_games))
    prior = position_prior(stat, position)
    return (n * empirical_rate + _PRIOR_STRENGTH * prior) / (n + _PRIOR_STRENGTH)


def project_lambda(stat: str, empirical_rate: float, n_games: int,
                   position: Optional[str], opponent_code: Optional[str]) -> float:
    """Expected per-match value (Poisson lambda) for a stat.

    Combines position-shrunk recent form with the opponent adjustment. Cards are
    not opponent-scaled (a booking depends on the player/referee, not how leaky
    the opponent's defense is).
    """
    base = shrunk_rate(empirical_rate, n_games, stat, position)
    if stat == "cards":
        return max(0.0, base)
    return max(0.0, base * opponent_defensive_factor(opponent_code))


def confidence_score(p_over: float, n_games: int) -> int:
    """0–100 confidence for a true over/under market (symmetric around a coin flip).

    Used for shots / shots on target / assists, where both sides are bettable at
    roughly even money. Calibrated to the app's shared recommendation thresholds
    (neutral < 55, trending 55–72, strong >= 72): a strong lean (P≈0.75) with a
    full sample scores ~74, a mild lean (P≈0.65) ~60, and a coin-flip ~37.
    """
    edge = abs(p_over - 0.5)                       # 0 .. 0.5
    edge_pts = edge * 150.0                         # up to 75
    sample_pts = min(12.0, max(0, int(n_games)) * 2.4)
    return int(round(max(0.0, min(100.0, 25.0 + edge_pts + sample_pts))))


# Minimum scoring/booking probability for a Yes/No pick to be worth surfacing.
# Below this the player simply isn't a credible scorer/booking candidate (e.g.
# most defenders for goals), so we drop the pick rather than show a dead "under".
YESNO_MIN_PROB = 0.18


def confidence_score_yes(p_yes: float, n_games: int) -> int:
    """0–100 confidence for a Yes/No market (anytime goalscorer, to be carded).

    Unlike a pick'em line, these are plus-money "does it happen at all" bets, so
    confidence must rise monotonically with the probability of Yes — a 55% scorer
    is a stronger play than a 30% one (the 30% one is *further* from a coin flip
    but it's the wrong direction). Maps P(yes) roughly: 0.20→39, 0.35→53,
    0.50→67, 0.65→82, so credible scorers/bookings sort sensibly against the
    other sports without a sub-coin-flip event ever reading as "strong".
    """
    base = 20.0 + max(0.0, min(1.0, p_yes)) * 95.0
    sample_pts = min(10.0, max(0, int(n_games)) * 2.0)
    return int(round(max(0.0, min(100.0, base + sample_pts))))


def project_prop(stat: str, line: float, empirical_rate: float, n_games: int,
                 position: Optional[str], opponent_code: Optional[str],
                 yes_no: bool = False) -> dict:
    """Full projection for one (player, stat, line).

    ``yes_no`` marks the synthetic Yes/No markets (anytime goalscorer at a 0.5
    goals line, to-receive-a-card at a 0.5 cards line). For those the only
    actionable side is "Yes" (i.e. Over), confidence rises with P(yes) — these
    are plus-money "does it happen" bets — and a pick is flagged not-actionable
    when the player isn't a credible scorer/booking candidate. True over/under
    markets (shots / SOT / assists) use the symmetric pick'em logic instead.

    Returns a dict with:
      lam         — projected per-match expected value (the projection)
      p_over      — P(stat > line) under Poisson(lam); for Yes/No, P(yes)
      edge        — lam - line (signed, like the US sports' projection edge)
      side        — "over" | "under"
      confidence  — 0–100
      actionable  — whether the pick is worth surfacing
    """
    lam = project_lambda(stat, empirical_rate, n_games, position, opponent_code)
    p = prob_over(line, lam)
    if yes_no:
        side = "over"
        confidence = confidence_score_yes(p, n_games)
        actionable = p >= YESNO_MIN_PROB
    else:
        side = "over" if p >= 0.5 else "under"
        confidence = confidence_score(p, n_games)
        actionable = True
    return {
        "lam": round(lam, 3),
        "p_over": round(p, 4),
        "edge": round(lam - line, 3),
        "side": side,
        "confidence": confidence,
        "actionable": actionable,
    }
