"""Unit tests for soccer_model — the World Cup Poisson projection engine.

Covers the Poisson primitives, the anytime-goalscorer probability, the opponent
defensive factor, position-aware shrinkage (the "forwards vs defenders" lever),
and the end-to-end project_prop output.
"""
import math

import pytest

import soccer_model as sm


# --- Poisson primitives -----------------------------------------------------

def test_poisson_pmf_matches_closed_form():
    # P(X=0) = e^-lam ; P(X=2; lam=2) = e^-2 * 2^2/2!
    assert sm.poisson_pmf(0, 1.0) == pytest.approx(math.exp(-1.0))
    assert sm.poisson_pmf(2, 2.0) == pytest.approx(math.exp(-2.0) * 4 / 2)


def test_poisson_pmf_edge_cases():
    assert sm.poisson_pmf(0, 0.0) == 1.0
    assert sm.poisson_pmf(3, 0.0) == 0.0
    assert sm.poisson_pmf(-1, 1.0) == 0.0


def test_poisson_cdf_is_cumulative():
    lam = 1.7
    assert sm.poisson_cdf(0, lam) == pytest.approx(sm.poisson_pmf(0, lam))
    expected = sum(sm.poisson_pmf(i, lam) for i in range(3))
    assert sm.poisson_cdf(2, lam) == pytest.approx(expected)


def test_prob_over_half_lines():
    lam = 1.0
    # Over 0.5 == P(X>=1) == 1 - e^-lam
    assert sm.prob_over(0.5, lam) == pytest.approx(1 - math.exp(-lam))
    # Over 1.5 == P(X>=2) == 1 - P(0) - P(1)
    expected = 1 - sm.poisson_pmf(0, lam) - sm.poisson_pmf(1, lam)
    assert sm.prob_over(1.5, lam) == pytest.approx(expected)


def test_prob_over_is_monotonic_in_lambda():
    # A higher expected rate should never lower the over probability.
    probs = [sm.prob_over(1.5, lam) for lam in (0.5, 1.0, 2.0, 3.0)]
    assert probs == sorted(probs)
    assert all(0.0 <= p <= 1.0 for p in probs)


def test_prob_over_zero_lambda():
    assert sm.prob_over(0.5, 0.0) == 0.0


def test_anytime_scorer_prob():
    # Classic: lambda 0.7 -> ~50.3% to score at least once.
    assert sm.anytime_scorer_prob(0.7) == pytest.approx(1 - math.exp(-0.7))
    assert sm.anytime_scorer_prob(0.0) == 0.0


# --- Opponent strength ------------------------------------------------------

def test_opponent_factor_direction():
    strong = sm.opponent_defensive_factor("BRA")   # elite defense
    weak = sm.opponent_defensive_factor("KSA")      # weaker defense
    assert strong < 1.0 < weak


def test_opponent_factor_unknown_is_neutral():
    assert sm.opponent_defensive_factor("ZZZ") == 1.0
    assert sm.opponent_defensive_factor(None) == 1.0
    assert sm.opponent_defensive_factor("") == 1.0


def test_opponent_factor_is_clamped():
    for code in sm.NATIONAL_TEAM_RATING:
        f = sm.opponent_defensive_factor(code)
        assert sm._OPP_FACTOR_FLOOR <= f <= sm._OPP_FACTOR_CEIL


def test_opponent_factor_case_insensitive():
    assert sm.opponent_defensive_factor("bra") == sm.opponent_defensive_factor("BRA")


# --- Position handling ------------------------------------------------------

def test_position_group_normalization():
    assert sm.position_group("F") == "F"
    assert sm.position_group("ST") == "F"
    assert sm.position_group("CB") == "D"
    assert sm.position_group("CDM") == "M"
    assert sm.position_group("GK") == "G"
    assert sm.position_group(None) == "M"     # default
    assert sm.position_group("???") == "M"    # unknown -> default


def test_position_prior_orders_goals_by_position():
    # Forwards should carry the highest goal prior, keepers ~0.
    f = sm.position_prior("goals", "F")
    m = sm.position_prior("goals", "M")
    d = sm.position_prior("goals", "D")
    g = sm.position_prior("goals", "G")
    assert f > m > d > g
    assert g == 0.0


# --- Shrinkage --------------------------------------------------------------

def test_shrink_pulls_toward_prior_with_small_sample():
    # Same empirical rate, but a forward's prior is much higher than a defender's,
    # so the shrunk rate for a forward stays higher. This is the core
    # "forwards vs defenders" behavior on tiny samples.
    fwd = sm.shrunk_rate(0.8, 5, "goals", "F")
    dfd = sm.shrunk_rate(0.8, 5, "goals", "D")
    assert fwd > dfd


def test_shrink_with_zero_games_returns_prior():
    assert sm.shrunk_rate(99.0, 0, "goals", "F") == pytest.approx(
        sm.position_prior("goals", "F")
    )


def test_shrink_with_large_sample_approaches_empirical():
    val = sm.shrunk_rate(2.0, 500, "shots", "F")
    assert val == pytest.approx(2.0, abs=0.05)


# --- project_lambda / project_prop ------------------------------------------

def test_cards_not_opponent_scaled():
    # Cards depend on the player/referee, not the opponent's leakiness.
    a = sm.project_lambda("cards", 0.3, 5, "D", "BRA")
    b = sm.project_lambda("cards", 0.3, 5, "D", "KSA")
    assert a == b


def test_goals_are_opponent_scaled():
    strong = sm.project_lambda("goals", 0.6, 5, "F", "BRA")
    weak = sm.project_lambda("goals", 0.6, 5, "F", "KSA")
    assert weak > strong


def test_project_prop_shape_and_side():
    out = sm.project_prop("goals", 0.5, 0.9, 6, "F", "KSA")
    assert set(out) >= {"lam", "p_over", "edge", "side", "confidence"}
    assert 0.0 <= out["p_over"] <= 1.0
    assert 0 <= out["confidence"] <= 100
    assert out["edge"] == pytest.approx(out["lam"] - 0.5, abs=1e-9)
    assert out["side"] == ("over" if out["p_over"] >= 0.5 else "under")


def test_confidence_thresholds_align_with_app_buckets():
    # Strong lean (P~0.75) should clear the "strong" cutoff (>=72); a coin flip
    # should land in "neutral" (<55).
    strong = sm.confidence_score(0.75, 6)
    flip = sm.confidence_score(0.50, 5)
    assert strong >= 72
    assert flip < 55


def test_keeper_goal_projection_is_zero():
    out = sm.project_prop("goals", 0.5, 0.0, 5, "G", "KSA")
    assert out["lam"] == 0.0
    assert out["p_over"] == 0.0


# --- Yes/No markets (anytime goalscorer / to be carded) ---------------------

def test_yesno_confidence_is_monotonic_in_prob():
    # A more likely scorer is a stronger Yes bet (unlike the symmetric pick'em
    # confidence, which would rate a 30% event highly for being far from 0.5).
    cs = [sm.confidence_score_yes(p, 5) for p in (0.2, 0.35, 0.5, 0.65)]
    assert cs == sorted(cs)
    assert all(0 <= c <= 100 for c in cs)


def test_yesno_pick_always_over_side():
    # Even a sub-50% scorer is bet on the Yes/Over side, not "under".
    out = sm.project_prop("goals", 0.5, 0.4, 5, "F", "KSA", yes_no=True)
    assert out["side"] == "over"


def test_yesno_actionable_gate():
    # Credible scorer (forward) -> actionable; non-scorer (defender ~0 goals) -> not.
    striker = sm.project_prop("goals", 0.5, 0.7, 5, "F", "KSA", yes_no=True)
    defender = sm.project_prop("goals", 0.5, 0.0, 5, "D", "BRA", yes_no=True)
    assert striker["actionable"] is True
    assert defender["actionable"] is False
    assert defender["p_over"] < sm.YESNO_MIN_PROB


def test_over_under_market_always_actionable():
    # Shots/SOT/assists keep both sides and are always surfaced.
    out = sm.project_prop("shots", 2.5, 0.5, 5, "D", "BRA", yes_no=False)
    assert out["actionable"] is True
    assert out["side"] in ("over", "under")
