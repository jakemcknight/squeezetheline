"""NRFI/YRFI daily board — standalone game-level vertical for Squeeze the Line.

Estimates, for each MLB game today, the probability that at least one run
scores in the 1st inning (YRFI) using free MLB Stats API data only.

Run:  python -m nrfi        (from the repo root)

This module is intentionally self-contained (requests + stdlib) and separate
from the player-prop projection engine. v1 outputs model probabilities only;
a sportsbook-odds/edge comparison layer can be added later on top of the
`GameBoardRow` output without touching the model.
"""
