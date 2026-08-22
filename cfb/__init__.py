"""Standalone college football (FBS) game-level betting board.

`python -m cfb` prints this week's board — model spread / total / win
probability vs the market for every FBS game — and writes CSV + markdown
copies under cfb/out/.

Self-contained: only `requests` + stdlib. Data sources:
  * CollegeFootballData.com (CFBD) — schedule, preseason SP+ ratings,
    returning production, results. Free API key required (CFBD_API_KEY).
  * The Odds API — market spreads/totals/moneylines (repo's ODDS_API_KEY).

See model.py for the power-rating math and odds.py for the edge layer.
"""
