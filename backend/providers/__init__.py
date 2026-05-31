"""Data providers for Squeeze the Line.

The API depends only on the :class:`DataProvider` interface. Two implementations
exist:

* :class:`providers.live.LiveProvider` — wraps the legacy ``scrapers/`` +
  ``analysis.py`` pipeline at the repo root and serves real, scraped data.
* :class:`providers.mock.MockProvider` — deterministic sample data.

:func:`get_provider` prefers the live provider when ``ODDS_API_KEY`` is set and
the root modules import cleanly; otherwise it falls back to the mock provider so
the backend always starts and serves *something*.
"""
from __future__ import annotations

import os

from .base import DataProvider
from .mock import MockProvider


def _build_provider() -> DataProvider:
    if not os.environ.get("ODDS_API_KEY"):
        print("[providers] ODDS_API_KEY not set — using MockProvider.")
        return MockProvider()
    try:
        from .live import LiveProvider
        print("[providers] ODDS_API_KEY found — using LiveProvider.")
        return LiveProvider()
    except Exception as exc:  # missing deps, import errors, etc.
        print(f"[providers] LiveProvider unavailable ({exc!r}); falling back to MockProvider.")
        return MockProvider()


_provider: DataProvider = _build_provider()


def get_provider() -> DataProvider:
    return _provider
