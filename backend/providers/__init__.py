"""Data providers for Squeeze the Line.

The API depends only on the :class:`DataProvider` interface. The current
implementation (:class:`MockProvider`) serves realistic sample data. When
API keys / live scrapers are available, a ``LiveProvider`` can implement
the same interface (wrapping the legacy ``scrapers/`` and ``analysis.py``
logic) and be swapped in via :func:`get_provider`.
"""
from __future__ import annotations

from .base import DataProvider
from .mock import MockProvider

_provider: DataProvider = MockProvider()


def get_provider() -> DataProvider:
    return _provider
