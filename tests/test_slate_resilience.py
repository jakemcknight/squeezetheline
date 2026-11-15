"""A slow or dead upstream must never hang /api/slate.

Exercises LiveProvider's deadline + graceful-degradation path with the scraping
pipeline stubbed out (no network): a hung pipeline yields a fast empty
"pending" slate, a failing one a fast empty "unavailable" slate, and concurrent
requests share one upstream run instead of stacking blocked threads.
"""
import os
import sys
import threading
import time

import pytest

_BACKEND = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "backend")
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

live = pytest.importorskip("providers.live")
from scrapers import odds_api  # noqa: E402

SPORT = "basketball_nba"


def _provider(monkeypatch, pipeline, deadline=0.5):
    p = live.LiveProvider(deadline_seconds=deadline)
    monkeypatch.setattr(p, "_run_pipeline", pipeline)
    return p


def test_hung_upstream_returns_fast_pending_slate(monkeypatch):
    release = threading.Event()
    calls = []

    def hung_pipeline(sport_key, game_date):
        calls.append(sport_key)
        release.wait(30)  # simulates a socket stuck on a dead upstream
        raise RuntimeError("upstream never answered")

    p = _provider(monkeypatch, hung_pipeline)
    try:
        start = time.monotonic()
        slate = p.get_slate(SPORT)
        elapsed = time.monotonic() - start

        assert elapsed < 2.0, f"get_slate blocked for {elapsed:.1f}s"
        assert slate.picks == []
        assert slate.status == "pending"
        assert slate.message

        # A retry while the first run is still stuck must not start a second one.
        assert p.get_slate(SPORT).status == "pending"
        assert len(calls) == 1
    finally:
        release.set()


def test_failing_upstream_returns_fast_unavailable_slate(monkeypatch):
    def dead_pipeline(sport_key, game_date):
        raise odds_api.OddsAPIQuotaError("401 — credits exhausted")

    p = _provider(monkeypatch, dead_pipeline)
    start = time.monotonic()
    slate = p.get_slate(SPORT)
    assert time.monotonic() - start < 2.0
    assert slate.picks == []
    assert slate.status == "unavailable"


def test_stale_cache_served_when_refresh_hangs(monkeypatch):
    release = threading.Event()
    payload = {"results": {}, "summaries": {}, "game_times": {},
               "todays_games": {}, "generated_at": "2026-01-01T00:00:00"}
    state = {"n": 0}

    def pipeline(sport_key, game_date):
        state["n"] += 1
        if state["n"] == 1:
            return payload
        release.wait(30)
        raise RuntimeError("refresh hung")

    p = _provider(monkeypatch, pipeline)
    try:
        assert p.get_slate(SPORT).status == "ok"
        # Expire the cache entry so the next call triggers a (hanging) refresh.
        for k, (_, v) in list(p._cache.items()):
            p._cache[k] = (time.monotonic() - live._CACHE_TTL_SECONDS - 1, v)
        start = time.monotonic()
        slate = p.get_slate(SPORT)
        assert time.monotonic() - start < 2.0
        assert slate.status == "ok"
        assert slate.generated_at == payload["generated_at"]
    finally:
        release.set()


def test_odds_api_calls_set_a_timeout(monkeypatch):
    seen = []

    class _Resp:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            pass

        def json(self):
            return [] if not seen or "events/" not in seen[-1][0] else {"bookmakers": []}

    def fake_get(url, *args, **kwargs):
        seen.append((url, kwargs.get("timeout")))
        return _Resp()

    monkeypatch.setattr(odds_api.requests, "get", fake_get)
    monkeypatch.setattr(odds_api, "get_odds_api_key", lambda: "test")
    odds_api.get_events(SPORT)
    odds_api.get_event_props("evt", sport_key=SPORT)
    odds_api.get_event_alt_props("evt", "Someone", "player_points_alternate", sport_key=SPORT)

    assert len(seen) == 3
    for url, timeout in seen:
        assert timeout is not None, f"no timeout on {url}"
