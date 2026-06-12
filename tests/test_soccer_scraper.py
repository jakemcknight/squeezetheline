"""Unit tests for the ESPN soccer scraper parsers (no network).

Covers the per-match stat parser, the appearances→minutes estimate, and the
gameLog→rows assembly in espn.get_soccer_gamelog, driven by a committed fixture
that mirrors the real ESPN soccer /overview shape.
"""
import json
import os

import pytest

from scrapers import espn

_FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "soccer_overview.json")

# The exact stat-name order ESPN returns for soccer game logs.
_NAMES = ["appearances", "totalGoals", "goalAssists", "totalShots", "shotsOnTarget",
          "foulsCommitted", "foulsSuffered", "offsides", "yellowCards", "redCards"]


def _load_fixture():
    with open(_FIXTURE) as f:
        return json.load(f)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    """Stands in for a requests.Session; returns the fixture for any GET."""
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        return _FakeResponse(self._payload)


# --- _parse_soccer ----------------------------------------------------------

def test_parse_soccer_reads_counts_by_name():
    stats = ["Started", "2", "1", "6", "3", "1", "4", "0", "1", "0"]
    row = espn._parse_soccer(stats, _NAMES)
    assert row["goals"] == 2.0
    assert row["assists"] == 1.0
    assert row["shots"] == 6.0
    assert row["shots_on_target"] == 3.0
    assert row["fouls"] == 1.0
    assert row["yellow_cards"] == 1.0
    assert row["red_cards"] == 0.0
    # cards is the combined yellow+red total (the prop is either colour).
    assert row["cards"] == 1.0


def test_parse_soccer_cards_combine_yellow_and_red():
    stats = ["Started", "0", "0", "1", "0", "2", "0", "0", "1", "1"]
    row = espn._parse_soccer(stats, _NAMES)
    assert row["cards"] == 2.0  # 1 yellow + 1 red


def test_soccer_minutes_from_appearances():
    assert espn._soccer_minutes("Started") == (90.0, 1)
    assert espn._soccer_minutes("Sub") == (30.0, 0)
    assert espn._soccer_minutes("--") == (0.0, 0)
    assert espn._soccer_minutes("") == (0.0, 0)
    assert espn._soccer_minutes("DNP") == (0.0, 0)


def test_parse_soccer_started_flag_and_minutes():
    started = espn._parse_soccer(["Started", "0", "0", "0", "0", "0", "0", "0", "0", "0"], _NAMES)
    sub = espn._parse_soccer(["Sub", "0", "0", "0", "0", "0", "0", "0", "0", "0"], _NAMES)
    assert started["minutes"] == 90.0 and started["started"] == 1.0
    assert sub["minutes"] == 30.0 and sub["started"] == 0.0


# --- get_soccer_gamelog (fixture-driven) ------------------------------------

def test_get_soccer_gamelog_assembles_rows():
    fixture = _load_fixture()
    sess = _FakeSession(fixture)
    rows = espn.get_soccer_gamelog("fifa.world", "277206", session=sess)

    # Four matches in the fixture (including a DNP "--" row).
    assert len(rows) == 4
    # Each row carries the meta + the soccer stat columns.
    first = rows[0]
    assert first["opponent"] == "BAR"
    assert first["gameday"].startswith("2026-05-13")
    assert first["goals"] == 2.0
    assert first["shots"] == 6.0
    assert first["shots_on_target"] == 3.0
    # The session was actually used (no real network).
    assert sess.calls and "athletes/277206/overview" in sess.calls[0]


def test_get_soccer_gamelog_opponents_and_goals_align():
    rows = espn.get_soccer_gamelog("fifa.world", "277206", session=_FakeSession(_load_fixture()))
    by_opp = {r["opponent"]: r for r in rows}
    assert by_opp["BAR"]["goals"] == 2.0
    assert by_opp["RMA"]["goals"] == 0.0 and by_opp["RMA"]["assists"] == 1.0
    assert by_opp["SEV"]["goals"] == 1.0 and by_opp["SEV"]["minutes"] == 30.0  # sub
    assert by_opp["VAL"]["minutes"] == 0.0  # DNP


def test_get_soccer_gamelog_handles_empty_payload():
    assert espn.get_soccer_gamelog("fifa.world", "1", session=_FakeSession({})) == []
    assert espn.get_soccer_gamelog(
        "fifa.world", "1", session=_FakeSession({"gameLog": {"statistics": []}})
    ) == []


def test_soccer_stat_columns_registered():
    cols = espn._SPORT_STAT_COLUMNS["soccer"]
    for c in ("minutes", "goals", "assists", "shots", "shots_on_target", "cards"):
        assert c in cols


def test_fifa_world_maps_to_soccer_sport():
    assert espn._sport_for("fifa.world") == "soccer"


# --- token-set name reconciliation (Odds API <-> ESPN) ----------------------

def test_name_tokens_order_independent():
    # The crux: a reversed name reduces to the same token set.
    assert espn.name_tokens("Heung-Min Son") == espn.name_tokens("Son Heung-Min")
    assert espn.name_tokens("Kylian Mbappé") == espn.name_tokens("Mbappe Kylian")


def test_name_tokens_drops_suffix_noise():
    assert espn.name_tokens("Vinícius Júnior") == espn.name_tokens("Vinicius Junior Jr")


def test_name_tokens_distinguishes_different_players():
    assert espn.name_tokens("Kangin Lee") != espn.name_tokens("Jae-Sung Lee")


def test_match_names_by_tokens_resolves_reordered_names():
    odds = ["Heung-Min Son", "Patrik Schick", "No Scorer"]
    espn_names = ["Son Heung-Min", "Patrik Schick", "Kim Min-Jae"]
    out = espn.match_names_by_tokens(odds, espn_names)
    assert out["Heung-Min Son"] == "Son Heung-Min"   # reordered -> matched
    assert out["Patrik Schick"] == "Patrik Schick"    # exact -> matched
    assert "No Scorer" not in out                      # novelty line -> unmatched
