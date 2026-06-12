"""Mock data provider.

Generates realistic, *deterministic* sample data from the static rosters
in :mod:`providers.rosters`. Determinism (seeded by player name + stat +
date) keeps the slate and the player-detail endpoints consistent, so a
pick on the dashboard matches its player page.
"""
from __future__ import annotations

import hashlib
import math
from datetime import date as date_cls, datetime, timedelta
from typing import Dict, List, Optional

from models import (
    GameLog,
    Injury,
    PlayerDetail,
    Pick,
    Recommendation,
    Slate,
    StatHitRate,
    Sport,
)
from providers.base import DataProvider
from providers.rosters import INJURIES, ROSTERS, SPORTS

# A small pool of plausible opponents per sport, keyed by sport.
_OPPONENTS = {
    "baseball_mlb": [("Boston Red Sox", "BOS"), ("Chicago Cubs", "CHC"), ("Atlanta Braves", "ATL"),
                     ("San Diego Padres", "SD"), ("Philadelphia Phillies", "PHI"), ("Seattle Mariners", "SEA"),
                     ("Minnesota Twins", "MIN"), ("Arizona Diamondbacks", "ARI")],
    "basketball_wnba": [("Connecticut Sun", "CONN"), ("Seattle Storm", "SEA"), ("Atlanta Dream", "ATL"),
                        ("Washington Mystics", "WAS"), ("Chicago Sky", "CHI"), ("Indiana Fever", "IND"),
                        ("Phoenix Mercury", "PHX"), ("Las Vegas Aces", "LV")],
    "americanfootball_nfl": [("Buffalo Bills", "BUF"), ("Dallas Cowboys", "DAL"), ("Green Bay Packers", "GB"),
                             ("Seattle Seahawks", "SEA"), ("Baltimore Ravens", "BAL"), ("Detroit Lions", "DET"),
                             ("Houston Texans", "HOU"), ("Los Angeles Rams", "LAR")],
    "americanfootball_ncaaf": [("Georgia Bulldogs", "UGA"), ("Texas Longhorns", "TEX"), ("Oregon Ducks", "ORE"),
                               ("Notre Dame Fighting Irish", "ND"), ("Michigan Wolverines", "MICH"),
                               ("Tennessee Volunteers", "TENN"), ("Ole Miss Rebels", "MISS"), ("Georgia Tech", "GT")],
    "basketball_ncaab": [("Houston Cougars", "HOU"), ("Tennessee Volunteers", "TENN"), ("Iowa State Cyclones", "ISU"),
                         ("Marquette Golden Eagles", "MARQ"), ("Arizona Wildcats", "ARIZ"),
                         ("Connecticut Huskies", "UCONN"), ("Gonzaga Bulldogs", "GONZ"), ("Baylor Bears", "BAY")],
    "soccer_fifa_world_cup": [("Mexico", "MEX"), ("Croatia", "CRO"), ("Morocco", "MAR"),
                              ("Japan", "JPN"), ("Senegal", "SEN"), ("Saudi Arabia", "KSA"),
                              ("South Korea", "KOR"), ("Ghana", "GHA"), ("Uruguay", "URU"),
                              ("Switzerland", "SUI")],
}


def _rand(*parts: str) -> float:
    """Deterministic float in [0, 1) derived from the given key parts."""
    key = "|".join(str(p) for p in parts)
    digest = hashlib.md5(key.encode()).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _round_line(avg: float, stat: str) -> float:
    """Round a baseline average into a plausible posted prop line."""
    integer_stats = {"Passing Yards", "Rushing Yards", "Receiving Yards", "Points",
                     "Rebounds", "Assists", "Pts+Reb+Ast"}
    if stat in integer_stats and avg >= 10:
        return float(round(avg * 2) / 2)  # to nearest 0.5 for big numbers reads odd; keep .5
    # small-count stats -> .5 lines (classic over/under)
    base = round(avg - 0.5) + 0.5
    if base < 0.5:
        base = 0.5
    return float(base)


def _decimals(stat: str) -> int:
    big = {"Passing Yards", "Rushing Yards", "Receiving Yards"}
    if stat in big:
        return 1
    return 1


class MockProvider(DataProvider):
    def _today(self, date: Optional[str]) -> str:
        if date:
            return date
        return date_cls(2026, 5, 31).isoformat()

    def list_sports(self) -> List[Sport]:
        return [Sport(**s) for s in SPORTS]

    def _opponent(self, sport: str, player_name: str, day: str):
        pool = _OPPONENTS.get(sport, [("Opponent", "OPP")])
        idx = int(_rand(sport, player_name, day, "opp") * len(pool))
        return pool[idx % len(pool)]

    def _game_time(self, sport: str, player_name: str, day: str) -> str:
        slots = ["1:05 PM ET", "4:10 PM ET", "7:05 PM ET", "7:10 PM ET",
                 "8:00 PM ET", "9:40 PM ET", "10:00 PM ET"]
        idx = int(_rand(sport, player_name, day, "time") * len(slots))
        return slots[idx % len(slots)]

    def _feature_stat(self, player: dict, day: str) -> str:
        """Pick which stat to feature for this player on this slate.

        Varies across the board so the dashboard shows a realistic mix of
        stat types rather than the same stat for everyone.
        """
        candidates = [s for s, v in player["stats"].items() if v > 0]
        if not candidates:
            return next(iter(player["stats"]))
        idx = int(_rand(player["name"], day, "feature") * len(candidates))
        return candidates[idx % len(candidates)]

    def _make_pick(self, sport: str, player: dict, day: str) -> Pick:
        primary = self._feature_stat(player, day)
        avg = player["stats"][primary]
        line = _round_line(avg, primary)

        # projection: model nudges the line up or down based on a seeded factor.
        factor = 0.78 + _rand(sport, player["name"], primary, day, "proj") * 0.50  # 0.78 .. 1.28
        projection = round(avg * factor, _decimals(primary))
        edge = round(projection - line, _decimals(primary))

        # recent form vs season -> trend
        trend_delta = (_rand(sport, player["name"], day, "trend") - 0.45) * 0.5  # -0.225 .. 0.275
        trend_pct = round(trend_delta * 100, 1)
        trend = "up" if trend_pct > 4 else ("down" if trend_pct < -4 else "flat")

        # hit rates: anchored to edge direction with seeded noise.
        edge_norm = max(-1.0, min(1.0, edge / max(line, 1.0)))
        base_hr = 0.5 + edge_norm * 0.30
        hit_rate_l10 = round(min(0.95, max(0.10, base_hr + (_rand(sport, player["name"], day, "hr10") - 0.5) * 0.25)), 2)
        hit_rate_season = round(min(0.92, max(0.15, base_hr + (_rand(sport, player["name"], "hrs") - 0.5) * 0.18)), 2)

        # confidence blends edge magnitude, hit rate and trend agreement.
        side = "over" if edge >= 0 else "under"
        directional_hr = hit_rate_l10 if side == "over" else (1 - hit_rate_l10)
        conf = 40 + abs(edge_norm) * 90 + (directional_hr - 0.5) * 60
        if (trend == "up" and side == "over") or (trend == "down" and side == "under"):
            conf += 6
        confidence = int(max(35, min(97, round(conf))))

        rec = self._recommend(side, confidence, edge)
        team_name = player["team"]
        opp_name, opp_abbr = self._opponent(sport, player["name"], day)

        return Pick(
            id=f"{sport}:{player['name']}".replace(" ", "_"),
            player=player["name"],
            team=team_name,
            team_abbr=player["team_abbr"],
            opponent=opp_name,
            opponent_abbr=opp_abbr,
            game_time=self._game_time(sport, player["name"], day),
            stat_type=primary,
            line=line,
            projection=projection,
            edge=edge,
            confidence=confidence,
            hit_rate_l10=hit_rate_l10,
            hit_rate_season=hit_rate_season,
            trend=trend,
            trend_pct=trend_pct,
            recommendation=rec,
        )

    def _recommend(self, side: str, confidence: int, edge: float) -> Recommendation:
        if abs(edge) < 1e-9 or confidence < 55:
            return Recommendation(bucket="neutral", label="Lean " + ("Over" if side == "over" else "Under"),
                                  side=side if confidence >= 50 else "none")
        strength = "strong" if confidence >= 72 else "trending"
        label = ("Strong " if strength == "strong" else "Trending ") + ("Over" if side == "over" else "Under")
        return Recommendation(bucket=f"{strength}_{side}", label=label, side=side)

    def get_slate(self, sport: str, date: Optional[str] = None) -> Slate:
        day = self._today(date)
        roster = ROSTERS.get(sport, [])
        # pick ~8 players for the slate, seeded by the date for a little rotation.
        scored = sorted(roster, key=lambda p: _rand(sport, p["name"], day, "slate"))
        chosen = scored[:8] if len(scored) >= 8 else scored
        picks = [self._make_pick(sport, p, day) for p in chosen]
        # sort by confidence desc for a sensible default ordering
        picks.sort(key=lambda pk: pk.confidence, reverse=True)
        return Slate(
            sport=sport,
            date=day,
            generated_at=f"{day}T11:30:00",
            picks=picks,
        )

    def _find_player(self, name: str, sport: str) -> Optional[dict]:
        for p in ROSTERS.get(sport, []):
            if p["name"].lower() == name.lower():
                return p
        # also allow lookup across all sports if sport mismatch
        for sp, roster in ROSTERS.items():
            for p in roster:
                if p["name"].lower() == name.lower():
                    return {**p, "_sport": sp}
        return None

    def get_player(self, name: str, sport: str) -> Optional[PlayerDetail]:
        player = self._find_player(name, sport)
        if not player:
            return None
        sport = player.get("_sport", sport)
        day = self._today(None)
        primary = next(iter(player["stats"]))

        # last 10 games for the primary stat
        last_games = self._game_logs(sport, player, primary, day, count=10)

        # per-stat hit rates
        hit_rates: List[StatHitRate] = []
        for stat, avg in player["stats"].items():
            if avg <= 0:
                continue
            line = _round_line(avg, stat)
            logs = self._game_logs(sport, player, stat, day, count=10)
            season_logs = self._game_logs(sport, player, stat, day, count=30)
            hr10 = round(sum(1 for g in logs if g.hit) / len(logs), 2)
            hrs = round(sum(1 for g in season_logs if g.hit) / len(season_logs), 2)
            avg_l10 = round(sum(g.value for g in logs) / len(logs), _decimals(stat))
            avg_season = round(sum(g.value for g in season_logs) / len(season_logs), _decimals(stat))
            hit_rates.append(StatHitRate(
                stat_type=stat, line=line, hit_rate_l10=hr10, hit_rate_season=hrs,
                avg_l10=avg_l10, avg_season=avg_season,
            ))

        season_averages = {k: round(v, _decimals(k)) for k, v in player["stats"].items() if v > 0}

        return PlayerDetail(
            name=player["name"],
            team=player["team"],
            team_abbr=player["team_abbr"],
            position=player["position"],
            sport=sport,
            season_averages=season_averages,
            primary_stat=primary,
            last_games=last_games,
            hit_rates=hit_rates,
        )

    def _game_logs(self, sport: str, player: dict, stat: str, day: str, count: int) -> List[GameLog]:
        avg = player["stats"][stat]
        line = _round_line(avg, stat)
        pool = _OPPONENTS.get(sport, [("Opponent", "OPP")])
        base_day = datetime.fromisoformat(self._today(None))
        logs: List[GameLog] = []
        # game cadence: daily-ish for baseball/basketball, weekly for football
        step = 7 if "football" in sport else 2
        for i in range(count):
            seed = (sport, player["name"], stat, str(i))
            r = _rand(*seed)
            # value distributed around the average with realistic spread
            spread = 0.55 if stat not in {"Home Runs", "Stolen Bases", "Passing TDs", "3-Pointers Made"} else 0.9
            value = avg * (1 + (r - 0.5) * 2 * spread)
            if stat in {"Home Runs", "RBIs", "Runs", "Stolen Bases", "Receptions", "Passing TDs",
                        "3-Pointers Made", "Hits", "Total Bases", "Points", "Rebounds", "Assists", "Pts+Reb+Ast"}:
                value = max(0, round(value))
            else:
                value = max(0.0, round(value, 0))
            opp = pool[int(_rand(*seed, "opp") * len(pool)) % len(pool)]
            gdate = (base_day - timedelta(days=step * (i + 1))).date().isoformat()
            logs.append(GameLog(
                date=gdate,
                opponent_abbr=opp[1],
                home=_rand(*seed, "home") > 0.5,
                stat_type=stat,
                value=float(value),
                line=line,
                hit=value >= line,
            ))
        return logs

    def get_injuries(self, sport: str) -> List[Injury]:
        day = self._today(None)
        out: List[Injury] = []
        team_lookup = {p["team_abbr"]: p["team"] for p in ROSTERS.get(sport, [])}
        for i, (name, abbr, pos, status, desc) in enumerate(INJURIES.get(sport, [])):
            # updated date: within the last week
            offset = int(_rand(sport, name, "upd") * 6)
            updated = (datetime.fromisoformat(day) - timedelta(days=offset)).date().isoformat()
            note = self._injury_note(status, desc)
            out.append(Injury(
                player=name,
                team=team_lookup.get(abbr, abbr),
                team_abbr=abbr,
                position=pos,
                status=status,
                injury=desc,
                updated=updated,
                note=note,
            ))
        return out

    def _injury_note(self, status: str, desc: str) -> str:
        s = status.lower()
        if "out" in s or "il" in s:
            return f"Ruled out — {desc.lower()}. No timetable for return."
        if "doubtful" in s:
            return f"Unlikely to play; {desc.lower()} limiting availability."
        if "questionable" in s:
            return f"Game-time decision with a {desc.lower()}."
        if "probable" in s:
            return f"Expected to play through {desc.lower()}."
        return f"Monitoring {desc.lower()}; listed as {status}."
