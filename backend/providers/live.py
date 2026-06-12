"""Live data provider.

Bridges the FastAPI backend to the legacy scraping/analysis pipeline that
lives at the repo root (``config.py``, ``analysis.py``, ``data.py`` and the
``scrapers/`` package). It re-implements the ``fetch_fresh_data`` flow from
``app.py`` (minus the Streamlit UI plumbing) and reshapes the resulting
pandas frames into the Pydantic models the frontend consumes.

Selection happens in :mod:`providers` — :class:`LiveProvider` is only chosen
when ``ODDS_API_KEY`` is set and the root modules import cleanly; otherwise the
backend falls back to :class:`providers.mock.MockProvider`.

The root modules expect to be importable as top-level packages (``config``,
``scrapers`` …), so we add the repo root to ``sys.path`` at import time. The
backend's own ``models``/``providers`` packages still take precedence because
the backend directory is already first on the path.
"""
from __future__ import annotations

import datetime
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# --- Make the repo-root modules importable (config, analysis, data, scrapers).
# providers/live.py -> providers -> backend -> repo root.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.append(_REPO_ROOT)

# Legacy pipeline imports (resolved from the repo root).
from config import (  # noqa: E402
    active_sports,
    player_photo_url,
    stat_configs_for,
    stat_label_map,
    stat_keys_for,
    team_name_to_code,
    TEAM_NAME_TO_CODE_BY_SPORT,
)
from data import prepare_stats, prepare_props  # noqa: E402
from analysis import analyze_stat, build_player_summaries  # noqa: E402
import soccer_model  # noqa: E402  (soccer Poisson projection model)
from scrapers.odds_api import (  # noqa: E402
    get_all_props,
    get_events_for_date,
    get_game_times,
    get_todays_games,
)
from scrapers.sources import (  # noqa: E402
    get_season_stats,
    get_positions,
    get_defense_by_position,
    get_injury_report,
)
from scrapers.espn import match_names_by_tokens  # noqa: E402

from models import (  # noqa: E402  (backend's own models)
    GameLog,
    Injury,
    Pick,
    PlayerDetail,
    Recommendation,
    Slate,
    Sport,
    StatHitRate,
)
from providers.base import DataProvider  # noqa: E402

# --- Static per-sport presentation metadata (config doesn't carry these). ----
_SPORT_META = {
    "basketball_nba": ("National Basketball Association", "\U0001f3c0", "in-season"),
    "basketball_wnba": ("Women's National Basketball Association", "\U0001f3c0", "in-season"),
    "basketball_ncaab": ("NCAA Basketball", "\U0001f3c0", "offseason"),
    "baseball_mlb": ("Major League Baseball", "⚾", "in-season"),
    "americanfootball_nfl": ("National Football League", "\U0001f3c8", "offseason"),
    "americanfootball_ncaaf": ("NCAA Football", "\U0001f393", "offseason"),
    "soccer_fifa_world_cup": ("FIFA World Cup 2026", "⚽", "in-season"),
}

# Sports that use the bespoke soccer projection model (Poisson + opponent +
# position shrinkage) instead of the linear last-5/last-10/season blend.
_SOCCER_KEY = "soccer_fifa_world_cup"

# Reverse team-code -> full-name maps, derived from config's name->code maps,
# so picks can show a readable team/opponent name. Sports with no map (NCAAF)
# fall through to the bare code.
_CODE_TO_NAME_BY_SPORT: Dict[str, Dict[str, str]] = {
    key: {code: name for name, code in mapping.items()}
    for key, mapping in TEAM_NAME_TO_CODE_BY_SPORT.items()
}

_CACHE_TTL_SECONDS = 300


def _isnum(v) -> bool:
    return v is not None and not pd.isna(v)


def _f(v, default: float = 0.0) -> float:
    return float(v) if _isnum(v) else default


class LiveProvider(DataProvider):
    """Scraper-backed implementation of :class:`DataProvider`."""

    def __init__(self) -> None:
        # (sport_key, iso_date) -> (fetched_at_monotonic, payload)
        self._cache: Dict[Tuple[str, str], Tuple[float, dict]] = {}

    # ------------------------------------------------------------------ sports
    def list_sports(self) -> List[Sport]:
        sports: List[Sport] = []
        for name, cfg in active_sports().items():
            key = cfg["key"]
            full_name, icon, season = _SPORT_META.get(key, (name, "\U0001f3c5", "in-season"))
            sports.append(Sport(
                key=key,
                name=name,
                full_name=full_name,
                icon=icon,
                season=season,
                active=bool(cfg.get("active")),
                stat_types=list(cfg.get("stat_categories", [])),
            ))
        return sports

    # ----------------------------------------------------------------- helpers
    def _sport_name(self, sport_key: str) -> str:
        for name, cfg in active_sports().items():
            if cfg["key"] == sport_key:
                return name
        return sport_key

    def _team_name(self, sport_key: str, code: str) -> str:
        if not code:
            return ""
        return _CODE_TO_NAME_BY_SPORT.get(sport_key, {}).get(code, code)

    @staticmethod
    def _parse_date(date: Optional[str]) -> datetime.date:
        if date:
            try:
                return datetime.date.fromisoformat(date)
            except ValueError:
                pass
        return datetime.date.today()

    @staticmethod
    def _format_game_time(commence: Optional[str]) -> str:
        if not commence:
            return ""
        try:
            from zoneinfo import ZoneInfo
            dt = datetime.datetime.fromisoformat(str(commence).replace("Z", "+00:00"))
            dt = dt.astimezone(ZoneInfo("America/New_York"))
            hour = dt.hour % 12 or 12
            meridiem = "AM" if dt.hour < 12 else "PM"
            return f"{hour}:{dt.minute:02d} {meridiem} ET"
        except Exception:
            return ""

    # ------------------------------------------------------------------ fetch
    def _fetch(self, sport_key: str, game_date: datetime.date) -> dict:
        """Run the full pipeline for a sport/date, with a short-lived cache.

        Returns a payload dict: ``results`` (stat_key -> analysis DataFrame),
        ``summaries`` (player name -> summary dict), ``game_times`` (team code
        -> ISO commence time) and ``generated_at``.
        """
        cache_key = (sport_key, game_date.isoformat())
        hit = self._cache.get(cache_key)
        if hit and (time.monotonic() - hit[0]) < _CACHE_TTL_SECONDS:
            return hit[1]

        payload = self._run_pipeline(sport_key, game_date)
        self._cache[cache_key] = (time.monotonic(), payload)
        return payload

    def _run_pipeline(self, sport_key: str, game_date: datetime.date) -> dict:
        """Replicate app.fetch_fresh_data without the Streamlit UI layer."""
        todays_games = get_todays_games(game_date, sport_key)
        slate_teams = list(todays_games.keys())

        raw_props = get_all_props(game_date, all_books=False, sport_key=sport_key)
        prop_players = (
            sorted(raw_props["player"].dropna().unique().tolist())
            if not raw_props.empty and "player" in raw_props.columns else []
        )

        stats = get_season_stats(sport_key, team_codes=slate_teams, player_names=prop_players)
        positions = get_positions(sport_key, team_codes=slate_teams)
        df = prepare_stats(stats, positions)
        has_stats = not df.empty

        # Soccer: the Odds API and ESPN order/spell names differently (e.g.
        # "Heung-Min Son" vs "Son Heung-Min"). Reconcile the prop player names to
        # the ESPN roster names (which the stats + positions use) by token-set
        # identity so the props join to projections. Unmatched names pass through
        # and simply get no projection — graceful, never a wrong join.
        if sport_key == _SOCCER_KEY and not raw_props.empty and not df.empty:
            alias = match_names_by_tokens(
                raw_props["player"].dropna().unique().tolist(),
                df["name"].dropna().unique().tolist(),
            )
            if alias:
                raw_props = raw_props.copy()
                raw_props["player"] = raw_props["player"].map(lambda n: alias.get(n, n))

        props = prepare_props(raw_props.copy())
        defense = get_defense_by_position(sport_key)
        game_times = get_game_times(game_date, sport_key)

        player_id_map: Dict[str, object] = {}
        if not positions.empty and {"name", "player_id"}.issubset(positions.columns):
            meta = positions[["name", "player_id"]].drop_duplicates(subset="name")
            player_id_map = dict(zip(meta["name"], meta["player_id"]))

        injuries = get_injury_report(sport_key)

        results: Dict[str, pd.DataFrame] = {}
        summaries: Dict[str, dict] = {}

        if has_stats and not props.empty:
            for stat, prop_type, _label in stat_configs_for(sport_key):
                results[stat] = analyze_stat(
                    stat, prop_type, df, props, todays_games, defense,
                    game_date=game_date, sport_key=sport_key,
                )

            all_players = sorted(set(props["name"].dropna().unique()))
            summaries = build_player_summaries(
                all_players, df, props, todays_games=todays_games, sport_key=sport_key,
            )
            for name, summary in summaries.items():
                pid = player_id_map.get(name)
                if pid is not None and _isnum(pid):
                    summary["player_id"] = int(pid)

            if not injuries.empty:
                inj_lookup = injuries.drop_duplicates("name").set_index("name").to_dict("index")
                for name, summary in summaries.items():
                    if name in inj_lookup:
                        row = inj_lookup[name]
                        summary["injury"] = {
                            "status": row.get("status", ""),
                            "status_short": row.get("status_short", ""),
                            "comment": row.get("comment", ""),
                        }

        return {
            "results": results,
            "summaries": summaries,
            "game_times": game_times,
            "todays_games": todays_games,
            "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        }

    # ------------------------------------------------------------------- slate
    def get_slate(self, sport: str, date: Optional[str] = None) -> Slate:
        game_date = self._parse_date(date)
        day = game_date.isoformat()
        try:
            payload = self._fetch(sport, game_date)
        except Exception as exc:  # network / quota / parse failures degrade gracefully
            print(f"[LiveProvider] get_slate({sport}) failed: {exc!r}")
            return Slate(sport=sport, date=day, generated_at=f"{day}T00:00:00", picks=[])

        results = payload["results"]
        game_times = payload["game_times"]
        label_map = stat_label_map(sport)

        # Build one candidate pick per (player, stat); keep the highest-confidence
        # stat per player so the board shows a varied, best-edge slate.
        best: Dict[str, Pick] = {}
        for stat_key, df in results.items():
            if df is None or df.empty:
                continue
            label = label_map.get(stat_key, stat_key)
            for _, row in df.iterrows():
                pick = self._row_to_pick(sport, stat_key, label, row, game_times)
                if pick is None:
                    continue
                existing = best.get(pick.player)
                if existing is None or pick.confidence > existing.confidence:
                    best[pick.player] = pick

        picks = sorted(best.values(), key=lambda p: p.confidence, reverse=True)
        return Slate(
            sport=sport,
            date=day,
            generated_at=payload["generated_at"],
            picks=picks,
        )

    def _row_to_pick(self, sport_key: str, stat_key: str, label: str,
                     row: pd.Series, game_times: dict) -> Optional[Pick]:
        if sport_key == _SOCCER_KEY:
            return self._soccer_row_to_pick(sport_key, stat_key, label, row, game_times)

        line = row.get("spread")
        if not _isnum(line):
            return None
        line = float(line)

        # Projection: recent-form-weighted blend of available windows.
        l5 = row.get(f"{stat_key}_5g")
        l10 = row.get(f"{stat_key}_10g")
        season = row.get(stat_key)
        parts = [(l5, 0.5), (l10, 0.3), (season, 0.2)]
        num = sum(_f(v) * w for v, w in parts if _isnum(v))
        den = sum(w for v, w in parts if _isnum(v))
        projection = round(num / den, 2) if den else line
        edge = round(projection - line, 2)
        side = "over" if edge >= 0 else "under"

        confidence = int(round(_f(row.get("confidence"), 0.0)))
        confidence = max(0, min(100, confidence))

        # Hit rates: l10 from the last-10 sample vs tonight's line; season from
        # the analysis hit% (already a percentage).
        last10 = row.get("last10")
        hr_l10 = None
        if isinstance(last10, (list, tuple)) and last10:
            cleared = sum(1 for v in last10 if _isnum(v) and float(v) >= line)
            hr_l10 = round(cleared / len(last10), 2)
        hit_pct = row.get("hit%")
        hr_season = round(_f(hit_pct) / 100.0, 2) if _isnum(hit_pct) else None
        if hr_l10 is None:
            hr_l10 = hr_season if hr_season is not None else 0.5
        if hr_season is None:
            hr_season = hr_l10

        trend_arrow = str(row.get("trend", ""))
        trend = "up" if trend_arrow == "↑" else ("down" if trend_arrow == "↓" else "flat")
        trend_pct = 0.0
        if _isnum(l5) and _isnum(l10) and float(l10) != 0:
            trend_pct = round((float(l5) - float(l10)) / float(l10) * 100, 1)

        team_code = str(row.get("team-code", "") or "")
        opp_code = str(row.get("opponent", "") or "")
        name = str(row.get("name", ""))

        return Pick(
            id=f"{sport_key}:{name}".replace(" ", "_"),
            player=name,
            team=self._team_name(sport_key, team_code),
            team_abbr=team_code,
            opponent=self._team_name(sport_key, opp_code),
            opponent_abbr=opp_code,
            game_time=self._format_game_time(game_times.get(team_code)),
            stat_type=label,
            line=line,
            projection=projection,
            edge=edge,
            confidence=confidence,
            hit_rate_l10=hr_l10,
            hit_rate_season=hr_season,
            trend=trend,
            trend_pct=trend_pct,
            recommendation=self._recommend(side, confidence, edge),
        )

    def _soccer_row_to_pick(self, sport_key: str, stat_key: str, label: str,
                            row: pd.Series, game_times: dict) -> Optional[Pick]:
        """Build a soccer pick via the Poisson model (soccer_model.py).

        Unlike the linear-blend path, the projection is an opponent-adjusted,
        position-shrunk per-match expectation, and the season hit rate we report
        is the model's P(over the line) (e.g. the anytime-goalscorer probability
        for the synthetic 0.5 goals line), not a raw historical frequency.
        """
        line = row.get("spread")
        if not _isnum(line):
            return None
        line = float(line)

        # Empirical per-match rate: prefer recent form (last 5), fall back to the
        # full fetched sample. last10 holds the player's recent club-match values.
        last10 = row.get("last10")
        sample = [float(v) for v in last10 if _isnum(v)] if isinstance(last10, (list, tuple)) else []
        n_games = len(sample)
        l5 = row.get(f"{stat_key}_5g")
        season = row.get(stat_key)
        empirical_rate = _f(l5) if _isnum(l5) else _f(season)

        position = str(row.get("position", "") or "")
        opponent = str(row.get("opponent", "") or "")

        # goals / cards are Yes/No markets rewritten to an Over-0.5 line. Treat
        # them as plus-money "does it happen" bets (always the Yes/Over side,
        # confidence rising with the probability) and drop players who aren't
        # credible scorers/booking candidates, rather than showing a dead "under".
        is_yesno = stat_key in ("goals", "cards")
        proj = soccer_model.project_prop(
            stat=stat_key, line=line, empirical_rate=empirical_rate,
            n_games=n_games, position=position, opponent_code=opponent,
            yes_no=is_yesno,
        )
        if not proj["actionable"]:
            return None

        side = proj["side"]
        projection = round(proj["lam"], 2)
        edge = round(proj["edge"], 2)
        confidence = max(0, min(100, int(proj["confidence"])))
        p_over = float(proj["p_over"])

        # Recent empirical clearance of the line (share of club matches over it).
        hr_l10 = round(sum(1 for v in sample if v > line) / n_games, 2) if n_games else round(p_over, 2)
        hr_season = round(p_over, 2)  # the model's projected probability

        trend_arrow = str(row.get("trend", ""))
        trend = "up" if trend_arrow == "↑" else ("down" if trend_arrow == "↓" else "flat")
        trend_pct = 0.0
        if _isnum(l5) and _isnum(row.get(f"{stat_key}_10g")) and float(row.get(f"{stat_key}_10g")) != 0:
            trend_pct = round((float(l5) - float(row.get(f"{stat_key}_10g"))) / float(row.get(f"{stat_key}_10g")) * 100, 1)

        team_code = str(row.get("team-code", "") or "")
        name = str(row.get("name", ""))

        return Pick(
            id=f"{sport_key}:{name}".replace(" ", "_"),
            player=name,
            team=self._team_name(sport_key, team_code),
            team_abbr=team_code,
            opponent=self._team_name(sport_key, opponent),
            opponent_abbr=opponent,
            game_time=self._format_game_time(game_times.get(team_code)),
            stat_type=label,
            line=line,
            projection=projection,
            edge=edge,
            confidence=confidence,
            hit_rate_l10=hr_l10,
            hit_rate_season=hr_season,
            trend=trend,
            trend_pct=trend_pct,
            recommendation=self._recommend(side, confidence, edge),
        )

    @staticmethod
    def _recommend(side: str, confidence: int, edge: float) -> Recommendation:
        if abs(edge) < 1e-9 or confidence < 55:
            return Recommendation(
                bucket="neutral",
                label="Lean " + ("Over" if side == "over" else "Under"),
                side=side if confidence >= 50 else "none",
            )
        strength = "strong" if confidence >= 72 else "trending"
        label = ("Strong " if strength == "strong" else "Trending ") + ("Over" if side == "over" else "Under")
        return Recommendation(bucket=f"{strength}_{side}", label=label, side=side)

    # ------------------------------------------------------------------ player
    def get_player(self, name: str, sport: str) -> Optional[PlayerDetail]:
        game_date = self._parse_date(None)
        try:
            payload = self._fetch(sport, game_date)
        except Exception as exc:
            print(f"[LiveProvider] get_player({name}, {sport}) failed: {exc!r}")
            return None

        summaries = payload["summaries"]
        summary = summaries.get(name)
        if summary is None:  # case-insensitive fallback
            for pname, psum in summaries.items():
                if pname.lower() == name.lower():
                    name, summary = pname, psum
                    break
        if summary is None:
            return None

        label_map = stat_label_map(sport)
        stat_keys = stat_keys_for(sport)
        season_avg = summary.get("season_avg", {}) or {}
        today_lines = summary.get("today_lines", {}) or {}
        last_20 = summary.get("last_20", []) or []

        season_averages = {
            label_map.get(k, k): round(_f(season_avg.get(k)), 1)
            for k in stat_keys
            if _f(season_avg.get(k)) > 0
        }

        # Primary stat: first stat with a posted line, else first with an average.
        primary_key = next((k for k in stat_keys if k in today_lines), None)
        if primary_key is None:
            primary_key = next((k for k in stat_keys if _f(season_avg.get(k)) > 0), stat_keys[0] if stat_keys else "")
        primary_label = label_map.get(primary_key, primary_key)

        # Game log for the primary stat (most-recent-first, up to 10).
        primary_line = today_lines.get(primary_key)
        if not _isnum(primary_line):
            primary_line = round(_f(season_avg.get(primary_key)))
        primary_line = float(primary_line)
        last_games: List[GameLog] = []
        for g in last_20[:10]:
            value = _f(g.get(primary_key))
            last_games.append(GameLog(
                date=str(g.get("date", "")),
                opponent_abbr=str(g.get("opponent", "") or ""),
                home=False,
                stat_type=primary_label,
                value=value,
                line=primary_line,
                hit=value >= primary_line,
            ))

        # Per-stat hit rates (stats with a posted line or a non-zero average).
        hit_rates: List[StatHitRate] = []
        for k in stat_keys:
            line = today_lines.get(k)
            avg_season = _f(season_avg.get(k))
            if not _isnum(line) and avg_season <= 0:
                continue
            stat_line = float(line) if _isnum(line) else float(round(avg_season))
            values = [_f(g.get(k)) for g in last_20]
            l10_vals = values[:10]
            avg_l10 = round(sum(l10_vals) / len(l10_vals), 1) if l10_vals else round(avg_season, 1)
            hr_l10 = round(sum(1 for v in l10_vals if v >= stat_line) / len(l10_vals), 2) if l10_vals else 0.0
            hr_season = round(sum(1 for v in values if v >= stat_line) / len(values), 2) if values else hr_l10
            hit_rates.append(StatHitRate(
                stat_type=label_map.get(k, k),
                line=stat_line,
                hit_rate_l10=hr_l10,
                hit_rate_season=hr_season,
                avg_l10=avg_l10,
                avg_season=round(avg_season, 1),
            ))

        team_code = str(summary.get("team", "") or "")
        pid = summary.get("player_id")
        headshot = player_photo_url(pid, sport) if pid else None

        return PlayerDetail(
            name=name,
            team=self._team_name(sport, team_code),
            team_abbr=team_code,
            position=str(summary.get("position", "") or ""),
            sport=sport,
            headshot=headshot or None,
            season_averages=season_averages,
            primary_stat=primary_label,
            last_games=last_games,
            hit_rates=hit_rates,
        )

    # ---------------------------------------------------------------- injuries
    def get_injuries(self, sport: str) -> List[Injury]:
        try:
            df = get_injury_report(sport)
        except Exception as exc:
            print(f"[LiveProvider] get_injuries({sport}) failed: {exc!r}")
            return []
        if df is None or df.empty:
            return []

        out: List[Injury] = []
        for _, row in df.iterrows():
            team_name = str(row.get("team", "") or "")
            team_abbr = team_name_to_code(team_name, sport) if team_name else ""
            comment = str(row.get("comment", "") or "")
            status = str(row.get("status", "") or row.get("status_short", "") or "")
            out.append(Injury(
                player=str(row.get("name", "")),
                team=team_name,
                team_abbr=team_abbr if team_abbr != team_name else "",
                position="",
                status=status,
                injury="",
                updated=str(row.get("date", "") or ""),
                note=comment or (f"Listed as {status}." if status else ""),
            ))
        return out
