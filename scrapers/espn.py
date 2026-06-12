"""
Generic ESPN scrapers (injuries, rosters/positions, season stats).

ESPN exposes free, no-auth JSON endpoints for every league under a common
shape. The path is /sports/{sport}/{league}, e.g.:
    basketball/nba | basketball/wnba | basketball/mens-college-basketball
    football/nfl   | football/college-football
    baseball/mlb

This module is sport+league-parameterized. The *injury* and *teams* endpoints
share an identical JSON shape across all sports, so they work everywhere. The
*positions* and *season-stats* parsers below are basketball-specific (they read
basketball box-score fields) and are only called for basketball-backed sports
via scrapers/sources.py. Football/baseball get odds + injuries but no
projections — see config.SPORTS and scrapers/{mlb,nfl,ncaaf}.py.

Cost note: ESPN has no single league-wide game-log endpoint, so building season
stats means walking teams -> rosters -> per-athlete game logs. That's ~one
request per team plus one per player (≈200 calls for a 15-team WNBA slate). It's
only run on an explicit admin "refresh" and the result is cached, but it is
meaningfully slower than the NBA one-shot LeagueGameLog call.
"""

import datetime
from typing import Optional

import pandas as pd
import requests
from unidecode import unidecode

# Site API (teams, rosters, injuries) and Web API (per-athlete game logs) roots.
# The ESPN sport segment is derived from the league slug below.
_SITE_API_ROOT = "https://site.api.espn.com/apis/site/v2/sports"
_WEB_API_ROOT = "https://site.web.api.espn.com/apis/common/v3/sports"

# ESPN groups leagues under a sport segment. Map each league slug we use to its
# segment; unknown slugs default to basketball (preserves prior behavior).
ESPN_SPORT_BY_LEAGUE = {
    "nba": "basketball",
    "wnba": "basketball",
    "mens-college-basketball": "basketball",
    "womens-college-basketball": "basketball",
    "nfl": "football",
    "college-football": "football",
    "mlb": "baseball",
    # Soccer leagues use a dotted slug (e.g. "fifa.world" for the World Cup,
    # "eng.1" for the Premier League). Only the World Cup is wired today.
    "fifa.world": "soccer",
}


def _sport_for(league: str) -> str:
    return ESPN_SPORT_BY_LEAGUE.get(league, "basketball")


def _site_api(league: str) -> str:
    return f"{_SITE_API_ROOT}/{_sport_for(league)}/{league}"


def _web_api(league: str) -> str:
    return f"{_WEB_API_ROOT}/{_sport_for(league)}/{league}"


_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; squeezetheline/1.0)"}
_TIMEOUT = 15

# ESPN basketball position abbreviations -> the 5-position scheme the analysis/
# defense code uses. ESPN returns generic (G/F/C) or specific (PG/SG/SF/PF).
POSITION_MAP = {
    "PG": "PG", "SG": "SG", "SF": "SF", "PF": "PF", "C": "C",
    "G": "PG", "F": "SF", "G-F": "SG", "F-G": "SF", "F-C": "PF", "C-F": "C",
}

# Baseball/football don't feed a defense-vs-position model, so their positions
# are kept as ESPN's own abbreviations (display only). A blank/unknown position
# falls back to a sport-appropriate default so data.prepare_stats (which drops
# rows with a missing position) keeps every player who recorded stats.
_DEFAULT_POSITION = {"basketball": "SF", "baseball": "DH", "football": "ATH",
                     "soccer": "M"}


def _map_position(sport: str, abbr: str) -> str:
    """Normalize a roster position abbreviation for the given ESPN sport."""
    if sport == "basketball":
        return POSITION_MAP.get(abbr, "SF")
    # Soccer rosters report G/D/M/F (goalkeeper/defender/midfielder/forward),
    # which the soccer model keys off directly, so pass them through.
    return abbr or _DEFAULT_POSITION.get(sport, "ATH")

# Match injuries.py so statuses render consistently across sources.
STATUS_SHORT = {
    "Day-To-Day": "DTD",
    "Out": "OUT",
    "Doubtful": "DBT",
    "Questionable": "Q",
    "Probable": "PROB",
    "Active": "ACT",
    "Suspended": "SUS",
}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


# --- Injuries ---------------------------------------------------------------

def get_injuries(league: str) -> pd.DataFrame:
    """Fetch the injury report for an ESPN basketball league.

    Returns a DataFrame: name, team, status, status_short, comment, date.
    Same schema as scrapers.injuries.get_injury_report so callers are
    interchangeable.
    """
    url = f"{_site_api(league)}/injuries"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch ESPN injuries for {league}: {e}")
        return pd.DataFrame(columns=["name", "team", "status", "status_short", "comment", "date"])

    rows = []
    for team_block in data.get("injuries", []):
        team_name = team_block.get("displayName", "")
        for inj in team_block.get("injuries", []):
            athlete = inj.get("athlete") or {}
            name = athlete.get("displayName", "").strip()
            if not name:
                continue
            status = inj.get("status", "")
            rows.append({
                "name": unidecode(name),
                "team": team_name,
                "status": status,
                "status_short": STATUS_SHORT.get(status, status),
                "comment": inj.get("shortComment", ""),
                "date": inj.get("date", ""),
            })
    cols = ["name", "team", "status", "status_short", "comment", "date"]
    return pd.DataFrame(rows, columns=cols)


# --- Teams & rosters --------------------------------------------------------

def get_teams(league: str) -> list[dict]:
    """Return [{id, abbreviation, displayName}] for every team in the league.

    ESPN paginates the teams endpoint (default page size 50), which matters for
    college football's ~130+ FBS teams, so we request a large page size.
    """
    url = f"{_site_api(league)}/teams"
    try:
        resp = requests.get(url, headers=_HEADERS, params={"limit": 1000}, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch ESPN teams for {league}: {e}")
        return []
    teams = []
    for entry in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
        t = entry.get("team", {})
        if t.get("id"):
            teams.append({
                "id": t["id"],
                "abbreviation": t.get("abbreviation", ""),
                "displayName": t.get("displayName", ""),
            })
    return teams


def _athlete_url(league: str, athlete_id: str, slug: str) -> str:
    web_league = {"mens-college-basketball": "mens-college-basketball"}.get(league, league)
    if slug:
        return f"https://www.espn.com/{web_league}/player/_/id/{athlete_id}/{slug}"
    return f"https://www.espn.com/{web_league}/player/_/id/{athlete_id}"


def _iter_roster_athletes(athletes):
    """Yield individual athlete dicts from an ESPN roster payload.

    Basketball rosters are a flat list of athletes; baseball/football rosters
    are grouped by unit ("Pitchers"/"Infielders", or "offense"/"defense"), each
    group carrying an "items" list. This flattens both shapes.
    """
    for entry in athletes or []:
        if isinstance(entry, dict) and "items" in entry:
            for a in entry.get("items", []) or []:
                yield a
        elif isinstance(entry, dict):
            yield entry  # plain (flat-roster) athlete dict


def _norm(s: str) -> str:
    return unidecode(str(s or "")).lower().strip()


# Name suffixes that shouldn't affect identity matching.
_NAME_NOISE_TOKENS = {"jr", "sr", "ii", "iii", "iv", "de", "da", "do", "dos", "del"}


def name_tokens(s: str) -> frozenset:
    """Order-independent identity key for a player name.

    Returns the set of significant lowercased/unidecoded word tokens, dropping
    common noise (suffixes, particles). Two names match iff their token sets are
    equal — which reconciles the *order* differences between sources without any
    fuzzy matching: the Odds API's "Heung-Min Son" and ESPN's "Son Heung-Min"
    both reduce to {heung, min, son}. Exact-set equality keeps this safe (no
    accidental joins) while fixing the common reversed-name case that matters a
    lot for a World Cup of non-Western names.
    """
    raw = unidecode(str(s or "")).lower()
    toks = [t for t in "".join(c if c.isalnum() else " " for c in raw).split() if t]
    sig = [t for t in toks if t not in _NAME_NOISE_TOKENS]
    return frozenset(sig or toks)


def match_names_by_tokens(source_names, target_names) -> dict:
    """Map each source name to a target name with the same token set.

    Used to reconcile Odds-API player names to ESPN roster names for soccer.
    Returns {source_name: target_name} only for names that match exactly (by
    normalized string) or by token set; unmatched names are omitted so callers
    fall back gracefully (no projection rather than a wrong join).
    """
    target_by_norm = {_norm(t): t for t in target_names}
    target_by_tokens = {}
    for t in target_names:
        target_by_tokens.setdefault(name_tokens(t), t)
    out = {}
    for s in source_names:
        if _norm(s) in target_by_norm:
            out[s] = target_by_norm[_norm(s)]
            continue
        hit = target_by_tokens.get(name_tokens(s))
        if hit is not None:
            out[s] = hit
    return out


def _slate_teams(league: str, team_codes, session: requests.Session) -> list[dict]:
    """Resolve a set of slate team codes/names to ESPN team dicts.

    Matches each requested code against an ESPN team's abbreviation first, then
    its (normalized) displayName/name — so abbreviation-based sports (MLB/NFL)
    and name-passthrough sports (NCAAF) both resolve. When team_codes is falsy,
    returns every team (full-league walk, used by the small WNBA slate).
    """
    teams = get_teams(league)
    if not team_codes:
        return teams
    wanted = {_norm(c) for c in team_codes if c}
    out = []
    for t in teams:
        abbr = _norm(t.get("abbreviation"))
        disp = _norm(t.get("displayName"))
        # Exact abbreviation/name handles MLB/NFL; substring (either direction)
        # bridges NCAAF naming gaps like "Ohio State" vs "Ohio State Buckeyes".
        if (abbr in wanted or disp in wanted
                or any(w and (w in disp or disp in w) for w in wanted)):
            out.append(t)
    return out


def _roster_map(league: str, team_codes, session: requests.Session) -> dict:
    """Walk the slate teams' rosters → {normalized name: athlete meta}.

    Meta: {name, position, player_id, player_url, team-code}. One request per
    slate team (cheap); game-log requests are issued separately, only for the
    players that actually have prop lines.
    """
    sport = _sport_for(league)
    out = {}
    for team in _slate_teams(league, team_codes, session):
        url = f"{_site_api(league)}/teams/{team['id']}/roster"
        try:
            resp = session.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            athletes = resp.json().get("athletes", [])
        except Exception as e:
            print(f"  espn roster failed for {team.get('abbreviation')}: {type(e).__name__}")
            continue
        for a in _iter_roster_athletes(athletes):
            name = (a.get("displayName") or "").strip()
            aid = a.get("id")
            if not name or not aid:
                continue
            abbr = ((a.get("position") or {}).get("abbreviation")) or ""
            out[_norm(name)] = {
                "name": unidecode(name),
                "position": _map_position(sport, abbr),
                "player_id": aid,
                "player_url": _athlete_url(league, aid, a.get("slug", "")),
                "team-code": team["abbreviation"],
            }
    return out


def get_player_positions(league: str, team_codes=None,
                         session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Player positions for the slate teams (or every team when team_codes is None).

    Returns a DataFrame: name, position, player_id, player_url. Mirrors
    scrapers.nba.get_player_positions so data.prepare_stats and the headshot
    logic work unchanged across sports.
    """
    sess = session or _session()
    rmap = _roster_map(league, team_codes, sess)
    rows = [
        {"name": m["name"], "position": m["position"],
         "player_id": m["player_id"], "player_url": m["player_url"]}
        for m in rmap.values()
    ]
    return pd.DataFrame(rows, columns=["name", "position", "player_id", "player_url"])


# --- Season stats -----------------------------------------------------------

def _stat_value(stats: list, names: list, key: str, made_only: bool = False) -> float:
    """Read one stat from an event's parallel stats/names arrays.

    ESPN reports made-attempted fields like "3PM-3PA" as "2-5"; made_only
    pulls just the leading 'made' number.
    """
    try:
        idx = names.index(key)
        raw = stats[idx]
    except (ValueError, IndexError):
        return 0.0
    if raw in (None, "", "--", "-"):
        return 0.0
    if made_only and isinstance(raw, str) and "-" in raw:
        raw = raw.split("-")[0]
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _innings_to_outs(stats: list, names: list) -> float:
    """Convert a pitcher's innings-pitched (baseball '.1'/'.2' notation) to outs.

    IP 6.2 == 6 innings + 2 outs == 20 outs. ESPN reports it as the string "6.2".
    """
    try:
        idx = names.index("innings")
        raw = str(stats[idx])
    except (ValueError, IndexError):
        return 0.0
    if not raw or raw in ("--", "-"):
        return 0.0
    try:
        whole, _, frac = raw.partition(".")
        outs = int(whole) * 3
        if frac:
            outs += int(frac[0])
        return float(outs)
    except (TypeError, ValueError):
        return 0.0


def _parse_basketball(stats: list, names: list) -> dict:
    pts = _stat_value(stats, names, "points")
    reb = _stat_value(stats, names, "totalRebounds")
    ast = _stat_value(stats, names, "assists")
    return {
        "minutes": _stat_value(stats, names, "minutes"),
        "points": pts, "rebounds": reb, "assists": ast,
        "threes": _stat_value(
            stats, names,
            "threePointFieldGoalsMade-threePointFieldGoalsAttempted", made_only=True),
        "steals": _stat_value(stats, names, "steals"),
        "blocks": _stat_value(stats, names, "blocks"),
        "pra": pts + reb + ast,
    }


def _parse_baseball(stats: list, names: list) -> dict:
    is_pitcher = "innings" in names
    hits = _stat_value(stats, names, "hits")
    doubles = _stat_value(stats, names, "doubles")
    triples = _stat_value(stats, names, "triples")
    hr = _stat_value(stats, names, "homeRuns")
    # Total bases = singles + 2·doubles + 3·triples + 4·HR
    #            = hits + doubles + 2·triples + 3·HR
    total_bases = hits + doubles + 2 * triples + 3 * hr
    walks = _stat_value(stats, names, "walks")
    at_bats = _stat_value(stats, names, "atBats")
    outs = _innings_to_outs(stats, names)
    return {
        # Batting (0 for pitcher rows)
        "hits": 0.0 if is_pitcher else hits,
        "total_bases": 0.0 if is_pitcher else total_bases,
        "home_runs": 0.0 if is_pitcher else hr,
        "rbis": _stat_value(stats, names, "RBIs"),
        "runs": _stat_value(stats, names, "runs"),
        "stolen_bases": _stat_value(stats, names, "stolenBases"),
        # Pitching (0 for batter rows)
        "strikeouts_pitcher": _stat_value(stats, names, "strikeouts") if is_pitcher else 0.0,
        "hits_allowed": hits if is_pitcher else 0.0,
        "earned_runs": _stat_value(stats, names, "earnedRuns"),
        "outs": outs,
        # Participation proxy: outs recorded (pitchers) or plate appearances.
        "minutes": outs if is_pitcher else (at_bats + walks),
    }


def _parse_football(stats: list, names: list) -> dict:
    return {
        "pass_yards": _stat_value(stats, names, "passingYards"),
        "pass_tds": _stat_value(stats, names, "passingTouchdowns"),
        "completions": _stat_value(stats, names, "completions"),
        "pass_attempts": _stat_value(stats, names, "passingAttempts"),
        "interceptions": _stat_value(stats, names, "interceptions"),
        "rush_yards": _stat_value(stats, names, "rushingYards"),
        "rush_attempts": _stat_value(stats, names, "rushingAttempts"),
        "rush_tds": _stat_value(stats, names, "rushingTouchdowns"),
        "receptions": _stat_value(stats, names, "receptions"),
        "receiving_yards": _stat_value(stats, names, "receivingYards"),
        "receiving_tds": _stat_value(stats, names, "receivingTouchdowns"),
        # Football game logs carry no minutes; use 1.0 as a "played" flag so the
        # DNP filter and per-game rate math stay well-defined.
        "minutes": 1.0,
    }


def _soccer_minutes(appearances_raw) -> tuple[float, int]:
    """Turn the soccer gamelog 'appearances' cell into (minutes, started).

    ESPN's per-match appearances cell is a label, not a number: "Started" for a
    starter, "Sub" (or a substitution minute) for a substitute, and "--"/""/
    "DNP" when the player didn't feature. ESPN soccer game logs don't expose
    minutes played, so we estimate: a start ≈ 90 minutes, a substitute appearance
    ≈ 30, and a non-appearance 0. The estimate only drives the DNP filter and the
    per-minute rate column; the projection model works off per-match counts, not
    minutes, so the approximation is low-stakes. Returns (minutes, started_flag).
    """
    s = str(appearances_raw or "").strip().lower()
    if not s or s in ("--", "-", "dnp", "0"):
        return 0.0, 0
    if s.startswith("start"):
        return 90.0, 1
    # "sub", "subbed", or a numeric minute like "63'" → treat as a sub appearance.
    return 30.0, 0


def _parse_soccer(stats: list, names: list) -> dict:
    """Parse one soccer match's stat line (gamelog 'statistics' names/values).

    Expected names: appearances, totalGoals, goalAssists, totalShots,
    shotsOnTarget, foulsCommitted, foulsSuffered, offsides, yellowCards,
    redCards. 'cards' is the combined yellow+red count (the prop is "to receive
    a card" of either colour).
    """
    appearances = ""
    try:
        appearances = stats[names.index("appearances")]
    except (ValueError, IndexError):
        appearances = "Started" if stats else ""
    minutes, started = _soccer_minutes(appearances)
    yellow = _stat_value(stats, names, "yellowCards")
    red = _stat_value(stats, names, "redCards")
    return {
        "minutes": minutes,
        "started": float(started),
        "goals": _stat_value(stats, names, "totalGoals"),
        "assists": _stat_value(stats, names, "goalAssists"),
        "shots": _stat_value(stats, names, "totalShots"),
        "shots_on_target": _stat_value(stats, names, "shotsOnTarget"),
        "fouls": _stat_value(stats, names, "foulsCommitted"),
        "yellow_cards": yellow,
        "red_cards": red,
        "cards": yellow + red,
    }


_EVENT_PARSER = {
    "basketball": _parse_basketball,
    "baseball": _parse_baseball,
    "football": _parse_football,
    "soccer": _parse_soccer,
}

# Stat columns (besides the meta columns) each sport's parser emits — used to
# numeric-coerce and to know what the analysis engine can average.
_SPORT_STAT_COLUMNS = {
    "basketball": ["minutes", "points", "rebounds", "assists", "threes", "steals", "blocks", "pra"],
    "baseball": ["minutes", "hits", "total_bases", "home_runs", "rbis", "runs",
                 "stolen_bases", "strikeouts_pitcher", "hits_allowed", "earned_runs", "outs"],
    "football": ["minutes", "pass_yards", "pass_tds", "completions", "pass_attempts",
                 "interceptions", "rush_yards", "rush_attempts", "rush_tds",
                 "receptions", "receiving_yards", "receiving_tds"],
    "soccer": ["minutes", "started", "goals", "assists", "shots",
               "shots_on_target", "fouls", "yellow_cards", "red_cards", "cards"],
}


def get_soccer_gamelog(league: str, athlete_id: str,
                       session: Optional[requests.Session] = None) -> list[dict]:
    """Fetch one soccer player's recent match log via the athlete overview feed.

    Soccer has no league-wide game-log endpoint and the generic
    /athletes/{id}/gamelog 404s for soccer, so we read the player's "overview",
    whose ``gameLog`` node carries the most recent ~5 *club* matches with
    per-match counting stats (goals, shots, shots on target, assists, cards).
    Recent club form is the best public per-player rate signal for an
    international tournament, where national-team samples are tiny — this is a
    deliberate, documented approximation (the player's club, not country, is the
    opponent in these rows; the World Cup opponent adjustment is applied later by
    soccer_model).

    Returns rows with gameday, opponent, minutes and the soccer stat columns
    (see _SPORT_STAT_COLUMNS["soccer"]). Empty list on any failure.
    """
    sess = session or _session()
    url = f"{_web_api(league)}/athletes/{athlete_id}/overview"
    try:
        resp = sess.get(url, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    gamelog = data.get("gameLog") or {}
    stat_blocks = gamelog.get("statistics") or []
    if not stat_blocks:
        return []
    block = stat_blocks[0]
    names = block.get("names") or []
    if not names:
        return []
    meta = gamelog.get("events") or {}

    out = []
    for ev in block.get("events", []) or []:
        eid = ev.get("eventId")
        stats = ev.get("stats")
        if not eid or not stats:
            continue
        m = meta.get(eid, {})
        row = {
            "gameday": m.get("gameDate", ""),
            "opponent": (m.get("opponent") or {}).get("abbreviation", ""),
        }
        row.update(_parse_soccer(stats, names))
        out.append(row)
    return out


def get_player_gamelog(league: str, athlete_id: str, season: int,
                       session: Optional[requests.Session] = None) -> list[dict]:
    """Fetch one athlete's game log for a season, parsed into sport stat rows.

    Each row has gameday, opponent, minutes, and the sport's stat columns
    (see _SPORT_STAT_COLUMNS). Player name/team are attached by the caller.
    """
    sess = session or _session()
    if _sport_for(league) == "soccer":
        # Soccer reads the overview feed (recent matches), not a season gamelog.
        return get_soccer_gamelog(league, athlete_id, session=sess)
    parse = _EVENT_PARSER.get(_sport_for(league), _parse_basketball)
    url = f"{_web_api(league)}/athletes/{athlete_id}/gamelog"
    try:
        resp = sess.get(url, params={"season": season}, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    names = data.get("names", []) or []
    if not names:
        return []
    events_meta = data.get("events", {}) or {}
    out = []
    for season_type in data.get("seasonTypes", []) or []:
        for category in season_type.get("categories", []) or []:
            for ev in category.get("events", []) or []:
                eid = ev.get("eventId")
                stats = ev.get("stats", [])
                if not eid or not stats:
                    continue
                meta = events_meta.get(eid, {})
                row = {
                    "gameday": meta.get("gameDate", ""),
                    "opponent": (meta.get("opponent") or {}).get("abbreviation", ""),
                }
                row.update(parse(stats, names))
                out.append(row)
    return out


def _finalize_stats(rows: list, sport: str) -> pd.DataFrame:
    """Coerce a list of game-row dicts into the canonical stats DataFrame."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["gameday"] = pd.to_datetime(df["gameday"], errors="coerce", utc=True).dt.tz_localize(None)
    df = df[df["gameday"].notna()]
    for col in _SPORT_STAT_COLUMNS.get(sport, []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def get_slate_stats(league: str, team_codes=None, player_names=None,
                    season: Optional[int] = None) -> pd.DataFrame:
    """Per-game stats for the players on a slate, via ESPN.

    Walks only the slate teams' rosters, then fetches game logs only for players
    in `player_names` (the slate's prop players) — keeping the request count
    proportional to the slate, not the whole league. Pass team_codes=None to
    walk every team (small leagues like WNBA).

    Columns: name, team-code, opponent, gameday + the sport's stat columns.
    """
    if season is None:
        season = datetime.date.today().year
    sess = _session()
    sport = _sport_for(league)
    rmap = _roster_map(league, team_codes, sess)

    # Soccer: select roster players by token-set identity as well as exact name,
    # so reversed/non-Western prop names ("Heung-Min Son") still resolve to the
    # roster ("Son Heung-Min"). Rows are labeled with the ESPN roster name; the
    # caller reconciles prop names to those via match_names_by_tokens.
    if sport == "soccer" and player_names:
        wanted_norm = {_norm(n) for n in player_names}
        wanted_tokens = {name_tokens(n) for n in player_names}

        def _is_wanted(meta) -> bool:
            return (_norm(meta["name"]) in wanted_norm
                    or name_tokens(meta["name"]) in wanted_tokens)
    else:
        wanted = {_norm(n) for n in player_names} if player_names else None

        def _is_wanted(meta) -> bool:
            return wanted is None or _norm(meta["name"]) in wanted

    rows = []
    for key, meta in rmap.items():
        if not _is_wanted(meta):
            continue
        for g in get_player_gamelog(league, meta["player_id"], season, session=sess):
            rows.append({"name": meta["name"], "team-code": meta["team-code"], **g})
    return _finalize_stats(rows, sport)


def get_season_stats(league: str, season: Optional[int] = None) -> pd.DataFrame:
    """Full-league per-game stats via ESPN (every roster). Used by small slates
    (WNBA). For larger leagues prefer get_slate_stats to bound request cost.
    """
    return get_slate_stats(league, team_codes=None, player_names=None, season=season)
