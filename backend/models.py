"""Pydantic models describing the API response shapes.

These define the contract between the FastAPI backend and the Next.js
frontend. They are intentionally decoupled from the legacy Streamlit
data layer so that real scrapers can be swapped in behind the
``DataProvider`` interface without changing the wire format.
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class Sport(BaseModel):
    key: str            # e.g. "baseball_mlb" (matches the odds-API style key)
    name: str           # e.g. "MLB"
    full_name: str      # e.g. "Major League Baseball"
    icon: str           # emoji used by the frontend tabs
    season: str         # e.g. "in-season" / "offseason"
    active: bool        # whether a slate is available today
    stat_types: List[str]


class Recommendation(BaseModel):
    """A categorised verdict the frontend uses to bucket picks."""
    bucket: str         # strong_over | trending_over | strong_under | trending_under | neutral
    label: str          # human readable, e.g. "Strong Over"
    side: str           # over | under | none


class Pick(BaseModel):
    id: str
    player: str
    team: str
    team_abbr: str
    opponent: str
    opponent_abbr: str
    game_time: str            # ISO-ish local time string for today's game
    stat_type: str            # e.g. "Hits", "Points", "Passing Yards"
    line: float               # the posted prop line
    projection: float         # model projection
    edge: float               # projection - line
    confidence: int           # 0-100
    hit_rate_l10: float       # share of last 10 games clearing the line (0-1)
    hit_rate_season: float    # season-long share clearing the line (0-1)
    trend: str                # "up" | "down" | "flat"
    trend_pct: float          # recent form delta vs season, as a percentage
    recommendation: Recommendation


class GameLog(BaseModel):
    date: str
    opponent_abbr: str
    home: bool
    stat_type: str
    value: float
    line: float
    hit: bool                 # value >= line


class StatHitRate(BaseModel):
    stat_type: str
    line: float
    hit_rate_l10: float
    hit_rate_season: float
    avg_l10: float
    avg_season: float


class PlayerDetail(BaseModel):
    name: str
    team: str
    team_abbr: str
    position: str
    sport: str
    headshot: Optional[str] = None
    season_averages: dict           # stat_type -> average
    primary_stat: str
    last_games: List[GameLog]
    hit_rates: List[StatHitRate]


class Injury(BaseModel):
    player: str
    team: str
    team_abbr: str
    position: str
    status: str               # Out | Doubtful | Questionable | Day-To-Day | IL-10 ...
    injury: str               # description, e.g. "Hamstring"
    updated: str              # date string
    note: str


class Slate(BaseModel):
    sport: str
    date: str
    generated_at: str
    picks: List[Pick]


# --- User-specific models (require auth) ------------------------------------


class SavePickRequest(BaseModel):
    """Payload the frontend sends to track a pick. Mirrors the fields shown on
    a pick card so ``My Picks`` can render without re-fetching the slate."""
    pick_id: str
    player: str
    team_abbr: str
    opponent_abbr: str
    sport: str
    stat_type: str
    line: float
    projection: float
    edge: float
    confidence: int
    side: str                      # over | under | none
    recommendation: str            # human label, e.g. "Strong Over"
    game_time: Optional[str] = None


class SavedPick(BaseModel):
    """A tracked pick, optionally joined with its settled result."""
    id: str
    pick_id: str
    player: str
    team_abbr: str
    opponent_abbr: str
    sport: str
    stat_type: str
    line: float
    projection: float
    edge: float
    confidence: int
    side: str
    recommendation: str
    game_time: Optional[str] = None
    saved_at: str
    result: str = "pending"        # pending | win | loss | push
    actual_value: Optional[float] = None
    settled_at: Optional[str] = None


class HistoryStats(BaseModel):
    """Aggregate performance over a user's settled picks."""
    total: int
    wins: int
    losses: int
    pushes: int
    pending: int
    win_rate: float                # wins / (wins + losses), 0-1


class PickHistory(BaseModel):
    stats: HistoryStats
    picks: List[SavedPick]


class AlertRequest(BaseModel):
    """Set up a line-movement alert for a pick."""
    pick_id: str
    player: str
    stat_type: str
    sport: str
    direction: str                 # over | under
    threshold: float               # notify when the line crosses this value
    note: Optional[str] = None


class Alert(BaseModel):
    id: str
    pick_id: str
    player: str
    stat_type: str
    sport: str
    direction: str
    threshold: float
    note: Optional[str] = None
    active: bool = True
    created_at: str
