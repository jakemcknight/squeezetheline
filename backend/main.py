"""Squeeze the Line — FastAPI backend.

Wraps the data layer behind a small REST API consumed by the Next.js
frontend. Currently serves realistic mock data via ``MockProvider``;
a live scraper-backed provider can be swapped in (see ``providers/``)
without changing these routes.

Run locally:
    uvicorn main:app --reload --port 8000
"""
from __future__ import annotations

from typing import List, Optional

# Load backend/.env (e.g. ODDS_API_KEY) before the providers package decides
# between the live and mock data sources. No-op if python-dotenv isn't present.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import Injury, PlayerDetail, Slate, Sport
from providers import get_provider

app = FastAPI(
    title="Squeeze the Line API",
    description="Player-prop projections, slates and injury reports.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

provider = get_provider()


@app.get("/")
def root() -> dict:
    return {"service": "squeeze-the-line", "status": "ok", "docs": "/docs"}


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/sports", response_model=List[Sport])
def list_sports() -> List[Sport]:
    """List available sports with metadata (name, icon, stat types, season)."""
    return provider.list_sports()


def _validate_sport(sport: str) -> None:
    valid = {s.key for s in provider.list_sports()}
    if sport not in valid:
        raise HTTPException(status_code=404, detail=f"Unknown sport '{sport}'. Valid: {sorted(valid)}")


@app.get("/api/slate", response_model=Slate)
def get_slate(
    sport: str = Query("baseball_mlb", description="Sport key, e.g. baseball_mlb"),
    date: Optional[str] = Query(None, description="ISO date (defaults to today)"),
) -> Slate:
    """Today's projected picks for a sport."""
    _validate_sport(sport)
    return provider.get_slate(sport, date)


@app.get("/api/player/{name}", response_model=PlayerDetail)
def get_player(
    name: str,
    sport: str = Query("baseball_mlb", description="Sport key"),
) -> PlayerDetail:
    """Player detail: season averages, last games, and hit rates by stat."""
    detail = provider.get_player(name, sport)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Player '{name}' not found")
    return detail


@app.get("/api/injuries", response_model=List[Injury])
def get_injuries(
    sport: str = Query("baseball_mlb", description="Sport key"),
) -> List[Injury]:
    """Injury report for a sport."""
    _validate_sport(sport)
    return provider.get_injuries(sport)
