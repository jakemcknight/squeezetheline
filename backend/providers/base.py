"""The DataProvider interface.

Any data source (mock, or a future live scraper wrapping the legacy
Streamlit code) implements these methods. The FastAPI routes call the
interface only, so swapping implementations requires no route changes.
"""
from __future__ import annotations

from typing import List, Optional

from models import Injury, PlayerDetail, Slate, Sport


class DataProvider:
    def list_sports(self) -> List[Sport]:
        raise NotImplementedError

    def get_slate(self, sport: str, date: Optional[str] = None) -> Slate:
        raise NotImplementedError

    def get_player(self, name: str, sport: str) -> Optional[PlayerDetail]:
        raise NotImplementedError

    def get_injuries(self, sport: str) -> List[Injury]:
        raise NotImplementedError
