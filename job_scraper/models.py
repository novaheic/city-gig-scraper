"""Data models used across the Berlin job scraper MVP."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(slots=True)
class Place:
    """Represents a venue discovered via Overpass."""

    osm_id: str
    name: str
    amenity: str
    latitude: float
    longitude: float
    website: str


@dataclass(slots=True)
class ScrapeResult:
    """Result of scraping a place's website for hiring signals."""

    place: Place
    job_page_url: Optional[str]
    hiring: bool
    evidence_snippet: Optional[str]
    matched_keyword: Optional[str]
    http_status: Optional[int]
    last_checked_utc: str

