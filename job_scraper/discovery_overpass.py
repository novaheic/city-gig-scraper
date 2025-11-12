"""Helpers for querying Overpass API for venues."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterable, Sequence
from urllib.parse import quote, urlparse

import httpx

from .models import Place

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WEBSITE_TAG_KEYS = ("website", "contact:website", "url")

REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

AREA_CACHE: dict[str, int] = {}
BBOX_CACHE: dict[str, tuple[float, float, float, float]] = {}


class OverpassError(RuntimeError):
    """Raised when the Overpass API returns an unexpected response."""


def _redis_enabled() -> bool:
    return bool(REDIS_URL and REDIS_TOKEN)


def _redis_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {REDIS_TOKEN}"}


def _redis_get(key: str) -> str | None:
    if not _redis_enabled():
        return None
    try:
        encoded_key = quote(key, safe="")
        resp = httpx.get(
            f"{REDIS_URL}/get/{encoded_key}",
            headers=_redis_headers(),
            timeout=httpx.Timeout(5.0, connect=3.0, read=3.0),
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data.get("result")
    except Exception:
        return None


def _redis_set(key: str, value: str, *, ttl_seconds: int | None = None) -> None:
    if not _redis_enabled():
        return
    try:
        encoded_key = quote(key, safe="")
        encoded_val = quote(value, safe="")
        if ttl_seconds and ttl_seconds > 0:
            url = f"{REDIS_URL}/setex/{encoded_key}/{ttl_seconds}/{encoded_val}"
        else:
            url = f"{REDIS_URL}/set/{encoded_key}/{encoded_val}"
        httpx.get(
            url,
            headers=_redis_headers(),
            timeout=httpx.Timeout(5.0, connect=3.0, read=3.0),
        )
    except Exception:
        # Best-effort cache; ignore failures.
        pass


def _normalize_overpass_urls(
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
) -> list[str]:
    ordered: list[str] = []
    if overpass_urls:
        ordered.extend([url.strip() for url in overpass_urls if url and url.strip()])
    if overpass_url:
        ordered.append(overpass_url.strip())
    if not ordered:
        ordered.append(OVERPASS_URL)

    seen: set[str] = set()
    unique: list[str] = []
    for url in ordered:
        if url and url not in seen:
            seen.add(url)
            unique.append(url)
    return unique


def _area_cache_key(area_name: str) -> str:
    return area_name.strip().lower()


def _get_cached_relation_id(area_name: str) -> int | None:
    key = _area_cache_key(area_name)
    cached = AREA_CACHE.get(key)
    if cached is not None:
        return cached if cached > 0 else None
    raw = _redis_get(f"overpass:area:{key}")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    AREA_CACHE[key] = value
    return value if value > 0 else None


def _set_cached_relation_id(area_name: str, relation_id: int | None) -> None:
    key = _area_cache_key(area_name)
    if relation_id and relation_id > 0:
        AREA_CACHE[key] = relation_id
        _redis_set(f"overpass:area:{key}", str(relation_id), ttl_seconds=7 * 24 * 3600)
    else:
        AREA_CACHE[key] = 0
        _redis_set(f"overpass:area:{key}", "0", ttl_seconds=24 * 3600)


def _get_cached_bbox(area_name: str) -> tuple[float, float, float, float] | None:
    key = _area_cache_key(area_name)
    if key in BBOX_CACHE:
        return BBOX_CACHE[key]
    raw = _redis_get(f"overpass:bbox:{key}")
    if raw is None:
        return None
    try:
        coords = json.loads(raw)
        if isinstance(coords, (list, tuple)) and len(coords) == 4:
            bbox = tuple(float(c) for c in coords)  # type: ignore[arg-type]
            BBOX_CACHE[key] = bbox  # type: ignore[assignment]
            return bbox
    except Exception:
        return None
    return None


def _set_cached_bbox(area_name: str, bbox: tuple[float, float, float, float] | None) -> None:
    key = _area_cache_key(area_name)
    if bbox:
        BBOX_CACHE[key] = bbox
        _redis_set(
            f"overpass:bbox:{key}",
            json.dumps([bbox[0], bbox[1], bbox[2], bbox[3]]),
            ttl_seconds=7 * 24 * 3600,
        )
    else:
        if key in BBOX_CACHE:
            BBOX_CACHE.pop(key, None)
        _redis_set(f"overpass:bbox:{key}", "null", ttl_seconds=24 * 3600)


def _nominatim_bbox(area_name: str) -> tuple[float, float, float, float] | None:
    try:
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": area_name, "format": "json", "limit": 1},
            headers={"User-Agent": "CityGigScraper/1.0"},
            timeout=httpx.Timeout(20.0, connect=5.0, read=10.0),
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        bbox = data[0].get("boundingbox")
        if not bbox or len(bbox) != 4:
            return None
        south = float(bbox[0])
        north = float(bbox[1])
        west = float(bbox[2])
        east = float(bbox[3])
        return (south, west, north, east)
    except Exception:
        return None
def _resolve_area_relation_id(
    area_name: str,
    *,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    client: httpx.Client | None = None,
) -> int | None:
    """Resolve an area name to an OSM relation ID.

    If multiple relations share the same name (e.g., Stuttgart, US vs DE),
    prefer likely administrative city relations and bias away from US entries
    unless clearly requested.
    """
    cached = _get_cached_relation_id(area_name)
    if cached is not None:
        return cached

    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    urls = _normalize_overpass_urls(overpass_url, overpass_urls)

    # Query for relations matching the area name and include tags for ranking
    query = f"""
[out:json][timeout:25];
relation["name"="{area_name}"]["type"~"^(boundary|administrative)$"]["admin_level"];
out ids tags;
"""

    last_error: Exception | None = None
    try:
        for attempt in range(3):
            for url in urls:
                try:
                    response = client.post(url, data={"data": query})
                    response.raise_for_status()
                    payload = response.json()
                    elements = payload.get("elements", [])
                    if not elements:
                        continue

                    def rank(elem: dict) -> int:
                        tags = elem.get("tags", {}) or {}
                        score = 0
                        admin_level = str(tags.get("admin_level", "")).strip()
                        name_de = tags.get("name:de")
                        is_in = " ".join(
                            [
                                str(tags.get(k, ""))
                                for k in (
                                    "is_in",
                                    "is_in:country",
                                    "addr:country",
                                    "country",
                                    "ISO3166-1",
                                )
                            ]
                        ).lower()

                        if admin_level in {"6", "7", "8"}:
                            score += 5
                        elif admin_level in {"4", "5"}:
                            score += 2

                        if any(token in is_in for token in ("germany", "de", "deu")):
                            score += 10
                        if any(token in is_in for token in ("europe", "eu")):
                            score += 3
                        if any(token in is_in for token in ("united states", "usa", "us")):
                            score -= 10

                        if name_de:
                            score += 3

                        name_len = len((tags.get("name") or "").strip())
                        score += max(0, 20 - min(20, name_len))

                        return score

                    best = max(elements, key=rank)
                    relation_id = int(best.get("id", 0) or 0)
                    if relation_id > 0:
                        logger.info("Resolved area '%s' via %s to relation ID %d", area_name, url, relation_id)
                        _set_cached_relation_id(area_name, relation_id)
                        return relation_id
                except Exception as exc:
                    last_error = exc
                    logger.debug(
                        "Failed to resolve area '%s' using %s (attempt %d): %s",
                        area_name,
                        url,
                        attempt + 1,
                        exc,
                    )
            if attempt < 2:
                time.sleep(min(1.0 * (attempt + 1), 4.0))
    finally:
        if owns_client:
            client.close()

    if last_error:
        logger.debug("Giving up on resolving '%s': %s", area_name, last_error)
    _set_cached_relation_id(area_name, None)
    return None


def get_sub_areas(
    parent_area_name: str,
    *,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    client: httpx.Client | None = None,
) -> list[str]:
    """Get sub-areas (districts) of a parent area from OSM.
    
    For example, if parent_area_name is "Berlin", this will return
    a list of Berlin district names like ["Bezirk Mitte, Berlin", ...].
    """
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    urls = _normalize_overpass_urls(overpass_url, overpass_urls)

    # First, resolve the parent area
    parent_id = _resolve_area_relation_id(
        parent_area_name,
        overpass_url=overpass_url,
        overpass_urls=overpass_urls,
        client=client,
    )
    if not parent_id:
        logger.warning("Could not resolve parent area '%s', cannot get sub-areas", parent_area_name)
        return []

    # Query for sub-relations (districts) within the parent area
    # Look for relations that are members of the parent relation with appropriate admin_level
    query = f"""
[out:json][timeout:25];
relation({parent_id});
rel(r)["admin_level"~"^(6|7|8|9|10)$"]["type"="boundary"]["name"];
out tags;
"""

    try:
        last_error: Exception | None = None
        for attempt in range(3):
            for url in urls:
                try:
                    response = client.post(url, data={"data": query})
                    response.raise_for_status()
                    payload = response.json()
                    elements = payload.get("elements", [])

                    sub_areas = []
                    for element in elements:
                        tags = element.get("tags", {})
                        name = tags.get("name")
                        if not name:
                            continue
                        if parent_area_name.lower() not in name.lower():
                            sub_areas.append(f"{name}, {parent_area_name}")
                        else:
                            sub_areas.append(name)

                    if sub_areas:
                        logger.info("Found %d sub-areas for '%s'", len(sub_areas), parent_area_name)
                    else:
                        logger.info("No sub-areas found for '%s'", parent_area_name)

                    return sub_areas
                except Exception as exc:
                    last_error = exc
                    logger.debug(
                        "Failed to get sub-areas for '%s' via %s (attempt %d): %s",
                        parent_area_name,
                        url,
                        attempt + 1,
                        exc,
                    )
            if attempt < 2:
                time.sleep(min(1.0 * (attempt + 1), 4.0))
        if last_error:
            logger.warning("Failed to get sub-areas for '%s': %s", parent_area_name, last_error)
        return []
    finally:
        if owns_client:
            client.close()


def _get_relation_bbox(
    relation_id: int,
    *,
    area_name: str | None = None,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    client: httpx.Client | None = None,
) -> tuple[float, float, float, float] | None:
    """Return (south, west, north, east) bbox for a relation via Overpass."""
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    if area_name:
        cached_bbox = _get_cached_bbox(area_name)
        if cached_bbox:
            return cached_bbox

    urls = _normalize_overpass_urls(overpass_url, overpass_urls)

    query = f"""
[out:json][timeout:25];
relation({relation_id});
out bb;
"""
    last_error: Exception | None = None
    try:
        for attempt in range(3):
            for url in urls:
                try:
                    response = client.post(url, data={"data": query})
                    response.raise_for_status()
                    payload = response.json()
                    elements = payload.get("elements", [])
                    if elements:
                        bounds = elements[0].get("bounds") or {}
                        south = bounds.get("minlat")
                        west = bounds.get("minlon")
                        north = bounds.get("maxlat")
                        east = bounds.get("maxlon")
                        if None not in (south, west, north, east):
                            bbox = float(south), float(west), float(north), float(east)
                            if area_name:
                                _set_cached_bbox(area_name, bbox)
                            return bbox
                    bounds = payload.get("bounds") or {}
                    south = bounds.get("minlat")
                    west = bounds.get("minlon")
                    north = bounds.get("maxlat")
                    east = bounds.get("maxlon")
                    if None not in (south, west, north, east):
                        bbox = float(south), float(west), float(north), float(east)
                        if area_name:
                            _set_cached_bbox(area_name, bbox)
                        return bbox
                except Exception as exc:
                    last_error = exc
                    logger.debug(
                        "Failed to get bbox for relation %s via %s (attempt %d): %s",
                        relation_id,
                        url,
                        attempt + 1,
                        exc,
                    )
            if attempt < 2:
                time.sleep(min(1.0 * (attempt + 1), 4.0))
    finally:
        if owns_client:
            client.close()

    if last_error:
        logger.debug("Unable to get bbox for relation %s: %s", relation_id, last_error)
    if area_name:
        _set_cached_bbox(area_name, None)
    return None


def _tile_bbox(
    south: float, west: float, north: float, east: float, *, tiles_per_side: int = 3
) -> list[tuple[float, float, float, float]]:
    """Split a bbox into a grid to reduce Overpass result sizes."""
    tiles: list[tuple[float, float, float, float]] = []
    dlat = (north - south) / tiles_per_side
    dlon = (east - west) / tiles_per_side
    for i in range(tiles_per_side):
        for j in range(tiles_per_side):
            s = south + i * dlat
            n = south + (i + 1) * dlat
            w = west + j * dlon
            e = west + (j + 1) * dlon
            tiles.append((s, w, n, e))
    return tiles


def _build_bbox_query(s: float, w: float, n: float, e: float, amenities: Iterable[str]) -> str:
    amenity_patterns = sorted({a.strip() for a in amenities if a.strip()})
    amenity_regex = "|".join(amenity_patterns)
    query = f"""
[out:json][timeout:60];
(
  node["amenity"~"^({amenity_regex})$"]({s},{w},{n},{e});
  way["amenity"~"^({amenity_regex})$"]({s},{w},{n},{e});
  relation["amenity"~"^({amenity_regex})$"]({s},{w},{n},{e});
);
out center tags;
"""
    return "\n".join(line.rstrip() for line in query.strip().splitlines()) + "\n"


def fetch_places_by_grid(
    area: str,
    amenities: Iterable[str],
    *,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    client: httpx.Client | None = None,
    tiles_per_side: int = 3,
) -> list[Place]:
    """Resolve city bbox and query in a grid to avoid Overpass timeouts."""
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    urls = _normalize_overpass_urls(overpass_url, overpass_urls)

    try:
        relation_id = _resolve_area_relation_id(
            area,
            overpass_url=overpass_url,
            overpass_urls=overpass_urls,
            client=client,
        )

        bbox = None
        if relation_id:
            bbox = _get_relation_bbox(
                relation_id,
                area_name=area,
                overpass_url=overpass_url,
                overpass_urls=overpass_urls,
                client=client,
            )
        else:
            logger.warning("Cannot grid-query '%s' (no relation ID) - trying cached/Nominatim bbox", area)
            bbox = _get_cached_bbox(area)
            if not bbox:
                bbox = _nominatim_bbox(area)
                if bbox:
                    _set_cached_bbox(area, bbox)

        if not bbox:
            logger.warning("Cannot determine bbox for '%s'", area)
            return []

        south, west, north, east = bbox
        tiles = _tile_bbox(south, west, north, east, tiles_per_side=tiles_per_side)

        all_places: list[Place] = []
        seen_websites: set[str] = set()

        for idx, (s, w, n, e) in enumerate(tiles, 1):
            logger.info("Grid tile %d/%d for %s: (%f,%f,%f,%f)", idx, len(tiles), area, s, w, n, e)
            query = _build_bbox_query(s, w, n, e, amenities)

            payload = None
            last_error: Exception | None = None
            for attempt in range(2):
                for url in urls:
                    try:
                        response = client.post(url, data={"data": query})
                        response.raise_for_status()
                        payload = response.json()
                        break
                    except Exception as exc:
                        last_error = exc
                        logger.debug(
                            "Overpass failed for tile %d/%d via %s (attempt %d): %s",
                            idx,
                            len(tiles),
                            url,
                            attempt + 1,
                            exc,
                        )
                if payload is not None:
                    break
                time.sleep(0.5 * (attempt + 1))

            if payload is None:
                logger.warning("Overpass failed for tile %d/%d: %s", idx, len(tiles), last_error)
                continue

            elements = payload.get("elements", [])
            for element in elements:
                tags = element.get("tags") or {}
                website = _select_website(tags)
                if not website:
                    continue

                osm_type = element.get("type", "unknown")
                osm_id = element.get("id")
                if osm_id is None:
                    continue

                name = tags.get("name") or f"{osm_type.title()} {osm_id}"

                if "lat" in element and "lon" in element:
                    lat = float(element["lat"])
                    lon = float(element["lon"])
                else:
                    center = element.get("center")
                    if not center or "lat" not in center or "lon" not in center:
                        continue
                    lat = float(center["lat"])
                    lon = float(center["lon"])

                amenity = tags.get("amenity") or "unknown"

                place = Place(
                    osm_id=f"{osm_type}/{osm_id}",
                    name=name,
                    amenity=amenity,
                    latitude=lat,
                    longitude=lon,
                    website=_normalize_website(website),
                )

                normalized_url = place.website.lower().rstrip("/")
                if normalized_url not in seen_websites:
                    seen_websites.add(normalized_url)
                    all_places.append(place)

        logger.info("Grid results: %d unique places across %d tiles for %s", len(all_places), len(tiles), area)
        return all_places
    finally:
        if owns_client:
            client.close()
def build_query(area: str, amenities: Iterable[str], *, area_relation_id: int | None = None) -> str:
    """Build an Overpass QL query for the given area and amenities."""

    amenity_patterns = sorted({a.strip() for a in amenities if a.strip()})
    if not amenity_patterns:
        raise ValueError("At least one amenity must be provided")

    amenity_regex = "|".join(amenity_patterns)

    if area_relation_id is not None:
        # Use relation ID directly (add 3600000000 for area ID)
        area_id = 3600000000 + area_relation_id
        query = f"""
[out:json][timeout:60];
area({area_id})->.searchArea;
(
  node["amenity"~"^({amenity_regex})$"](area.searchArea);
  way["amenity"~"^({amenity_regex})$"](area.searchArea);
  relation["amenity"~"^({amenity_regex})$"](area.searchArea);
);
out center tags;
"""
    else:
        # Fallback: use bounding box for Berlin-Mitte
        # Approximate bounding box for Bezirk Mitte, Berlin
        # South, West, North, East
        query = f"""
[out:json][timeout:60];
(
  node["amenity"~"^({amenity_regex})$"](52.50,13.35,52.54,13.42);
  way["amenity"~"^({amenity_regex})$"](52.50,13.35,52.54,13.42);
  relation["amenity"~"^({amenity_regex})$"](52.50,13.35,52.54,13.42);
);
out center tags;
"""

    return "\n".join(line.rstrip() for line in query.strip().splitlines()) + "\n"


def _select_website(tags: dict[str, str]) -> str | None:
    """Return the best website URL from the element tags, if any."""

    for key in WEBSITE_TAG_KEYS:
        if key in tags:
            value = tags[key].strip()
            if not value:
                continue
            # Some entries contain multiple URLs separated by ; or ,
            for delimiter in (";", ",", " "):
                if delimiter in value:
                    value = value.split(delimiter)[0].strip()
            if value:
                return value
    return None


def fetch_places(
    area: str,
    amenities: Iterable[str],
    *,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    client: httpx.Client | None = None,
) -> list[Place]:
    """Query Overpass for matching venues and return normalized places."""

    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None  # for type checkers

    urls = _normalize_overpass_urls(overpass_url, overpass_urls)

    # Try to resolve area name to relation ID
    area_relation_id = None
    if area and area.strip():
        area_relation_id = _resolve_area_relation_id(
            area.strip(),
            overpass_url=overpass_url,
            overpass_urls=overpass_urls,
            client=client,
        )
        if area_relation_id:
            logger.info("Resolved area '%s' to relation ID %d", area, area_relation_id)
        else:
            # Do NOT silently fall back to Berlin-Mitte; surface an error instead
            logger.warning("Could not resolve area '%s' to relation ID", area)
            raise OverpassError(
                f"Unable to resolve area '{area}'. Please use an exact OSM administrative name "
                "(e.g., 'Frankfurt am Main', 'Frankfurt (Oder)', 'Berlin')."
            )

    query = build_query(area, amenities, area_relation_id=area_relation_id)

    response_payload: dict | None = None
    last_error: Exception | None = None

    try:
        for attempt in range(3):
            for url in urls:
                try:
                    response = client.post(url, data={"data": query})
                    response.raise_for_status()
                    response_payload = response.json()
                    break
                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "Overpass request failed for '%s' via %s (attempt %d): %s",
                        area,
                        url,
                        attempt + 1,
                        exc,
                    )
            if response_payload is not None:
                break
            time.sleep(min(1.5 * (attempt + 1), 5.0))
    finally:
        if owns_client:
            client.close()

    if response_payload is None:
        msg = f"Overpass request failed after retries: {last_error}"
        raise OverpassError(msg) from last_error

    try:
        payload = response_payload
    except ValueError as exc:  # pragma: no cover - defensive
        raise OverpassError("Overpass response was not valid JSON") from exc

    elements = payload.get("elements", [])
    places: list[Place] = []

    for element in elements:
        tags = element.get("tags") or {}
        website = _select_website(tags)
        if not website:
            continue

        osm_type = element.get("type", "unknown")
        osm_id = element.get("id")
        if osm_id is None:
            logger.debug("Skipping element without id: %s", element)
            continue

        name = tags.get("name") or f"{osm_type.title()} {osm_id}"

        # Determine coordinates for nodes / ways / relations
        if "lat" in element and "lon" in element:
            lat = float(element["lat"])
            lon = float(element["lon"])
        else:
            center = element.get("center")
            if not center or "lat" not in center or "lon" not in center:
                logger.debug("Skipping element without coordinates: %s", element)
                continue
            lat = float(center["lat"])
            lon = float(center["lon"])

        amenity = tags.get("amenity") or "unknown"

        place = Place(
            osm_id=f"{osm_type}/{osm_id}",
            name=name,
            amenity=amenity,
            latitude=lat,
            longitude=lon,
            website=_normalize_website(website),
        )
        places.append(place)

    return places


def _normalize_website(url: str) -> str:
    url = url.strip()
    if not url:
        return url

    parsed = urlparse(url)

    if parsed.scheme:
        return url

    if url.startswith("//"):
        return f"https:{url}"

    return f"https://{url}"


def fetch_places_from_multiple_areas(
    areas: list[str],
    amenities: Iterable[str],
    *,
    overpass_url: str | None = None,
    overpass_urls: Sequence[str] | None = None,
    client: httpx.Client | None = None,
) -> list[Place]:
    """Fetch places from multiple areas and combine results.
    
    This queries each area sequentially and combines all results.
    Useful for querying large cities by district to avoid timeouts.
    """
    all_places: list[Place] = []
    seen_websites: set[str] = set()  # Deduplicate by website URL
    
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))
    
    assert client is not None
    
    try:
        for i, area in enumerate(areas, 1):
            logger.info("Querying area %d/%d: %s", i, len(areas), area)
            try:
                places = fetch_places(
                    area,
                    amenities,
                    overpass_url=overpass_url,
                    overpass_urls=overpass_urls,
                    client=client,
                )
                # Deduplicate by website URL
                for place in places:
                    normalized_url = place.website.lower().rstrip("/")
                    if normalized_url not in seen_websites:
                        seen_websites.add(normalized_url)
                        all_places.append(place)
                logger.info("Found %d places in %s (total so far: %d)", len(places), area, len(all_places))
            except OverpassError as exc:
                logger.error("Failed to query area '%s': %s", area, exc)
                # Continue with other areas
                continue
    finally:
        if owns_client:
            client.close()
    
    logger.info("Combined results: %d unique places from %d areas", len(all_places), len(areas))
    return all_places

