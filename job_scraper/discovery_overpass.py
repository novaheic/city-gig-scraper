"""Helpers for querying Overpass API for venues."""

from __future__ import annotations

import logging
from typing import Iterable
from urllib.parse import urlparse

import httpx

from .models import Place

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
WEBSITE_TAG_KEYS = ("website", "contact:website", "url")


class OverpassError(RuntimeError):
    """Raised when the Overpass API returns an unexpected response."""


def _resolve_area_relation_id(
    area_name: str,
    *,
    overpass_url: str = OVERPASS_URL,
    client: httpx.Client | None = None,
) -> int | None:
    """Resolve an area name to an OSM relation ID."""
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    # Query for relations matching the area name
    query = f"""
[out:json][timeout:25];
relation["name"="{area_name}"]["type"~"^(boundary|administrative)$"];
out ids;
"""

    try:
        response = client.post(overpass_url, data={"data": query})
        response.raise_for_status()
        payload = response.json()
        elements = payload.get("elements", [])
        if elements:
            # Return the first matching relation ID
            return elements[0].get("id")
    except Exception as exc:
        logger.debug("Failed to resolve area '%s': %s", area_name, exc)
    finally:
        if owns_client:
            client.close()

    return None


def get_sub_areas(
    parent_area_name: str,
    *,
    overpass_url: str = OVERPASS_URL,
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

    # First, resolve the parent area
    parent_id = _resolve_area_relation_id(parent_area_name, overpass_url=overpass_url, client=client)
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
        response = client.post(overpass_url, data={"data": query})
        response.raise_for_status()
        payload = response.json()
        elements = payload.get("elements", [])
        
        sub_areas = []
        for element in elements:
            tags = element.get("tags", {})
            name = tags.get("name")
            if name:
                # Prefer fully-qualified "Name, Parent" to reduce ambiguity on resolution
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
        logger.warning("Failed to get sub-areas for '%s': %s", parent_area_name, exc)
        return []
    finally:
        if owns_client:
            client.close()


def _get_relation_bbox(
    relation_id: int,
    *,
    overpass_url: str = OVERPASS_URL,
    client: httpx.Client | None = None,
) -> tuple[float, float, float, float] | None:
    """Return (south, west, north, east) bbox for a relation via Overpass."""
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    query = f"""
[out:json][timeout:25];
relation({relation_id});
out bb;
"""
    try:
        response = client.post(overpass_url, data={"data": query})
        response.raise_for_status()
        payload = response.json()
        # Try element bounds first
        elements = payload.get("elements", [])
        if elements:
            bounds = elements[0].get("bounds") or {}
            south = bounds.get("minlat")
            west = bounds.get("minlon")
            north = bounds.get("maxlat")
            east = bounds.get("maxlon")
            if None not in (south, west, north, east):
                return float(south), float(west), float(north), float(east)
        # Fallback to top-level bounds if present
        bounds = payload.get("bounds") or {}
        south = bounds.get("minlat")
        west = bounds.get("minlon")
        north = bounds.get("maxlat")
        east = bounds.get("maxlon")
        if None not in (south, west, north, east):
            return float(south), float(west), float(north), float(east)
    except Exception as exc:
        logger.debug("Failed to get bbox for relation %s: %s", relation_id, exc)
    finally:
        if owns_client:
            client.close()
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
    overpass_url: str = OVERPASS_URL,
    client: httpx.Client | None = None,
    tiles_per_side: int = 3,
) -> list[Place]:
    """Resolve city bbox and query in a grid to avoid Overpass timeouts."""
    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None

    try:
        relation_id = _resolve_area_relation_id(area, overpass_url=overpass_url, client=client)
        if not relation_id:
            logger.warning("Cannot grid-query '%s' (no relation ID)", area)
            return []

        bbox = _get_relation_bbox(relation_id, overpass_url=overpass_url, client=client)
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
            try:
                response = client.post(overpass_url, data={"data": query})
                response.raise_for_status()
                payload = response.json()
            except httpx.HTTPError as exc:
                logger.warning("Overpass failed for tile %d/%d: %s", idx, len(tiles), exc)
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
    overpass_url: str = OVERPASS_URL,
    client: httpx.Client | None = None,
) -> list[Place]:
    """Query Overpass for matching venues and return normalized places."""

    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0, read=30.0))

    assert client is not None  # for type checkers

    # Try to resolve area name to relation ID
    area_relation_id = None
    if area and area.strip():
        area_relation_id = _resolve_area_relation_id(area.strip(), overpass_url=overpass_url, client=client)
        if area_relation_id:
            logger.info("Resolved area '%s' to relation ID %d", area, area_relation_id)
        else:
            logger.info("Could not resolve area '%s' to relation ID, using bounding box fallback", area)

    query = build_query(area, amenities, area_relation_id=area_relation_id)

    try:
        response = client.post(overpass_url, data={"data": query})
        response.raise_for_status()
    except httpx.HTTPError as exc:
        msg = f"Overpass request failed: {exc}"
        raise OverpassError(msg) from exc
    finally:
        if owns_client:
            client.close()

    try:
        payload = response.json()
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
    overpass_url: str = OVERPASS_URL,
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
                places = fetch_places(area, amenities, overpass_url=overpass_url, client=client)
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

