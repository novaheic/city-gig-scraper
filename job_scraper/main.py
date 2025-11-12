"""CLI entry point for the Berlin job scraper MVP."""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from .crawler import AsyncCrawler
from .detection import (
    STRONG_KEYWORDS,
    VENDOR_KEYWORDS,
    WEAK_KEYWORDS,
    detect_hiring_signals,
    extract_job_links,
)
from .discovery_overpass import (
    OverpassError,
    fetch_places,
    fetch_places_from_multiple_areas,
    get_sub_areas,
    fetch_places_by_grid,
)
from .models import Place, ScrapeResult

DEFAULT_AREA = "Bezirk Mitte, Berlin"
DEFAULT_AMENITIES = (
    "cafe,restaurant,bar,pub,fast_food,bakery,ice_cream,biergarten,food_court"
)
DEFAULT_OUTPUT = "output/berlin_mitte_jobs.csv"
DEFAULT_USER_AGENT = "BerlinJobScraper/0.1 (+https://example.com/contact)"
DEFAULT_CONCURRENCY = 5
DEFAULT_MAX_JOB_LINKS = 12
DEFAULT_CRAWL_DEPTH = 3

# Combined keywords for scoring (both strong and weak)
JOB_PAGE_KEYWORDS = tuple(k.lower() for k in STRONG_KEYWORDS + WEAK_KEYWORDS)

NAVIGATION_BLOCKLIST = (
    "impressum",
    "datenschutz",
    "privacy",
    "agb",
    "newsletter",
    "kontakt",
    "contact",
    "reservierung",
    "reservation",
    "events",
    "news",
    "blog",
    "home",
    "startseite",
    "zurück",
    "menu",
    "menü",
    "gutscheine",
    "shop",
    "presse",
    "press",
)

VENDOR_HOST_FRAGMENTS = (
    "jobs.personio.de",
    "lever.co",
    "greenhouse.io",
    "smartrecruiters.com",
    "join.com",
    "workable.com",
    "recruitee.com",
    "teamtailor",
    "ashbyhq",
    "bamboohr.com",
    "jobylon",
    "icims.com",
    "hirehive",
    "workday",
)

FALLBACK_PATHS = (
    "/jobs",
    "/jobs/",
    "/karriere",
    "/karriere/",
    "/karriere/jobs",
    "/careers",
    "/careers/",
    "/stellen",
    "/stellenangebote",
    "/offene-stellen",
    "/join",
    "/join-us",
    "/join-our-team",
)

SCORE_THRESHOLD = 1.5


@dataclass(slots=True)
class CandidateLink:
    url: str
    text: str
    score: float


def main(argv: Sequence[str] | None = None) -> None:
    """Execute the CLI."""

    args = _parse_args(argv)
    _configure_logging(args.log_level)

    amenities = _parse_amenities(args.amenities)
    logging.info(
        "Fetching places for area='%s' with amenities=%s", args.area, ",".join(amenities)
    )

    kwargs: dict[str, object] = {}
    overpass_urls: list[str] = []
    env_multi = os.getenv("OVERPASS_URLS")
    if env_multi:
        overpass_urls.extend([url.strip() for url in env_multi.split(",") if url.strip()])
    env_single = os.getenv("OVERPASS_URL")
    if env_single:
        overpass_urls.append(env_single.strip())
    if args.overpass_url:
        overpass_urls.append(args.overpass_url.strip())

    seen_urls: set[str] = set()
    unique_urls: list[str] = []
    for url in overpass_urls:
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_urls.append(url)

    if unique_urls:
        kwargs["overpass_urls"] = unique_urls
        kwargs["overpass_url"] = unique_urls[0]

    # Adaptive strategy:
    # 1) If user explicitly asked for districts, use them.
    # 2) Otherwise try single-area query; on failure (timeout/too large), fall back to grid tiling.
    if args.split_into_districts:
        logging.info("Attempting to split '%s' into districts...", args.area)
        sub_areas = get_sub_areas(args.area, **kwargs)
        if sub_areas:
            logging.info("Found %d districts, querying each separately", len(sub_areas))
            places = fetch_places_from_multiple_areas(sub_areas, amenities, **kwargs)
        else:
            logging.warning("Could not find sub-areas for '%s'; trying single-area then grid fallback if needed", args.area)
            try:
                places = fetch_places(args.area, amenities, **kwargs)
            except OverpassError as exc:
                logging.warning("Single-area query failed (%s); attempting grid fallback", exc)
                places = fetch_places_by_grid(args.area, amenities, **kwargs)
    else:
        try:
            places = fetch_places(args.area, amenities, **kwargs)
        except OverpassError as exc:
            logging.warning("Single-area query failed (%s); attempting grid fallback", exc)
            places = fetch_places_by_grid(args.area, amenities, **kwargs)

    if args.limit is not None and args.limit >= 0:
        places = places[: args.limit]

    if not places:
        logging.warning("No places found for the selected criteria")
        return

    logging.info("Discovered %d places to inspect", len(places))

    crawl_depth = max(args.crawl_depth, 1)
    place_processing = asyncio.run(
        _scrape_places(
            places,
            user_agent=args.user_agent,
            concurrency=args.concurrency,
            max_job_links=args.max_job_links,
            crawl_depth=crawl_depth,
        )
    )

    hiring_venues = [result for result in place_processing if result.hiring]
    deduplicated_venues = _deduplicate_by_job_page(hiring_venues)
    output_path = Path(args.output)
    _write_results(output_path, deduplicated_venues)
    logging.info(
        "Wrote %d hiring venue rows to %s (filtered from %d total venues, %d duplicates removed)",
        len(deduplicated_venues),
        output_path,
        len(place_processing),
        len(hiring_venues) - len(deduplicated_venues),
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--area", default=DEFAULT_AREA, help="Area definition for Overpass")
    parser.add_argument(
        "--amenities",
        default=DEFAULT_AMENITIES,
        help="Comma-separated amenity types",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Path to the venue diagnostics CSV",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to send with HTTP requests",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="Maximum concurrent HTTP requests",
    )
    parser.add_argument(
        "--max-job-links",
        type=int,
        default=DEFAULT_MAX_JOB_LINKS,
        help="Maximum number of candidate job links to follow per site",
    )
    parser.add_argument(
        "--crawl-depth",
        type=int,
        default=DEFAULT_CRAWL_DEPTH,
        help="Maximum crawl depth for job pages (>=1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on the number of places to process",
    )
    parser.add_argument(
        "--overpass-url",
        default=None,
        help="Override the Overpass API endpoint",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (e.g. INFO, DEBUG)",
    )
    parser.add_argument(
        "--split-into-districts",
        action="store_true",
        help="Automatically split large areas (e.g., 'Berlin') into districts and query each separately to avoid timeouts",
    )

    return parser.parse_args(argv)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )


def _parse_amenities(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    amenities = [item.strip() for item in items if item.strip()]
    if not amenities:
        raise SystemExit("At least one amenity must be specified")
    return amenities


async def _scrape_places(
    places: Sequence[Place],
    *,
    user_agent: str,
    concurrency: int,
    max_job_links: int,
    crawl_depth: int,
) -> list[ScrapeResult]:
    async with AsyncCrawler(user_agent=user_agent, concurrency=concurrency) as crawler:
        tasks = [
            asyncio.create_task(
                _process_place(
                    place,
                    crawler,
                    max_job_links=max_job_links,
                    crawl_depth=crawl_depth,
                )
            )
            for place in places
        ]
        return await asyncio.gather(*tasks)


async def _process_place(
    place: Place,
    crawler: AsyncCrawler,
    *,
    max_job_links: int,
    crawl_depth: int,  # unused but kept for CLI compatibility
) -> ScrapeResult:
    timestamp = _utc_now()
    homepage_result = await crawler.fetch(place.website)

    if homepage_result.error:
        snippet = f"Error fetching homepage: {homepage_result.error}"
        return ScrapeResult(
            place=place,
            job_page_url=None,
            hiring=False,
            evidence_snippet=snippet,
            matched_keyword=None,
            http_status=None,
            last_checked_utc=timestamp,
        )

    base_url = homepage_result.final_url or place.website
    http_status = homepage_result.status_code

    homepage_html = homepage_result.text or ""
    base_host = urlparse(base_url).hostname or ""

    preselected_urls = {
        _canonicalize_url(url)
        for url in extract_job_links(homepage_html, base_url)
    }
    candidate_pairs = _extract_candidate_links(homepage_html, base_url)
    ranked_candidates = _rank_candidates(
        candidate_pairs,
        preselected_urls,
        base_host,
        base_url,
    )

    max_candidates = len(ranked_candidates)
    if max_job_links > 0:
        max_candidates = min(max_candidates, max_job_links)

    hiring_flag = False
    job_page_url: str | None = None
    matched_keyword: str | None = None
    evidence_snippet: str | None = None

    homepage_detection = detect_hiring_signals(homepage_html)

    tried_urls: set[str] = set()

    for candidate in ranked_candidates[:max_candidates]:
        canonical_candidate = _canonicalize_url(candidate.url) or candidate.url
        if canonical_candidate in tried_urls:
            continue
        tried_urls.add(canonical_candidate)

        job_result = await crawler.fetch(candidate.url)
        if job_result.error:
            logging.debug("Failed to fetch candidate jobs page %s: %s", candidate.url, job_result.error)
            continue

        status = job_result.status_code or 0
        if status >= 400:
            continue

        resolved_url = job_result.final_url or candidate.url
        canonical_resolved = _canonicalize_url(resolved_url) or resolved_url
        detection = detect_hiring_signals(job_result.text or "")
        if detection[0]:
            # Preserve fragment from original candidate URL if present
            parsed_candidate = urlparse(candidate.url)
            if parsed_candidate.fragment:
                # Reconstruct URL with fragment preserved
                parsed_resolved = urlparse(resolved_url)
                job_page_url = f"{canonical_resolved}#{parsed_candidate.fragment}"
            else:
                job_page_url = canonical_resolved
            matched_keyword = detection[1]
            evidence_snippet = detection[2]
            http_status = status or http_status
            hiring_flag = True
            break

    # Only try fallbacks if we have a signal from homepage or found candidates
    if not hiring_flag and (homepage_detection[0] or ranked_candidates):
        fallback_urls = _iter_fallback_urls(base_url)
        max_fallbacks = min(5, len(fallback_urls))  # Limit to top 5 fallbacks
        for fallback_url in fallback_urls[:max_fallbacks]:
            if fallback_url in tried_urls:
                continue
            try:
                job_result = await crawler.fetch(fallback_url)
            except Exception as exc:
                logging.debug("Fallback fetch exception for %s: %s", fallback_url, exc)
                continue
            if job_result.error:
                continue
            status = job_result.status_code or 0
            if status >= 400:
                continue
            resolved_url = job_result.final_url or fallback_url
            canonical_resolved = _canonicalize_url(resolved_url) or resolved_url
            detection = detect_hiring_signals(job_result.text or "")
            if detection[0]:
                # Preserve fragment from fallback URL if present
                parsed_fallback = urlparse(fallback_url)
                if parsed_fallback.fragment:
                    parsed_resolved = urlparse(resolved_url)
                    job_page_url = f"{canonical_resolved}#{parsed_fallback.fragment}"
                else:
                    job_page_url = canonical_resolved
                matched_keyword = detection[1]
                evidence_snippet = detection[2]
                http_status = status or http_status
                hiring_flag = True
                break

    if not hiring_flag and homepage_detection[0]:
        # If vendor keyword detected on homepage but no job page found,
        # require vendor links to exist (to avoid false positives)
        matched_keyword_value = homepage_detection[1]
        all_job_links = extract_job_links(homepage_html, base_url)
        
        if matched_keyword_value and matched_keyword_value.lower() in {v.lower() for v in VENDOR_KEYWORDS}:
            # Check if vendor keyword appears in actual links
            vendor_links = [
                url for url in all_job_links
                if any(vendor.lower() in url.lower() for vendor in VENDOR_KEYWORDS)
            ]
            if not vendor_links:
                # Vendor keyword found but no vendor links - likely false positive
                hiring_flag = False
            else:
                # Found vendor links, treat as valid
                hiring_flag = True
                job_page_url = _canonicalize_url(vendor_links[0]) or vendor_links[0]
                matched_keyword = matched_keyword_value
                evidence_snippet = homepage_detection[2]
        else:
            # Non-vendor keyword detected - check if we found any job links
            if all_job_links:
                # Use the first job link found (they're already filtered for job keywords)
                # Preserve fragments in the final URL (they're useful for navigation)
                first_link = all_job_links[0]
                # Only canonicalize if it doesn't have a fragment, to preserve fragments
                parsed_link = urlparse(first_link)
                if parsed_link.fragment:
                    job_page_url = first_link  # Keep fragment
                else:
                    job_page_url = _canonicalize_url(first_link) or first_link
                hiring_flag = True
                matched_keyword = matched_keyword_value
                evidence_snippet = homepage_detection[2]
            else:
                # No job links found, fall back to homepage
                hiring_flag = True
                job_page_url = _canonicalize_url(base_url) or base_url
                matched_keyword = matched_keyword_value
                evidence_snippet = homepage_detection[2]

    return ScrapeResult(
        place=place,
        job_page_url=job_page_url,
        hiring=hiring_flag,
        evidence_snippet=evidence_snippet,
        matched_keyword=matched_keyword,
        http_status=http_status,
        last_checked_utc=timestamp,
    )

def _write_results(path: Path, results: Sequence[ScrapeResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "name",
        "type",
        "homepage",
        "job_page_url",
    ]

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "name": result.place.name,
                    "type": result.place.amenity,
                    "homepage": result.place.website,
                    "job_page_url": result.job_page_url or "",
                }
            )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _deduplicate_by_job_page(results: Sequence[ScrapeResult]) -> list[ScrapeResult]:
    """Remove duplicates based on canonicalized job_page_url.
    
    When multiple venues share the same job page URL, keep the first one encountered.
    Prefers entries with shorter website URLs (likely main locations).
    """
    seen_job_pages: dict[str, ScrapeResult] = {}
    deduplicated: list[ScrapeResult] = []
    
    for result in results:
        if not result.job_page_url:
            # Keep entries without job_page_url (they can't be duplicates)
            deduplicated.append(result)
            continue
            
        # Canonicalize the job_page_url for comparison (strip fragments for dedup)
        canonical_job_url = _canonicalize_url(result.job_page_url) or result.job_page_url
        
        if canonical_job_url not in seen_job_pages:
            seen_job_pages[canonical_job_url] = result
            deduplicated.append(result)
        else:
            # If we already have this job page, prefer the one with shorter website URL
            # (likely the main location rather than a specific branch)
            existing = seen_job_pages[canonical_job_url]
            if len(result.place.website) < len(existing.place.website):
                # Replace the existing one in the list
                deduplicated.remove(existing)
                deduplicated.append(result)
                seen_job_pages[canonical_job_url] = result
    
    return deduplicated


def _extract_candidate_links(html: str, base_url: str) -> list[tuple[str, str]]:
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        absolute = urljoin(base_url, href)
        canonical = _canonicalize_url(absolute)
        if not canonical or canonical in seen:
            continue

        parsed = urlparse(canonical)
        if parsed.scheme not in {"http", "https"}:
            continue

        seen.add(canonical)
        text = anchor.get_text(" ", strip=True)
        candidates.append((canonical, text))

    return candidates


def _rank_candidates(
    candidates: Sequence[tuple[str, str]],
    preselected_urls: set[str],
    base_host: str,
    base_url: str,
) -> list[CandidateLink]:
    ranked: list[CandidateLink] = []
    for url, text in candidates:
        score = _score_candidate(url, text, preselected_urls, base_host, base_url)
        if score >= SCORE_THRESHOLD:
            ranked.append(CandidateLink(url=url, text=text, score=score))

    ranked.sort(key=lambda candidate: candidate.score, reverse=True)
    return ranked


def _score_candidate(
    url: str,
    text: str,
    preselected_urls: set[str],
    base_host: str,
    base_url: str,
) -> float:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return float("-inf")

    lower_url = url.lower()
    lower_text = (text or "").lower()
    score = 0.0

    if url in preselected_urls:
        score += 1.5

    if any(keyword in lower_text for keyword in JOB_PAGE_KEYWORDS):
        score += 2.5
    if any(keyword in lower_url for keyword in JOB_PAGE_KEYWORDS):
        score += 1.5

    host = parsed.hostname or ""
    # Check for job-related subdomains (jobs.*, careers.*, etc.)
    if host:
        host_lower = host.lower()
        job_subdomain_patterns = ("jobs.", "careers.", "karriere.", "stellen.", "recruiting.", "hiring.")
        if any(host_lower.startswith(pattern) for pattern in job_subdomain_patterns):
            score += 5.0  # Very strong signal
        elif any(fragment in host_lower for fragment in VENDOR_HOST_FRAGMENTS):
            score += 3.0
        elif base_host and host_lower != base_host.lower():
            # Different host but not a job subdomain - slight penalty
            score -= 1.0

    # Check if URL/fragment contains job keywords before penalizing navigation blocklist
    has_job_keyword_in_url = any(keyword in lower_url for keyword in JOB_PAGE_KEYWORDS)
    has_job_keyword_in_text = any(keyword in lower_text for keyword in JOB_PAGE_KEYWORDS)
    
    if any(block in lower_text for block in NAVIGATION_BLOCKLIST):
        # Don't penalize navigation blocklist items if they have job keywords
        if not (has_job_keyword_in_text or has_job_keyword_in_url):
            score -= 2.5
    if any(block in lower_url for block in NAVIGATION_BLOCKLIST):
        # Don't penalize navigation blocklist items if they have job keywords
        if not (has_job_keyword_in_text or has_job_keyword_in_url):
            score -= 2.5

    # Penalize "team" links unless they're clearly job-related
    if "team" in lower_text or "/team" in lower_url:
        # Check if it's job-related context
        job_context_indicators = ("join", "hiring", "career", "karriere", "bewerb", "stellen", "job")
        has_job_context = any(indicator in lower_text or indicator in lower_url for indicator in job_context_indicators)
        if not has_job_context:
            score -= 2.0  # "Meet the team" pages, not hiring pages

    # Only penalize fragments if they don't contain job keywords
    if parsed.fragment:
        fragment_lower = parsed.fragment.lower()
        has_job_keyword_in_fragment = any(keyword in fragment_lower for keyword in JOB_PAGE_KEYWORDS)
        if not has_job_keyword_in_fragment:
            score -= 0.5

    if lower_url == base_url.lower():
        score -= 1.0

    if lower_url.endswith(('.pdf', '.jpg', '.jpeg', '.png', '.gif', '.webp')):
        return float('-inf')

    return score


def _canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    cleaned = parsed._replace(fragment="").geturl()
    if parsed.path and parsed.path != "/":
        cleaned = cleaned.rstrip("/")
    return cleaned


def _iter_fallback_urls(base_url: str) -> list[str]:
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        return []

    base_root = f"{parsed.scheme}://{parsed.netloc}"
    base_with_slash = base_url if base_url.endswith("/") else base_url + "/"
    base_canonical = _canonicalize_url(base_url)

    seen: set[str] = set()
    ordered: list[str] = []

    for path in FALLBACK_PATHS:
        absolute_root = _canonicalize_url(urljoin(base_root, path.lstrip("/")))
        if (
            absolute_root
            and absolute_root not in seen
            and absolute_root != base_canonical
        ):
            seen.add(absolute_root)
            ordered.append(absolute_root)

        absolute_relative = _canonicalize_url(urljoin(base_with_slash, path.lstrip("/")))
        if (
            absolute_relative
            and absolute_relative not in seen
            and absolute_relative != base_canonical
        ):
            seen.add(absolute_relative)
            ordered.append(absolute_relative)

    return ordered


if __name__ == "__main__":
    main()

