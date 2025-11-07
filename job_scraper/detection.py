"""Hiring signal detection utilities for the Berlin job scraper MVP."""

from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

# Strong keywords (unambiguous job-related terms)
STRONG_KEYWORDS: tuple[str, ...] = (
    "jobs",
    "karriere",
    "stellenangebote",
    "offene stellen",
    "career",
    "careers",
    "vacancy",
    "vacancies",
    "hiring",
    "bewerben",
    "bewerbung",
    "bewirb",
    "arbeiten bei",
    "mitarbeiten",
    "join our team",
    "teilzeit",
    "minijob",
    "aushilfe",
)

# Ambiguous keywords (need context checking)
WEAK_KEYWORDS: tuple[str, ...] = (
    "job",
    "stellen",
    "apply",
    "join",
    "team",
)

# Patterns that indicate false positives (exclude these contexts)
FALSE_POSITIVE_PATTERNS: tuple[str, ...] = (
    r"stellen\s+sie",  # "Stellen Sie" = ask
    r"zur\s+verfügung\s+stellen",  # "zur Verfügung stellen" = provide
    r"frage\s+stellen",  # "Frage stellen" = ask a question
    r"cookie",  # Cookie/privacy text
    r"datenschutz",  # Privacy
    r"impressum",  # Legal notice
    r"reservier",  # Reservation
    r"kontaktformular",  # Contact form
    r"weihnachts",  # Christmas menu
    r"speisekarte",  # Menu
)

VENDOR_KEYWORDS: tuple[str, ...] = (
    "personio",
    "greenhouse",
    "workable",
    "lever",
    "smartrecruiters",
    "join.com",
    "teamtailor",
    "recruitee",
    "ashby",
    "bamboohr",
    "jobylon",
    "workday",
    "icims",
)

SNIPPET_RADIUS = 200


def extract_job_links(
    html: str,
    base_url: str,
    *,
    keywords: Iterable[str] | None = None,
) -> list[str]:
    """Return candidate job links discovered in the given HTML."""

    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    found: list[str] = []
    seen: set[str] = set()

    keyword_list = keywords if keywords is not None else list(STRONG_KEYWORDS) + list(WEAK_KEYWORDS)
    lowered_keywords = tuple(k.lower() for k in keyword_list)

    for anchor in soup.find_all("a"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        href_lower = href.lower()
        # Skip mailto, tel, and javascript links
        if href_lower.startswith(("mailto:", "tel:", "javascript:")):
            continue

        text = anchor.get_text(" ", strip=True)
        text_lower = text.lower()

        # Check if link contains job keywords before processing
        has_job_keyword = any(keyword in text_lower or keyword in href_lower for keyword in lowered_keywords)
        
        # Skip pure anchor links (#something) unless they contain job keywords
        if href_lower.startswith("#") and not has_job_keyword:
            continue

        if not has_job_keyword:
            continue

        absolute_url = urljoin(base_url, href)
        if not absolute_url or absolute_url in seen:
            continue

        seen.add(absolute_url)
        found.append(absolute_url)

    return found


def detect_hiring_signals(
    html: str,
    *,
    keywords: Iterable[str] | None = None,
    vendor_keywords: Iterable[str] = VENDOR_KEYWORDS,
) -> tuple[bool, str | None, str | None]:
    """Inspect HTML for hiring signals.

    Returns:
        Tuple of (is_hiring, matched_keyword, snippet)
    """

    if not html:
        return False, None, None

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    normalized = _normalize_whitespace(text)
    lowered = normalized.lower()

    # Check if this is clearly a "team/about" page, not a jobs page
    team_page_indicators = (
        "meet the team",
        "meet our team",
        "unser team",
        "unser team kennenlernen",
        "about us",
        "über uns",
        "our story",
        "unsere geschichte",
    )
    if any(indicator in lowered for indicator in team_page_indicators):
        # Only accept if there are strong job signals beyond just navigation
        strong_job_signals = ("bewerb", "apply", "offene stellen", "stellenangebote", "hiring", "karriere")
        if not any(signal in lowered for signal in strong_job_signals):
            return False, None, None

    # Check for false positive patterns
    has_false_positive_context = any(
        re.search(pattern, lowered, re.IGNORECASE) for pattern in FALSE_POSITIVE_PATTERNS
    )

    # Determine which keywords to check
    if keywords is not None:
        keyword_list = list(keywords)
        weak_keywords_set = {k.lower() for k in WEAK_KEYWORDS}
        strong_keywords_set = {k.lower() for k in STRONG_KEYWORDS}
    else:
        keyword_list = list(STRONG_KEYWORDS) + list(WEAK_KEYWORDS)
        weak_keywords_set = {k.lower() for k in WEAK_KEYWORDS}
        strong_keywords_set = {k.lower() for k in STRONG_KEYWORDS}

    # Check keywords in order: strong first, then weak
    for keyword in keyword_list:
        keyword_lower = keyword.lower()
        is_weak = keyword_lower in weak_keywords_set
        is_strong = keyword_lower in strong_keywords_set

        # Skip weak keywords if false positive context detected
        if is_weak and has_false_positive_context:
            continue

        match = lowered.find(keyword_lower)
        if match != -1:
            # Always validate context for weak keywords
            if is_weak and not _is_valid_job_context(normalized, match, keyword_lower):
                continue
            snippet = _make_snippet(normalized, match, len(keyword_lower))
            return True, keyword, snippet

    lowered_html = html.lower()
    for vendor in vendor_keywords:
        if vendor in lowered_html:
            snippet = f"Detected vendor keyword '{vendor}'"
            return True, vendor, snippet

    return False, None, None


def _is_valid_job_context(text: str, match_pos: int, keyword: str) -> bool:
    """Check if keyword match is in a valid job-related context."""
    # Extract context around the match (50 chars before and after)
    start = max(0, match_pos - 50)
    end = min(len(text), match_pos + len(keyword) + 50)
    context = text[start:end].lower()

    # Check for false positive indicators in context
    false_positive_indicators = (
        "stellen sie",
        "zur verfügung stellen",
        "frage stellen",
        "cookie",
        "datenschutz",
        "impressum",
        "reservier",
        "kontaktformular",
        "weihnachts",
        "speisekarte",
        "menü",
        "menu",
    )
    for indicator in false_positive_indicators:
        if indicator in context:
            return False

    # Check for positive indicators (job-related words nearby)
    positive_indicators = (
        "job",
        "karriere",
        "bewerb",
        "mitarbeit",
        "aushilfe",
        "teilzeit",
        "minijob",
        "offene",
        "position",
        "stelle",
    )
    for indicator in positive_indicators:
        if indicator in context and indicator != keyword:
            return True

    # For "stellen" specifically, require it to be capitalized or in compound
    if keyword == "stellen":
        # Check if it's "Stellen" (capitalized) or part of "Stellenangebote"
        original_context = text[start:end]
        if "stellenangebote" in original_context.lower():
            return True
        # Check if it appears capitalized (more likely to be noun = positions)
        if "Stellen" in original_context:
            return True
        # Otherwise, it's likely a verb = ask/provide
        return False

    # Default: accept weak keywords if no false positives found
    return True


def _make_snippet(text: str, start: int, match_length: int) -> str:
    half_radius = SNIPPET_RADIUS // 2
    begin = max(start - half_radius, 0)
    end = min(len(text), start + match_length + half_radius)
    snippet = text[begin:end].strip()
    snippet = re.sub(r"\s+", " ", snippet)
    if len(snippet) > SNIPPET_RADIUS:
        snippet = snippet[: SNIPPET_RADIUS - 1].rstrip() + "…"
    return snippet


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()

