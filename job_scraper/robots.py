"""Robots.txt utilities for the Berlin job scraper MVP."""

from __future__ import annotations

import asyncio
import logging
from typing import Dict
from urllib import robotparser
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


def _origin_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


class RobotsCache:
    """Per-host robots.txt cache backed by an async HTTP client."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        request_timeout: float = 10.0,
    ) -> None:
        self._client = client
        self._request_timeout = request_timeout
        self._parsers: Dict[str, robotparser.RobotFileParser | None] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def allows(self, url: str, user_agent: str) -> bool:
        """Return True if crawling *url* is permitted for *user_agent*."""

        origin = _origin_from_url(url)
        if not origin:
            logger.debug("URL %s lacks a valid origin; allowing crawl", url)
            return True

        parser = await self._get_parser(origin)
        if parser is None:
            return True

        return parser.can_fetch(user_agent, url)

    async def _get_parser(
        self,
        origin: str,
    ) -> robotparser.RobotFileParser | None:
        parser = self._parsers.get(origin)
        if parser is not None or origin in self._parsers:
            return parser

        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:
            parser = self._parsers.get(origin)
            if parser is not None or origin in self._parsers:
                return parser

            robots_url = f"{origin}/robots.txt"

            try:
                response = await self._client.get(
                    robots_url,
                    timeout=self._request_timeout,
                    headers={"User-Agent": "BerlinJobScraper/robots-fetch"},
                )
            except httpx.RequestError as exc:  # pragma: no cover - network failure
                logger.debug("Failed to fetch robots.txt for %s: %s", origin, exc)
                self._parsers[origin] = None
                return None

            status = response.status_code
            if status == 404:
                logger.debug("robots.txt not found for %s (404); allowing crawl", origin)
                self._parsers[origin] = None
                return None

            if status in (401, 403):
                logger.info("robots.txt restricted for %s (status %s); disallowing", origin, status)
                parser = robotparser.RobotFileParser()
                parser.parse(["User-agent: *", "Disallow: /"])
                self._parsers[origin] = parser
                return parser

            if status >= 500:
                logger.debug("robots.txt unavailable for %s (status %s); allowing crawl", origin, status)
                self._parsers[origin] = None
                return None

            text = response.text

            parser = robotparser.RobotFileParser()
            parser.set_url(robots_url)
            parser.parse(text.splitlines())
            self._parsers[origin] = parser
            return parser

