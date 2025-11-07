"""HTTP crawling utilities for the Berlin job scraper MVP."""

from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass
from collections import defaultdict
from urllib.parse import urlparse
import time

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from .robots import RobotsCache

logger = logging.getLogger(__name__)

DEFAULT_ACCEPT_HEADER = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
)


@dataclass(slots=True)
class FetchResult:
    """Describes the outcome of fetching a URL."""

    url: str
    final_url: str | None
    status_code: int | None
    content_type: str | None
    text: str | None
    error: str | None


class AsyncCrawler:
    """Polite async crawler with concurrency limits and robots.txt checks."""

    def __init__(
        self,
        *,
        user_agent: str,
        concurrency: int = 5,
        request_jitter: tuple[float, float] = (0.2, 0.8),
        max_attempts: int = 3,
        respect_robots: bool = True,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")

        jitter_min, jitter_max = sorted(request_jitter)
        self._request_jitter = (jitter_min, jitter_max)

        self._user_agent = user_agent
        self._max_attempts = max_attempts

        timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=read_timeout,
            pool=connect_timeout,
        )
        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": user_agent,
                "Accept": DEFAULT_ACCEPT_HEADER,
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
            },
            timeout=timeout,
            http2=True,
            follow_redirects=True,
        )

        self._semaphore = asyncio.Semaphore(concurrency)
        self._robots = RobotsCache(self._client) if respect_robots else None

        # Per-host throttling to reduce 429s
        self._host_semaphores: dict[str, asyncio.Semaphore] = defaultdict(lambda: asyncio.Semaphore(2))
        self._host_last_request_ts: dict[str, float] = defaultdict(float)
        self._host_min_interval_s: float = 1.0  # seconds between requests per host

    async def __aenter__(self) -> "AsyncCrawler":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(self, url: str) -> FetchResult:
        """Fetch *url* while respecting concurrency and robots.txt."""

        # Sanitize URL to avoid invalid characters (e.g., embedded newlines)
        original_url = url
        url = _sanitize_url(url)
        if url != original_url:
            logger.debug("Sanitized URL from %r to %r", original_url, url)

        async with self._semaphore:
            host = urlparse(url).hostname or ""
            host_sem = self._host_semaphores[host]
            async with host_sem:
                if self._robots:
                    allowed = await self._robots.allows(url, self._user_agent)
                    if not allowed:
                        logger.debug("Robots disallows %s", url)
                        return FetchResult(
                            url=url,
                            final_url=None,
                            status_code=None,
                            content_type=None,
                            text=None,
                            error="disallowed_by_robots",
                        )

                # Enforce minimal spacing per host and add jitter
                now = time.monotonic()
                since_last = now - self._host_last_request_ts[host]
                wait_for = max(0.0, self._host_min_interval_s - since_last)
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
                await asyncio.sleep(random.uniform(*self._request_jitter))

                try:
                    response = await self._get_with_retries(url)
                except httpx.RequestError as exc:
                    logger.debug("Request error for %s: %s", url, exc)
                    return FetchResult(
                        url=url,
                        final_url=None,
                        status_code=None,
                        content_type=None,
                        text=None,
                        error=str(exc),
                    )
                finally:
                    self._host_last_request_ts[host] = time.monotonic()

        return self._build_result(url, response)

    async def _get_with_retries(self, url: str) -> httpx.Response:
        """Retry on transient errors and back off on HTTP 429 Too Many Requests."""
        last_exc: httpx.RequestError | None = None
        response: httpx.Response | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.get(url)
            except httpx.RequestError as exc:
                last_exc = exc
                # exponential backoff with jitter
                delay = min(2.0 * attempt, 6.0) + random.uniform(0.2, 0.8)
                await asyncio.sleep(delay)
                continue

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after is not None else None
                except ValueError:
                    delay = None
                if delay is None:
                    delay = min(2.0 * attempt, 6.0) + random.uniform(0.2, 0.8)
                await asyncio.sleep(delay)
                continue

            # For all other status codes, return immediately
            return response

        if last_exc is not None:
            raise last_exc
        assert response is not None  # for type checkers
        return response

    def _build_result(self, url: str, response: httpx.Response) -> FetchResult:
        content_type = response.headers.get("content-type")
        text: str | None = None

        try:
            if content_type is None or any(
                token in content_type.lower()
                for token in ("text", "html", "xml", "json")
            ):
                text = response.text
        except UnicodeDecodeError:  # pragma: no cover - extremely rare
            logger.debug("Failed to decode response text for %s", url)
            text = None

        return FetchResult(
            url=url,
            final_url=str(response.url) if response.url is not None else url,
            status_code=response.status_code,
            content_type=content_type,
            text=text,
            error=None,
        )


def _sanitize_url(url: str) -> str:
    """Remove control characters and normalize basic whitespace in URLs.

    - Strips leading/trailing whitespace
    - Removes ASCII control chars (including CR/LF, tabs)
    - Replaces literal spaces with %20
    """
    if not url:
        return url
    # Remove control characters (0x00-0x1F, 0x7F)
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", url).strip()
    # Replace remaining spaces with %20
    if " " in cleaned:
        cleaned = cleaned.replace(" ", "%20")
    return cleaned

