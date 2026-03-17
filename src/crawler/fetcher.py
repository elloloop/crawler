from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from .config import CrawlConfig


@dataclass
class FetchResult:
    url: str
    final_url: str
    status_code: int
    content_type: str
    html: str | None
    headers: dict[str, str]
    error: str | None = None

    @property
    def is_html(self) -> bool:
        return self.html is not None and "text/html" in self.content_type


class Fetcher:
    def __init__(self, config: CrawlConfig) -> None:
        self.config = config
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(config.timeout),
            limits=httpx.Limits(
                max_connections=config.concurrency * 2,
                max_keepalive_connections=config.concurrency,
            ),
            headers={"User-Agent": config.user_agent},
        )
        self._domain_timestamps: dict[str, float] = {}
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._delay_overrides: dict[str, float] = {}

    def set_domain_delay(self, domain: str, delay: float) -> None:
        self._delay_overrides[domain] = delay

    def _get_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._domain_locks:
            self._domain_locks[domain] = asyncio.Lock()
        return self._domain_locks[domain]

    async def fetch(self, url: str) -> FetchResult:
        domain = urlparse(url).netloc
        lock = self._get_lock(domain)

        async with lock:
            delay = max(self.config.delay, self._delay_overrides.get(domain, 0))
            last = self._domain_timestamps.get(domain, 0)
            wait = delay - (time.monotonic() - last)
            if wait > 0:
                await asyncio.sleep(wait)

            try:
                response = await self._client.get(url)
                self._domain_timestamps[domain] = time.monotonic()

                content_type = response.headers.get("content-type", "")
                html = None
                if "text/html" in content_type:
                    html = response.text

                return FetchResult(
                    url=url,
                    final_url=str(response.url),
                    status_code=response.status_code,
                    content_type=content_type,
                    html=html,
                    headers=dict(response.headers),
                )
            except httpx.HTTPError as e:
                self._domain_timestamps[domain] = time.monotonic()
                return FetchResult(
                    url=url,
                    final_url=url,
                    status_code=0,
                    content_type="",
                    html=None,
                    headers={},
                    error=str(e),
                )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> Fetcher:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
