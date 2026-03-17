from __future__ import annotations

import asyncio
import fnmatch
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from .config import CrawlConfig


class Frontier:
    def __init__(self, config: CrawlConfig) -> None:
        self.config = config
        self._queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        self._seen: set[str] = set()
        self._allowed_domains: set[str] = set()
        self._count = 0

    def add_seeds(self, urls: list[str]) -> None:
        if self.config.stay_on_domain:
            for url in urls:
                domain = urlparse(url).netloc
                if domain:
                    self._allowed_domains.add(domain)
        for url in urls:
            self.add(url, depth=0)

    def add(self, url: str, depth: int) -> bool:
        if self.config.max_depth and depth > self.config.max_depth:
            return False

        normalized = _normalize(url)
        if normalized in self._seen:
            return False

        parsed = urlparse(normalized)
        if self.config.stay_on_domain and self._allowed_domains:
            if parsed.netloc not in self._allowed_domains:
                return False

        if self.config.include_patterns:
            if not any(fnmatch.fnmatch(normalized, p) for p in self.config.include_patterns):
                return False

        if self.config.exclude_patterns:
            if any(fnmatch.fnmatch(normalized, p) for p in self.config.exclude_patterns):
                return False

        self._seen.add(normalized)
        self._queue.put_nowait((url, depth))
        self._count += 1
        return True

    async def get(self) -> tuple[str, int]:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def seen_count(self) -> int:
        return len(self._seen)


def _normalize(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    # Sort query parameters for consistent dedup
    query = urlencode(sorted(parse_qsl(parsed.query)))
    # Drop fragment
    return urlunparse((scheme, netloc, path, parsed.params, query, ""))
