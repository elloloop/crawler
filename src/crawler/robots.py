from __future__ import annotations

import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

from .fetcher import Fetcher

logger = logging.getLogger(__name__)


class RobotsChecker:
    def __init__(self, user_agent: str, polite: bool) -> None:
        self.user_agent = user_agent
        self.polite = polite
        self._parsers: dict[str, RobotFileParser | None] = {}

    async def _load(self, domain: str, fetcher: Fetcher) -> RobotFileParser | None:
        if domain in self._parsers:
            return self._parsers[domain]

        robots_url = f"https://{domain}/robots.txt"
        result = await fetcher.fetch(robots_url)

        if result.error or result.status_code >= 500:
            # Server error — be conservative, assume disallowed
            logger.warning("robots.txt fetch failed for %s: %s", domain, result.error or result.status_code)
            self._parsers[domain] = None
            return None

        if result.status_code == 404 or result.status_code >= 400:
            # No robots.txt — everything is allowed (per RFC 9309)
            parser = RobotFileParser()
            parser.parse([])
            self._parsers[domain] = parser
            return parser

        parser = RobotFileParser()
        content = result.html or ""
        parser.parse(content.splitlines())
        self._parsers[domain] = parser
        return parser

    async def is_allowed(self, url: str, fetcher: Fetcher) -> bool:
        if not self.polite:
            return True

        domain = urlparse(url).netloc
        parser = await self._load(domain, fetcher)

        if parser is None:
            # Failed to load — conservative: disallow
            return False

        return parser.can_fetch(self.user_agent, url)

    def get_crawl_delay(self, domain: str) -> float | None:
        parser = self._parsers.get(domain)
        if parser is None:
            return None
        delay = parser.crawl_delay(self.user_agent)
        if delay is not None:
            return float(delay)
        return None

    def get_sitemaps(self, domain: str) -> list[str]:
        parser = self._parsers.get(domain)
        if parser is None:
            return []
        return parser.site_maps() or []
