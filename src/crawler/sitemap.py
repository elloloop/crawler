from __future__ import annotations

import gzip
import logging
import xml.etree.ElementTree as ET

from .fetcher import Fetcher

logger = logging.getLogger(__name__)

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


async def parse_sitemap(url: str, fetcher: Fetcher, max_urls: int = 10000) -> list[str]:
    """Fetch and parse a sitemap, returning discovered URLs."""
    urls: list[str] = []
    await _fetch_sitemap(url, fetcher, urls, max_urls, depth=0)
    return urls


async def _fetch_sitemap(
    url: str, fetcher: Fetcher, urls: list[str], max_urls: int, depth: int
) -> None:
    if depth > 2 or len(urls) >= max_urls:
        return

    result = await fetcher.fetch(url)
    if result.error or result.status_code != 200:
        logger.warning("Failed to fetch sitemap %s: %s", url, result.error or result.status_code)
        return

    content = result.html or ""

    # Handle gzipped sitemaps
    if url.endswith(".gz"):
        try:
            raw = await _fetch_raw(url, fetcher)
            if raw:
                content = gzip.decompress(raw).decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("Failed to decompress gzipped sitemap %s: %s", url, e)
            return

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logger.warning("Failed to parse sitemap XML %s: %s", url, e)
        return

    tag = _strip_ns(root.tag)

    if tag == "sitemapindex":
        # Sitemap index — recurse into sub-sitemaps
        for sitemap in root.findall("sm:sitemap/sm:loc", SITEMAP_NS):
            if sitemap.text and len(urls) < max_urls:
                await _fetch_sitemap(sitemap.text.strip(), fetcher, urls, max_urls, depth + 1)
        # Also try without namespace
        for sitemap in root.findall("sitemap/loc"):
            if sitemap.text and len(urls) < max_urls:
                await _fetch_sitemap(sitemap.text.strip(), fetcher, urls, max_urls, depth + 1)
    elif tag == "urlset":
        for loc in root.findall("sm:url/sm:loc", SITEMAP_NS):
            if loc.text and len(urls) < max_urls:
                urls.append(loc.text.strip())
        # Also try without namespace
        for loc in root.findall("url/loc"):
            if loc.text and len(urls) < max_urls:
                urls.append(loc.text.strip())


async def _fetch_raw(url: str, fetcher: Fetcher) -> bytes | None:
    """Fetch raw bytes for gzipped content."""
    try:
        response = await fetcher._client.get(url)
        return response.content
    except Exception:
        return None


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag
