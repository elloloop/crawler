from __future__ import annotations

import asyncio
import logging
import time
from urllib.parse import urlparse

from .config import CrawlConfig
from .extractor import PageData, check_meta_robots, extract
from .fetcher import FetchResult, Fetcher
from .frontier import Frontier
from .robots import RobotsChecker
from .sitemap import parse_sitemap
from .writer import Writer

logger = logging.getLogger(__name__)


async def run(config: CrawlConfig) -> None:
    frontier = Frontier(config)
    robots = RobotsChecker(config.user_agent, config.is_polite)
    writer = Writer(config.output_path)
    pages_crawled = 0
    errors = 0
    start_time = time.monotonic()
    shutdown = asyncio.Event()

    async with Fetcher(config) as fetcher:
        # Seed the frontier
        frontier.add_seeds(config.seed_urls)

        # In polite mode, fetch robots.txt for seed domains and discover sitemaps
        if config.is_polite:
            seed_domains = {urlparse(u).netloc for u in config.seed_urls}
            for domain in seed_domains:
                await robots.is_allowed(f"https://{domain}/", fetcher)

                # Apply crawl-delay from robots.txt
                crawl_delay = robots.get_crawl_delay(domain)
                if crawl_delay is not None:
                    fetcher.set_domain_delay(domain, crawl_delay)
                    logger.info("Robots crawl-delay for %s: %.1fs", domain, crawl_delay)

                # Discover and parse sitemaps
                sitemaps = robots.get_sitemaps(domain)
                for sitemap_url in sitemaps:
                    logger.info("Parsing sitemap: %s", sitemap_url)
                    sitemap_urls = await parse_sitemap(sitemap_url, fetcher)
                    for url in sitemap_urls:
                        frontier.add(url, depth=0)
                    logger.info("Discovered %d URLs from sitemap", len(sitemap_urls))

        logger.info(
            "Starting crawl: %d seed URLs, %d queued, mode=%s, concurrency=%d",
            len(config.seed_urls), frontier.size, config.mode, config.concurrency,
        )

        active_workers = 0
        active_lock = asyncio.Lock()

        async def worker() -> None:
            nonlocal pages_crawled, errors, active_workers

            while not shutdown.is_set():
                try:
                    url, depth = await asyncio.wait_for(frontier.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    # Check if all workers are idle and queue is empty
                    async with active_lock:
                        if frontier.empty and active_workers == 0:
                            shutdown.set()
                    continue

                async with active_lock:
                    active_workers += 1

                try:
                    if config.max_pages and pages_crawled >= config.max_pages:
                        shutdown.set()
                        frontier.task_done()
                        break

                    # Check robots.txt
                    if config.is_polite:
                        if not await robots.is_allowed(url, fetcher):
                            logger.debug("Blocked by robots.txt: %s", url)
                            frontier.task_done()
                            continue

                    # Fetch the page
                    result = await fetcher.fetch(url)

                    if result.error:
                        logger.warning("Error fetching %s: %s", url, result.error)
                        errors += 1
                        frontier.task_done()
                        continue

                    if not result.is_html:
                        logger.debug("Skipping non-HTML: %s (%s)", url, result.content_type)
                        frontier.task_done()
                        continue

                    # Extract content
                    page = extract(result.final_url, result.html)

                    # Check meta robots in polite mode
                    should_index, should_follow = True, True
                    if config.is_polite:
                        should_index, should_follow = check_meta_robots(page.meta_robots)

                    # Write output
                    if should_index:
                        writer.write(page, result, depth)
                        pages_crawled += 1
                        logger.info(
                            "[%d] %s — %s (depth=%d)",
                            pages_crawled, result.status_code, result.final_url, depth,
                        )

                    # Enqueue discovered links
                    if should_follow:
                        for link in page.links:
                            frontier.add(link, depth + 1)

                    frontier.task_done()

                except Exception as e:
                    logger.error("Unexpected error processing %s: %s", url, e)
                    errors += 1
                    frontier.task_done()
                finally:
                    async with active_lock:
                        active_workers -= 1

        # Launch worker tasks
        workers = [asyncio.create_task(worker()) for _ in range(config.concurrency)]

        # Wait for completion
        await asyncio.gather(*workers)

    writer.close()

    elapsed = time.monotonic() - start_time
    print(f"\nCrawl complete:")
    print(f"  Pages crawled: {pages_crawled}")
    print(f"  Errors: {errors}")
    print(f"  URLs seen: {frontier.seen_count}")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Output: {config.output_path}")
