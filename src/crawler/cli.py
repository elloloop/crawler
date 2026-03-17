from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .config import CrawlConfig
from .engine import run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler",
        description="A two-mode async web crawler",
    )
    parser.add_argument(
        "urls",
        nargs="+",
        help="Seed URL(s) to start crawling from",
    )
    parser.add_argument(
        "--mode",
        choices=["polite", "full"],
        default="polite",
        help="Crawl mode: 'polite' respects robots.txt/sitemaps, 'full' crawls everything (default: polite)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=3,
        help="Maximum crawl depth (default: 3)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Maximum number of pages to crawl, 0 for unlimited (default: 100)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent workers (default: 5)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Minimum delay between requests to the same domain in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--no-stay-on-domain",
        action="store_true",
        help="Allow crawling external domains (default: stay on seed domains)",
    )
    parser.add_argument(
        "--output", "-o",
        default="output.jsonl",
        help="Output file path (default: output.jsonl)",
    )
    parser.add_argument(
        "--user-agent",
        default="CrawlerBot/0.1 (+https://github.com/elloloop/crawler)",
        help="User-Agent string",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--include",
        action="append",
        help="URL pattern to include (glob-style, can be specified multiple times)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        help="URL pattern to exclude (glob-style, can be specified multiple times)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = CrawlConfig.from_cli_args(args)

    level = logging.DEBUG if config.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        print("\nCrawl interrupted by user.")
        sys.exit(1)
