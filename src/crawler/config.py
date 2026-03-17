from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class CrawlConfig:
    seed_urls: list[str] = field(default_factory=list)
    mode: Literal["polite", "full"] = "polite"
    max_depth: int = 3
    max_pages: int = 100
    concurrency: int = 5
    delay: float = 1.0
    stay_on_domain: bool = True
    output_path: str = "output.jsonl"
    user_agent: str = "CrawlerBot/0.1 (+https://github.com/elloloop/crawler)"
    timeout: float = 30.0
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    verbose: bool = False

    @property
    def is_polite(self) -> bool:
        return self.mode == "polite"

    @classmethod
    def from_cli_args(cls, args: argparse.Namespace) -> CrawlConfig:
        return cls(
            seed_urls=args.urls,
            mode=args.mode,
            max_depth=args.depth,
            max_pages=args.max_pages,
            concurrency=args.concurrency,
            delay=args.delay,
            stay_on_domain=not args.no_stay_on_domain,
            output_path=args.output,
            user_agent=args.user_agent,
            timeout=args.timeout,
            include_patterns=args.include or [],
            exclude_patterns=args.exclude or [],
            verbose=args.verbose,
        )
