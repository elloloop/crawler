from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import IO

from .extractor import PageData
from .fetcher import FetchResult


class Writer:
    def __init__(self, output_path: str) -> None:
        self._path = Path(output_path)
        self._file: IO[str] | None = None

    def _ensure_open(self) -> IO[str]:
        if self._file is None:
            self._file = open(self._path, "a", encoding="utf-8")
        return self._file

    def write(self, page: PageData, result: FetchResult, depth: int) -> None:
        f = self._ensure_open()
        record = {
            "url": page.url,
            "final_url": result.final_url,
            "status": result.status_code,
            "depth": depth,
            "title": page.title,
            "text": page.text,
            "links": page.links,
            "meta_description": page.meta_description,
            "canonical_url": page.canonical_url,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None

    def __enter__(self) -> Writer:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
