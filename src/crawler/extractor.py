from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urljoin

from bs4 import BeautifulSoup


@dataclass
class PageData:
    url: str
    title: str | None = None
    text: str = ""
    links: list[str] = field(default_factory=list)
    meta_description: str | None = None
    meta_robots: str | None = None
    canonical_url: str | None = None


def extract(url: str, html: str) -> PageData:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # Remove script/style elements before extracting text
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)[:5000]

    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        absolute = urljoin(url, href)
        if absolute.startswith(("http://", "https://")):
            links.append(absolute)

    meta_desc_tag = soup.find("meta", attrs={"name": "description"})
    meta_description = meta_desc_tag.get("content") if meta_desc_tag else None

    meta_robots_tag = soup.find("meta", attrs={"name": "robots"})
    meta_robots = meta_robots_tag.get("content") if meta_robots_tag else None

    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    canonical_url = canonical_tag.get("href") if canonical_tag else None

    return PageData(
        url=url,
        title=title,
        text=text,
        links=links,
        meta_description=meta_description,
        meta_robots=meta_robots,
        canonical_url=canonical_url,
    )


def check_meta_robots(meta_robots: str | None) -> tuple[bool, bool]:
    """Returns (should_index, should_follow)."""
    if not meta_robots:
        return True, True

    directives = [d.strip().lower() for d in meta_robots.split(",")]
    should_index = "noindex" not in directives
    should_follow = "nofollow" not in directives
    return should_index, should_follow
