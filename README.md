# crawler

A distributed web crawler and entertainment data pipeline. Two parts: a general-purpose gRPC crawling service, and purpose-built tools for aggregating movies, TV shows, OTT availability, trailers, and cinema showtimes from multiple sources.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Data Sources                          │
├──────────┬──────────┬──────────┬──────────┬────────────┤
│   TMDB   │   IMDb   │Wikipedia │   OTT    │  Cinemas   │
│  API     │ Datasets │  Dumps   │ Sitemaps │  Probes    │
└────┬─────┴────┬─────┴────┬─────┴────┬─────┴─────┬──────┘
     │          │          │          │            │
     ▼          ▼          ▼          ▼            ▼
┌─────────────────────────────────────────────────────────┐
│               Pipeline Tools (cmd/)                     │
│  tmdbfetch · enrich · wikifilter · ottcrawl · cinemaprobe│
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│           Cloudflare R2 (S3-compatible)                 │
│  final/all_movies.jsonl.gz  ·  final/all_tv.jsonl.gz   │
└─────────────────────────────────────────────────────────┘
```

## Data Pipeline Tools

### tmdbfetch

Fetches movies or TV shows from the TMDB API with full details in a single call per title: credits (with person IDs and profile photos), keywords, videos/trailers, and watch provider availability across 19 countries.

```bash
tmdbfetch -token $TMDB_TOKEN -type movie -output data/tmdb/movies_full.jsonl
tmdbfetch -token $TMDB_TOKEN -type tv    -output data/tmdb/tv_full.jsonl
```

- Uses TMDB daily exports for ID discovery
- `append_to_response=credits,keywords,videos,watch/providers` — one call gets everything
- 38 req/s with 10 workers, resume support
- Full cast: person_id, name, character, profile_path, gender, popularity
- Watch providers: Netflix, Prime, Disney+, Hotstar, etc. per country (US, GB, IN, AU, + 15 more)
- Videos: trailers, teasers, clips with YouTube URLs

### enrich

Merges TMDB data with IMDb ratings/cast and Wikipedia plot summaries using multi-signal confidence scoring.

```bash
enrich \
  -tmdb-movies data/tmdb/movies.jsonl \
  -tmdb-tv data/tmdb/tv_shows.jsonl \
  -wiki-movies data/movies/films.jsonl \
  -wiki-tv data/movies/tv.jsonl \
  -imdb-ratings data/imdb/title.ratings.tsv.gz \
  -imdb-cast data/imdb/title.principals.tsv.gz \
  -imdb-names data/imdb/name.basics.tsv.gz \
  -imdb-basics data/imdb/title.basics.tsv.gz \
  -output data/enriched/
```

Wikipedia matching uses multiple signals stacked for confidence:

| Signal | Score |
|--------|-------|
| Exact title match | +0.35 |
| Year match | +0.30 |
| Director match | +0.20 |
| Cast overlap (up to 3) | +0.05 each |
| Minimum threshold | 0.50 |

### wikifilter

Streams the full English Wikipedia dump (25GB compressed) and extracts movie/TV articles by detecting `{{Infobox film}}` and `{{Infobox television}}` templates.

```bash
wikifilter -input data/wiki/enwiki-latest-pages-articles.xml.bz2 -output data/movies/
```

- Streaming bzip2 + XML parse — constant memory regardless of dump size
- Extracts: title, year, director, cast, genre, plot summary, categories
- ~6,000 pages/sec, processes full dump in ~66 minutes
- Output: 193K films + 113K TV shows

### ottcrawl

Crawls OTT platform websites via their sitemaps, extracting structured data (JSON-LD, meta tags) from each content page.

```bash
ottcrawl -platform aha -output data/ott/
ottcrawl -platform hotstar -output data/ott/
ottcrawl -platform hotstar -urls-file urls.txt -output data/ott/
```

Supported platforms:

| Platform | Strategy | Content |
|----------|----------|---------|
| aha | Sitemap + JSON-LD | 1,632 movies, 2,938 shows |
| Hotstar | Googlebot SSR + JSON-LD | 6,368 movies, 4,791 shows |

Extracts: title, description, year, language, genres, cast, director, duration, thumbnail URL, banner URL, age rating, free/paid status.

### watchproviders

Fetches watch provider availability and trailers for all TMDB titles. Superseded by `tmdbfetch` which does this in the same call, but kept for backward compatibility with existing data.

### cinemaprobe

Investigates cinema chain websites for crawlability: checks robots.txt, sitemaps, JSON-LD structured data, and API endpoints.

```bash
cinemaprobe -all
cinemaprobe -domain www.cineworld.co.uk
```

Covers 14 chains across UK, India, and US.

## Crawler Service (gRPC)

A distributed web crawler with three crawl strategies, gRPC API, and pluggable infrastructure backends.

### Crawl Strategies

| Strategy | Use Case |
|----------|----------|
| **polite** | Respects robots.txt, sitemaps, crawl-delay, meta robots tags |
| **full** | Unrestricted crawling with configurable rate limits |
| **wiki** | Wikipedia dump import — zero network calls, streaming parse |

### gRPC API (4 services, 23 RPCs)

- **ProjectService** — CRUD for crawl projects with per-domain strategy configuration
- **CrawlService** — Start/stop/pause/resume crawls, live stats, page listing, streaming export
- **RateLimitService** — Runtime per-domain rate limit configuration
- **RobotsService** — Cached robots.txt data

### Infrastructure (pluggable)

| Layer | Local Mode | Distributed Mode |
|-------|-----------|-----------------|
| Queue | Go channels | Redis Streams |
| State | SQLite | Redis |
| Storage | Filesystem (JSONL) | Cloudflare R2 |
| Rate limiter | In-process | Redis token buckets |

### Running

```bash
# Local mode — zero dependencies
make build && ./bin/crawler

# Distributed mode
docker compose up
```

### Proto definitions

```
proto/crawler/v1/
├── types.proto        # CrawlStrategy, CrawlStatus, PageStatus enums
├── models.proto       # Project, SiteConfig, CrawlJob, Page, DomainRateLimit
├── service.proto      # 4 gRPC services, 23 RPCs
└── events.proto       # URLFrontierEvent, PageCrawledEvent, CrawlStatusEvent
```

## Dataset

Current dataset in Cloudflare R2 (`crawler-data` bucket):

| File | Records | Description |
|------|---------|-------------|
| `final/all_movies.jsonl.gz` | 333K | Movies: TMDB + IMDb + Wikipedia + availability + trailers |
| `final/all_tv.jsonl.gz` | 217K | TV shows: same enrichment |
| `ott/hotstar.jsonl` | 11K | Hotstar catalog with thumbnails/banners |
| `ott/aha.jsonl` | 4.5K | aha catalog with thumbnails/banners |
| `imdb/*` | 6 files | IMDb ratings, cast, names, titles, crew, episodes |
| `watchproviders/*` | 550K | Where-to-watch + trailers for all titles |

Per-title data includes:
- Title, overview, tagline, genres, keywords
- Director, full cast with person IDs and profile photos
- IMDb rating + votes, TMDB rating + votes
- Wikipedia plot summary (with confidence score)
- Availability across Netflix, Prime, Disney+, Hotstar, etc. (19 countries)
- Trailers with YouTube URLs
- Poster and backdrop image paths
- OTT-specific thumbnails and banners (where crawled)

## OTT & Cinema Registry

`data/ott/registry.json` tracks:
- **50+ OTT platforms** across 8 regions with crawl strategy per platform
- **14 cinema chains** (UK, India, US) with investigation status
- **4 showtime APIs** for global cinema coverage
- **Refresh schedule**: daily (showtimes), weekly (OTT catalogs), monthly (metadata)

## CI

`.github/workflows/crawler-health.yml` runs daily:
- Builds all tools, runs test suite
- Checks OTT sitemaps (aha, Hotstar) still accessible
- Verifies TMDB API returns expected fields
- Checks IMDb datasets still downloadable

## Storage

Cloudflare R2 (S3-compatible, zero egress fees).

```python
import boto3
s3 = boto3.client("s3",
    endpoint_url="https://<account-id>.r2.cloudflarestorage.com",
    aws_access_key_id="<key>",
    aws_secret_access_key="<secret>",
    region_name="auto",
)
s3.download_file("crawler-data", "final/all_movies.jsonl.gz", "movies.jsonl.gz")
```

## Development

```bash
make proto    # Generate Go code from proto definitions
make build    # Build all binaries
make test     # Run test suite with race detector
make docker-up # Start Redis + MinIO + crawler
```

Requires: Go 1.22+, buf (for proto generation)
