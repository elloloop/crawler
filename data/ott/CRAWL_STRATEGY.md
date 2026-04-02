# OTT & Cinema Crawl Strategy

Based on probing 37 OTT platforms and 14 cinema chains (April 2026).

## Priority 1: Already Done

| Platform | Records | Strategy |
|----------|---------|----------|
| aha (India) | 4,572 | Sitemap + JSON-LD |
| Hotstar (India) | 11,159 | Googlebot SSR + JSON-LD |

## Priority 2: Ready to Crawl (clean data, no blocking)

| Platform | Region | URLs | Data Format | Add to ottcrawl |
|----------|--------|------|-------------|-----------------|
| **Sun NXT** | India | 10,400+ | Movie JSON-LD, SSR for Googlebot | Yes |
| **Viki** | Global/Asian | 5,000+ | TVSeries JSON-LD with cast/ratings | Yes |
| **Now TV** | UK | 50,000+ | TVSeries JSON-LD | Yes |
| **WeTV** | SE Asia | 4,000+ sub-sitemaps | VideoObject JSON-LD | Yes |
| **Fandango** | US | 45,000 movies + 3,008 theaters | Movie JSON-LD | Yes (cinema) |
| **Cineworld** | UK | 119 cinemas | Open REST API (`/uk/data-api-service`) | New tool |
| **Vue** | UK | 90+ cinemas | Public API + Next.js SSR | New tool |
| **Everyman** | UK | 673 URLs | Gatsby SSR, sitemap | Yes |
| **Showcase** | UK | 350 URLs | Gatsby SSR, sitemap | Yes |
| **Cinepolis India** | India | City-based | Next.js `__NEXT_DATA__` SSR | New tool |

## Priority 3: Crawlable with Googlebot UA

| Platform | Region | URLs | Notes |
|----------|--------|------|-------|
| **ZEE5** | India | 4,226 movies | Dramatic SSR for Googlebot (1843x response difference). Akamai blocks regular UA. |
| **Shahid** | Middle East | 100,000+ | Needs recognized bot UA. Bilingual ar/en. |
| **Mubi** | Global | 150,000 films | No JSON-LD but rich meta tags. 5s crawl-delay. |

## Priority 4: Partial/Stale Data

| Platform | Region | Issue | Workaround |
|----------|--------|-------|------------|
| ETV Win | India (Telugu) | 1,662 URLs, no JSON-LD, blocks AI bots | Googlebot UA, parse HTML |
| Eros Now | India | 50K URLs but stale (2019 lastmod) | Crawl but mark as potentially outdated |
| BritBox | UK/US | Geo-restricted | VPN/proxy needed |
| Crave | Canada | SPA, no SSR | Headless browser |
| iQIYI | Global | Stale sitemaps | Investigate mobile API |
| Globoplay | Brazil | 500K+ URLs, blocks AI bots | Standard UA, headless |

## Priority 5: Not Crawlable (use TMDB watch/providers instead)

| Platform | Reason |
|----------|--------|
| Netflix | No sitemap, blocks everything |
| Amazon Prime Video | JS-rendered, auth required |
| Disney+ | No sitemap, blocks crawlers |
| HBO Max / Max | JS-rendered |
| Hulu | Auth required |
| Peacock | Auth required |
| Apple TV+ | No sitemap |
| Paramount+ | Auth required |
| JioCinema | Intentionally non-crawlable (Disallow: /) |
| Crunchyroll | Cloudflare challenge on everything |
| ALTBalaji | SPA-only, no content in HTML |
| Hoichoi | Cloudflare blocks |
| Lionsgate Play | Appears defunct (403 on everything) |
| Stan (Australia) | Tiny sitemap, SPA |
| Viu | Strict geo-restriction |
| Showmax | Migrating to DStv Stream |

**For these platforms, TMDB watch/providers API already gives us availability data.**

## Cinema Chains

### Crawlable
| Chain | Country | Strategy |
|-------|---------|----------|
| **Fandango** | US | Sitemap (45K movies, 3K theaters) + Movie JSON-LD |
| **Cineworld** | UK | REST API at `/uk/data-api-service` (119 locations) |
| **Vue** | UK | API at `/api/microservice/showings/cinemas` (90+ cinemas) |
| **Cinepolis** | India | Next.js SSR `__NEXT_DATA__` per city |
| **Everyman** | UK | Gatsby sitemap (673 URLs, 50+ theaters) |
| **Showcase** | UK | Gatsby sitemap (350 URLs, 16 cinemas, 130 movies) |
| **Picturehouse** | UK | Laravel PWA, sitemap (1,174 URLs) |
| **AMC** | US | Sitemaps accessible (7 sub-sitemaps) but Queue-it on pages |

### Blocked
| Chain | Country | Issue |
|-------|---------|-------|
| PVR INOX | India | Akamai blocks everything |
| BookMyShow | India | Cloudflare blocks everything |
| Odeon | UK | Cloudflare managed challenge |
| Curzon | UK | Cloudflare blocks |
| Regal | US | Cloudflare blocks |
| Cinemark | US | Cloudflare blocks |

### Recommended Aggregator APIs (for blocked chains)
| API | Coverage | Notes |
|-----|----------|-------|
| International Showtimes | 120+ countries, 25K+ cinemas | Free trial |
| MovieGlu | 125+ countries | Requires API key |
| Fandango API | US + Canada | Covers AMC, Regal, Cinemark |

**No cinema site implements `ScreeningEvent` JSON-LD** — this is an industry-wide gap.

## Refresh Schedule

| Data Type | Frequency | Reason |
|-----------|-----------|--------|
| Cinema showtimes | Daily 6 AM local | Shows change daily |
| OTT catalogs (direct crawl) | Weekly | New content weekly |
| TMDB watch/providers | Weekly | Availability changes slowly |
| TMDB metadata | Monthly | Movie data rarely changes |
| IMDb datasets | Monthly | Updated daily but changes are incremental |
| Wikipedia dump | Quarterly | Dump refreshed monthly, content changes slowly |
