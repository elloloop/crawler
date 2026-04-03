# Crawl Priorities (Updated April 2026)

## OTT Platforms

### Principle
Only crawl platforms whose content is NOT in TMDB's watch/providers API.
TMDB covers 81 providers in India, 134 in UK, including all major ones.

### Must Crawl (NOT in TMDB)

| Platform | Region | URLs | Data Quality | Status |
|----------|--------|------|-------------|--------|
| ETV Win | India (Telugu) | 1,662 | HTML only, no JSON-LD | Ready |
| ALTBalaji | India (Hindi) | ? | SPA only | Needs headless |
| JioCinema | India | ? | Blocked (Disallow: /) | Needs headless |
| WeTV | SE Asia | 4,000+ subs | VideoObject JSON-LD | Ready |
| Shahid | Middle East | 100,000+ | Movie JSON-LD | Ready (needs bot UA) |
| Globoplay | Brazil | 500,000+ | Video sitemap | Blocks AI bots |

### Worth Crawling (in TMDB but get thumbnails/banners/local data)

| Platform | Region | URLs | Unique Value | Status |
|----------|--------|------|-------------|--------|
| aha | India | 4,572 | Telugu thumbnails, banners | Done |
| Hotstar | India | 11,159 | Hindi/regional thumbnails, banners | Done |
| ZEE5 | India | 4,226 | Regional thumbnails, local content | Ready (Googlebot) |
| Sun NXT | India | 10,400+ | South Indian thumbnails, Movie JSON-LD | Ready |

### Skip (TMDB watch/providers is sufficient)

Netflix, Prime Video, Disney+, HBO Max, Hulu, Peacock, Paramount+, Apple TV+,
SonyLIV, MX Player, Hoichoi, Crunchyroll, Lionsgate Play, MUBI, Viki, Now TV,
BritBox, Stan, Crave, Viu, Showmax, iQIYI

## Cinema Showtimes

### Working APIs (verified)

| Source | Coverage | Data | Access |
|--------|----------|------|--------|
| **Cineworld API** | 87 UK cinemas | Films + showtimes + booking links + posters | Open REST API, no auth |
| **Cinepolis India** | 22 Indian cities | Coming soon (now-showing needs city selection) | Next.js SSR `__NEXT_DATA__` |

### Cineworld API Details
```
Base: https://www.cineworld.co.uk/uk/data-api-service/v1/quickbook/10108

Cinemas list:
GET /cinemas/with-event/until/{date}?attr=&lang=en_GB
→ 87 cinemas with IDs and names

Showtimes per cinema per date:
GET /film-events/in-cinema/{cinemaId}/at-date/{date}?attr=&lang=en_GB
→ Films: name, length, rating, poster URL
→ Events: filmId, dateTime, bookingLink, attributes (IMAX, 3D, etc.)

Rate: No visible limits (test carefully)
Schedule: Daily at 6 AM GMT
```

### Needs Investigation
| Source | Coverage | Issue |
|--------|----------|-------|
| Vue | 90+ UK cinemas | Cinema list API works, showings API needs auth |
| Fandango | All US cinemas | Sitemaps accessible, but pages may need JS |
| AMC | US | Queue-it waiting room on pages, sitemaps work |

### Blocked
| Source | Issue |
|--------|-------|
| PVR INOX | Akamai blocks everything |
| BookMyShow | Cloudflare blocks everything |
| Odeon | Cloudflare managed challenge |
| Regal | Cloudflare blocks |
| Cinemark | Cloudflare blocks |

### Recommended: Use Showtime Aggregator API
For blocked chains, sign up for one of:
- **International Showtimes API** (internationalshowtimes.com) — 120+ countries, 25K+ cinemas, free trial
- **MovieGlu** (developer.movieglu.com) — 125+ countries, <500ms responses

These cover PVR INOX, BookMyShow, Odeon, AMC, Regal, Cinemark and everything else.
