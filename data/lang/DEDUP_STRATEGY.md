# Deduplication Strategy

## The Problem

Same movie appears differently across sources:
- "Brundavanam" (Wiki) vs "Brindavanam" (TMDB) — transliteration
- "Baahubali: The Beginning" vs "Baahubali - The Beginning" — punctuation
- "Pushpa: The Rise" vs "Pushpa: The Rise - Part 01" — subtitle variation
- "RRR" vs "RRR (Rise Roar Revolt)" — abbreviation

Telugu alone has 10,154 unique title+year keys across 5 sources, but only 2,606 confirmed matches.

## Multi-Pass Matching Strategy

### Pass 1: Exact normalized match (highest confidence)
```
normalize(title) + "::" + year
```
Strip punctuation, lowercase, remove spaces. Match if identical.
- Confidence: 0.95
- Catches: ~70% of true matches

### Pass 2: Transliteration-aware match
Common Indian language transliteration swaps:
```
u ↔ oo (Brundavanam ↔ Brindavanam)
i ↔ ee (Sita ↔ Seeta)
sh ↔ s (Shiva ↔ Siva)
th ↔ t (Prabhas ↔ Prabhath)
aa ↔ a (Baahubali ↔ Bahubali)
double consonants (Nuvvu ↔ Nuvu)
```
Apply all swaps to both titles, compare normalized versions.
- Confidence: 0.85
- Catches: ~15% more matches

### Pass 3: Subtitle/suffix stripping
```
"Pushpa: The Rise - Part 01" → "pushpatherise"
"Pushpa: The Rise" → "pushpatherise"
```
Strip common suffixes: Part 1/2/3, Chapter 1, Volume 1, The Movie, Reloaded.
Strip content after `:`, `-`, `(`.
- Confidence: 0.80
- Catches: sequels, extended titles

### Pass 4: Director + year match (no title needed)
If two entries from different sources have same director + same year, and the fuzzy title similarity > 0.7, they're likely the same.
- Confidence: 0.75
- Catches: completely different transliterations

### Pass 5: TMDB ID / IMDb ID bridge
If a Wikipedia article contains the TMDB or IMDb ID in its external links, that's a definitive match regardless of title.
- Confidence: 1.0
- Catches: edge cases where titles are completely different (localized vs international)

## Confidence Scoring

Each match gets a score based on which passes it matched through:

| Match Type | Score |
|------------|-------|
| Same TMDB/IMDb ID | 1.00 |
| Exact title + year | 0.95 |
| Transliteration match + year | 0.85 |
| Stripped title match + year | 0.80 |
| Director + year + fuzzy > 0.7 | 0.75 |
| Title-only (no year) | 0.40 (below threshold) |

Minimum threshold: 0.50 — below this, treat as separate titles.

## Conflict Resolution

When the same title matches from multiple sources:
1. TMDB is the **primary** record (richest metadata, person IDs)
2. Wikipedia adds: plot summary, categories
3. Wiki year-lists add: director, cast, production company (fills TMDB gaps)
4. OTT adds: platform-specific thumbnails, banners, availability
5. IMDb adds: ratings, votes

If two TMDB entries might be duplicates (same title, same year, different IDs), keep both and flag for manual review.

## Implementation

The `langmerge` tool handles this. Planned enhancements:
- [ ] Add transliteration swap table for Indian languages
- [ ] Add subtitle/suffix stripping
- [ ] Add director+year matching
- [ ] Output confidence score per merge
- [ ] Output `_dedup_info` field tracking all matches and their scores
- [ ] Separate "confirmed" vs "unconfirmed" output files
