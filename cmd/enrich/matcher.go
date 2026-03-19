package main

import (
	"strings"
	"unicode"
)

// WikiMatch holds a matched Wikipedia record with confidence scoring.
type WikiMatch struct {
	Record     *WikiRecord
	Confidence float64
	Signals    []string // what matched: "exact_title", "year", "director", "cast:3", etc.
}

// WikiMatcher indexes Wikipedia records for fast multi-signal matching.
type WikiMatcher struct {
	// Multiple wiki records per normalized title (handles disambiguation)
	byTitle map[string][]*WikiRecord
	// Director+year → records (secondary index)
	byDirectorYear map[string][]*WikiRecord
	// All records
	all []*WikiRecord
}

func NewWikiMatcher(movies, tv []WikiRecord) *WikiMatcher {
	m := &WikiMatcher{
		byTitle:        make(map[string][]*WikiRecord, len(movies)+len(tv)),
		byDirectorYear: make(map[string][]*WikiRecord),
	}
	for i := range movies {
		m.index(&movies[i])
	}
	for i := range tv {
		m.index(&tv[i])
	}
	return m
}

func (m *WikiMatcher) index(r *WikiRecord) {
	m.all = append(m.all, r)

	// Index by normalized title (with and without disambiguation)
	key := normTitle(r.Title)
	m.byTitle[key] = append(m.byTitle[key], r)

	clean := stripDisambig(r.Title)
	if clean != r.Title {
		cleanKey := normTitle(clean)
		m.byTitle[cleanKey] = append(m.byTitle[cleanKey], r)
	}

	// Index by director+year for secondary lookup
	if r.Director != "" && r.Year != "" {
		dirKey := normTitle(r.Director) + "::" + r.Year
		m.byDirectorYear[dirKey] = append(m.byDirectorYear[dirKey], r)
	}
}

// Match finds the best Wikipedia match for a TMDB entry.
// Returns nil if no match above the confidence threshold (0.50).
func (m *WikiMatcher) Match(title, origTitle, year, director string, castNames []string) *WikiMatch {
	candidates := m.findCandidates(title, origTitle, year, director)
	if len(candidates) == 0 {
		return nil
	}

	var best *WikiMatch
	for _, cand := range candidates {
		match := m.score(cand, title, origTitle, year, director, castNames)
		if match.Confidence >= 0.50 && (best == nil || match.Confidence > best.Confidence) {
			best = match
		}
	}
	return best
}

// findCandidates gathers all possible wiki records that could match.
func (m *WikiMatcher) findCandidates(title, origTitle, year, director string) []*WikiRecord {
	seen := make(map[*WikiRecord]bool)
	var candidates []*WikiRecord

	add := func(records []*WikiRecord) {
		for _, r := range records {
			if !seen[r] {
				seen[r] = true
				candidates = append(candidates, r)
			}
		}
	}

	// By title (primary)
	add(m.byTitle[normTitle(title)])

	// By original title
	if origTitle != "" && origTitle != title {
		add(m.byTitle[normTitle(origTitle)])
	}

	// By title without "The" prefix
	noThe := strings.TrimPrefix(strings.ToLower(title), "the ")
	if noThe != strings.ToLower(title) {
		add(m.byTitle[normTitle(noThe)])
	}

	// By director+year (catches title mismatches, e.g., localized titles)
	if director != "" && year != "" {
		dirKey := normTitle(director) + "::" + year
		add(m.byDirectorYear[dirKey])
	}

	return candidates
}

// score computes confidence for a single candidate.
//
// Scoring:
//   title exact match:      +0.35
//   title normalized match: +0.25
//   year match:             +0.30
//   director match:         +0.20
//   each cast overlap:      +0.05 (up to 3 = +0.15)
//   max: 1.0
func (m *WikiMatcher) score(cand *WikiRecord, title, origTitle, year, director string, castNames []string) *WikiMatch {
	var confidence float64
	var signals []string

	// --- Title signal ---
	candTitle := cand.Title
	candClean := stripDisambig(candTitle)

	if strings.EqualFold(title, candTitle) || strings.EqualFold(title, candClean) {
		confidence += 0.35
		signals = append(signals, "exact_title")
	} else if strings.EqualFold(origTitle, candTitle) || strings.EqualFold(origTitle, candClean) {
		confidence += 0.30
		signals = append(signals, "exact_original_title")
	} else if normTitle(title) == normTitle(candClean) || normTitle(title) == normTitle(candTitle) {
		confidence += 0.25
		signals = append(signals, "normalized_title")
	} else if origTitle != "" && (normTitle(origTitle) == normTitle(candClean) || normTitle(origTitle) == normTitle(candTitle)) {
		confidence += 0.20
		signals = append(signals, "normalized_original_title")
	} else {
		// Title doesn't match at all — this candidate came from director+year index
		confidence += 0.05
		signals = append(signals, "director_year_only")
	}

	// --- Year signal ---
	if year != "" && cand.Year != "" && year == cand.Year {
		confidence += 0.30
		signals = append(signals, "year")
	}

	// --- Director signal ---
	if director != "" && cand.Director != "" {
		if fuzzyNameMatch(director, cand.Director) {
			confidence += 0.20
			signals = append(signals, "director")
		}
	}

	// --- Cast signal ---
	if len(castNames) > 0 && len(cand.Starring) > 0 {
		wikiCastSet := make(map[string]bool, len(cand.Starring))
		for _, name := range cand.Starring {
			wikiCastSet[normTitle(name)] = true
		}
		overlap := 0
		for _, name := range castNames {
			if wikiCastSet[normTitle(name)] {
				overlap++
			}
		}
		if overlap > 3 {
			overlap = 3
		}
		if overlap > 0 {
			confidence += float64(overlap) * 0.05
			signals = append(signals, "cast:"+strings.Repeat("*", overlap))
		}
	}

	// Cap at 1.0
	if confidence > 1.0 {
		confidence = 1.0
	}

	return &WikiMatch{
		Record:     cand,
		Confidence: confidence,
		Signals:    signals,
	}
}

// fuzzyNameMatch compares two person names loosely.
// Handles "Christopher Nolan" vs "Christopher Nolan", "C. Nolan", reversed order, etc.
func fuzzyNameMatch(a, b string) bool {
	na := normTitle(a)
	nb := normTitle(b)
	if na == nb {
		return true
	}
	// Check if one contains the other's last name
	partsA := strings.Fields(strings.ToLower(a))
	partsB := strings.Fields(strings.ToLower(b))
	if len(partsA) > 0 && len(partsB) > 0 {
		lastA := partsA[len(partsA)-1]
		lastB := partsB[len(partsB)-1]
		if lastA == lastB && len(lastA) > 3 {
			return true
		}
	}
	return false
}

// extractCastNames pulls just the names from a "Name as Character|Name2 as Char2" string.
func extractCastNames(castStr string) []string {
	if castStr == "" {
		return nil
	}
	var names []string
	for _, entry := range strings.Split(castStr, "|") {
		parts := strings.SplitN(strings.TrimSpace(entry), " as ", 2)
		name := strings.TrimSpace(parts[0])
		if name != "" {
			names = append(names, name)
		}
	}
	return names
}

// normTitle strips non-alphanumeric characters and lowercases.
// Redefined here since it's in main.go — using the same logic.
func normTitleMatcher(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
