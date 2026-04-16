package main

import (
	"regexp"
	"strings"
	"unicode"
)

// Deduplicator handles multi-pass title matching across sources.
type Deduplicator struct {
	// Primary index: exact normalized title::year → key in unified map
	exactIndex map[string]string

	// Transliteration index: transliterated title::year → key
	translitIndex map[string]string

	// Stripped index: stripped title::year → key (removes subtitles/suffixes)
	strippedIndex map[string]string

	// Director+year index: normalized director::year → []keys
	directorIndex map[string][]string

	// Original title index: normalized original_title::year → key
	origTitleIndex map[string]string
}

func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		exactIndex:     make(map[string]string),
		translitIndex:  make(map[string]string),
		strippedIndex:  make(map[string]string),
		directorIndex:  make(map[string][]string),
		origTitleIndex: make(map[string]string),
	}
}

// Index adds a title to all dedup indexes.
func (d *Deduplicator) Index(title, origTitle, year, director, key string) {
	// Pass 1: exact
	exactKey := normKey(title, year)
	if _, exists := d.exactIndex[exactKey]; !exists {
		d.exactIndex[exactKey] = key
	}

	// Also index original title
	if origTitle != "" && origTitle != title {
		origKey := normKey(origTitle, year)
		if _, exists := d.origTitleIndex[origKey]; !exists {
			d.origTitleIndex[origKey] = key
		}
	}

	// Pass 2: transliteration variants
	for _, variant := range transliterate(title) {
		tKey := normKey(variant, year)
		if _, exists := d.translitIndex[tKey]; !exists {
			d.translitIndex[tKey] = key
		}
	}

	// Pass 3: stripped title (remove subtitles/suffixes)
	stripped := stripSubtitle(title)
	if stripped != title {
		sKey := normKey(stripped, year)
		if _, exists := d.strippedIndex[sKey]; !exists {
			d.strippedIndex[sKey] = key
		}
	}

	// Also strip disambiguation
	clean := stripDisambig(title)
	if clean != title {
		cKey := normKey(clean, year)
		if _, exists := d.exactIndex[cKey]; !exists {
			d.exactIndex[cKey] = key
		}
	}

	// Pass 4: director+year
	if director != "" && year != "" {
		dirKey := normalize(director) + "::" + year
		d.directorIndex[dirKey] = append(d.directorIndex[dirKey], key)
	}
}

// Match tries to find a matching key using multi-pass strategy.
// Returns (matched_key, confidence, match_method) or ("", 0, "").
type MatchResult struct {
	Key        string
	Confidence float64
	Method     string
}

func (d *Deduplicator) Match(title, origTitle, year, director string) *MatchResult {
	// Pass 1: exact normalized title + year (0.95)
	exactKey := normKey(title, year)
	if key, ok := d.exactIndex[exactKey]; ok {
		return &MatchResult{Key: key, Confidence: 0.95, Method: "exact"}
	}

	// Pass 1b: exact original title + year
	if origTitle != "" {
		origKey := normKey(origTitle, year)
		if key, ok := d.exactIndex[origKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.93, Method: "original_title"}
		}
		if key, ok := d.origTitleIndex[origKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.93, Method: "original_title_cross"}
		}
	}

	// Pass 1c: stripped disambiguation
	clean := stripDisambig(title)
	if clean != title {
		cleanKey := normKey(clean, year)
		if key, ok := d.exactIndex[cleanKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.90, Method: "disambig_stripped"}
		}
	}

	// Pass 2: transliteration variants (0.85)
	for _, variant := range transliterate(title) {
		tKey := normKey(variant, year)
		if key, ok := d.exactIndex[tKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.85, Method: "transliteration"}
		}
		if key, ok := d.translitIndex[tKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.83, Method: "transliteration_cross"}
		}
	}

	// Pass 3: subtitle stripping (0.80)
	stripped := stripSubtitle(title)
	if stripped != title {
		sKey := normKey(stripped, year)
		if key, ok := d.exactIndex[sKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.80, Method: "subtitle_stripped"}
		}
		if key, ok := d.strippedIndex[sKey]; ok {
			return &MatchResult{Key: key, Confidence: 0.78, Method: "subtitle_stripped_cross"}
		}
	}

	// Pass 4: director + year + fuzzy title (0.75)
	if director != "" && year != "" {
		dirKey := normalize(director) + "::" + year
		if keys, ok := d.directorIndex[dirKey]; ok {
			normTitle := normalize(title)
			for _, candidate := range keys {
				// Check fuzzy similarity on the key (which is normTitle::year)
				candidateTitle := strings.Split(candidate, "::")[0]
				if fuzzyMatch(normTitle, candidateTitle) {
					return &MatchResult{Key: candidate, Confidence: 0.75, Method: "director_year_fuzzy"}
				}
			}
		}
	}

	return nil
}

// --- Transliteration ---

// Common Indian language transliteration swaps
var translitSwaps = []struct{ from, to string }{
	// Vowels
	{"aa", "a"}, {"a", "aa"},
	{"ee", "i"}, {"i", "ee"},
	{"oo", "u"}, {"u", "oo"},
	{"ou", "u"}, {"u", "ou"},
	// Consonants
	{"sh", "s"}, {"s", "sh"},
	{"th", "t"}, {"t", "th"},
	{"dh", "d"}, {"d", "dh"},
	{"bh", "b"}, {"b", "bh"},
	{"ph", "f"}, {"f", "ph"},
	{"kh", "k"}, {"k", "kh"},
	{"gh", "g"}, {"g", "gh"},
	{"ch", "c"}, {"c", "ch"},
	// Common specific swaps
	{"v", "w"}, {"w", "v"},
	{"z", "j"}, {"j", "z"},
	{"x", "ksh"}, {"ksh", "x"},
	// Double consonants
	{"mm", "m"}, {"m", "mm"},
	{"nn", "n"}, {"n", "nn"},
	{"ll", "l"}, {"l", "ll"},
	{"tt", "t"}, {"rr", "r"},
	{"pp", "p"}, {"ss", "s"},
}

func transliterate(title string) []string {
	lower := strings.ToLower(title)
	variants := make(map[string]bool)

	for _, swap := range translitSwaps {
		if strings.Contains(lower, swap.from) {
			variant := strings.ReplaceAll(lower, swap.from, swap.to)
			variants[variant] = true
		}
	}

	// Also try "The " prefix removal
	noThe := strings.TrimPrefix(lower, "the ")
	if noThe != lower {
		variants[noThe] = true
	}

	result := make([]string, 0, len(variants))
	for v := range variants {
		result = append(result, v)
	}
	return result
}

// --- Subtitle stripping ---

var (
	reSuffix  = regexp.MustCompile(`(?i)\s*[-–:]\s*(part|chapter|volume|episode|the movie|reloaded|returns|resurrection|the final|the beginning|the conclusion|special edition).*$`)
	reParens  = regexp.MustCompile(`\s*\([^)]*\)\s*$`)
	rePartNum = regexp.MustCompile(`(?i)\s+(part|vol|chapter|pt)\.?\s*\d+\s*$`)
	reRoman   = regexp.MustCompile(`(?i)\s+(I{1,3}|IV|V|VI{0,3})\s*$`)
)

func stripSubtitle(title string) string {
	s := title
	s = reSuffix.ReplaceAllString(s, "")
	s = reParens.ReplaceAllString(s, "")
	s = rePartNum.ReplaceAllString(s, "")
	// Don't strip roman numerals by default — too aggressive (Rocky II is different from Rocky)
	return strings.TrimSpace(s)
}

// --- Fuzzy matching ---

func fuzzyMatch(a, b string) bool {
	if a == b {
		return true
	}
	// Levenshtein-like: check if edit distance is < 20% of shorter string
	shorter := a
	longer := b
	if len(a) > len(b) {
		shorter, longer = b, a
	}
	if len(shorter) < 3 {
		return false
	}

	// Quick check: do they share enough common characters?
	commonChars := 0
	used := make([]bool, len(longer))
	for _, c := range shorter {
		for j, d := range longer {
			if !used[j] && c == d {
				commonChars++
				used[j] = true
				break
			}
		}
	}

	ratio := float64(commonChars) / float64(len(longer))
	return ratio > 0.75

}

// normalize for dedup: strip all non-alphanumeric, lowercase
func normalizeDedup(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
