// tagger reads TMDB JSONL data and adds internal quality/completeness tags
// to each title for filtering during training and website display.
//
// Tags are booleans grouped by category:
//   - data completeness: has_overview, has_poster, has_cast, has_crew, etc.
//   - enrichment: has_wiki, has_imdb_rating, has_trailers, has_availability
//   - quality signals: has_budget, high_vote_count, popular, has_collection
//   - content flags: is_adult, is_released, is_english, is_indian
//   - training tiers: tier_gold (best for training), tier_silver, tier_bronze
//
// Usage:
//
//	tagger -input data/tmdb/movies_v2.jsonl -output data/tmdb/movies_tagged.jsonl
//	tagger -input data/tmdb/tv_v2.jsonl -type tv -output data/tmdb/tv_tagged.jsonl
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"
)

type Tags struct {
	// Data completeness
	HasOverview     bool `json:"has_overview"`
	HasPoster       bool `json:"has_poster"`
	HasBackdrop     bool `json:"has_backdrop"`
	HasCast         bool `json:"has_cast"`
	HasCrew         bool `json:"has_crew"`
	HasDirector     bool `json:"has_director"`
	HasGenres       bool `json:"has_genres"`
	HasKeywords     bool `json:"has_keywords"`
	HasRuntime      bool `json:"has_runtime"`
	HasReleaseDate  bool `json:"has_release_date"`
	HasOrigTitle    bool `json:"has_original_title"`
	HasTagline      bool `json:"has_tagline"`
	HasHomepage     bool `json:"has_homepage"`
	HasCollection   bool `json:"has_collection"`
	CastCount       int  `json:"cast_count"`
	CrewCount       int  `json:"crew_count"`
	KeywordCount    int  `json:"keyword_count"`

	// Enrichment availability
	HasIMDBID       bool `json:"has_imdb_id"`
	HasWikidata     bool `json:"has_wikidata_id"`
	HasExternalIDs  bool `json:"has_external_ids"`
	HasTrailers     bool `json:"has_trailers"`
	HasVideos       bool `json:"has_videos"`
	HasAvailability bool `json:"has_availability"`
	HasSimilar      bool `json:"has_similar"`
	HasAltTitles    bool `json:"has_alt_titles"`
	HasCertification bool `json:"has_certification"`
	TrailerCount    int  `json:"trailer_count"`
	VideoCount      int  `json:"video_count"`
	AvailCountries  int  `json:"avail_countries"`

	// Quality signals
	HasBudget        bool    `json:"has_budget"`
	HasRevenue       bool    `json:"has_revenue"`
	HighVoteCount    bool    `json:"high_vote_count"`    // > 100 votes
	VeryHighVotes    bool    `json:"very_high_votes"`    // > 1000 votes
	Popular          bool    `json:"popular"`             // popularity > 10
	VeryPopular      bool    `json:"very_popular"`        // popularity > 50
	HighRated        bool    `json:"high_rated"`          // vote_average >= 7.0
	HasIMDBRating    bool    `json:"has_imdb_rating"`
	VoteCount        int     `json:"vote_count"`
	Popularity       float64 `json:"popularity"`
	VoteAverage      float64 `json:"vote_average"`

	// Content flags
	IsAdult          bool   `json:"is_adult"`
	IsReleased       bool   `json:"is_released"`
	IsEnglish        bool   `json:"is_english"`
	IsIndian         bool   `json:"is_indian"`
	IsKorean         bool   `json:"is_korean"`
	IsJapanese       bool   `json:"is_japanese"`
	OriginalLanguage string `json:"original_language"`
	ReleaseYear      string `json:"release_year,omitempty"`

	// Training tiers
	// Gold: has overview + poster + cast + genres + vote_count > 100 + has_availability
	// Silver: has overview + poster + genres + vote_count > 10
	// Bronze: has overview + has title (basically any real entry)
	Tier string `json:"tier"` // "gold", "silver", "bronze", "stub"

	// Website display readiness
	DisplayReady bool `json:"display_ready"` // has enough for a good card: title + poster + overview + genres + year
}

func main() {
	input := flag.String("input", "", "Input JSONL path")
	output := flag.String("output", "", "Output JSONL path (input with _tags appended if empty)")
	contentType := flag.String("type", "movie", "Content type: movie or tv")
	flag.Parse()

	if *input == "" {
		log.Fatal("-input is required")
	}
	if *output == "" {
		*output = strings.TrimSuffix(*input, ".jsonl") + "_tagged.jsonl"
	}

	start := time.Now()

	in, err := os.Open(*input)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	defer in.Close()

	out, err := os.Create(*output)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer out.Close()

	w := bufio.NewWriter(out)
	defer w.Flush()

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 16<<20), 16<<20) // 16MB buffer for large records

	var total int
	tiers := map[string]int{"gold": 0, "silver": 0, "bronze": 0, "stub": 0}
	var displayReady int

	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue
		}

		tags := computeTags(record, *contentType)
		record["_tags"] = tags
		tiers[tags.Tier]++
		if tags.DisplayReady {
			displayReady++
		}
		total++

		line, err := json.Marshal(record)
		if err != nil {
			continue
		}
		w.Write(line)
		w.WriteByte('\n')

		if total%50000 == 0 {
			log.Printf("Tagged %d titles...", total)
		}
	}

	log.Printf("Done in %s: %d titles tagged → %s", time.Since(start).Round(time.Second), total, *output)
	log.Printf("  Gold:   %d (%d%%)", tiers["gold"], pct(tiers["gold"], total))
	log.Printf("  Silver: %d (%d%%)", tiers["silver"], pct(tiers["silver"], total))
	log.Printf("  Bronze: %d (%d%%)", tiers["bronze"], pct(tiers["bronze"], total))
	log.Printf("  Stub:   %d (%d%%)", tiers["stub"], pct(tiers["stub"], total))
	log.Printf("  Display ready: %d (%d%%)", displayReady, pct(displayReady, total))
}

func computeTags(record map[string]any, contentType string) *Tags {
	t := &Tags{}

	// --- Data completeness ---
	t.HasOverview = strField(record, "overview") != ""
	t.HasPoster = strField(record, "poster_path") != ""
	t.HasBackdrop = strField(record, "backdrop_path") != ""
	t.HasGenres = lenField(record, "genres") > 0
	t.HasKeywords = lenField(record, "keywords") > 0
	t.HasCast = lenField(record, "cast") > 0
	t.HasCrew = lenField(record, "crew") > 0
	t.HasTagline = strField(record, "tagline") != ""
	t.HasHomepage = strField(record, "homepage") != ""
	t.CastCount = lenField(record, "cast")
	t.CrewCount = lenField(record, "crew")
	t.KeywordCount = lenField(record, "keywords")

	if contentType == "movie" {
		t.HasRuntime = intField(record, "runtime") > 0
		t.HasReleaseDate = strField(record, "release_date") != ""
		t.HasOrigTitle = strField(record, "original_title") != "" && strField(record, "original_title") != strField(record, "title")
		t.HasCollection = record["belongs_to_collection"] != nil
		t.HasBudget = intField(record, "budget") > 0
		t.HasRevenue = intField(record, "revenue") > 0
		if len(strField(record, "release_date")) >= 4 {
			t.ReleaseYear = strField(record, "release_date")[:4]
		}
	} else {
		t.HasReleaseDate = strField(record, "first_air_date") != ""
		t.HasOrigTitle = strField(record, "original_name") != "" && strField(record, "original_name") != strField(record, "name")
		if len(strField(record, "first_air_date")) >= 4 {
			t.ReleaseYear = strField(record, "first_air_date")[:4]
		}
	}

	// Check for director in crew
	if crew, ok := record["crew"].([]any); ok {
		for _, c := range crew {
			if cm, ok := c.(map[string]any); ok {
				if job, _ := cm["job"].(string); job == "Director" {
					t.HasDirector = true
					break
				}
			}
		}
	}

	// --- Enrichment ---
	if ext, ok := record["external_ids"].(map[string]any); ok {
		t.HasIMDBID = strAny(ext["imdb_id"]) != ""
		t.HasWikidata = strAny(ext["wikidata_id"]) != ""
		t.HasExternalIDs = t.HasIMDBID || t.HasWikidata
	} else {
		t.HasIMDBID = strField(record, "imdb_id") != ""
	}

	t.HasTrailers = false
	t.HasVideos = lenField(record, "videos") > 0
	t.VideoCount = lenField(record, "videos")
	if vids, ok := record["videos"].([]any); ok {
		for _, v := range vids {
			if vm, ok := v.(map[string]any); ok {
				if vtype, _ := vm["type"].(string); vtype == "Trailer" || vtype == "Teaser" {
					t.HasTrailers = true
					t.TrailerCount++
				}
			}
		}
	}

	t.HasAvailability = lenField(record, "watch_providers") > 0
	t.AvailCountries = lenField(record, "watch_providers")
	t.HasSimilar = lenField(record, "similar") > 0
	t.HasAltTitles = lenField(record, "alternative_titles") > 0
	t.HasCertification = lenField(record, "certifications") > 0

	// --- Quality signals ---
	t.VoteCount = intField(record, "vote_count")
	t.Popularity = floatField(record, "popularity")
	t.VoteAverage = floatField(record, "vote_average")
	t.HighVoteCount = t.VoteCount > 100
	t.VeryHighVotes = t.VoteCount > 1000
	t.Popular = t.Popularity > 10
	t.VeryPopular = t.Popularity > 50
	t.HighRated = t.VoteAverage >= 7.0 && t.VoteCount > 50

	// --- Content flags ---
	t.IsAdult = boolField(record, "adult")
	status := strField(record, "status")
	t.IsReleased = status == "Released" || status == "Ended" || status == "Returning Series"
	t.OriginalLanguage = strField(record, "original_language")
	t.IsEnglish = t.OriginalLanguage == "en"

	originCountries := arrStrField(record, "origin_country")
	for _, c := range originCountries {
		if c == "IN" {
			t.IsIndian = true
		}
		if c == "KR" {
			t.IsKorean = true
		}
		if c == "JP" {
			t.IsJapanese = true
		}
	}
	// Fallback: check language for Indian content
	if !t.IsIndian {
		indianLangs := map[string]bool{"hi": true, "te": true, "ta": true, "ml": true, "kn": true, "bn": true, "mr": true, "pa": true, "gu": true}
		t.IsIndian = indianLangs[t.OriginalLanguage]
	}

	// --- Training tiers ---
	if t.HasOverview && t.HasPoster && t.HasCast && t.HasGenres && t.VoteCount > 100 && t.HasAvailability {
		t.Tier = "gold"
	} else if t.HasOverview && t.HasPoster && t.HasGenres && t.VoteCount > 10 {
		t.Tier = "silver"
	} else if t.HasOverview {
		t.Tier = "bronze"
	} else {
		t.Tier = "stub"
	}

	// --- Display readiness ---
	title := strField(record, "title")
	if title == "" {
		title = strField(record, "name")
	}
	t.DisplayReady = title != "" && t.HasPoster && t.HasOverview && t.HasGenres && t.ReleaseYear != ""

	return t
}

// --- Field accessors ---

func strField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func strAny(v any) string {
	s, _ := v.(string)
	return s
}

func intField(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func floatField(m map[string]any, key string) float64 {
	v, _ := m[key].(float64)
	return v
}

func boolField(m map[string]any, key string) bool {
	v, _ := m[key].(bool)
	return v
}

func lenField(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case []any:
		return len(v)
	case map[string]any:
		return len(v)
	}
	return 0
}

func arrStrField(m map[string]any, key string) []string {
	arr, ok := m[key].([]any)
	if !ok {
		return nil
	}
	var result []string
	for _, v := range arr {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func pct(n, total int) int {
	if total == 0 {
		return 0
	}
	return n * 100 / total
}
