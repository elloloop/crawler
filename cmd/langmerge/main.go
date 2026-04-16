// langmerge builds a unified movie database per language by merging:
//   - TMDB v2 (primary, richest metadata)
//   - Wikipedia filtered films (plot summaries, categories)
//   - OTT crawl data (aha, sunnxt, zee5, hotstar — thumbnails, banners)
//   - IMDb datasets (ratings for any title with imdb_id)
//
// Uses fuzzy title matching to deduplicate across sources.
// Outputs one JSONL per language with all data merged and source tracking.
//
// Usage:
//
//	langmerge -lang te -output data/lang/telugu.jsonl
//	langmerge -lang all -output data/lang/
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var langConfig = map[string]langInfo{
	// Indian
	"te": {Name: "Telugu", WikiKeywords: []string{"telugu"}, OTTKeywords: []string{"telugu"}},
	"ta": {Name: "Tamil", WikiKeywords: []string{"tamil"}, OTTKeywords: []string{"tamil"}},
	"ml": {Name: "Malayalam", WikiKeywords: []string{"malayalam"}, OTTKeywords: []string{"malayalam"}},
	"hi": {Name: "Hindi", WikiKeywords: []string{"hindi", "bollywood"}, OTTKeywords: []string{"hindi"}},
	"kn": {Name: "Kannada", WikiKeywords: []string{"kannada"}, OTTKeywords: []string{"kannada"}},
	"bn": {Name: "Bengali", WikiKeywords: []string{"bengali", "bangla"}, OTTKeywords: []string{"bengali", "bangla"}},
	"mr": {Name: "Marathi", WikiKeywords: []string{"marathi"}, OTTKeywords: []string{"marathi"}},
	"pa": {Name: "Punjabi", WikiKeywords: []string{"punjabi"}, OTTKeywords: []string{"punjabi"}},
	"gu": {Name: "Gujarati", WikiKeywords: []string{"gujarati"}, OTTKeywords: []string{"gujarati"}},
	// Global
	"en": {Name: "English", WikiKeywords: []string{"english-language", "american", "british"}, OTTKeywords: []string{"english"}},
	"fr": {Name: "French", WikiKeywords: []string{"french"}, OTTKeywords: []string{"french"}},
	"es": {Name: "Spanish", WikiKeywords: []string{"spanish"}, OTTKeywords: []string{"spanish"}},
	"ja": {Name: "Japanese", WikiKeywords: []string{"japanese"}, OTTKeywords: []string{"japanese"}},
	"ko": {Name: "Korean", WikiKeywords: []string{"korean"}, OTTKeywords: []string{"korean"}},
	"de": {Name: "German", WikiKeywords: []string{"german"}, OTTKeywords: []string{"german"}},
	"it": {Name: "Italian", WikiKeywords: []string{"italian"}, OTTKeywords: []string{"italian"}},
	"zh": {Name: "Chinese", WikiKeywords: []string{"chinese", "mandarin", "cantonese", "hong kong"}, OTTKeywords: []string{"chinese"}},
	"pt": {Name: "Portuguese", WikiKeywords: []string{"portuguese", "brazilian"}, OTTKeywords: []string{"portuguese"}},
	"ru": {Name: "Russian", WikiKeywords: []string{"russian"}, OTTKeywords: []string{"russian"}},
	"tr": {Name: "Turkish", WikiKeywords: []string{"turkish"}, OTTKeywords: []string{"turkish"}},
	"th": {Name: "Thai", WikiKeywords: []string{"thai"}, OTTKeywords: []string{"thai"}},
	"ar": {Name: "Arabic", WikiKeywords: []string{"arabic"}, OTTKeywords: []string{"arabic"}},
	"fa": {Name: "Persian", WikiKeywords: []string{"persian", "iranian"}, OTTKeywords: []string{"persian"}},
}

type langInfo struct {
	Name         string
	WikiKeywords []string
	OTTKeywords  []string
}

type UnifiedTitle struct {
	// Identity
	Title            string   `json:"title"`
	OriginalTitle    string   `json:"original_title,omitempty"`
	Year             string   `json:"year,omitempty"`
	Language         string   `json:"language"`
	LanguageName     string   `json:"language_name"`

	// Sources present
	Sources          []string `json:"sources"` // ["tmdb", "wikipedia", "aha", "imdb", ...]
	TMDBID           int      `json:"tmdb_id,omitempty"`
	IMDBID           string   `json:"imdb_id,omitempty"`
	WikiURL          string   `json:"wiki_url,omitempty"`

	// TMDB data (richest)
	Overview         string   `json:"overview,omitempty"`
	Genres           any      `json:"genres,omitempty"`
	Cast             any      `json:"cast,omitempty"`
	Crew             any      `json:"crew,omitempty"`
	VoteAverage      float64  `json:"vote_average,omitempty"`
	VoteCount        int      `json:"vote_count,omitempty"`
	Popularity       float64  `json:"popularity,omitempty"`
	PosterPath       string   `json:"poster_path,omitempty"`
	BackdropPath     string   `json:"backdrop_path,omitempty"`
	Runtime          int      `json:"runtime,omitempty"`
	ReleaseDate      string   `json:"release_date,omitempty"`
	Keywords         any      `json:"keywords,omitempty"`
	Videos           any      `json:"videos,omitempty"`
	WatchProviders   any      `json:"watch_providers,omitempty"`
	Similar          any      `json:"similar,omitempty"`
	Certifications   any      `json:"certifications,omitempty"`
	AltTitles        any      `json:"alternative_titles,omitempty"`
	ExternalIDs      any      `json:"external_ids,omitempty"`
	Collection       any      `json:"belongs_to_collection,omitempty"`

	// IMDb
	IMDBRating       float64  `json:"imdb_rating,omitempty"`
	IMDBVotes        int      `json:"imdb_votes,omitempty"`

	// Wikipedia
	WikiPlotSummary  string   `json:"wiki_plot_summary,omitempty"`
	WikiCategories   []string `json:"wiki_categories,omitempty"`

	// Wiki year-list data (fills gaps in TMDB)
	WikiDirector     string   `json:"wiki_director,omitempty"`
	WikiCast         []string `json:"wiki_cast,omitempty"`
	WikiProdCompany  string   `json:"wiki_production_company,omitempty"`

	// OTT
	OTTPlatforms     []OTTInfo `json:"ott_platforms,omitempty"`

	// Dedup metadata
	DedupConfidence  float64  `json:"_dedup_confidence,omitempty"`
	DedupMethod      string   `json:"_dedup_method,omitempty"`
}

type OTTInfo struct {
	Platform     string `json:"platform"`
	URL          string `json:"url"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
	BannerURL    string `json:"banner_url,omitempty"`
}

func main() {
	lang := flag.String("lang", "", "Language code (te, ta, ml, hi, kn, bn, etc.) or 'all'")
	outputDir := flag.String("output", "data/lang", "Output directory")
	flag.Parse()

	if *lang == "" {
		fmt.Println("Available languages:")
		for code, info := range langConfig {
			fmt.Printf("  %s — %s\n", code, info.Name)
		}
		fmt.Println("\nUsage: langmerge -lang te|all -output data/lang/")
		return
	}

	os.MkdirAll(*outputDir, 0o755)

	var langs []string
	if *lang == "all" {
		for code := range langConfig {
			langs = append(langs, code)
		}
		sort.Strings(langs)
	} else {
		if _, ok := langConfig[*lang]; !ok {
			log.Fatalf("Unknown language: %s", *lang)
		}
		langs = []string{*lang}
	}

	// Load shared data once
	start := time.Now()
	log.Println("Loading IMDb ratings...")
	imdbRatings := loadIMDBRatings()
	log.Printf("  %d ratings", len(imdbRatings))

	log.Println("Loading Wikipedia films...")
	wikiFilms := loadWikiFilms()
	log.Printf("  %d films", len(wikiFilms))

	log.Println("Loading OTT data...")
	ottData := loadOTTData()
	log.Printf("  %d titles across platforms", len(ottData))

	log.Println("Loading Wikipedia year-lists...")
	wikiYearData := loadWikiYearLists()
	log.Printf("  %d titles from year-lists", len(wikiYearData))

	for _, langCode := range langs {
		info := langConfig[langCode]
		log.Printf("\n=== Processing %s (%s) ===", info.Name, langCode)
		processLanguage(langCode, info, *outputDir, imdbRatings, wikiFilms, wikiYearData, ottData)
	}

	log.Printf("\nAll done in %s", time.Since(start).Round(time.Second))
}

func processLanguage(langCode string, info langInfo, outputDir string, imdbRatings map[string][2]float64, wikiFilms []wikiFilm, wikiYearData []wikiYearEntry, ottData []ottEntry) {
	unified := make(map[string]*UnifiedTitle)
	dedup := NewDeduplicator()

	// 1. Load TMDB titles (primary source)
	tmdbCount := 0
	scanner := openJSONL("data/tmdb/movies_v2.jsonl")
	for scanner.Scan() {
		var m map[string]any
		json.Unmarshal(scanner.Bytes(), &m)
		if strVal(m, "original_language") != langCode {
			continue
		}
		tmdbCount++
		title := strVal(m, "title")
		origTitle := strVal(m, "original_title")
		year := strVal(m, "release_date")
		if len(year) >= 4 {
			year = year[:4]
		}
		director := extractDirector(m)

		key := normKey(title, year)
		u := &UnifiedTitle{
			Title:         title,
			OriginalTitle: origTitle,
			Year:          year,
			Language:      langCode,
			LanguageName:  info.Name,
			Sources:       []string{"tmdb"},
			TMDBID:        intVal(m, "tmdb_id"),
			IMDBID:        strVal(m, "imdb_id"),
			Overview:      strVal(m, "overview"),
			Genres:        m["genres"],
			Cast:          m["cast"],
			Crew:          m["crew"],
			VoteAverage:   floatVal(m, "vote_average"),
			VoteCount:     intVal(m, "vote_count"),
			Popularity:    floatVal(m, "popularity"),
			PosterPath:    strVal(m, "poster_path"),
			BackdropPath:  strVal(m, "backdrop_path"),
			Runtime:       intVal(m, "runtime"),
			ReleaseDate:   strVal(m, "release_date"),
			Keywords:      m["keywords"],
			Videos:        m["videos"],
			WatchProviders: m["watch_providers"],
			Similar:       m["similar"],
			Certifications: m["certifications"],
			AltTitles:     m["alternative_titles"],
			ExternalIDs:   m["external_ids"],
			Collection:    m["belongs_to_collection"],
		}

		if u.IMDBID != "" {
			if r, ok := imdbRatings[u.IMDBID]; ok {
				u.IMDBRating = r[0]
				u.IMDBVotes = int(r[1])
			}
		}

		unified[key] = u
		dedup.Index(title, origTitle, year, director, key)
	}
	log.Printf("  TMDB: %d titles", tmdbCount)

	// 2. Merge Wikipedia article films (multi-pass dedup)
	wikiMerged, wikiNew := 0, 0
	for _, wf := range wikiFilms {
		if !matchesLang(wf.Categories, info.WikiKeywords) {
			continue
		}
		mergeWiki(unified, dedup, wf, langCode, info.Name, &wikiMerged, &wikiNew)
	}
	log.Printf("  Wiki articles: %d merged, %d new", wikiMerged, wikiNew)

	// 3. Merge Wikipedia year-list films (multi-pass dedup)
	wyMerged, wyNew := 0, 0
	for _, wy := range wikiYearData {
		if !strings.EqualFold(wy.Language, info.Name) {
			continue
		}
		mergeWikiYear(unified, dedup, wy, langCode, info.Name, &wyMerged, &wyNew)
	}
	log.Printf("  Wiki year-lists: %d merged, %d new", wyMerged, wyNew)

	// 4. Merge OTT data (multi-pass dedup)
	ottMerged, ottNew := 0, 0
	for _, oe := range ottData {
		if !matchesLangOTT(oe, info.OTTKeywords) {
			continue
		}
		mergeOTT(unified, dedup, oe, langCode, info.Name, &ottMerged, &ottNew)
	}
	log.Printf("  OTT: %d merged, %d new", ottMerged, ottNew)

	// 5. Write output
	outPath := filepath.Join(outputDir, info.Name+".jsonl")
	f, err := os.Create(outPath)
	if err != nil {
		log.Printf("  Error: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)

	sourceStats := map[string]int{}
	dedupStats := map[string]int{}
	for _, u := range unified {
		enc.Encode(u)
		for _, s := range u.Sources {
			sourceStats[s]++
		}
		if u.DedupMethod != "" {
			dedupStats[u.DedupMethod]++
		}
	}
	w.Flush()

	log.Printf("  Total: %d unique titles → %s", len(unified), outPath)
	log.Printf("  Sources: %v", sourceStats)
	if len(dedupStats) > 0 {
		log.Printf("  Dedup matches: %v", dedupStats)
	}
}

func mergeWiki(unified map[string]*UnifiedTitle, dedup *Deduplicator, wf wikiFilm, langCode, langName string, merged, newCount *int) {
	key := normKey(wf.Title, wf.Year)

	// Try multi-pass match
	if u, exists := unified[key]; exists {
		applyWikiToUnified(u, wf, 0.95, "exact")
		*merged++
		return
	}

	if match := dedup.Match(wf.Title, "", wf.Year, ""); match != nil {
		if u, exists := unified[match.Key]; exists {
			applyWikiToUnified(u, wf, match.Confidence, match.Method)
			*merged++
			return
		}
	}

	// New title
	u := &UnifiedTitle{
		Title:           wf.Title,
		Year:            wf.Year,
		Language:        langCode,
		LanguageName:    langName,
		Sources:         []string{"wikipedia"},
		WikiPlotSummary: wf.PlotSummary,
		WikiURL:         wf.URL,
		WikiCategories:  wf.Categories,
	}
	unified[key] = u
	dedup.Index(wf.Title, "", wf.Year, "", key)
	*newCount++
}

func mergeWikiYear(unified map[string]*UnifiedTitle, dedup *Deduplicator, wy wikiYearEntry, langCode, langName string, merged, newCount *int) {
	year := strconv.Itoa(wy.Year)
	key := normKey(wy.Title, year)

	if u, exists := unified[key]; exists {
		applyWikiYearToUnified(u, wy, 0.95, "exact")
		*merged++
		return
	}

	if match := dedup.Match(wy.Title, "", year, wy.Director); match != nil {
		if u, exists := unified[match.Key]; exists {
			applyWikiYearToUnified(u, wy, match.Confidence, match.Method)
			*merged++
			return
		}
	}

	// New title
	u := &UnifiedTitle{
		Title:            wy.Title,
		Year:             year,
		Language:         langCode,
		LanguageName:     langName,
		Sources:          []string{"wikiyear"},
		WikiDirector:     wy.Director,
		WikiCast:         wy.Cast,
		WikiProdCompany:  wy.ProductionCompany,
	}
	unified[key] = u
	dedup.Index(wy.Title, "", year, wy.Director, key)
	*newCount++
}

func mergeOTT(unified map[string]*UnifiedTitle, dedup *Deduplicator, oe ottEntry, langCode, langName string, merged, newCount *int) {
	key := normKey(oe.Title, oe.Year)
	oi := OTTInfo{
		Platform:     oe.Platform,
		URL:          oe.URL,
		ThumbnailURL: oe.ThumbnailURL,
		BannerURL:    oe.BannerURL,
	}

	if u, exists := unified[key]; exists {
		u.OTTPlatforms = append(u.OTTPlatforms, oi)
		u.Sources = appendUnique(u.Sources, oe.Platform)
		*merged++
		return
	}

	if match := dedup.Match(oe.Title, "", oe.Year, ""); match != nil {
		if u, exists := unified[match.Key]; exists {
			u.OTTPlatforms = append(u.OTTPlatforms, oi)
			u.Sources = appendUnique(u.Sources, oe.Platform)
			if u.DedupConfidence == 0 || match.Confidence > u.DedupConfidence {
				u.DedupConfidence = match.Confidence
				u.DedupMethod = match.Method
			}
			*merged++
			return
		}
	}

	u := &UnifiedTitle{
		Title:        oe.Title,
		Year:         oe.Year,
		Language:     langCode,
		LanguageName: langName,
		Sources:      []string{oe.Platform},
		OTTPlatforms: []OTTInfo{oi},
	}
	unified[key] = u
	dedup.Index(oe.Title, "", oe.Year, "", key)
	*newCount++
}

func applyWikiToUnified(u *UnifiedTitle, wf wikiFilm, confidence float64, method string) {
	if wf.PlotSummary != "" && u.WikiPlotSummary == "" {
		u.WikiPlotSummary = wf.PlotSummary
	}
	if wf.URL != "" {
		u.WikiURL = wf.URL
	}
	if len(wf.Categories) > 0 {
		u.WikiCategories = wf.Categories
	}
	u.Sources = appendUnique(u.Sources, "wikipedia")
	if confidence < 0.95 {
		u.DedupConfidence = confidence
		u.DedupMethod = method
	}
}

func applyWikiYearToUnified(u *UnifiedTitle, wy wikiYearEntry, confidence float64, method string) {
	if wy.Director != "" && u.WikiDirector == "" {
		u.WikiDirector = wy.Director
	}
	if len(wy.Cast) > 0 && len(u.WikiCast) == 0 {
		u.WikiCast = wy.Cast
	}
	if wy.ProductionCompany != "" && u.WikiProdCompany == "" {
		u.WikiProdCompany = wy.ProductionCompany
	}
	u.Sources = appendUnique(u.Sources, "wikiyear")
	if confidence < 0.95 {
		u.DedupConfidence = confidence
		u.DedupMethod = method
	}
}

func extractDirector(m map[string]any) string {
	crew, ok := m["crew"].([]any)
	if !ok {
		return ""
	}
	for _, c := range crew {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if job, _ := cm["job"].(string); job == "Director" {
			if name, _ := cm["name"].(string); name != "" {
				return name
			}
		}
	}
	return ""
}

// --- Data loaders ---

type wikiFilm struct {
	Title       string
	Year        string
	PlotSummary string
	URL         string
	Categories  []string
}

func loadWikiFilms() []wikiFilm {
	var films []wikiFilm
	for _, path := range []string{"data/movies/films.jsonl", "data/movies/tv.jsonl"} {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			var r struct {
				Title       string   `json:"title"`
				Year        string   `json:"year"`
				PlotSummary string   `json:"plot_summary"`
				URL         string   `json:"url"`
				Categories  []string `json:"categories"`
			}
			json.Unmarshal(scanner.Bytes(), &r)
			if r.Title != "" {
				films = append(films, wikiFilm{
					Title: r.Title, Year: r.Year,
					PlotSummary: r.PlotSummary, URL: r.URL, Categories: r.Categories,
				})
			}
		}
		f.Close()
	}
	return films
}

type wikiYearEntry struct {
	Title             string
	Year              int
	Language          string
	Director          string
	Cast              []string
	ProductionCompany string
}

func loadWikiYearLists() []wikiYearEntry {
	var entries []wikiYearEntry
	files, _ := filepath.Glob("data/wikiyear/*.jsonl")
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			var r struct {
				Title             string   `json:"title"`
				Year              int      `json:"year"`
				Language          string   `json:"language"`
				Director          string   `json:"director"`
				Cast              []string `json:"cast"`
				ProductionCompany string   `json:"production_company"`
			}
			json.Unmarshal(scanner.Bytes(), &r)
			if r.Title != "" {
				entries = append(entries, wikiYearEntry{
					Title: r.Title, Year: r.Year, Language: r.Language,
					Director: r.Director, Cast: r.Cast, ProductionCompany: r.ProductionCompany,
				})
			}
		}
		f.Close()
	}
	return entries
}

func loadIMDBRatings() map[string][2]float64 {
	ratings := make(map[string][2]float64)
	path := "data/imdb/title.ratings.tsv.gz"
	f, err := os.Open(path)
	if err != nil {
		return ratings
	}
	defer f.Close()
	gr, _ := gzip.NewReader(f)
	defer gr.Close()
	scanner := bufio.NewScanner(gr)
	scanner.Scan() // header
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) >= 3 {
			r, _ := strconv.ParseFloat(fields[1], 64)
			v, _ := strconv.ParseFloat(fields[2], 64)
			ratings[fields[0]] = [2]float64{r, v}
		}
	}
	return ratings
}

type ottEntry struct {
	Platform     string
	Title        string
	Year         string
	URL          string
	Language     string
	ThumbnailURL string
	BannerURL    string
}

func loadOTTData() []ottEntry {
	var entries []ottEntry
	for _, path := range []string{
		"data/ott/aha.jsonl", "data/ott/hotstar.jsonl",
		"data/ott/zee5.jsonl", "data/ott/sunnxt.jsonl",
	} {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		platform := strings.TrimSuffix(filepath.Base(path), ".jsonl")
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			var r struct {
				Title        string `json:"title"`
				Year         string `json:"year"`
				URL          string `json:"url"`
				Language     string `json:"language"`
				ThumbnailURL string `json:"thumbnail_url"`
				BannerURL    string `json:"banner_url"`
			}
			json.Unmarshal(scanner.Bytes(), &r)
			if r.Title != "" {
				entries = append(entries, ottEntry{
					Platform: platform, Title: r.Title, Year: r.Year,
					URL: r.URL, Language: strings.ToLower(r.Language),
					ThumbnailURL: r.ThumbnailURL, BannerURL: r.BannerURL,
				})
			}
		}
		f.Close()
	}
	return entries
}

// --- Helpers ---

func normKey(title, year string) string {
	n := normalize(title)
	if year != "" {
		return n + "::" + year
	}
	return n
}

func normalize(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

var disambigRe = regexp.MustCompile(`\s*\((?:film|movie|TV series|[^)]*film)\)\s*$`)

func stripDisambig(title string) string {
	return strings.TrimSpace(disambigRe.ReplaceAllString(title, ""))
}

func matchesLang(categories []string, keywords []string) bool {
	cats := strings.ToLower(strings.Join(categories, " "))
	for _, kw := range keywords {
		if strings.Contains(cats, kw) {
			return true
		}
	}
	return false
}

func matchesLangOTT(oe ottEntry, keywords []string) bool {
	text := strings.ToLower(oe.Language + " " + oe.URL)
	for _, kw := range keywords {
		if strings.Contains(text, kw) {
			return true
		}
	}
	return false
}

func appendUnique(slice []string, val string) []string {
	for _, s := range slice {
		if s == val {
			return slice
		}
	}
	return append(slice, val)
}

func openJSONL(path string) *bufio.Scanner {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 16<<20), 16<<20)
	return scanner
}

func strVal(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func intVal(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func floatVal(m map[string]any, key string) float64 {
	v, _ := m[key].(float64)
	return v
}
