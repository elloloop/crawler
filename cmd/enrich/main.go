// enrich merges TMDB data (movies + TV) with IMDb ratings/cast and Wikipedia plots.
//
// Strategy:
//   - TMDB → IMDb: direct join on imdb_id (100% accurate, no fuzzy matching)
//   - TMDB → Wikipedia: multi-pass matching (exact title+year, then normalized title+year)
//   - Fail-fast: skip records that don't validate, log them, never output garbage
//
// Usage:
//
//	enrich \
//	  -tmdb-movies ~/Downloads/tmdb_movies.jsonl \
//	  -tmdb-tv data/tmdb/tv_shows.jsonl \
//	  -wiki-movies data/movies/films.jsonl \
//	  -wiki-tv data/movies/tv.jsonl \
//	  -imdb-ratings data/imdb/title.ratings.tsv.gz \
//	  -imdb-cast data/imdb/title.principals.tsv.gz \
//	  -imdb-names data/imdb/name.basics.tsv.gz \
//	  -output data/enriched/
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// --- Input types ---

type TMDBMovie struct {
	TMDBID              int     `json:"tmdb_id"`
	IMDBID              string  `json:"imdb_id"`
	Title               string  `json:"title"`
	OriginalTitle       string  `json:"original_title"`
	Overview            string  `json:"overview"`
	Tagline             string  `json:"tagline"`
	ReleaseDate         string  `json:"release_date"`
	Runtime             int     `json:"runtime"`
	Budget              int64   `json:"budget"`
	Revenue             int64   `json:"revenue"`
	Popularity          float64 `json:"popularity"`
	VoteAverage         float64 `json:"vote_average"`
	VoteCount           int     `json:"vote_count"`
	Status              string  `json:"status"`
	OriginalLanguage    string  `json:"original_language"`
	SpokenLanguages     string  `json:"spoken_languages"`
	ProductionCountries string  `json:"production_countries"`
	ProductionCompanies string  `json:"production_companies"`
	Genres              string  `json:"genres"`
	Keywords            string  `json:"keywords"`
	CastTop10           string  `json:"cast_top10"`
	Director            string  `json:"director"`
	PosterPath          string  `json:"poster_path"`
	BackdropPath        string  `json:"backdrop_path"`
	Homepage            string  `json:"homepage"`
	IMDBRating          string  `json:"imdb_rating"`
	IMDBVotes           string  `json:"imdb_votes"`
	BelongsToCollection string  `json:"belongs_to_collection"`
}

type TMDBTV struct {
	TMDBID           int      `json:"tmdb_id"`
	Name             string   `json:"name"`
	OriginalName     string   `json:"original_name"`
	Overview         string   `json:"overview"`
	Tagline          string   `json:"tagline"`
	FirstAirDate     string   `json:"first_air_date"`
	LastAirDate      string   `json:"last_air_date"`
	Status           string   `json:"status"`
	Type             string   `json:"type"`
	InProduction     bool     `json:"in_production"`
	NumberOfSeasons  int      `json:"number_of_seasons"`
	NumberOfEpisodes int      `json:"number_of_episodes"`
	Runtime          []int    `json:"episode_run_time"`
	Popularity       float64  `json:"popularity"`
	VoteAverage      float64  `json:"vote_average"`
	VoteCount        int      `json:"vote_count"`
	OriginalLanguage string   `json:"original_language"`
	OriginCountry    []string `json:"origin_country"`
	Genres           string   `json:"genres"`
	Networks         string   `json:"networks"`
	Creators         string   `json:"created_by"`
	ProductionCompanies string `json:"production_companies"`
	Keywords         string   `json:"keywords"`
	CastTop10        string   `json:"cast_top10"`
	PosterPath       string   `json:"poster_path"`
	BackdropPath     string   `json:"backdrop_path"`
	Homepage         string   `json:"homepage"`
}

type WikiRecord struct {
	Title       string   `json:"title"`
	URL         string   `json:"url"`
	Type        string   `json:"type"`
	Year        string   `json:"year"`
	Director    string   `json:"director"`
	Starring    []string `json:"starring"`
	PlotSummary string   `json:"plot_summary"`
	Categories  []string `json:"categories"`
	PageID      int64    `json:"page_id"`
	Creator     string   `json:"creator"`
	Network     string   `json:"network"`
	NumSeasons  string   `json:"num_seasons"`
	NumEpisodes string   `json:"num_episodes"`
}

// --- Output types ---

type EnrichedMovie struct {
	// Identity
	TMDBID string `json:"tmdb_id"`
	IMDBID string `json:"imdb_id,omitempty"`
	Title  string `json:"title"`
	Year   string `json:"year"`

	// TMDB core
	OriginalTitle       string  `json:"original_title,omitempty"`
	Overview            string  `json:"overview"`
	Tagline             string  `json:"tagline,omitempty"`
	ReleaseDate         string  `json:"release_date"`
	Runtime             int     `json:"runtime,omitempty"`
	Budget              int64   `json:"budget,omitempty"`
	Revenue             int64   `json:"revenue,omitempty"`
	Status              string  `json:"status,omitempty"`
	OriginalLanguage    string  `json:"original_language,omitempty"`
	SpokenLanguages     string  `json:"spoken_languages,omitempty"`
	ProductionCountries string  `json:"production_countries,omitempty"`
	ProductionCompanies string  `json:"production_companies,omitempty"`
	Genres              string  `json:"genres"`
	Keywords            string  `json:"keywords,omitempty"`
	Director            string  `json:"director,omitempty"`
	PosterPath          string  `json:"poster_path,omitempty"`
	BackdropPath        string  `json:"backdrop_path,omitempty"`
	Homepage            string  `json:"homepage,omitempty"`
	BelongsToCollection string  `json:"belongs_to_collection,omitempty"`

	// Ratings (IMDb authoritative, TMDB supplementary)
	IMDBRating    float64 `json:"imdb_rating,omitempty"`
	IMDBVotes     int     `json:"imdb_votes,omitempty"`
	TMDBRating    float64 `json:"tmdb_rating,omitempty"`
	TMDBVoteCount int     `json:"tmdb_vote_count,omitempty"`

	// Cast
	Cast []CastMember `json:"cast"`

	// Wikipedia enrichment
	WikiPlotSummary  string   `json:"wiki_plot_summary,omitempty"`
	WikiCategories   []string `json:"wiki_categories,omitempty"`
	WikiURL          string   `json:"wiki_url,omitempty"`
	WikiConfidence   float64  `json:"wiki_confidence,omitempty"`   // 0.0–1.0
	WikiMatchSignals []string `json:"wiki_match_signals,omitempty"` // what matched

	// Metadata
	ContentType string `json:"content_type"` // "movie" or "tv"
}

type CastMember struct {
	Name      string `json:"name"`
	Character string `json:"character,omitempty"`
	Role      string `json:"role,omitempty"` // actor, director, writer, composer, etc.
}

// --- Stats ---

type Stats struct {
	TotalMovies        int
	TotalTV            int
	IMDBRatingsMatch   int
	IMDBCastMatch      int
	IMDBTVRatingsMatch int
	IMDBTVCastMatch    int
	WikiMoviesMatch    int
	WikiTVMatch        int
	ValidationFails    int
	OutputMovies       int
	OutputTV           int
}

func (s *Stats) Print() {
	log.Println("=== Enrichment Stats ===")
	log.Printf("  Input:  %d movies, %d TV shows", s.TotalMovies, s.TotalTV)
	log.Printf("  IMDb Movies: %d ratings, %d extra cast", s.IMDBRatingsMatch, s.IMDBCastMatch)
	log.Printf("  IMDb TV:     %d ratings, %d extra cast", s.IMDBTVRatingsMatch, s.IMDBTVCastMatch)
	log.Printf("  Wiki:   %d movies matched, %d TV matched", s.WikiMoviesMatch, s.WikiTVMatch)
	log.Printf("  Skipped: %d (validation failures)", s.ValidationFails)
	log.Printf("  Output: %d movies, %d TV shows", s.OutputMovies, s.OutputTV)
}

func main() {
	tmdbMoviesPath := flag.String("tmdb-movies", "", "TMDB movies JSONL")
	tmdbTVPath := flag.String("tmdb-tv", "", "TMDB TV shows JSONL")
	wikiMoviesPath := flag.String("wiki-movies", "", "Wiki filtered films JSONL")
	wikiTVPath := flag.String("wiki-tv", "", "Wiki filtered TV JSONL")
	imdbRatingsPath := flag.String("imdb-ratings", "", "IMDb title.ratings.tsv.gz")
	imdbCastPath := flag.String("imdb-cast", "", "IMDb title.principals.tsv.gz")
	imdbNamesPath := flag.String("imdb-names", "", "IMDb name.basics.tsv.gz")
	imdbBasicsPath := flag.String("imdb-basics", "", "IMDb title.basics.tsv.gz (for TV title→imdb_id matching)")
	outputDir := flag.String("output", "data/enriched", "Output directory")
	flag.Parse()

	if *tmdbMoviesPath == "" {
		log.Fatal("-tmdb-movies is required")
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	start := time.Now()
	stats := &Stats{}

	// Step 1: Load IMDb ratings (imdb_id → rating+votes). Tiny file, load all.
	log.Println("[1/6] Loading IMDb ratings...")
	ratings := loadIMDBRatings(*imdbRatingsPath)
	log.Printf("        %d ratings loaded", len(ratings))

	// Step 2: Load IMDb names (nconst → name). Need this before cast.
	log.Println("[2/6] Loading IMDb names...")
	names := loadIMDBNames(*imdbNamesPath)
	log.Printf("        %d names loaded", len(names))

	// Step 3: Load IMDb cast (imdb_id → []CastMember).
	log.Println("[3/6] Loading IMDb cast...")
	casts := loadIMDBCast(*imdbCastPath, names)
	log.Printf("        %d titles with cast", len(casts))

	// Step 4: Load IMDb title index (title+year → imdb_id) for TV matching.
	log.Println("[4/7] Loading IMDb title index for TV...")
	imdbTitleIndex := loadIMDBTitleIndex(*imdbBasicsPath)
	log.Printf("        %d TV titles indexed", len(imdbTitleIndex))

	// Step 5: Load Wikipedia records, build multi-signal matcher.
	log.Println("[5/7] Loading Wikipedia data...")
	wikiMovies := loadWikiRecords(*wikiMoviesPath)
	wikiTV := loadWikiRecords(*wikiTVPath)
	log.Printf("        %d wiki films, %d wiki TV", len(wikiMovies), len(wikiTV))
	matcher := NewWikiMatcher(wikiMovies, wikiTV)
	log.Printf("        %d wiki entries indexed", len(matcher.all))

	// Step 6: Process TMDB movies.
	log.Println("[6/7] Enriching movies...")
	moviesOut := filepath.Join(*outputDir, "movies_enriched.jsonl")
	processMovies(*tmdbMoviesPath, moviesOut, ratings, casts, matcher, stats)

	// Step 7: Process TMDB TV shows.
	if *tmdbTVPath != "" {
		log.Println("[7/7] Enriching TV shows...")
		tvOut := filepath.Join(*outputDir, "tv_enriched.jsonl")
		processTV(*tmdbTVPath, tvOut, ratings, casts, matcher, imdbTitleIndex, stats)
	} else {
		log.Println("[6/6] Skipping TV (no path)")
	}

	stats.Print()
	log.Printf("Done in %s", time.Since(start).Round(time.Second))
	log.Printf("Output: %s", *outputDir)
}

// --- IMDb Ratings ---

type ratingEntry struct {
	Rating float64
	Votes  int
}

func loadIMDBRatings(path string) map[string]*ratingEntry {
	m := make(map[string]*ratingEntry, 1_700_000)
	if path == "" {
		return m
	}
	r := openTSV(path)
	defer r.Close()
	scanner := bufio.NewScanner(r)
	scanner.Scan() // header
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 3 {
			continue
		}
		rating, err1 := strconv.ParseFloat(fields[1], 64)
		votes, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			continue
		}
		m[fields[0]] = &ratingEntry{Rating: rating, Votes: votes}
	}
	return m
}

// --- IMDb Names ---

func loadIMDBNames(path string) map[string]string {
	m := make(map[string]string, 15_000_000)
	if path == "" {
		return m
	}
	r := openTSV(path)
	defer r.Close()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	scanner.Scan() // header
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) >= 2 {
			m[fields[0]] = fields[1]
		}
	}
	return m
}

// --- IMDb Cast ---

func loadIMDBCast(path string, names map[string]string) map[string][]CastMember {
	m := make(map[string][]CastMember, 500_000)
	if path == "" {
		return m
	}
	r := openTSV(path)
	defer r.Close()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	scanner.Scan() // header
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 4 {
			continue
		}
		tconst := fields[0]
		nconst := fields[2]
		category := fields[3]
		name := names[nconst]
		if name == "" {
			continue
		}
		chars := ""
		if len(fields) > 5 && fields[5] != "\\N" {
			chars = cleanIMDBChars(fields[5])
		}
		m[tconst] = append(m[tconst], CastMember{
			Name:      name,
			Character: chars,
			Role:      category,
		})
	}
	return m
}

// --- IMDb Title Index (for TV shows without imdb_id) ---

func loadIMDBTitleIndex(path string) map[string]string {
	// Maps "normalized_title::year" → imdb_id for TV series/miniseries
	m := make(map[string]string, 400_000)
	if path == "" {
		return m
	}
	r := openTSV(path)
	defer r.Close()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	scanner.Scan() // header: tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, ...
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 6 {
			continue
		}
		titleType := fields[1]
		if titleType != "tvSeries" && titleType != "tvMiniSeries" {
			continue
		}
		tconst := fields[0]
		primaryTitle := fields[2]
		originalTitle := fields[3]
		startYear := fields[5]
		if startYear == "\\N" {
			continue
		}
		// Index by primary title
		key := normTitle(primaryTitle) + "::" + startYear
		if _, exists := m[key]; !exists {
			m[key] = tconst
		}
		// Also index by original title
		if originalTitle != primaryTitle {
			key = normTitle(originalTitle) + "::" + startYear
			if _, exists := m[key]; !exists {
				m[key] = tconst
			}
		}
	}
	return m
}

func cleanIMDBChars(raw string) string {
	raw = strings.Trim(raw, "[]\"")
	parts := strings.Split(raw, "\",\"")
	return strings.Join(parts, ", ")
}

// --- Wikipedia ---

func loadWikiRecords(path string) []WikiRecord {
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		log.Printf("Warning: cannot open %s: %v", path, err)
		return nil
	}
	defer f.Close()
	var records []WikiRecord
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var rec WikiRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		records = append(records, rec)
	}
	return records
}

var disambigRe = regexp.MustCompile(`\s*\((?:film|movie|TV series|TV show|TV program|miniseries|season \d+|[^)]*film)\)\s*$`)

func stripDisambig(title string) string {
	return strings.TrimSpace(disambigRe.ReplaceAllString(title, ""))
}

// --- Process Movies ---

func processMovies(inPath, outPath string, ratings map[string]*ratingEntry, casts map[string][]CastMember, matcher *WikiMatcher, stats *Stats) {
	in, err := os.Open(inPath)
	if err != nil {
		log.Fatalf("open tmdb movies: %v", err)
	}
	defer in.Close()

	out, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer out.Close()

	w := bufio.NewWriter(out)
	defer w.Flush()
	enc := json.NewEncoder(w)

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	for scanner.Scan() {
		var t TMDBMovie
		if err := json.Unmarshal(scanner.Bytes(), &t); err != nil {
			stats.ValidationFails++
			continue
		}
		// Validate: must have title
		if t.Title == "" {
			stats.ValidationFails++
			continue
		}
		stats.TotalMovies++

		year := extractYear(t.ReleaseDate)

		// Build cast from TMDB top10
		cast := parseCastString(t.CastTop10, "actor")

		// Enrich: IMDb rating
		var imdbRating float64
		var imdbVotes int
		if r, ok := ratings[t.IMDBID]; ok && t.IMDBID != "" {
			imdbRating = r.Rating
			imdbVotes = r.Votes
			stats.IMDBRatingsMatch++
		}

		// Enrich: IMDb cast (add people not in TMDB top10)
		if imdbCast, ok := casts[t.IMDBID]; ok && t.IMDBID != "" {
			existingNames := make(map[string]bool, len(cast))
			for _, c := range cast {
				existingNames[strings.ToLower(c.Name)] = true
			}
			var added int
			for _, ic := range imdbCast {
				if !existingNames[strings.ToLower(ic.Name)] {
					cast = append(cast, ic)
					existingNames[strings.ToLower(ic.Name)] = true
					added++
				}
			}
			if added > 0 {
				stats.IMDBCastMatch++
			}
		}

		// Enrich: Wikipedia (multi-signal matching with confidence)
		var wikiPlot string
		var wikiCats []string
		var wikiURL string
		var wikiConf float64
		var wikiSignals []string
		castNames := extractCastNames(t.CastTop10)
		if wm := matcher.Match(t.Title, t.OriginalTitle, year, t.Director, castNames); wm != nil {
			wikiPlot = wm.Record.PlotSummary
			wikiCats = wm.Record.Categories
			wikiURL = wm.Record.URL
			wikiConf = wm.Confidence
			wikiSignals = wm.Signals
			stats.WikiMoviesMatch++

			// Add wiki cast not already present
			existingNames := make(map[string]bool, len(cast))
			for _, c := range cast {
				existingNames[strings.ToLower(c.Name)] = true
			}
			for _, name := range wm.Record.Starring {
				if !existingNames[strings.ToLower(name)] {
					cast = append(cast, CastMember{Name: name, Role: "actor"})
					existingNames[strings.ToLower(name)] = true
				}
			}
		}

		enriched := &EnrichedMovie{
			TMDBID:              strconv.Itoa(t.TMDBID),
			IMDBID:              t.IMDBID,
			Title:               t.Title,
			Year:                year,
			OriginalTitle:       nonEmpty(t.OriginalTitle, t.Title),
			Overview:            t.Overview,
			Tagline:             t.Tagline,
			ReleaseDate:         t.ReleaseDate,
			Runtime:             t.Runtime,
			Budget:              t.Budget,
			Revenue:             t.Revenue,
			Status:              t.Status,
			OriginalLanguage:    t.OriginalLanguage,
			SpokenLanguages:     t.SpokenLanguages,
			ProductionCountries: t.ProductionCountries,
			ProductionCompanies: t.ProductionCompanies,
			Genres:              t.Genres,
			Keywords:            t.Keywords,
			Director:            t.Director,
			PosterPath:          t.PosterPath,
			BackdropPath:        t.BackdropPath,
			Homepage:            t.Homepage,
			BelongsToCollection: t.BelongsToCollection,
			IMDBRating:          imdbRating,
			IMDBVotes:           imdbVotes,
			TMDBRating:          t.VoteAverage,
			TMDBVoteCount:       t.VoteCount,
			Cast:                cast,
			WikiPlotSummary:     wikiPlot,
			WikiCategories:      wikiCats,
			WikiURL:             wikiURL,
			WikiConfidence:      wikiConf,
			WikiMatchSignals:    wikiSignals,
			ContentType:         "movie",
		}

		if err := enc.Encode(enriched); err != nil {
			stats.ValidationFails++
			continue
		}
		stats.OutputMovies++

		if stats.OutputMovies%50000 == 0 {
			log.Printf("  Movies: %d processed, %d with IMDb, %d with Wiki",
				stats.OutputMovies, stats.IMDBRatingsMatch, stats.WikiMoviesMatch)
		}
	}
}

// --- Process TV Shows ---

func processTV(inPath, outPath string, ratings map[string]*ratingEntry, casts map[string][]CastMember, matcher *WikiMatcher, imdbTitleIndex map[string]string, stats *Stats) {
	in, err := os.Open(inPath)
	if err != nil {
		log.Fatalf("open tmdb tv: %v", err)
	}
	defer in.Close()

	out, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer out.Close()

	w := bufio.NewWriter(out)
	defer w.Flush()
	enc := json.NewEncoder(w)

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	for scanner.Scan() {
		var t TMDBTV
		if err := json.Unmarshal(scanner.Bytes(), &t); err != nil {
			stats.ValidationFails++
			continue
		}
		if t.Name == "" {
			stats.ValidationFails++
			continue
		}
		stats.TotalTV++

		year := extractYear(t.FirstAirDate)
		cast := parseCastString(t.CastTop10, "actor")

		// IMDb: match TV shows by title+year → imdb_id, then get ratings+cast
		var imdbID string
		var imdbRating float64
		var imdbVotes int
		if year != "" {
			// Try primary name
			key := normTitle(t.Name) + "::" + year
			imdbID = imdbTitleIndex[key]
			// Try original name
			if imdbID == "" && t.OriginalName != "" && t.OriginalName != t.Name {
				key = normTitle(t.OriginalName) + "::" + year
				imdbID = imdbTitleIndex[key]
			}
		}
		if imdbID != "" {
			if r, ok := ratings[imdbID]; ok {
				imdbRating = r.Rating
				imdbVotes = r.Votes
				stats.IMDBTVRatingsMatch++
			}
			if imdbCast, ok := casts[imdbID]; ok {
				existingNames := make(map[string]bool, len(cast))
				for _, c := range cast {
					existingNames[strings.ToLower(c.Name)] = true
				}
				var added int
				for _, ic := range imdbCast {
					if !existingNames[strings.ToLower(ic.Name)] {
						cast = append(cast, ic)
						existingNames[strings.ToLower(ic.Name)] = true
						added++
					}
				}
				if added > 0 {
					stats.IMDBTVCastMatch++
				}
			}
		}

		// Wikipedia match (multi-signal with confidence)
		var wikiPlot string
		var wikiCats []string
		var wikiURL string
		var wikiConf float64
		var wikiSignals []string
		castNames := extractCastNames(t.CastTop10)
		if wm := matcher.Match(t.Name, t.OriginalName, year, "", castNames); wm != nil {
			wikiPlot = wm.Record.PlotSummary
			wikiCats = wm.Record.Categories
			wikiURL = wm.Record.URL
			wikiConf = wm.Confidence
			wikiSignals = wm.Signals
			stats.WikiTVMatch++
		}

		enriched := &EnrichedMovie{
			TMDBID:              strconv.Itoa(t.TMDBID),
			IMDBID:              imdbID,
			Title:               t.Name,
			Year:                year,
			OriginalTitle:       nonEmpty(t.OriginalName, t.Name),
			Overview:            t.Overview,
			Tagline:             t.Tagline,
			ReleaseDate:         t.FirstAirDate,
			Status:              t.Status,
			OriginalLanguage:    t.OriginalLanguage,
			ProductionCompanies: t.ProductionCompanies,
			Genres:              t.Genres,
			Keywords:            t.Keywords,
			PosterPath:          t.PosterPath,
			BackdropPath:        t.BackdropPath,
			Homepage:            t.Homepage,
			IMDBRating:          imdbRating,
			IMDBVotes:           imdbVotes,
			TMDBRating:          t.VoteAverage,
			TMDBVoteCount:       t.VoteCount,
			Cast:                cast,
			WikiPlotSummary:     wikiPlot,
			WikiCategories:      wikiCats,
			WikiURL:             wikiURL,
			WikiConfidence:      wikiConf,
			WikiMatchSignals:    wikiSignals,
			ContentType:         "tv",
		}

		if err := enc.Encode(enriched); err != nil {
			stats.ValidationFails++
			continue
		}
		stats.OutputTV++

		if stats.OutputTV%50000 == 0 {
			log.Printf("  TV: %d processed, %d with Wiki", stats.OutputTV, stats.WikiTVMatch)
		}
	}
}

// --- Helpers ---

func normTitle(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

var yearRe = regexp.MustCompile(`^(\d{4})`)

func extractYear(dateStr string) string {
	if len(dateStr) >= 4 {
		if m := yearRe.FindString(dateStr); m != "" {
			return m
		}
	}
	return ""
}

func parseCastString(raw, defaultRole string) []CastMember {
	if raw == "" {
		return nil
	}
	var cast []CastMember
	for _, entry := range strings.Split(raw, "|") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, " as ", 2)
		cm := CastMember{Name: strings.TrimSpace(parts[0]), Role: defaultRole}
		if len(parts) == 2 {
			cm.Character = strings.TrimSpace(parts[1])
		}
		cast = append(cast, cm)
	}
	return cast
}

func nonEmpty(a, b string) string {
	if a != "" && a != b {
		return a
	}
	return ""
}

func openTSV(path string) io.ReadCloser {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	if strings.HasSuffix(path, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatalf("gzip %s: %v", path, err)
		}
		return &gzReadCloser{gr, f}
	}
	return f
}

type gzReadCloser struct {
	*gzip.Reader
	f *os.File
}

func (g *gzReadCloser) Close() error {
	g.Reader.Close()
	return g.f.Close()
}
