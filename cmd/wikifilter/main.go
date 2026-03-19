// wikifilter streams a Wikipedia dump and extracts only movie/TV articles.
//
// It detects films and TV shows by checking for infobox templates and category
// membership in the raw wikitext. Outputs JSONL with structured fields extracted
// from the infobox.
//
// Usage:
//
//	wikifilter -input data/wiki/enwiki-latest-pages-articles.xml.bz2 -output data/movies/
package main

import (
	"compress/bzip2"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	wikistrategy "github.com/elloloop/crawler/internal/strategy/wiki"
)

// Infobox templates that identify films and TV shows.
var filmTemplates = []string{
	"{{infobox film",
	"{{infobox movie",
	"{{infobox film series",
}

var tvTemplates = []string{
	"{{infobox television",
	"{{infobox tv",
	"{{infobox tv series",
	"{{infobox television series",
	"{{infobox television season",
	"{{infobox television episode",
}

// Category patterns that confirm film/TV articles.
var filmCategoryPatterns = []string{
	"films",
	"movies",
}

var tvCategoryPatterns = []string{
	"television series",
	"television shows",
	"tv series",
	"television seasons",
	"television episodes",
}

// Infobox field extraction patterns.
var (
	reInfoboxField = regexp.MustCompile(`(?mi)^\|\s*(\w[\w\s]*?)\s*=\s*(.*)$`)
	reWikiLink     = regexp.MustCompile(`\[\[([^|\]]+?)(?:\|([^\]]+?))?\]\]`)
	rePlainList    = regexp.MustCompile(`(?m)^\*\s*(.+)$`)
	reHTMLComment  = regexp.MustCompile(`<!--.*?-->`)
	reBrTag        = regexp.MustCompile(`(?i)<\s*br\s*/?\s*>`)
	reHTMLTags     = regexp.MustCompile(`<[^>]+>`)
)

type xmlPage struct {
	Title    string      `xml:"title"`
	NS       string      `xml:"ns"`
	ID       int64       `xml:"id"`
	Redirect *struct{}   `xml:"redirect"`
	Revision xmlRevision `xml:"revision"`
}

type xmlRevision struct {
	Text string `xml:"text"`
}

// MovieRecord is the output format for each extracted movie/TV article.
type MovieRecord struct {
	Title       string   `json:"title"`
	URL         string   `json:"url"`
	Type        string   `json:"type"` // "film" or "tv"
	Year        string   `json:"year,omitempty"`
	Director    string   `json:"director,omitempty"`
	Starring    []string `json:"starring,omitempty"`
	Genre       string   `json:"genre,omitempty"`
	Country     string   `json:"country,omitempty"`
	Language    string   `json:"language,omitempty"`
	Runtime     string   `json:"runtime,omitempty"`
	Creator     string   `json:"creator,omitempty"`
	Network     string   `json:"network,omitempty"`
	NumSeasons  string   `json:"num_seasons,omitempty"`
	NumEpisodes string   `json:"num_episodes,omitempty"`
	PlotSummary string   `json:"plot_summary,omitempty"`
	Categories  []string `json:"categories,omitempty"`
	ContentHash string   `json:"content_hash"`
	PageID      int64    `json:"page_id"`
}

func main() {
	inputPath := flag.String("input", "data/wiki/enwiki-latest-pages-articles.xml.bz2", "Path to Wikipedia dump file")
	outputDir := flag.String("output", "data/movies", "Output directory for filtered data")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	filmsPath := filepath.Join(*outputDir, "films.jsonl")
	tvPath := filepath.Join(*outputDir, "tv.jsonl")

	filmsFile, err := os.Create(filmsPath)
	if err != nil {
		log.Fatalf("create films file: %v", err)
	}
	defer filmsFile.Close()

	tvFile, err := os.Create(tvPath)
	if err != nil {
		log.Fatalf("create tv file: %v", err)
	}
	defer tvFile.Close()

	filmsEnc := json.NewEncoder(filmsFile)
	tvEnc := json.NewEncoder(tvFile)

	f, err := os.Open(*inputPath)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(*inputPath, ".bz2") {
		reader = bzip2.NewReader(f)
	}

	decoder := xml.NewDecoder(reader)
	var totalPages, filmCount, tvCount int64
	start := time.Now()

	log.Printf("Starting filter: %s → %s", *inputPath, *outputDir)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Interrupted. Films: %d, TV: %d, total scanned: %d", filmCount, tvCount, totalPages)
			return
		default:
		}

		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("xml token: %v", err)
		}

		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "page" {
			continue
		}

		var page xmlPage
		if err := decoder.DecodeElement(&page, &se); err != nil {
			log.Printf("decode error: %v", err)
			continue
		}

		totalPages++

		// Skip non-article namespaces and redirects.
		if page.NS != "0" || page.Redirect != nil {
			continue
		}

		rawText := page.Revision.Text
		lower := strings.ToLower(rawText)

		// Classify: film or TV?
		isFilm := matchesAny(lower, filmTemplates)
		isTV := matchesAny(lower, tvTemplates)

		// If no infobox match, check categories as fallback.
		if !isFilm && !isTV {
			cats := extractCategories(rawText)
			catsLower := strings.ToLower(strings.Join(cats, " "))
			isFilm = containsAny(catsLower, filmCategoryPatterns)
			if !isFilm {
				isTV = containsAny(catsLower, tvCategoryPatterns)
			}
		}

		if !isFilm && !isTV {
			continue
		}

		// Extract structured data from infobox.
		fields := extractInfoboxFields(rawText)
		cats := extractCategories(rawText)
		plot := extractPlotSection(rawText)

		hash := sha256.Sum256([]byte(rawText))
		record := MovieRecord{
			Title:       page.Title,
			URL:         "https://en.wikipedia.org/wiki/" + strings.ReplaceAll(page.Title, " ", "_"),
			ContentHash: fmt.Sprintf("%x", hash),
			PageID:      page.ID,
			Categories:  cats,
			PlotSummary: plot,
		}

		if isFilm {
			record.Type = "film"
			record.Director = cleanField(fields["director"])
			record.Year = extractYear(fields["released"], fields["release_date"], fields["year"])
			record.Starring = extractList(fields["starring"])
			record.Genre = cleanField(fields["genre"])
			record.Country = cleanField(fields["country"])
			record.Language = cleanField(fields["language"])
			record.Runtime = cleanField(fields["runtime"])

			if err := filmsEnc.Encode(record); err != nil {
				log.Printf("write film %q: %v", page.Title, err)
			}
			filmCount++
		} else {
			record.Type = "tv"
			record.Creator = cleanField(fields["creator"])
			record.Network = cleanField(fields["network"])
			record.NumSeasons = cleanField(fields["num_seasons"])
			record.NumEpisodes = cleanField(fields["num_episodes"])
			record.Starring = extractList(fields["starring"])
			record.Genre = cleanField(fields["genre"])
			record.Country = cleanField(fields["country"])
			record.Language = cleanField(fields["language"])
			record.Year = extractYear(fields["first_aired"], fields["released"], fields["year"])

			if err := tvEnc.Encode(record); err != nil {
				log.Printf("write tv %q: %v", page.Title, err)
			}
			tvCount++
		}

		if (filmCount+tvCount)%1000 == 0 {
			elapsed := time.Since(start)
			rate := float64(totalPages) / elapsed.Seconds()
			log.Printf("Progress: %d films, %d TV | scanned %d pages (%.0f pages/sec)",
				filmCount, tvCount, totalPages, rate)
		}
	}

	elapsed := time.Since(start)
	log.Printf("Done in %s: %d films → %s, %d TV shows → %s (scanned %d pages)",
		elapsed.Round(time.Second), filmCount, filmsPath, tvCount, tvPath, totalPages)
}

func matchesAny(lower string, templates []string) bool {
	for _, t := range templates {
		if strings.Contains(lower, t) {
			return true
		}
	}
	return false
}

func containsAny(s string, patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

func extractCategories(wikitext string) []string {
	var cats []string
	// [[Category:2024 films]] or [[Category:American television series]]
	re := regexp.MustCompile(`\[\[Category:([^\]|]+)`)
	for _, m := range re.FindAllStringSubmatch(wikitext, -1) {
		cats = append(cats, strings.TrimSpace(m[1]))
	}
	return cats
}

func extractInfoboxFields(wikitext string) map[string]string {
	fields := make(map[string]string)

	// Find the infobox section.
	lower := strings.ToLower(wikitext)
	start := -1
	for _, prefix := range append(filmTemplates, tvTemplates...) {
		idx := strings.Index(lower, prefix)
		if idx >= 0 {
			start = idx
			break
		}
	}
	if start < 0 {
		return fields
	}

	// Find the matching closing braces.
	depth := 0
	end := start
	for i := start; i < len(wikitext)-1; i++ {
		if wikitext[i] == '{' && wikitext[i+1] == '{' {
			depth++
			i++
		} else if wikitext[i] == '}' && wikitext[i+1] == '}' {
			depth--
			i++
			if depth == 0 {
				end = i + 1
				break
			}
		}
	}

	infobox := wikitext[start:end]
	for _, match := range reInfoboxField.FindAllStringSubmatch(infobox, -1) {
		key := strings.TrimSpace(strings.ToLower(match[1]))
		val := strings.TrimSpace(match[2])
		fields[key] = val
	}
	return fields
}

func extractPlotSection(wikitext string) string {
	// Find == Plot == or == Synopsis == section.
	lower := strings.ToLower(wikitext)
	plotHeaders := []string{"== plot ==", "== synopsis ==", "== premise ==", "==plot==", "==synopsis=="}
	start := -1
	for _, h := range plotHeaders {
		idx := strings.Index(lower, h)
		if idx >= 0 {
			start = idx + len(h)
			break
		}
	}
	if start < 0 {
		return ""
	}

	// Find the next section header.
	rest := wikitext[start:]
	endIdx := strings.Index(rest, "\n==")
	if endIdx < 0 {
		endIdx = len(rest)
	}
	if endIdx > 2000 {
		endIdx = 2000
	}

	plot := wikistrategy.StripWikitext(rest[:endIdx])
	plot = strings.TrimSpace(plot)
	if len(plot) > 1000 {
		plot = plot[:1000]
	}
	return plot
}

func cleanField(raw string) string {
	if raw == "" {
		return ""
	}
	s := reHTMLComment.ReplaceAllString(raw, "")
	s = reBrTag.ReplaceAllString(s, ", ")
	s = reHTMLTags.ReplaceAllString(s, "")

	// Resolve wiki links.
	s = reWikiLink.ReplaceAllStringFunc(s, func(match string) string {
		sub := reWikiLink.FindStringSubmatch(match)
		if len(sub) >= 3 && sub[2] != "" {
			return sub[2]
		}
		if len(sub) >= 2 {
			return sub[1]
		}
		return match
	})

	// Remove remaining {{ }} templates.
	depth := 0
	var buf strings.Builder
	for i := 0; i < len(s); i++ {
		if i < len(s)-1 && s[i] == '{' && s[i+1] == '{' {
			depth++
			i++
			continue
		}
		if i < len(s)-1 && s[i] == '}' && s[i+1] == '}' {
			depth--
			if depth < 0 {
				depth = 0
			}
			i++
			continue
		}
		if depth == 0 {
			buf.WriteByte(s[i])
		}
	}
	s = buf.String()

	s = strings.TrimSpace(s)
	// Collapse whitespace.
	parts := strings.Fields(s)
	return strings.Join(parts, " ")
}

func extractList(raw string) []string {
	if raw == "" {
		return nil
	}
	// Check for bulleted list items.
	matches := rePlainList.FindAllStringSubmatch(raw, -1)
	if len(matches) > 0 {
		var items []string
		for _, m := range matches {
			item := cleanField(m[1])
			if item != "" {
				items = append(items, item)
			}
		}
		return items
	}

	// Otherwise split on <br> or newlines.
	cleaned := cleanField(raw)
	parts := strings.Split(cleaned, ",")
	var items []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			items = append(items, p)
		}
	}
	return items
}

func extractYear(fields ...string) string {
	yearRe := regexp.MustCompile(`\b((?:19|20)\d{2})\b`)
	for _, f := range fields {
		if f == "" {
			continue
		}
		if m := yearRe.FindString(f); m != "" {
			return m
		}
	}
	return ""
}
