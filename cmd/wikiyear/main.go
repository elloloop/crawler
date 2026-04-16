// wikiyear crawls Wikipedia "List of {Language} films of {Year}" pages
// and extracts structured movie data from HTML tables via the MediaWiki API.
//
// These year-list tables are the most complete source of Indian film data,
// often listing 200-300+ films per year that aren't in TMDB or IMDb.
//
// Usage:
//
//	wikiyear -lang Telugu -from 2000 -to 2026 -output data/wikiyear/
//	wikiyear -lang all -from 2000 -to 2026 -output data/wikiyear/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var languages = map[string]string{
	"Telugu":    "te",
	"Tamil":     "ta",
	"Malayalam":  "ml",
	"Hindi":     "hi",
	"Kannada":   "kn",
	"Bengali":   "bn",
	"Marathi":   "mr",
	"Punjabi":   "pa",
	"Gujarati":  "gu",
	"Korean":    "ko",
	"Japanese":  "ja",
	"French":    "fr",
	"Spanish":   "es",
	"German":    "de",
	"Italian":   "it",
}

type WikiYearFilm struct {
	Title             string   `json:"title"`
	Year              int      `json:"year"`
	Language          string   `json:"language"`
	LanguageCode      string   `json:"language_code"`
	Director          string   `json:"director,omitempty"`
	Cast              []string `json:"cast,omitempty"`
	ProductionCompany string   `json:"production_company,omitempty"`
	Genre             string   `json:"genre,omitempty"`
	ReleaseMonth      string   `json:"release_month,omitempty"`
	WikiSourcePage    string   `json:"wiki_source_page"`
}

var (
	reHTML     = regexp.MustCompile(`<[^>]+>`)
	reRef      = regexp.MustCompile(`\[.*?\]`)
	reMultiWS  = regexp.MustCompile(`\s+`)
	reWikiLink = regexp.MustCompile(`<a [^>]*title="([^"]*)"[^>]*>([^<]*)</a>`)
)

func main() {
	lang := flag.String("lang", "", "Language name (Telugu, Tamil, etc.) or 'all'")
	from := flag.Int("from", 2000, "Start year")
	to := flag.Int("to", 2026, "End year")
	outputDir := flag.String("output", "data/wikiyear", "Output directory")
	flag.Parse()

	if *lang == "" {
		fmt.Println("Available languages:")
		for name, code := range languages {
			fmt.Printf("  %s (%s)\n", name, code)
		}
		fmt.Println("\nUsage: wikiyear -lang Telugu|all -from 2000 -to 2026")
		return
	}

	os.MkdirAll(*outputDir, 0o755)

	var targetLangs []string
	if *lang == "all" {
		for name := range languages {
			targetLangs = append(targetLangs, name)
		}
		sort.Strings(targetLangs)
	} else {
		if _, ok := languages[*lang]; !ok {
			log.Fatalf("Unknown language: %s", *lang)
		}
		targetLangs = []string{*lang}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	start := time.Now()

	for _, langName := range targetLangs {
		langCode := languages[langName]
		log.Printf("=== %s (%s) ===", langName, langCode)

		var allFilms []WikiYearFilm
		for year := *from; year <= *to; year++ {
			films := fetchYearList(client, langName, year)
			if len(films) > 0 {
				log.Printf("  %d: %d films", year, len(films))
				allFilms = append(allFilms, films...)
			}
			time.Sleep(500 * time.Millisecond) // be polite to Wikipedia API
		}

		if len(allFilms) == 0 {
			log.Printf("  No films found")
			continue
		}

		// Write output
		outPath := filepath.Join(*outputDir, langName+".jsonl")
		f, err := os.Create(outPath)
		if err != nil {
			log.Printf("  Error: %v", err)
			continue
		}
		enc := json.NewEncoder(f)
		for _, film := range allFilms {
			enc.Encode(film)
		}
		f.Close()

		log.Printf("  Total: %d films → %s", len(allFilms), outPath)
	}

	log.Printf("\nDone in %s", time.Since(start).Round(time.Second))
}

func fetchYearList(client *http.Client, langName string, year int) []WikiYearFilm {
	// Try different page name patterns (Wikipedia is inconsistent)
	pageNames := []string{
		fmt.Sprintf("List_of_%s_films_of_%d", langName, year),
		fmt.Sprintf("List_of_%s-language_films_of_%d", langName, year),
		fmt.Sprintf("List_of_Indian_%s_films_of_%d", langName, year),
		fmt.Sprintf("List_of_South_%s_films_of_%d", langName, year),
	}

	for _, pageName := range pageNames {
		apiURL := fmt.Sprintf(
			"https://en.wikipedia.org/w/api.php?action=parse&page=%s&format=json&prop=text",
			url.QueryEscape(pageName),
		)

		req, err := http.NewRequest("GET", apiURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", "CrawlerBot/0.1 (https://github.com/elloloop/crawler)")
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		var result struct {
			Parse struct {
				Text struct {
					Content string `json:"*"`
				} `json:"text"`
			} `json:"parse"`
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&result)

		if result.Error.Code != "" {
			continue
		}

		html := result.Parse.Text.Content
		if html == "" {
			continue
		}

		films := parseFilmTables(html, langName, languages[langName], year, pageName)
		if len(films) > 0 {
			return films
		}
	}

	return nil
}

func parseFilmTables(html, langName, langCode string, year int, pageName string) []WikiYearFilm {
	var films []WikiYearFilm

	// Find all tables — use greedy matching for nested content
	tableRe := regexp.MustCompile(`(?s)<table[^>]*>(.*?)</table>`)
	tableMatches := tableRe.FindAllStringSubmatch(html, -1)

	currentMonth := ""

	for _, tm := range tableMatches {
		table := tm[1]

		// Parse headers from first <tr> that has <th> elements
		headerRe := regexp.MustCompile(`(?s)<th[^>]*>(.*?)</th>`)
		headerMatches := headerRe.FindAllStringSubmatch(table, -1)
		if len(headerMatches) < 3 {
			continue
		}

		headers := make([]string, len(headerMatches))
		for i, h := range headerMatches {
			headers[i] = strings.ToLower(cleanText(h[1]))
		}

		// Must have "title" or "film" in headers — this is a film list table
		hasTitleHeader := false
		for _, h := range headers {
			if strings.Contains(h, "title") || strings.Contains(h, "film") {
				hasTitleHeader = true
				break
			}
		}
		if !hasTitleHeader {
			continue
		}

		// Map header names to column indices
		// These tables have: Opening | # | Title | Director | Cast | Production | Ref
		// Or: # | Title | Director | Cast | Production | Ref
		colMap := map[string]int{}
		for i, h := range headers {
			if strings.Contains(h, "title") || strings.Contains(h, "film") {
				colMap["title"] = i
			} else if strings.Contains(h, "director") {
				colMap["director"] = i
			} else if strings.Contains(h, "cast") || strings.Contains(h, "starring") {
				colMap["cast"] = i
			} else if strings.Contains(h, "production") || strings.Contains(h, "studio") || strings.Contains(h, "banner") {
				colMap["prod"] = i
			} else if strings.Contains(h, "genre") {
				colMap["genre"] = i
			} else if strings.Contains(h, "opening") {
				colMap["opening"] = i
			}
		}

		_, hasTitleIdx := colMap["title"]
		if !hasTitleIdx {
			continue
		}

		// Split table into rows — handle both <tr>...</tr> and <tr>\n<td>...
		rowRe := regexp.MustCompile(`(?s)<tr[^>]*>(.*?)</tr>`)
		rows := rowRe.FindAllStringSubmatch(table, -1)

		for _, row := range rows {
			rowHTML := row[1]
			if strings.Contains(rowHTML, "<th") {
				continue
			}

			cellRe := regexp.MustCompile(`(?s)<td[^>]*>(.*?)</td>`)
			cellMatches := cellRe.FindAllStringSubmatch(rowHTML, -1)
			if len(cellMatches) < 3 {
				continue
			}

			cells := make([]string, len(cellMatches))
			cellsRaw := make([]string, len(cellMatches))
			for i, c := range cellMatches {
				cellsRaw[i] = c[1]
				cells[i] = cleanText(c[1])
			}

			// Detect month in any of the first 2 cells
			for _, c := range cells[:min(2, len(cells))] {
				if isMonth(strings.ToUpper(c)) {
					currentMonth = strings.ToUpper(c[:1]) + strings.ToLower(c[1:])
				}
			}

			// Strategy: skip cells that are months, dates, or empty to find the title.
			// Title is the first cell that's not a month, not a number, and has letters.
			titleCellIdx := -1
			for i, c := range cells {
				if c == "" {
					continue
				}
				if isMonth(strings.ToUpper(c)) {
					continue
				}
				if _, err := strconv.Atoi(c); err == nil {
					continue
				}
				// This looks like a title
				titleCellIdx = i
				break
			}

			if titleCellIdx < 0 || titleCellIdx >= len(cells) {
				continue
			}

			title := cells[titleCellIdx]
			if isGarbage(title) {
				continue
			}

			film := WikiYearFilm{
				Title:          title,
				Year:           year,
				Language:        langName,
				LanguageCode:    langCode,
				ReleaseMonth:    currentMonth,
				WikiSourcePage:  fmt.Sprintf("https://en.wikipedia.org/wiki/%s", pageName),
			}

			// Fields follow title: director, cast, production company
			dirIdx := titleCellIdx + 1
			castIdx := titleCellIdx + 2
			prodIdx := titleCellIdx + 3

			if dirIdx < len(cells) {
				film.Director = cells[dirIdx]
			}
			if castIdx < len(cellsRaw) {
				names := extractNames(cellsRaw[castIdx])
				if len(names) > 0 {
					film.Cast = names
				} else if castIdx < len(cells) && cells[castIdx] != "" {
					film.Cast = splitNames(cells[castIdx])
				}
			}
			if prodIdx < len(cells) {
				prod := cells[prodIdx]
				// Skip if it looks like a reference [27]
				if !strings.HasPrefix(prod, "[") {
					film.ProductionCompany = prod
				}
			}

			films = append(films, film)
		}
	}

	return films
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func findCol(headers []string, keywords ...string) int {
	for i, h := range headers {
		lower := strings.ToLower(h)
		for _, kw := range keywords {
			if strings.Contains(lower, kw) {
				return i
			}
		}
	}
	return -1
}

func cleanText(html string) string {
	// Remove references [1], [2], etc.
	text := reRef.ReplaceAllString(html, "")
	// Remove HTML tags
	text = reHTML.ReplaceAllString(text, "")
	// Decode HTML entities
	text = strings.ReplaceAll(text, "&#91;", "[")
	text = strings.ReplaceAll(text, "&#93;", "]")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	text = strings.ReplaceAll(text, "&#160;", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	// Collapse whitespace
	text = reMultiWS.ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}

func extractNames(html string) []string {
	matches := reWikiLink.FindAllStringSubmatch(html, -1)
	var names []string
	seen := make(map[string]bool)
	for _, m := range matches {
		name := m[2] // display text
		if name == "" {
			name = m[1] // title attribute
		}
		name = strings.TrimSpace(name)
		if name == "" || len(name) < 2 || len(name) > 60 {
			continue
		}
		lower := strings.ToLower(name)
		if seen[lower] || isMonth(strings.ToUpper(name)) {
			continue
		}
		// Skip if it's a year or number
		if _, err := strconv.Atoi(name); err == nil {
			continue
		}
		seen[lower] = true
		names = append(names, name)
	}
	return names
}

func splitNames(text string) []string {
	// Split on common separators
	parts := regexp.MustCompile(`[,\n]`).Split(text, -1)
	var names []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" && len(p) > 1 && len(p) < 60 {
			names = append(names, p)
		}
	}
	return names
}

func isMonth(s string) bool {
	months := map[string]bool{
		"JANUARY": true, "FEBRUARY": true, "MARCH": true, "APRIL": true,
		"MAY": true, "JUNE": true, "JULY": true, "AUGUST": true,
		"SEPTEMBER": true, "OCTOBER": true, "NOVEMBER": true, "DECEMBER": true,
	}
	return months[s]
}

func isGarbage(title string) bool {
	// Skip titles that are just numbers, single chars, or common non-title text
	if len(title) < 2 {
		return true
	}
	if _, err := strconv.Atoi(title); err == nil {
		return true
	}
	// Check if mostly non-letter characters
	letters := 0
	for _, r := range title {
		if unicode.IsLetter(r) {
			letters++
		}
	}
	return letters < 2
}
