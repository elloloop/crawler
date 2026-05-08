// wikirefresh keeps our extracted Wikipedia film/TV data fresh without
// re-downloading the full 25 GB dump.
//
// Strategy:
//  1. Load existing films.jsonl + tv.jsonl (each record has a page_id).
//  2. Batch-query MediaWiki API for current `lastrevid` of each page_id.
//     This is metadata-only and very cheap (~50 ids per call).
//  3. Compare current revid to stored state in data/wiki/page_state.jsonl.
//     Pages with changed revid are stale and need refetching.
//  4. For stale pages, fetch full wikitext via the same API, re-run the
//     same extraction logic as wikifilter, and emit updated records.
//  5. Update page_state.jsonl with new revids.
//
// On first run, no state file exists — we capture revids for everything
// in one pass without refetching content. Subsequent runs only refetch
// what actually changed (typically <5% per month).
//
// Usage:
//
//	wikirefresh -input data/movies/ -state data/wiki/page_state.jsonl
//	wikirefresh -input data/movies/ -state data/wiki/page_state.jsonl -refetch
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	apiEndpoint   = "https://en.wikipedia.org/w/api.php"
	userAgent     = "CrawlerBot/0.1 (arun88m@gmail.com) wikirefresh"
	batchInfoSize = 50  // page IDs per metadata call
	batchTextSize = 20  // page IDs per content call (wikitext is large)
	concurrency   = 4   // parallel API calls
	rateDelay     = 100 * time.Millisecond
)

// MovieRecord mirrors wikifilter's output schema.
type MovieRecord struct {
	Title       string   `json:"title"`
	URL         string   `json:"url"`
	Type        string   `json:"type"`
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

// PageState tracks the latest seen revid per page.
type PageState struct {
	PageID    int64  `json:"page_id"`
	Revid     int64  `json:"revid"`
	LastCheck string `json:"last_check"`
}

func main() {
	inputDir := flag.String("input", "data/movies", "Directory containing films.jsonl and tv.jsonl")
	statePath := flag.String("state", "data/wiki/page_state.jsonl", "Path to revid state file")
	refetch := flag.Bool("refetch", false, "Fetch content for stale pages and re-extract")
	outputDir := flag.String("output", "", "Output directory for refreshed JSONL (defaults to input)")
	dryRun := flag.Bool("dry-run", false, "Only report what would change, don't write files")
	flag.Parse()

	if *outputDir == "" {
		*outputDir = *inputDir
	}

	start := time.Now()

	// Load existing records, indexed by page_id.
	log.Println("Loading existing records...")
	films, err := loadRecords(filepath.Join(*inputDir, "films.jsonl"))
	if err != nil {
		log.Fatalf("load films: %v", err)
	}
	tv, err := loadRecords(filepath.Join(*inputDir, "tv.jsonl"))
	if err != nil {
		log.Fatalf("load tv: %v", err)
	}
	log.Printf("  Loaded %d films, %d TV", len(films), len(tv))

	// Build a single map of all known pages.
	allPages := make(map[int64]*MovieRecord, len(films)+len(tv))
	for i := range films {
		allPages[films[i].PageID] = &films[i]
	}
	for i := range tv {
		allPages[tv[i].PageID] = &tv[i]
	}

	// Collect page IDs.
	pageIDs := make([]int64, 0, len(allPages))
	for id := range allPages {
		if id > 0 {
			pageIDs = append(pageIDs, id)
		}
	}
	log.Printf("  %d unique page_ids to check", len(pageIDs))

	// Load existing state.
	state, err := loadState(*statePath)
	if err != nil {
		log.Fatalf("load state: %v", err)
	}
	log.Printf("  State file has %d revids", len(state))

	// Phase 1: batch-fetch current revids for all pages.
	log.Println("Fetching current revids from MediaWiki API...")
	current, deleted := fetchCurrentRevids(pageIDs)
	log.Printf("  %d pages active, %d deleted/missing", len(current), len(deleted))

	// Phase 2: identify stale (revid changed) and new (no prior state).
	var stale, newPages []int64
	for _, id := range pageIDs {
		curRev, ok := current[id]
		if !ok {
			continue // deleted, handled below
		}
		prev, exists := state[id]
		if !exists {
			newPages = append(newPages, id)
		} else if prev.Revid != curRev {
			stale = append(stale, id)
		}
	}
	log.Printf("  Stale (revid changed): %d", len(stale))
	log.Printf("  New (no prior revid recorded): %d", len(newPages))
	log.Printf("  Deleted (page no longer exists): %d", len(deleted))

	if !*refetch {
		log.Println("\nMetadata-only pass complete. Pass -refetch to download changed content.")
		// Still update state to capture revids on first run.
		if err := writeState(*statePath, allPages, current, state); err != nil {
			log.Printf("write state: %v", err)
		}
		log.Printf("\nDone in %s", time.Since(start).Round(time.Second))
		return
	}

	// Phase 3: refetch content for stale pages and re-extract.
	if *dryRun {
		log.Printf("\nDry run: would refetch %d stale pages", len(stale))
		return
	}

	if len(stale) == 0 {
		log.Println("\nNothing stale to refetch.")
	} else {
		log.Printf("\nRefetching %d stale pages...", len(stale))
		updated := fetchAndExtract(stale)
		applied := 0
		for _, rec := range updated {
			if old, ok := allPages[rec.PageID]; ok {
				if rec.ContentHash != old.ContentHash {
					*old = rec
					applied++
				}
			}
		}
		log.Printf("  %d records actually changed (rest matched existing hash)", applied)
	}

	// Drop deleted pages.
	for _, id := range deleted {
		delete(allPages, id)
	}

	// Phase 4: write back to films.jsonl and tv.jsonl.
	if err := writeRecords(filepath.Join(*outputDir, "films.jsonl"), allPages, "film"); err != nil {
		log.Fatalf("write films: %v", err)
	}
	if err := writeRecords(filepath.Join(*outputDir, "tv.jsonl"), allPages, "tv"); err != nil {
		log.Fatalf("write tv: %v", err)
	}

	if err := writeState(*statePath, allPages, current, state); err != nil {
		log.Printf("write state: %v", err)
	}

	log.Printf("\nDone in %s", time.Since(start).Round(time.Second))
}

// --- Data loading ---

func loadRecords(path string) ([]MovieRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 8<<20)
	var records []MovieRecord
	for scanner.Scan() {
		var r MovieRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			continue
		}
		records = append(records, r)
	}
	return records, scanner.Err()
}

func loadState(path string) (map[int64]PageState, error) {
	state := make(map[int64]PageState)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var s PageState
		if err := json.Unmarshal(scanner.Bytes(), &s); err != nil {
			continue
		}
		state[s.PageID] = s
	}
	return state, scanner.Err()
}

func writeState(path string, allPages map[int64]*MovieRecord, current map[int64]int64, prior map[int64]PageState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	now := time.Now().UTC().Format(time.RFC3339)
	for id := range allPages {
		revid, ok := current[id]
		if !ok {
			// Page deleted — skip from state.
			continue
		}
		check := now
		if prev, ok := prior[id]; ok && prev.Revid == revid {
			// Unchanged, keep the older check timestamp.
			check = prev.LastCheck
		}
		enc.Encode(PageState{PageID: id, Revid: revid, LastCheck: check})
	}
	if err := w.Flush(); err != nil {
		return err
	}
	f.Close()
	return os.Rename(tmp, path)
}

func writeRecords(path string, allPages map[int64]*MovieRecord, recType string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	count := 0
	for _, rec := range allPages {
		if rec.Type != recType {
			continue
		}
		enc.Encode(rec)
		count++
	}
	if err := w.Flush(); err != nil {
		return err
	}
	f.Close()
	log.Printf("  Wrote %d %s records → %s", count, recType, path)
	return os.Rename(tmp, path)
}

// --- MediaWiki API ---

type apiResponse struct {
	Query struct {
		Pages map[string]struct {
			PageID  int64           `json:"pageid"`
			Title   string          `json:"title"`
			Missing json.RawMessage `json:"missing,omitempty"`
			LastRev int64           `json:"lastrevid,omitempty"`
			Revisions []struct {
				Slots struct {
					Main struct {
						Content string `json:"*"`
					} `json:"main"`
				} `json:"slots"`
				Content string `json:"*"`
				Revid   int64  `json:"revid"`
			} `json:"revisions,omitempty"`
		} `json:"pages"`
	} `json:"query"`
}

func fetchCurrentRevids(pageIDs []int64) (map[int64]int64, []int64) {
	current := make(map[int64]int64)
	var deleted []int64
	var mu sync.Mutex

	type batch struct{ ids []int64 }
	jobs := make(chan batch)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range jobs {
				revids, miss := fetchInfoBatch(b.ids)
				mu.Lock()
				for id, r := range revids {
					current[id] = r
				}
				deleted = append(deleted, miss...)
				mu.Unlock()
				time.Sleep(rateDelay)
			}
		}()
	}

	totalBatches := (len(pageIDs) + batchInfoSize - 1) / batchInfoSize
	done := 0
	for i := 0; i < len(pageIDs); i += batchInfoSize {
		end := i + batchInfoSize
		if end > len(pageIDs) {
			end = len(pageIDs)
		}
		jobs <- batch{ids: pageIDs[i:end]}
		done++
		if done%200 == 0 {
			log.Printf("  Progress: %d / %d batches", done, totalBatches)
		}
	}
	close(jobs)
	wg.Wait()

	return current, deleted
}

func fetchInfoBatch(ids []int64) (map[int64]int64, []int64) {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	params := url.Values{
		"action":  {"query"},
		"format":  {"json"},
		"prop":    {"info"},
		"pageids": {strings.Join(parts, "|")},
	}
	body, err := apiGET(params)
	if err != nil {
		log.Printf("  info batch error: %v", err)
		return nil, nil
	}

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Printf("  info parse: %v", err)
		return nil, nil
	}
	revids := make(map[int64]int64)
	var missing []int64
	for _, p := range resp.Query.Pages {
		if len(p.Missing) > 0 {
			if p.PageID > 0 {
				missing = append(missing, p.PageID)
			}
			continue
		}
		if p.LastRev > 0 {
			revids[p.PageID] = p.LastRev
		}
	}
	return revids, missing
}

func fetchAndExtract(pageIDs []int64) []MovieRecord {
	var out []MovieRecord
	var mu sync.Mutex

	jobs := make(chan []int64)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				records := fetchTextBatch(batch)
				mu.Lock()
				out = append(out, records...)
				mu.Unlock()
				time.Sleep(rateDelay)
			}
		}()
	}

	total := (len(pageIDs) + batchTextSize - 1) / batchTextSize
	done := 0
	for i := 0; i < len(pageIDs); i += batchTextSize {
		end := i + batchTextSize
		if end > len(pageIDs) {
			end = len(pageIDs)
		}
		jobs <- pageIDs[i:end]
		done++
		if done%50 == 0 {
			log.Printf("  Refetch progress: %d / %d batches", done, total)
		}
	}
	close(jobs)
	wg.Wait()
	return out
}

func fetchTextBatch(ids []int64) []MovieRecord {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	params := url.Values{
		"action":   {"query"},
		"format":   {"json"},
		"prop":     {"revisions"},
		"rvprop":   {"content|ids"},
		"rvslots":  {"main"},
		"pageids":  {strings.Join(parts, "|")},
		"formatversion": {"1"},
	}
	body, err := apiGET(params)
	if err != nil {
		log.Printf("  text batch error: %v", err)
		return nil
	}

	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Printf("  text parse: %v", err)
		return nil
	}

	var records []MovieRecord
	for _, p := range resp.Query.Pages {
		if len(p.Missing) > 0 || len(p.Revisions) == 0 {
			continue
		}
		text := p.Revisions[0].Slots.Main.Content
		if text == "" {
			text = p.Revisions[0].Content
		}
		if text == "" {
			continue
		}
		rec := extractFromWikitext(p.PageID, p.Title, text)
		if rec != nil {
			records = append(records, *rec)
		}
	}
	return records
}

var httpClient = &http.Client{Timeout: 60 * time.Second}

func apiGET(params url.Values) ([]byte, error) {
	req, err := http.NewRequest("GET", apiEndpoint+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := httpClient.Do(req)
		if err != nil {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			resp.Body.Close()
			time.Sleep(time.Duration(2<<attempt) * time.Second)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		return body, err
	}
	return nil, fmt.Errorf("api request failed after retries")
}

// --- Extraction (mirrors wikifilter) ---

var (
	filmTemplates = []string{"{{infobox film", "{{infobox movie"}
	tvTemplates   = []string{"{{infobox television", "{{infobox tv series"}
)

func extractFromWikitext(pageID int64, title, text string) *MovieRecord {
	lower := strings.ToLower(text)
	isFilm := containsAny(lower, filmTemplates)
	isTV := containsAny(lower, tvTemplates)
	if !isFilm && !isTV {
		return nil
	}
	fields := extractInfoboxFields(text)
	cats := extractCategories(text)
	plot := extractPlotSection(text)

	hash := sha256.Sum256([]byte(text))
	rec := &MovieRecord{
		Title:       title,
		URL:         "https://en.wikipedia.org/wiki/" + strings.ReplaceAll(title, " ", "_"),
		ContentHash: fmt.Sprintf("%x", hash),
		PageID:      pageID,
		Categories:  cats,
		PlotSummary: plot,
	}
	if isFilm {
		rec.Type = "film"
		rec.Director = cleanField(fields["director"])
		rec.Year = extractYear(fields["released"], fields["release_date"], fields["year"])
		rec.Starring = extractList(fields["starring"])
		rec.Genre = cleanField(fields["genre"])
		rec.Country = cleanField(fields["country"])
		rec.Language = cleanField(fields["language"])
		rec.Runtime = cleanField(fields["runtime"])
	} else {
		rec.Type = "tv"
		rec.Creator = cleanField(fields["creator"])
		rec.Network = cleanField(fields["network"])
		rec.NumSeasons = cleanField(fields["num_seasons"])
		rec.NumEpisodes = cleanField(fields["num_episodes"])
		rec.Starring = extractList(fields["starring"])
		rec.Genre = cleanField(fields["genre"])
		rec.Country = cleanField(fields["country"])
		rec.Language = cleanField(fields["language"])
		rec.Year = extractYear(fields["first_aired"], fields["released"], fields["year"])
	}
	return rec
}

func containsAny(s string, patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

var categoryRe = regexp.MustCompile(`\[\[Category:([^\]|]+)`)

func extractCategories(text string) []string {
	matches := categoryRe.FindAllStringSubmatch(text, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, strings.TrimSpace(m[1]))
	}
	return out
}

var infoboxFieldRe = regexp.MustCompile(`(?m)^\s*\|\s*([a-zA-Z_][\w-]*)\s*=\s*(.*?)$`)

func extractInfoboxFields(text string) map[string]string {
	fields := make(map[string]string)
	idx := strings.Index(strings.ToLower(text), "{{infobox")
	if idx < 0 {
		return fields
	}
	end := findInfoboxEnd(text, idx)
	if end < 0 {
		end = len(text)
	}
	chunk := text[idx:end]
	for _, m := range infoboxFieldRe.FindAllStringSubmatch(chunk, -1) {
		key := strings.ToLower(strings.TrimSpace(m[1]))
		val := strings.TrimSpace(m[2])
		if existing, ok := fields[key]; ok && existing != "" {
			continue
		}
		fields[key] = val
	}
	return fields
}

func findInfoboxEnd(text string, start int) int {
	depth := 0
	for i := start; i < len(text)-1; i++ {
		if text[i] == '{' && text[i+1] == '{' {
			depth++
			i++
		} else if text[i] == '}' && text[i+1] == '}' {
			depth--
			i++
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

var plotHeadingRe = regexp.MustCompile(`(?m)^\s*==\s*(Plot|Synopsis|Plot summary|Storyline)\s*==`)

func extractPlotSection(text string) string {
	loc := plotHeadingRe.FindStringIndex(text)
	if loc == nil {
		return ""
	}
	rest := text[loc[1]:]
	end := strings.Index(rest, "\n==")
	if end < 0 {
		end = len(rest)
	}
	plot := rest[:end]
	plot = stripWikiMarkup(plot)
	plot = strings.TrimSpace(plot)
	if len(plot) > 4000 {
		plot = plot[:4000]
	}
	return plot
}

var (
	wikilinkRe   = regexp.MustCompile(`\[\[(?:[^\]|]*\|)?([^\]]+)\]\]`)
	templateRe   = regexp.MustCompile(`\{\{[^{}]*\}\}`)
	htmlTagRe    = regexp.MustCompile(`<[^>]+>`)
	multiSpaceRe = regexp.MustCompile(`\s+`)
)

func stripWikiMarkup(s string) string {
	for {
		next := templateRe.ReplaceAllString(s, "")
		if next == s {
			break
		}
		s = next
	}
	s = wikilinkRe.ReplaceAllString(s, "$1")
	s = htmlTagRe.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "'''", "")
	s = strings.ReplaceAll(s, "''", "")
	s = multiSpaceRe.ReplaceAllString(s, " ")
	return s
}

func cleanField(raw string) string {
	if raw == "" {
		return ""
	}
	s := stripWikiMarkup(raw)
	s = strings.TrimSpace(s)
	if strings.Contains(s, "<br") {
		s = strings.SplitN(s, "<br", 2)[0]
	}
	return strings.TrimSpace(s)
}

var listSepRe = regexp.MustCompile(`\s*[,;]\s*|\s*<br\s*/?>\s*|\n\*\s*`)

func extractList(raw string) []string {
	if raw == "" {
		return nil
	}
	s := stripWikiMarkup(raw)
	parts := listSepRe.Split(s, -1)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

var yearRe = regexp.MustCompile(`\b(19|20)\d{2}\b`)

func extractYear(values ...string) string {
	for _, v := range values {
		if v == "" {
			continue
		}
		if m := yearRe.FindString(v); m != "" {
			return m
		}
	}
	return ""
}
