// ottcrawl crawls OTT platforms via their sitemaps and extracts structured data
// (JSON-LD, meta tags) from each content page.
//
// Respects robots.txt, rate limits, and outputs JSONL per platform.
//
// Usage:
//
//	ottcrawl -platform aha -output data/ott/
//	ottcrawl -platform hotstar -output data/ott/
package main

import (
	"context"
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/elloloop/crawler/internal/parser"
)

// --- Platform configs ---

type PlatformConfig struct {
	Name           string
	SitemapURL     string
	RatePerSec     float64
	Workers        int
	UserAgent      string
	ContentPaths   []string // URL path prefixes to include in page URLs (e.g., "/movie/", "/webseries/")
	SitemapFilters []string // Substrings to match in sub-sitemap URLs (e.g., "MOVIE", "SHOWS")
}

var platforms = map[string]*PlatformConfig{
	"aha": {
		Name:           "aha",
		SitemapURL:     "https://www.aha.video/sitemap/sitemapindex.xml",
		RatePerSec:     3,
		Workers:        3,
		UserAgent:      "CrawlerBot/0.1 (+https://github.com/elloloop/crawler)",
		ContentPaths:   []string{"/movie/", "/webseries/", "/webepisode/"},
		SitemapFilters: []string{}, // fetch all sub-sitemaps
	},
	"hotstar": {
		Name:           "hotstar",
		SitemapURL:     "https://www.hotstar.com/in/new-sitemap.xml",
		RatePerSec:     5,
		Workers:        5,
		UserAgent:      "Googlebot/2.1 (+http://www.google.com/bot.html)",
		ContentPaths:   []string{"/movies/", "/shows/"},
		SitemapFilters: []string{"MOVIE", "SHOWS"}, // only fetch movie and show sitemaps
	},
}

// --- Output record ---

type OTTRecord struct {
	Platform     string   `json:"platform"`
	URL          string   `json:"url"`
	ContentType  string   `json:"content_type"` // movie, webseries, webepisode
	Title        string   `json:"title"`
	Description  string   `json:"description,omitempty"`
	Year         string   `json:"year,omitempty"`
	Language     string   `json:"language,omitempty"`
	Genres       []string `json:"genres,omitempty"`
	Cast         []string `json:"cast,omitempty"`
	Director     string   `json:"director,omitempty"`
	Duration     string   `json:"duration,omitempty"`
	ThumbnailURL string   `json:"thumbnail_url,omitempty"`
	BannerURL    string   `json:"banner_url,omitempty"`
	AgeRating    string   `json:"age_rating,omitempty"`
	FreeOrPaid   string   `json:"free_or_paid,omitempty"`
	CrawledAt    string   `json:"crawled_at"`
	RawLDJSON    json.RawMessage `json:"raw_ld_json,omitempty"`
}

// --- Sitemap types ---

type sitemapIndex struct {
	XMLName  xml.Name       `xml:"sitemapindex"`
	Sitemaps []sitemapEntry `xml:"sitemap"`
}

type sitemapEntry struct {
	Loc string `xml:"loc"`
}

type urlSet struct {
	XMLName xml.Name   `xml:"urlset"`
	URLs    []urlEntry `xml:"url"`
}

type urlEntry struct {
	Loc string `xml:"loc"`
}

func main() {
	platformName := flag.String("platform", "", "Platform to crawl (aha, hotstar, etc.)")
	outputDir := flag.String("output", "data/ott", "Output directory")
	urlsFile := flag.String("urls-file", "", "File with URLs to crawl (one per line), skips sitemap discovery")
	flag.Parse()

	if *platformName == "" {
		log.Fatal("-platform is required. Available: aha")
	}

	cfg, ok := platforms[*platformName]
	if !ok {
		log.Fatalf("Unknown platform: %s", *platformName)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &uaTransport{
			base: http.DefaultTransport,
			ua:   cfg.UserAgent,
		},
		// Don't follow redirects automatically — some sites geo-redirect sitemaps
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	// Step 1: Get URLs — from file or sitemap.
	var urls []string
	if *urlsFile != "" {
		log.Printf("[%s] Loading URLs from %s", cfg.Name, *urlsFile)
		urls = loadURLsFromFile(*urlsFile)
	} else {
		log.Printf("[%s] Fetching sitemap: %s", cfg.Name, cfg.SitemapURL)
		urls = discoverURLs(client, cfg)
	}
	log.Printf("[%s] Found %d content URLs", cfg.Name, len(urls))

	if len(urls) == 0 {
		log.Fatal("No URLs found")
	}

	// Step 2: Crawl each URL.
	outPath := filepath.Join(*outputDir, cfg.Name+".jsonl")
	f, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer f.Close()

	var mu sync.Mutex
	enc := json.NewEncoder(f)

	urlCh := make(chan string, cfg.Workers*2)
	limiter := time.NewTicker(time.Duration(float64(time.Second) / cfg.RatePerSec))
	defer limiter.Stop()

	var wg sync.WaitGroup
	var crawled, failed atomic.Int64
	start := time.Now()

	for range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for url := range urlCh {
				select {
				case <-ctx.Done():
					return
				case <-limiter.C:
				}

				record, err := crawlPage(client, cfg, url)
				if err != nil {
					failed.Add(1)
					continue
				}

				mu.Lock()
				enc.Encode(record)
				mu.Unlock()

				n := crawled.Add(1)
				if n%100 == 0 {
					elapsed := time.Since(start).Seconds()
					log.Printf("[%s] Crawled %d/%d (%.1f/s), failed: %d",
						cfg.Name, n, len(urls), float64(n)/elapsed, failed.Load())
				}
			}
		}()
	}

	go func() {
		for _, u := range urls {
			select {
			case <-ctx.Done():
				close(urlCh)
				return
			case urlCh <- u:
			}
		}
		close(urlCh)
	}()

	wg.Wait()

	log.Printf("[%s] Done: %d crawled, %d failed, %s elapsed → %s",
		cfg.Name, crawled.Load(), failed.Load(),
		time.Since(start).Round(time.Second), outPath)
}

// --- Sitemap discovery ---

func loadURLsFromFile(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open urls file: %v", err)
	}
	defer f.Close()
	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && strings.HasPrefix(line, "http") {
			urls = append(urls, line)
		}
	}
	return urls
}

func discoverURLs(client *http.Client, cfg *PlatformConfig) []string {
	body := fetchURL(client, cfg.SitemapURL)
	if body == nil {
		return nil
	}

	// Try as sitemap index first.
	var idx sitemapIndex
	if err := xml.Unmarshal(body, &idx); err == nil && len(idx.Sitemaps) > 0 {
		log.Printf("  Sitemap index with %d sub-sitemaps", len(idx.Sitemaps))
		var allURLs []string
		for _, sm := range idx.Sitemaps {
			// Filter sub-sitemaps if SitemapFilters is set
			if len(cfg.SitemapFilters) > 0 {
				isRelevant := false
				for _, filter := range cfg.SitemapFilters {
					if strings.Contains(strings.ToUpper(sm.Loc), strings.ToUpper(filter)) {
						isRelevant = true
						break
					}
				}
				if !isRelevant {
					continue
				}
			}

			log.Printf("  Fetching sub-sitemap: %s", sm.Loc)
			smBody := fetchURL(client, sm.Loc)
			if smBody == nil {
				log.Printf("  Warning: failed to fetch %s", sm.Loc)
				continue
			}
			var us urlSet
			if err := xml.Unmarshal(smBody, &us); err == nil {
				before := len(allURLs)
				for _, u := range us.URLs {
					if isContentURL(u.Loc, cfg.ContentPaths) {
						allURLs = append(allURLs, u.Loc)
					}
				}
				log.Printf("  → %d URLs from %s", len(allURLs)-before, sm.Loc)
			}
		}
		return allURLs
	}

	// Try as direct urlset.
	var us urlSet
	if err := xml.Unmarshal(body, &us); err == nil {
		var urls []string
		for _, u := range us.URLs {
			if isContentURL(u.Loc, cfg.ContentPaths) {
				urls = append(urls, u.Loc)
			}
		}
		return urls
	}

	return nil
}

func isContentURL(url string, paths []string) bool {
	for _, p := range paths {
		if strings.Contains(url, p) {
			return true
		}
	}
	return false
}

// --- Page crawling ---

var (
	reLDJSON  = regexp.MustCompile(`(?s)<script type="application/ld\+json">\s*(.*?)\s*</script>`)
	reYear    = regexp.MustCompile(`\b((?:19|20)\d{2})\b`)
)

func crawlPage(client *http.Client, cfg *PlatformConfig, url string) (*OTTRecord, error) {
	body := fetchURL(client, url)
	if body == nil {
		return nil, fmt.Errorf("fetch failed: %s", url)
	}

	// Determine content type from URL.
	contentType := "unknown"
	if strings.Contains(url, "/movie/") {
		contentType = "movie"
	} else if strings.Contains(url, "/webseries/") {
		contentType = "webseries"
	} else if strings.Contains(url, "/webepisode/") {
		contentType = "webepisode"
	} else if strings.Contains(url, "/show/") {
		contentType = "show"
	}

	record := &OTTRecord{
		Platform:    cfg.Name,
		URL:         url,
		ContentType: contentType,
		CrawledAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// Extract from JSON-LD (most reliable).
	matches := reLDJSON.FindAllSubmatch(body, -1)
	for _, match := range matches {
		var ld map[string]any
		if err := json.Unmarshal(match[1], &ld); err != nil {
			// Try array
			var ldArr []map[string]any
			if err := json.Unmarshal(match[1], &ldArr); err == nil {
				for _, item := range ldArr {
					extractLDJSON(record, item)
				}
			}
			continue
		}
		extracted := extractLDJSON(record, ld)
		if extracted {
			record.RawLDJSON = match[1]
		}
	}

	// Extract og:image as banner (always available even when JSON-LD isn't)
	if ogImg := extractMeta(body, "og:image"); ogImg != "" {
		record.BannerURL = strings.ReplaceAll(ogImg, "&amp;", "&")
		if record.ThumbnailURL == "" {
			record.ThumbnailURL = record.BannerURL
		}
	}

	// Fallback: extract from meta tags if JSON-LD didn't give us a title.
	if record.Title == "" {
		pr := parser.Parse(url, body)
		record.Title = pr.Title
		if record.Description == "" {
			record.Description = pr.MetaDescription
		}
	}

	// Extract language from meta if not from JSON-LD
	if record.Language == "" {
		if lang := extractMeta(body, "inLanguage"); lang != "" {
			record.Language = lang
		}
	}

	// Validate: must have a title.
	if record.Title == "" {
		return nil, fmt.Errorf("no title found: %s", url)
	}

	return record, nil
}

func extractLDJSON(record *OTTRecord, ld map[string]any) bool {
	typ, _ := ld["@type"].(string)
	if typ != "VideoObject" && typ != "Movie" && typ != "TVSeries" && typ != "TVEpisode" {
		return false
	}

	if name, ok := ld["name"].(string); ok && name != "" {
		record.Title = name
	}
	if desc, ok := ld["description"].(string); ok && desc != "" {
		record.Description = desc
	}
	// Thumbnail — try multiple field names
	for _, field := range []string{"thumbnailURL", "thumbnailUrl", "thumbnail"} {
		if thumb, ok := ld[field].(string); ok && thumb != "" {
			record.ThumbnailURL = thumb
			break
		}
	}
	// Image/banner
	if img, ok := ld["image"].(string); ok && img != "" {
		if record.BannerURL == "" {
			record.BannerURL = img
		}
		if record.ThumbnailURL == "" {
			record.ThumbnailURL = img
		}
	}
	// Language directly on the object
	if lang, ok := ld["inLanguage"].(string); ok && lang != "" {
		record.Language = lang
	}
	// Age rating
	if age, ok := ld["typicalAgeRange"].(string); ok && age != "" {
		record.AgeRating = age
	}
	if dur, ok := ld["duration"].(string); ok {
		record.Duration = dur
	}

	// Year from releaseYear, uploadDate, dateCreated
	if ry, ok := ld["releaseYear"].(string); ok && ry != "" {
		record.Year = ry
	}
	if record.Year == "" {
		for _, field := range []string{"uploadDate", "dateCreated", "datePublished"} {
			if dateStr, ok := ld[field].(string); ok {
				if m := reYear.FindString(dateStr); m != "" {
					record.Year = m
					break
				}
			}
		}
	}

	// Genre
	if genres, ok := ld["genre"].([]any); ok {
		for _, g := range genres {
			if gs, ok := g.(string); ok {
				record.Genres = append(record.Genres, gs)
			}
		}
	} else if genre, ok := ld["genre"].(string); ok {
		record.Genres = []string{genre}
	}

	// Language from potentialAction
	if pa, ok := ld["potentialAction"].(map[string]any); ok {
		if targets, ok := pa["target"].([]any); ok {
			for _, t := range targets {
				if tm, ok := t.(map[string]any); ok {
					if lang, ok := tm["inLanguage"].(string); ok {
						record.Language = lang
					}
				}
			}
		}
	}

	// Price/subscription info
	if pa, ok := ld["potentialAction"].(map[string]any); ok {
		if offer, ok := pa["expectsAcceptanceOf"].(map[string]any); ok {
			if cat, ok := offer["category"].(string); ok {
				record.FreeOrPaid = cat
			}
			if name, ok := offer["name"].(string); ok {
				if strings.Contains(name, "free") {
					record.FreeOrPaid = "free"
				} else if strings.Contains(name, "premium") || strings.Contains(name, "subscription") {
					record.FreeOrPaid = "subscription"
				}
			}
		}
	}

	// Cast from actors/actor
	if actors, ok := ld["actor"].([]any); ok {
		for _, a := range actors {
			if am, ok := a.(map[string]any); ok {
				if name, ok := am["name"].(string); ok {
					record.Cast = append(record.Cast, name)
				}
			}
		}
	}

	// Director
	if dir, ok := ld["director"].(map[string]any); ok {
		if name, ok := dir["name"].(string); ok {
			record.Director = name
		}
	} else if dirs, ok := ld["director"].([]any); ok {
		for _, d := range dirs {
			if dm, ok := d.(map[string]any); ok {
				if name, ok := dm["name"].(string); ok {
					record.Director = name
					break
				}
			}
		}
	}

	// Content type from @type
	if typ == "Movie" {
		record.ContentType = "movie"
	} else if typ == "TVSeries" {
		record.ContentType = "show"
	} else if typ == "TVEpisode" {
		record.ContentType = "episode"
	}

	return true
}

// --- HTTP helpers ---

type uaTransport struct {
	base http.RoundTripper
	ua   string
}

func (t *uaTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", t.ua)
	return t.base.RoundTrip(req)
}

var reMetaProperty = regexp.MustCompile(`(?i)<meta\s+(?:property|name)="([^"]+)"\s+content="([^"]+)"`)

func extractMeta(body []byte, property string) string {
	for _, match := range reMetaProperty.FindAllSubmatch(body, -1) {
		if strings.EqualFold(string(match[1]), property) || strings.EqualFold(string(match[1]), "og:"+property) {
			return string(match[2])
		}
	}
	return ""
}

func fetchURL(client *http.Client, rawURL string) []byte {
	resp, err := client.Get(rawURL)
	if err != nil {
		log.Printf("  fetch error %s: %v", rawURL, err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Printf("  fetch %s → %d", rawURL, resp.StatusCode)
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	return body
}
