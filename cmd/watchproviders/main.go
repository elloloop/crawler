// watchproviders fetches watch provider availability for all TMDB movies and TV shows.
//
// Fetches from /movie/{id}/watch/providers and /tv/{id}/watch/providers,
// filters to target countries, and outputs JSONL with per-title availability.
// Supports resume — skips IDs already in the output file.
//
// Usage:
//
//	watchproviders \
//	  -token $TMDB_BEARER_TOKEN \
//	  -tmdb-movies ~/Downloads/tmdb_movies.jsonl \
//	  -tmdb-tv data/tmdb/tv_shows.jsonl \
//	  -output data/watchproviders/
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	rateLimit   = 38
	maxRetries  = 3
	workerCount = 10
)

// Countries to fetch — English-speaking + major markets
var targetCountries = map[string]bool{
	// English-speaking
	"US": true, "GB": true, "CA": true, "AU": true, "NZ": true,
	"IE": true, "ZA": true, "SG": true,
	// India (all languages)
	"IN": true,
	// Other major markets
	"DE": true, "FR": true, "JP": true, "KR": true, "BR": true,
	"MX": true, "ES": true, "IT": true, "SE": true, "NL": true,
}

type tmdbEntry struct {
	ID    int    `json:"tmdb_id"`
	Title string `json:"title"`
	Name  string `json:"name"` // TV shows use "name"
}

func (e *tmdbEntry) GetTitle() string {
	if e.Title != "" {
		return e.Title
	}
	return e.Name
}

type ProviderInfo struct {
	ProviderName string `json:"provider_name"`
	ProviderID   int    `json:"provider_id"`
	LogoPath     string `json:"logo_path"`
}

type CountryAvailability struct {
	Flatrate []ProviderInfo `json:"flatrate,omitempty"`
	Rent     []ProviderInfo `json:"rent,omitempty"`
	Buy      []ProviderInfo `json:"buy,omitempty"`
	Free     []ProviderInfo `json:"free,omitempty"`
	Ads      []ProviderInfo `json:"ads,omitempty"`
}

type Video struct {
	Name        string `json:"name"`
	Key         string `json:"key"`          // YouTube video ID
	Site        string `json:"site"`         // "YouTube"
	Type        string `json:"type"`         // Trailer, Teaser, Clip, Featurette, Behind the Scenes
	Official    bool   `json:"official"`
	Language    string `json:"language"`
	Country     string `json:"country"`
	Size        int    `json:"size"`         // resolution: 360, 480, 720, 1080, 2160
	PublishedAt string `json:"published_at"`
	URL         string `json:"url"`          // full YouTube URL
}

type WatchRecord struct {
	TMDBID       int                            `json:"tmdb_id"`
	Title        string                         `json:"title"`
	ContentType  string                         `json:"content_type"` // "movie" or "tv"
	Availability map[string]CountryAvailability `json:"availability"` // country code → providers
	Trailers     []Video                        `json:"trailers,omitempty"`
	AllVideos    []Video                        `json:"all_videos,omitempty"`
	FetchedAt    string                         `json:"fetched_at"`
}

func main() {
	token := flag.String("token", "", "TMDB Bearer token")
	tmdbMoviesPath := flag.String("tmdb-movies", "", "TMDB movies JSONL")
	tmdbTVPath := flag.String("tmdb-tv", "", "TMDB TV shows JSONL")
	outputDir := flag.String("output", "data/watchproviders", "Output directory")
	flag.Parse()

	if *token == "" {
		log.Fatal("-token is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	limiter := time.NewTicker(time.Second / rateLimit)
	defer limiter.Stop()

	// Process movies
	if *tmdbMoviesPath != "" {
		log.Println("=== Fetching movie watch providers ===")
		outPath := filepath.Join(*outputDir, "movies_providers.jsonl")
		processType(ctx, client, *token, *tmdbMoviesPath, outPath, "movie", limiter)
	}

	// Process TV
	if *tmdbTVPath != "" {
		log.Println("=== Fetching TV watch providers ===")
		outPath := filepath.Join(*outputDir, "tv_providers.jsonl")
		processType(ctx, client, *token, *tmdbTVPath, outPath, "tv", limiter)
	}
}

func processType(ctx context.Context, client *http.Client, token, inPath, outPath, contentType string, limiter *time.Ticker) {
	// Load IDs
	entries := loadEntries(inPath)
	log.Printf("  Loaded %d %s entries", len(entries), contentType)

	// Load existing for resume
	existing := loadExisting(outPath)
	log.Printf("  Already fetched: %d", len(existing))

	var remaining []tmdbEntry
	for _, e := range entries {
		if !existing[e.ID] {
			remaining = append(remaining, e)
		}
	}
	log.Printf("  Remaining: %d", len(remaining))

	if len(remaining) == 0 {
		log.Println("  All done!")
		return
	}

	// Open output (append)
	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer f.Close()

	var mu sync.Mutex
	enc := json.NewEncoder(f)

	idCh := make(chan tmdbEntry, workerCount*2)
	var wg sync.WaitGroup
	var fetched, failed, withProviders, withTrailers atomic.Int64
	start := time.Now()

	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range idCh {
				select {
				case <-ctx.Done():
					return
				case <-limiter.C:
				}

				record, err := fetchProviders(ctx, client, token, entry, contentType)
				if err != nil {
					failed.Add(1)
					continue
				}

				mu.Lock()
				enc.Encode(record)
				mu.Unlock()

				n := fetched.Add(1)
				if len(record.Availability) > 0 {
					withProviders.Add(1)
				}
				if len(record.Trailers) > 0 {
					withTrailers.Add(1)
				}

				if n%2000 == 0 {
					elapsed := time.Since(start).Seconds()
					rate := float64(n) / elapsed
					eta := time.Duration(float64(int64(len(remaining))-n)/rate) * time.Second
					log.Printf("  %s: %d/%d (%.1f/s), providers: %d, trailers: %d, failed: %d, ETA: %s",
						contentType, n, len(remaining), rate, withProviders.Load(), withTrailers.Load(), failed.Load(), eta.Round(time.Second))
				}
			}
		}()
	}

	go func() {
		for _, e := range remaining {
			select {
			case <-ctx.Done():
				close(idCh)
				return
			case idCh <- e:
			}
		}
		close(idCh)
	}()

	wg.Wait()

	log.Printf("  %s done: %d fetched, %d with providers, %d with trailers, %d failed, %s elapsed",
		contentType, fetched.Load(), withProviders.Load(), withTrailers.Load(), failed.Load(),
		time.Since(start).Round(time.Second))
}

func fetchProviders(ctx context.Context, client *http.Client, token string, entry tmdbEntry, contentType string) (*WatchRecord, error) {
	urlPath := "movie"
	if contentType == "tv" {
		urlPath = "tv"
	}
	// Fetch watch providers + videos in a single API call
	url := fmt.Sprintf("https://api.themoviedb.org/3/%s/%d?append_to_response=watch/providers,videos", urlPath, entry.ID)

	var lastErr error
	for attempt := range maxRetries {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 429 {
			time.Sleep(2 * time.Second)
			lastErr = fmt.Errorf("rate limited")
			continue
		}
		if resp.StatusCode == 404 {
			// No providers data — return empty record
			return &WatchRecord{
				TMDBID:       entry.ID,
				Title:        entry.GetTitle(),
				ContentType:  contentType,
				Availability: map[string]CountryAvailability{},
				FetchedAt:    time.Now().UTC().Format(time.RFC3339),
			}, nil
		}
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			continue
		}
		if err != nil {
			lastErr = err
			continue
		}

		return parseProviders(entry, contentType, body)
	}
	return nil, lastErr
}

func parseProviders(entry tmdbEntry, contentType string, body []byte) (*WatchRecord, error) {
	var raw struct {
		WatchProviders struct {
			Results map[string]struct {
				Flatrate []ProviderInfo `json:"flatrate"`
				Rent     []ProviderInfo `json:"rent"`
				Buy      []ProviderInfo `json:"buy"`
				Free     []ProviderInfo `json:"free"`
				Ads      []ProviderInfo `json:"ads"`
			} `json:"results"`
		} `json:"watch/providers"`
		Videos struct {
			Results []struct {
				Name        string `json:"name"`
				Key         string `json:"key"`
				Site        string `json:"site"`
				Type        string `json:"type"`
				Official    bool   `json:"official"`
				Language    string `json:"iso_639_1"`
				Country     string `json:"iso_3166_1"`
				Size        int    `json:"size"`
				PublishedAt string `json:"published_at"`
			} `json:"results"`
		} `json:"videos"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	// Parse availability
	availability := make(map[string]CountryAvailability)
	for country, data := range raw.WatchProviders.Results {
		if !targetCountries[country] {
			continue
		}
		availability[country] = CountryAvailability{
			Flatrate: data.Flatrate,
			Rent:     data.Rent,
			Buy:      data.Buy,
			Free:     data.Free,
			Ads:      data.Ads,
		}
	}

	// Parse videos
	var trailers []Video
	var allVideos []Video
	for _, v := range raw.Videos.Results {
		video := Video{
			Name:        v.Name,
			Key:         v.Key,
			Site:        v.Site,
			Type:        v.Type,
			Official:    v.Official,
			Language:    v.Language,
			Country:     v.Country,
			Size:        v.Size,
			PublishedAt: v.PublishedAt,
		}
		if v.Site == "YouTube" && v.Key != "" {
			video.URL = "https://www.youtube.com/watch?v=" + v.Key
		}
		allVideos = append(allVideos, video)
		if v.Type == "Trailer" || v.Type == "Teaser" {
			trailers = append(trailers, video)
		}
	}

	return &WatchRecord{
		TMDBID:       entry.ID,
		Title:        entry.GetTitle(),
		ContentType:  contentType,
		Availability: availability,
		Trailers:     trailers,
		AllVideos:    allVideos,
		FetchedAt:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}

func loadEntries(path string) []tmdbEntry {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var entries []tmdbEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)
	for scanner.Scan() {
		var e tmdbEntry
		if json.Unmarshal(scanner.Bytes(), &e) == nil && e.ID > 0 {
			entries = append(entries, e)
		}
	}
	return entries
}

func loadExisting(path string) map[int]bool {
	existing := make(map[int]bool)
	f, err := os.Open(path)
	if err != nil {
		return existing
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)
	for scanner.Scan() {
		var e struct {
			TMDBID int `json:"tmdb_id"`
		}
		if json.Unmarshal(scanner.Bytes(), &e) == nil && e.TMDBID > 0 {
			existing[e.TMDBID] = true
		}
	}
	return existing
}
