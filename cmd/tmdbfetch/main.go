// tmdbfetch fetches all TV shows from TMDB API with credits and keywords.
//
// It uses the daily export for IDs, then fetches details at 40 req/s.
// Supports resume — skips IDs already in the output file.
//
// Usage:
//
//	tmdbfetch -token $TMDB_BEARER_TOKEN -output data/tmdb/tv_shows.jsonl
package main

import (
	"bufio"
	"compress/gzip"
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	rateLimit    = 38 // slightly under 40 to be safe
	maxRetries   = 3
	workerCount  = 10
	dailyExport  = "http://files.tmdb.org/p/exports/tv_series_ids_%s.json.gz"
)

type tvID struct {
	ID int `json:"id"`
}

// TVShow is the enriched output record.
type TVShow struct {
	TMDBID           int      `json:"tmdb_id"`
	Name             string   `json:"name"`
	OriginalName     string   `json:"original_name"`
	Overview         string   `json:"overview"`
	Tagline          string   `json:"tagline,omitempty"`
	FirstAirDate     string   `json:"first_air_date"`
	LastAirDate      string   `json:"last_air_date,omitempty"`
	Status           string   `json:"status"`
	Type             string   `json:"type,omitempty"`
	InProduction     bool     `json:"in_production"`
	NumberOfSeasons  int      `json:"number_of_seasons"`
	NumberOfEpisodes int      `json:"number_of_episodes"`
	Runtime          []int    `json:"episode_run_time,omitempty"`
	Popularity       float64  `json:"popularity"`
	VoteAverage      float64  `json:"vote_average"`
	VoteCount        int      `json:"vote_count"`
	OriginalLanguage string   `json:"original_language"`
	OriginCountry    []string `json:"origin_country,omitempty"`
	Genres           string   `json:"genres"`
	Networks         string   `json:"networks,omitempty"`
	Creators         string   `json:"created_by,omitempty"`
	ProductionCompanies string `json:"production_companies,omitempty"`
	Keywords         string   `json:"keywords,omitempty"`
	CastTop10        string   `json:"cast_top10,omitempty"`
	PosterPath       string   `json:"poster_path,omitempty"`
	BackdropPath     string   `json:"backdrop_path,omitempty"`
	Homepage         string   `json:"homepage,omitempty"`
}

func main() {
	token := flag.String("token", "", "TMDB Bearer token")
	output := flag.String("output", "data/tmdb/tv_shows.jsonl", "Output JSONL path")
	flag.Parse()

	if *token == "" {
		log.Fatal("-token is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	// Load already-fetched IDs for resume support.
	existing := loadExistingIDs(*output)
	log.Printf("Found %d already-fetched shows in %s", len(existing), *output)

	// Fetch ID list from daily export.
	ids := fetchIDList()
	log.Printf("Total TV show IDs: %d, remaining: %d", len(ids), len(ids)-len(existing))

	// Filter out already-fetched.
	var remaining []int
	for _, id := range ids {
		if !existing[id] {
			remaining = append(remaining, id)
		}
	}

	if len(remaining) == 0 {
		log.Println("All shows already fetched!")
		return
	}

	// Open output file in append mode.
	f, err := os.OpenFile(*output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer f.Close()

	var mu sync.Mutex
	enc := json.NewEncoder(f)

	// Rate limiter: token bucket.
	limiter := time.NewTicker(time.Second / rateLimit)
	defer limiter.Stop()

	// Worker pool.
	idCh := make(chan int, workerCount*2)
	var wg sync.WaitGroup
	var fetched, failed atomic.Int64
	start := time.Now()

	client := &http.Client{Timeout: 30 * time.Second}

	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range idCh {
				select {
				case <-ctx.Done():
					return
				case <-limiter.C:
				}

				show, err := fetchTV(ctx, client, *token, id)
				if err != nil {
					failed.Add(1)
					if fetched.Load()%1000 == 0 {
						log.Printf("Error fetching %d: %v", id, err)
					}
					continue
				}

				mu.Lock()
				enc.Encode(show)
				mu.Unlock()

				n := fetched.Add(1)
				if n%1000 == 0 {
					elapsed := time.Since(start).Seconds()
					rate := float64(n) / elapsed
					eta := time.Duration(float64(int64(len(remaining))-n)/rate) * time.Second
					log.Printf("Fetched: %d/%d (%.1f/s), failed: %d, ETA: %s",
						n, len(remaining), rate, failed.Load(), eta.Round(time.Second))
				}
			}
		}()
	}

	// Feed IDs to workers.
	go func() {
		for _, id := range remaining {
			select {
			case <-ctx.Done():
				close(idCh)
				return
			case idCh <- id:
			}
		}
		close(idCh)
	}()

	wg.Wait()

	log.Printf("Done: fetched %d, failed %d, total time %s",
		fetched.Load(), failed.Load(), time.Since(start).Round(time.Second))
}

func fetchIDList() []int {
	// Try today and yesterday.
	for daysBack := range 3 {
		date := time.Now().AddDate(0, 0, -daysBack).Format("01_02_2006")
		url := fmt.Sprintf(dailyExport, date)
		resp, err := http.Get(url)
		if err != nil || resp.StatusCode != 200 {
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		defer resp.Body.Close()

		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			continue
		}
		defer gr.Close()

		var ids []int
		scanner := bufio.NewScanner(gr)
		for scanner.Scan() {
			var entry tvID
			if err := json.Unmarshal(scanner.Bytes(), &entry); err == nil && entry.ID > 0 {
				ids = append(ids, entry.ID)
			}
		}
		log.Printf("Loaded %d IDs from daily export (%s)", len(ids), date)
		return ids
	}
	log.Fatal("Failed to fetch daily export")
	return nil
}

func loadExistingIDs(path string) map[int]bool {
	existing := make(map[int]bool)
	f, err := os.Open(path)
	if err != nil {
		return existing
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)
	for scanner.Scan() {
		var entry struct {
			TMDBID int `json:"tmdb_id"`
		}
		if json.Unmarshal(scanner.Bytes(), &entry) == nil && entry.TMDBID > 0 {
			existing[entry.TMDBID] = true
		}
	}
	return existing
}

func fetchTV(ctx context.Context, client *http.Client, token string, id int) (*TVShow, error) {
	url := fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?append_to_response=credits,keywords", id)

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
			return nil, fmt.Errorf("not found")
		}
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			continue
		}
		if err != nil {
			lastErr = err
			continue
		}

		return parseTV(id, body)
	}
	return nil, lastErr
}

func parseTV(id int, body []byte) (*TVShow, error) {
	var raw struct {
		Name             string `json:"name"`
		OriginalName     string `json:"original_name"`
		Overview         string `json:"overview"`
		Tagline          string `json:"tagline"`
		FirstAirDate     string `json:"first_air_date"`
		LastAirDate      string `json:"last_air_date"`
		Status           string `json:"status"`
		Type             string `json:"type"`
		InProduction     bool   `json:"in_production"`
		NumberOfSeasons  int    `json:"number_of_seasons"`
		NumberOfEpisodes int    `json:"number_of_episodes"`
		EpisodeRunTime   []int  `json:"episode_run_time"`
		Popularity       float64 `json:"popularity"`
		VoteAverage      float64 `json:"vote_average"`
		VoteCount        int     `json:"vote_count"`
		OriginalLanguage string  `json:"original_language"`
		OriginCountry    []string `json:"origin_country"`
		Homepage         string   `json:"homepage"`
		PosterPath       string   `json:"poster_path"`
		BackdropPath     string   `json:"backdrop_path"`
		Genres           []struct {
			Name string `json:"name"`
		} `json:"genres"`
		Networks []struct {
			Name string `json:"name"`
		} `json:"networks"`
		CreatedBy []struct {
			Name string `json:"name"`
		} `json:"created_by"`
		ProductionCompanies []struct {
			Name string `json:"name"`
		} `json:"production_companies"`
		Credits struct {
			Cast []struct {
				Name      string `json:"name"`
				Character string `json:"character"`
				Order     int    `json:"order"`
			} `json:"cast"`
		} `json:"credits"`
		Keywords struct {
			Results []struct {
				Name string `json:"name"`
			} `json:"results"`
		} `json:"keywords"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	show := &TVShow{
		TMDBID:           id,
		Name:             raw.Name,
		OriginalName:     raw.OriginalName,
		Overview:         raw.Overview,
		Tagline:          raw.Tagline,
		FirstAirDate:     raw.FirstAirDate,
		LastAirDate:      raw.LastAirDate,
		Status:           raw.Status,
		Type:             raw.Type,
		InProduction:     raw.InProduction,
		NumberOfSeasons:  raw.NumberOfSeasons,
		NumberOfEpisodes: raw.NumberOfEpisodes,
		Runtime:          raw.EpisodeRunTime,
		Popularity:       raw.Popularity,
		VoteAverage:      raw.VoteAverage,
		VoteCount:        raw.VoteCount,
		OriginalLanguage: raw.OriginalLanguage,
		OriginCountry:    raw.OriginCountry,
		Homepage:         raw.Homepage,
		PosterPath:       raw.PosterPath,
		BackdropPath:     raw.BackdropPath,
	}

	// Flatten nested fields.
	show.Genres = joinNames(raw.Genres)
	show.Networks = joinNames(raw.Networks)
	show.Creators = joinNames(raw.CreatedBy)
	show.ProductionCompanies = joinNames(raw.ProductionCompanies)

	// Keywords.
	var kw []string
	for _, k := range raw.Keywords.Results {
		kw = append(kw, k.Name)
	}
	show.Keywords = strings.Join(kw, "|")

	// Top 10 cast.
	var castParts []string
	for i, c := range raw.Credits.Cast {
		if i >= 10 {
			break
		}
		if c.Character != "" {
			castParts = append(castParts, c.Name+" as "+c.Character)
		} else {
			castParts = append(castParts, c.Name)
		}
	}
	show.CastTop10 = strings.Join(castParts, "|")

	return show, nil
}

type named interface{ ~struct{ Name string } }

func joinNames[T any](items []T) string {
	var names []string
	// Use JSON round-trip to extract names generically.
	for _, item := range items {
		b, _ := json.Marshal(item)
		var m map[string]any
		json.Unmarshal(b, &m)
		if name, ok := m["name"].(string); ok && name != "" {
			names = append(names, name)
		}
	}
	return strings.Join(names, "|")
}
