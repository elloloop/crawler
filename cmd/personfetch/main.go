// personfetch fetches detailed person (actor/director/crew) data from TMDB API.
//
// The key field for training data is also_known_as — a list of alternate names
// across languages and scripts (e.g. Cyrillic, Chinese, Arabic variants of an actor's name).
//
// Two ID discovery modes:
//   - export (default): uses TMDB daily person export (~4M people, use -min-popularity to filter)
//   - jsonl: derives IDs from existing movie/TV JSONL files (only people in your dataset)
//
// Usage:
//
//	personfetch -token $TMDB_TOKEN -output data/persons.jsonl
//	personfetch -token $TMDB_TOKEN -min-popularity 2.0 -output data/persons.jsonl
//	personfetch -token $TMDB_TOKEN -source jsonl -input data/movies.jsonl,data/tv.jsonl -output data/persons.jsonl
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

// loadEnv reads a .env file and populates missing environment variables.
// Only sets variables that are not already set in the environment.
func loadEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return // .env is optional
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k != "" && os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}

const (
	rateLimit   = 35
	maxRetries  = 3
	workerCount = 10

	personDailyExport = "http://files.tmdb.org/p/exports/person_ids_%s.json.gz"
	personAppend      = "external_ids"
)

// PersonRecord is the output schema. also_known_as is the primary field of interest
// for training data — it contains alternate names in multiple languages and scripts.
type PersonRecord struct {
	TMDBID       int      `json:"tmdb_id"`
	IMDBID       string   `json:"imdb_id,omitempty"`
	WikidataID   string   `json:"wikidata_id,omitempty"`
	Name         string   `json:"name"`
	AlsoKnownAs  []string `json:"also_known_as,omitempty"`
	Biography    string   `json:"biography,omitempty"`
	Birthday     string   `json:"birthday,omitempty"`
	Deathday     string   `json:"deathday,omitempty"`
	Gender       int      `json:"gender,omitempty"`
	PlaceOfBirth string   `json:"place_of_birth,omitempty"`
	Popularity   float64  `json:"popularity"`
	ProfilePath  string   `json:"profile_path,omitempty"`
	KnownForDept string   `json:"known_for_department,omitempty"`
	Adult        bool     `json:"adult,omitempty"`
	FetchedAt    string   `json:"fetched_at"`
}

func main() {
	loadEnv(".env")

	token := flag.String("token", os.Getenv("TMDB_TOKEN"), "TMDB Bearer token (or set TMDB_TOKEN in .env)")
	output := flag.String("output", "", "Output JSONL path")
	source := flag.String("source", "export", "ID source: 'export' (TMDB daily) or 'jsonl' (from existing movie/TV JSONL)")
	input := flag.String("input", "", "Comma-separated JSONL files to extract person IDs from (used with -source jsonl)")
	minPopularity := flag.Float64("min-popularity", 0, "Minimum popularity filter (export mode only; 0 = all)")
	flag.Parse()

	if *token == "" || *output == "" {
		log.Fatal("Usage: personfetch -output PATH [-source export|jsonl] [-input files] [-min-popularity N]\n  Set TMDB_TOKEN in .env or pass -token TOKEN")
	}
	if *source != "export" && *source != "jsonl" {
		log.Fatal("-source must be 'export' or 'jsonl'")
	}
	if *source == "jsonl" && *input == "" {
		log.Fatal("-input is required when -source jsonl")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	existing := loadExisting(*output)
	log.Printf("Already fetched: %d", len(existing))

	var ids []int
	if *source == "export" {
		ids = fetchIDsFromExport(*minPopularity)
	} else {
		ids = fetchIDsFromJSONL(strings.Split(*input, ","))
	}
	log.Printf("Total IDs: %d, remaining: %d", len(ids), len(ids)-len(existing))

	var remaining []int
	for _, id := range ids {
		if !existing[id] {
			remaining = append(remaining, id)
		}
	}
	if len(remaining) == 0 {
		log.Println("All done!")
		return
	}

	f, err := os.OpenFile(*output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer f.Close()

	var mu sync.Mutex
	enc := json.NewEncoder(f)

	limiter := time.NewTicker(time.Second / rateLimit)
	defer limiter.Stop()

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

				record, err := fetchPerson(ctx, client, *token, id)
				if err != nil {
					failed.Add(1)
					continue
				}

				mu.Lock()
				enc.Encode(record)
				mu.Unlock()

				n := fetched.Add(1)
				if n%2000 == 0 {
					elapsed := time.Since(start).Seconds()
					rate := float64(n) / elapsed
					eta := time.Duration(float64(int64(len(remaining))-n)/rate) * time.Second
					log.Printf("Fetched: %d/%d (%.1f/s), failed: %d, ETA: %s",
						n, len(remaining), rate, failed.Load(), eta.Round(time.Second))
				}
			}
		}()
	}

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
	f.Sync()
	log.Printf("Done: fetched %d, failed %d, total time %s → %s",
		fetched.Load(), failed.Load(), time.Since(start).Round(time.Second), *output)
}

// fetchPerson calls /person/{id}?append_to_response=external_ids and parses the result.
func fetchPerson(ctx context.Context, client *http.Client, token string, id int) (*PersonRecord, error) {
	url := fmt.Sprintf("https://api.themoviedb.org/3/person/%d?append_to_response=%s", id, personAppend)

	var lastErr error
	for attempt := range maxRetries {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
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
			time.Sleep(3 * time.Second)
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

		return parsePerson(id, body)
	}
	return nil, lastErr
}

func parsePerson(id int, body []byte) (*PersonRecord, error) {
	var raw struct {
		Name         string   `json:"name"`
		AlsoKnownAs  []string `json:"also_known_as"`
		Biography    string   `json:"biography"`
		Birthday     string   `json:"birthday"`
		Deathday     string   `json:"deathday"`
		Gender       int      `json:"gender"`
		PlaceOfBirth string   `json:"place_of_birth"`
		Popularity   float64  `json:"popularity"`
		ProfilePath  string   `json:"profile_path"`
		KnownForDept string   `json:"known_for_department"`
		Adult        bool     `json:"adult"`
		ExternalIDs  struct {
			IMDB     string `json:"imdb_id"`
			Wikidata string `json:"wikidata_id"`
		} `json:"external_ids"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	return &PersonRecord{
		TMDBID:       id,
		IMDBID:       raw.ExternalIDs.IMDB,
		WikidataID:   raw.ExternalIDs.Wikidata,
		Name:         raw.Name,
		AlsoKnownAs:  raw.AlsoKnownAs,
		Biography:    raw.Biography,
		Birthday:     raw.Birthday,
		Deathday:     raw.Deathday,
		Gender:       raw.Gender,
		PlaceOfBirth: raw.PlaceOfBirth,
		Popularity:   raw.Popularity,
		ProfilePath:  raw.ProfilePath,
		KnownForDept: raw.KnownForDept,
		Adult:        raw.Adult,
		FetchedAt:    time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// fetchIDsFromExport downloads the TMDB daily person export and returns person IDs.
func fetchIDsFromExport(minPop float64) []int {
	for daysBack := range 5 {
		date := time.Now().AddDate(0, 0, -daysBack).Format("01_02_2006")
		url := fmt.Sprintf(personDailyExport, date)
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
		var skipped int
		scanner := bufio.NewScanner(gr)
		for scanner.Scan() {
			var entry struct {
				ID         int     `json:"id"`
				Popularity float64 `json:"popularity"`
			}
			if json.Unmarshal(scanner.Bytes(), &entry) == nil && entry.ID > 0 {
				if entry.Popularity >= minPop {
					ids = append(ids, entry.ID)
				} else {
					skipped++
				}
			}
		}
		log.Printf("Loaded %d person IDs from daily export (%s), skipped %d below popularity %g",
			len(ids), date, skipped, minPop)
		return ids
	}
	log.Fatal("Failed to fetch TMDB daily person export")
	return nil
}

// fetchIDsFromJSONL scans existing movie/TV JSONL files and extracts unique person IDs
// from cast and crew fields. This is the preferred mode for training data since it limits
// to people who actually appear in your movie/TV dataset.
func fetchIDsFromJSONL(files []string) []int {
	seen := make(map[int]bool)

	for _, file := range files {
		file = strings.TrimSpace(file)
		if file == "" {
			continue
		}
		f, err := os.Open(file)
		if err != nil {
			log.Printf("Warning: cannot open %s: %v", file, err)
			continue
		}

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 4<<20), 4<<20)
		var count int
		for scanner.Scan() {
			var record struct {
				Cast []struct {
					ID int `json:"id"`
				} `json:"cast"`
				Crew []struct {
					ID int `json:"id"`
				} `json:"crew"`
			}
			if json.Unmarshal(scanner.Bytes(), &record) != nil {
				continue
			}
			for _, c := range record.Cast {
				if c.ID > 0 && !seen[c.ID] {
					seen[c.ID] = true
					count++
				}
			}
			for _, c := range record.Crew {
				if c.ID > 0 && !seen[c.ID] {
					seen[c.ID] = true
					count++
				}
			}
		}
		f.Close()
		log.Printf("Extracted %d new person IDs from %s", count, file)
	}

	ids := make([]int, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	log.Printf("Total unique person IDs from JSONL: %d", len(ids))
	return ids
}

// loadExisting scans the output file and returns a set of already-fetched person IDs.
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
		var entry struct {
			TMDBID int `json:"tmdb_id"`
		}
		if json.Unmarshal(scanner.Bytes(), &entry) == nil && entry.TMDBID > 0 {
			existing[entry.TMDBID] = true
		}
	}
	return existing
}
