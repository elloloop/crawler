// tmdbfetch fetches all movies and/or TV shows from TMDB API with full details
// in a single API call per title using append_to_response.
//
// Fetches: credits, keywords, similar, release_dates, alternative_titles, external_ids,
// videos, watch/providers.
//
// Uses daily exports for ID discovery. Filters to popularity > 0 to skip placeholder entries.
// Resume support via checkpoint file.
//
// Usage:
//
//	tmdbfetch -token $TMDB_TOKEN -type movie -output data/tmdb/movies_v2.jsonl
//	tmdbfetch -token $TMDB_TOKEN -type tv    -output data/tmdb/tv_v2.jsonl
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
	rateLimit   = 35
	maxRetries  = 3
	workerCount = 10

	movieDailyExport = "http://files.tmdb.org/p/exports/movie_ids_%s.json.gz"
	tvDailyExport    = "http://files.tmdb.org/p/exports/tv_series_ids_%s.json.gz"

	movieAppend = "credits,keywords,similar,release_dates,alternative_titles,external_ids,videos,watch/providers"
	tvAppend    = "credits,keywords,similar,alternative_titles,content_ratings,external_ids,videos,watch/providers"

	maxCast = 20
)

// Crew jobs to keep — everything else is dropped.
var keepCrewJobs = map[string]bool{
	"Director":                  true,
	"Writer":                    true,
	"Screenplay":                true,
	"Story":                     true,
	"Producer":                  true,
	"Executive Producer":        true,
	"Director of Photography":   true,
	"Cinematography":            true,
	"Original Music Composer":   true,
	"Music":                     true,
	"Editor":                    true,
}

// Countries for watch providers.
var targetCountries = map[string]bool{
	"US": true, "GB": true, "CA": true, "AU": true, "NZ": true,
	"IE": true, "ZA": true, "SG": true, "IN": true,
	"DE": true, "FR": true, "JP": true, "KR": true, "BR": true,
	"MX": true, "ES": true, "IT": true, "SE": true, "NL": true,
}

// --- Shared types ---

type CastMember struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Character   string  `json:"character,omitempty"`
	Order       int     `json:"order"`
	ProfilePath string  `json:"profile_path,omitempty"`
	Gender      int     `json:"gender,omitempty"`
	Popularity  float64 `json:"popularity,omitempty"`
	KnownFor    string  `json:"known_for_department,omitempty"`
}

type CrewMember struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Job         string `json:"job"`
	Department  string `json:"department,omitempty"`
	ProfilePath string `json:"profile_path,omitempty"`
	Gender      int    `json:"gender,omitempty"`
}

type Genre struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Company struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	LogoPath      string `json:"logo_path,omitempty"`
	OriginCountry string `json:"origin_country,omitempty"`
}

type Country struct {
	ISO  string `json:"iso_3166_1"`
	Name string `json:"name"`
}

type Language struct {
	ISO  string `json:"iso_639_1"`
	Name string `json:"name"`
}

type Network struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	LogoPath      string `json:"logo_path,omitempty"`
	OriginCountry string `json:"origin_country,omitempty"`
}

type Creator struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	ProfilePath string `json:"profile_path,omitempty"`
}

type Video struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Key         string `json:"key"`
	Site        string `json:"site"`
	Type        string `json:"type"`
	Official    bool   `json:"official"`
	Size        int    `json:"size,omitempty"`
	PublishedAt string `json:"published_at,omitempty"`
	URL         string `json:"url,omitempty"`
}

type Keyword struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type SimilarTitle struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

type AltTitle struct {
	Country string `json:"iso_3166_1"`
	Title   string `json:"title"`
}

type Certification struct {
	Country       string `json:"country"`
	Certification string `json:"certification"`
}

type ExternalIDs struct {
	IMDB      string `json:"imdb_id,omitempty"`
	Wikidata  string `json:"wikidata_id,omitempty"`
	Facebook  string `json:"facebook_id,omitempty"`
	Instagram string `json:"instagram_id,omitempty"`
	Twitter   string `json:"twitter_id,omitempty"`
}

type ProviderInfo struct {
	ProviderID   int    `json:"provider_id"`
	ProviderName string `json:"provider_name"`
	LogoPath     string `json:"logo_path,omitempty"`
}

type CountryProviders struct {
	Flatrate []ProviderInfo `json:"flatrate,omitempty"`
	Rent     []ProviderInfo `json:"rent,omitempty"`
	Buy      []ProviderInfo `json:"buy,omitempty"`
	Free     []ProviderInfo `json:"free,omitempty"`
	Ads      []ProviderInfo `json:"ads,omitempty"`
}

type Collection struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// --- Movie record ---

type MovieRecord struct {
	TMDBID              int                          `json:"tmdb_id"`
	IMDBID              string                       `json:"imdb_id,omitempty"`
	Title               string                       `json:"title"`
	OriginalTitle       string                       `json:"original_title,omitempty"`
	Adult               bool                         `json:"adult"`
	Overview            string                       `json:"overview"`
	Tagline             string                       `json:"tagline,omitempty"`
	ReleaseDate         string                       `json:"release_date"`
	Runtime             int                          `json:"runtime,omitempty"`
	Budget              int64                        `json:"budget,omitempty"`
	Revenue             int64                        `json:"revenue,omitempty"`
	Popularity          float64                      `json:"popularity"`
	VoteAverage         float64                      `json:"vote_average"`
	VoteCount           int                          `json:"vote_count"`
	Status              string                       `json:"status,omitempty"`
	OriginalLanguage    string                       `json:"original_language,omitempty"`
	OriginCountry       []string                     `json:"origin_country,omitempty"`
	SpokenLanguages     []Language                   `json:"spoken_languages,omitempty"`
	ProductionCountries []Country                    `json:"production_countries,omitempty"`
	ProductionCompanies []Company                    `json:"production_companies,omitempty"`
	Genres              []Genre                      `json:"genres"`
	Collection          *Collection                  `json:"belongs_to_collection,omitempty"`
	Homepage            string                       `json:"homepage,omitempty"`
	PosterPath          string                       `json:"poster_path,omitempty"`
	BackdropPath        string                       `json:"backdrop_path,omitempty"`
	Cast                []CastMember                 `json:"cast"`
	Crew                []CrewMember                 `json:"crew,omitempty"`
	Keywords            []Keyword                    `json:"keywords,omitempty"`
	Videos              []Video                      `json:"videos,omitempty"`
	Similar             []SimilarTitle               `json:"similar,omitempty"`
	Certifications      []Certification              `json:"certifications,omitempty"`
	AlternativeTitles   []AltTitle                   `json:"alternative_titles,omitempty"`
	ExternalIDs         *ExternalIDs                 `json:"external_ids,omitempty"`
	WatchProviders      map[string]CountryProviders  `json:"watch_providers,omitempty"`
	FetchedAt           string                       `json:"fetched_at"`
}

// --- TV record ---

type TVRecord struct {
	TMDBID              int                          `json:"tmdb_id"`
	Name                string                       `json:"name"`
	OriginalName        string                       `json:"original_name,omitempty"`
	Adult               bool                         `json:"adult"`
	Overview            string                       `json:"overview"`
	Tagline             string                       `json:"tagline,omitempty"`
	FirstAirDate        string                       `json:"first_air_date"`
	LastAirDate         string                       `json:"last_air_date,omitempty"`
	Status              string                       `json:"status,omitempty"`
	Type                string                       `json:"type,omitempty"`
	InProduction        bool                         `json:"in_production"`
	NumberOfSeasons     int                          `json:"number_of_seasons"`
	NumberOfEpisodes    int                          `json:"number_of_episodes"`
	EpisodeRunTime      []int                        `json:"episode_run_time,omitempty"`
	Popularity          float64                      `json:"popularity"`
	VoteAverage         float64                      `json:"vote_average"`
	VoteCount           int                          `json:"vote_count"`
	OriginalLanguage    string                       `json:"original_language,omitempty"`
	OriginCountry       []string                     `json:"origin_country,omitempty"`
	SpokenLanguages     []Language                   `json:"spoken_languages,omitempty"`
	ProductionCompanies []Company                    `json:"production_companies,omitempty"`
	Genres              []Genre                      `json:"genres"`
	Networks            []Network                    `json:"networks,omitempty"`
	CreatedBy           []Creator                    `json:"created_by,omitempty"`
	Homepage            string                       `json:"homepage,omitempty"`
	PosterPath          string                       `json:"poster_path,omitempty"`
	BackdropPath        string                       `json:"backdrop_path,omitempty"`
	Cast                []CastMember                 `json:"cast"`
	Crew                []CrewMember                 `json:"crew,omitempty"`
	Keywords            []Keyword                    `json:"keywords,omitempty"`
	Videos              []Video                      `json:"videos,omitempty"`
	Similar             []SimilarTitle               `json:"similar,omitempty"`
	Certifications      []Certification              `json:"certifications,omitempty"`
	AlternativeTitles   []AltTitle                   `json:"alternative_titles,omitempty"`
	ExternalIDs         *ExternalIDs                 `json:"external_ids,omitempty"`
	WatchProviders      map[string]CountryProviders  `json:"watch_providers,omitempty"`
	FetchedAt           string                       `json:"fetched_at"`
}

// --- Main ---

func main() {
	token := flag.String("token", "", "TMDB Bearer token")
	contentType := flag.String("type", "", "Content type: movie or tv")
	output := flag.String("output", "", "Output JSONL path")
	minPopularity := flag.Float64("min-popularity", 0, "Minimum popularity to fetch (0 = all)")
	flag.Parse()

	if *token == "" || *contentType == "" || *output == "" {
		log.Fatal("Usage: tmdbfetch -token TOKEN -type movie|tv -output PATH")
	}
	if *contentType != "movie" && *contentType != "tv" {
		log.Fatal("-type must be 'movie' or 'tv'")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	existing := loadExisting(*output)
	log.Printf("Already fetched: %d", len(existing))

	ids := fetchIDList(*contentType, *minPopularity)
	log.Printf("Total IDs (popularity > %g): %d, remaining: %d", *minPopularity, len(ids), len(ids)-len(existing))

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

				record, err := fetchTitle(ctx, client, *token, *contentType, id)
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

// --- Fetch ---

func fetchTitle(ctx context.Context, client *http.Client, token, contentType string, id int) (any, error) {
	append := movieAppend
	if contentType == "tv" {
		append = tvAppend
	}
	url := fmt.Sprintf("https://api.themoviedb.org/3/%s/%d?append_to_response=%s", contentType, id, append)

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

		if contentType == "movie" {
			return parseMovie(id, body)
		}
		return parseTVShow(id, body)
	}
	return nil, lastErr
}

// --- Parse Movie ---

func parseMovie(id int, body []byte) (*MovieRecord, error) {
	var raw struct {
		IMDBID              string          `json:"imdb_id"`
		Title               string          `json:"title"`
		OriginalTitle       string          `json:"original_title"`
		Adult               bool            `json:"adult"`
		Overview            string          `json:"overview"`
		Tagline             string          `json:"tagline"`
		ReleaseDate         string          `json:"release_date"`
		Runtime             int             `json:"runtime"`
		Budget              int64           `json:"budget"`
		Revenue             int64           `json:"revenue"`
		Popularity          float64         `json:"popularity"`
		VoteAverage         float64         `json:"vote_average"`
		VoteCount           int             `json:"vote_count"`
		Status              string          `json:"status"`
		OriginalLanguage    string          `json:"original_language"`
		OriginCountry       []string        `json:"origin_country"`
		SpokenLanguages     []Language      `json:"spoken_languages"`
		ProductionCountries []Country       `json:"production_countries"`
		ProductionCompanies []Company       `json:"production_companies"`
		Genres              []Genre         `json:"genres"`
		Homepage            string          `json:"homepage"`
		PosterPath          string          `json:"poster_path"`
		BackdropPath        string          `json:"backdrop_path"`
		BelongsToCollection *Collection     `json:"belongs_to_collection"`
		Credits             struct {
			Cast []json.RawMessage `json:"cast"`
			Crew []json.RawMessage `json:"crew"`
		} `json:"credits"`
		Keywords struct {
			Keywords []Keyword `json:"keywords"`
		} `json:"keywords"`
		Similar struct {
			Results []struct {
				ID    int    `json:"id"`
				Title string `json:"title"`
			} `json:"results"`
		} `json:"similar"`
		ReleaseDates struct {
			Results []struct {
				Country  string `json:"iso_3166_1"`
				Releases []struct {
					Certification string `json:"certification"`
				} `json:"release_dates"`
			} `json:"results"`
		} `json:"release_dates"`
		AlternativeTitles struct {
			Titles []AltTitle `json:"titles"`
		} `json:"alternative_titles"`
		ExternalIDs struct {
			IMDB      string `json:"imdb_id"`
			Wikidata  string `json:"wikidata_id"`
			Facebook  string `json:"facebook_id"`
			Instagram string `json:"instagram_id"`
			Twitter   string `json:"twitter_id"`
		} `json:"external_ids"`
		Videos struct {
			Results []struct {
				ID          string `json:"id"`
				Name        string `json:"name"`
				Key         string `json:"key"`
				Site        string `json:"site"`
				Type        string `json:"type"`
				Official    bool   `json:"official"`
				Size        int    `json:"size"`
				PublishedAt string `json:"published_at"`
			} `json:"results"`
		} `json:"videos"`
		WatchProviders struct {
			Results map[string]struct {
				Flatrate []ProviderInfo `json:"flatrate"`
				Rent     []ProviderInfo `json:"rent"`
				Buy      []ProviderInfo `json:"buy"`
				Free     []ProviderInfo `json:"free"`
				Ads      []ProviderInfo `json:"ads"`
			} `json:"results"`
		} `json:"watch/providers"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	m := &MovieRecord{
		TMDBID:              id,
		IMDBID:              raw.IMDBID,
		Title:               raw.Title,
		OriginalTitle:       raw.OriginalTitle,
		Adult:               raw.Adult,
		Overview:            raw.Overview,
		Tagline:             raw.Tagline,
		ReleaseDate:         raw.ReleaseDate,
		Runtime:             raw.Runtime,
		Budget:              raw.Budget,
		Revenue:             raw.Revenue,
		Popularity:          raw.Popularity,
		VoteAverage:         raw.VoteAverage,
		VoteCount:           raw.VoteCount,
		Status:              raw.Status,
		OriginalLanguage:    raw.OriginalLanguage,
		OriginCountry:       raw.OriginCountry,
		SpokenLanguages:     raw.SpokenLanguages,
		ProductionCountries: raw.ProductionCountries,
		ProductionCompanies: raw.ProductionCompanies,
		Genres:              raw.Genres,
		Collection:          raw.BelongsToCollection,
		Homepage:            raw.Homepage,
		PosterPath:          raw.PosterPath,
		BackdropPath:        raw.BackdropPath,
		Cast:                parseCast(raw.Credits.Cast),
		Crew:                parseCrew(raw.Credits.Crew),
		Keywords:            raw.Keywords.Keywords,
		Videos:              parseVideos(raw.Videos.Results),
		WatchProviders:      filterProviders(raw.WatchProviders.Results),
		FetchedAt:           time.Now().UTC().Format(time.RFC3339),
	}

	// Similar (top 10)
	for i, s := range raw.Similar.Results {
		if i >= 10 {
			break
		}
		m.Similar = append(m.Similar, SimilarTitle{ID: s.ID, Title: s.Title})
	}

	// Certifications (per-country, first non-empty per country)
	for _, rd := range raw.ReleaseDates.Results {
		if !targetCountries[rd.Country] {
			continue
		}
		for _, r := range rd.Releases {
			if r.Certification != "" {
				m.Certifications = append(m.Certifications, Certification{
					Country:       rd.Country,
					Certification: r.Certification,
				})
				break
			}
		}
	}

	// Alternative titles (top 20)
	for i, t := range raw.AlternativeTitles.Titles {
		if i >= 20 {
			break
		}
		m.AlternativeTitles = append(m.AlternativeTitles, t)
	}

	// External IDs
	ext := &ExternalIDs{
		IMDB:      raw.ExternalIDs.IMDB,
		Wikidata:  raw.ExternalIDs.Wikidata,
		Facebook:  raw.ExternalIDs.Facebook,
		Instagram: raw.ExternalIDs.Instagram,
		Twitter:   raw.ExternalIDs.Twitter,
	}
	if ext.IMDB != "" || ext.Wikidata != "" || ext.Facebook != "" {
		m.ExternalIDs = ext
	}

	return m, nil
}

// --- Parse TV Show ---

func parseTVShow(id int, body []byte) (*TVRecord, error) {
	var raw struct {
		Name                string     `json:"name"`
		OriginalName        string     `json:"original_name"`
		Adult               bool       `json:"adult"`
		Overview            string     `json:"overview"`
		Tagline             string     `json:"tagline"`
		FirstAirDate        string     `json:"first_air_date"`
		LastAirDate         string     `json:"last_air_date"`
		Status              string     `json:"status"`
		Type                string     `json:"type"`
		InProduction        bool       `json:"in_production"`
		NumberOfSeasons     int        `json:"number_of_seasons"`
		NumberOfEpisodes    int        `json:"number_of_episodes"`
		EpisodeRunTime      []int      `json:"episode_run_time"`
		Popularity          float64    `json:"popularity"`
		VoteAverage         float64    `json:"vote_average"`
		VoteCount           int        `json:"vote_count"`
		OriginalLanguage    string     `json:"original_language"`
		OriginCountry       []string   `json:"origin_country"`
		SpokenLanguages     []Language `json:"spoken_languages"`
		ProductionCompanies []Company  `json:"production_companies"`
		Genres              []Genre    `json:"genres"`
		Networks            []Network  `json:"networks"`
		CreatedBy           []Creator  `json:"created_by"`
		Homepage            string     `json:"homepage"`
		PosterPath          string     `json:"poster_path"`
		BackdropPath        string     `json:"backdrop_path"`
		Credits             struct {
			Cast []json.RawMessage `json:"cast"`
			Crew []json.RawMessage `json:"crew"`
		} `json:"credits"`
		Keywords struct {
			Results []Keyword `json:"results"`
		} `json:"keywords"`
		Similar struct {
			Results []struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"results"`
		} `json:"similar"`
		ContentRatings struct {
			Results []struct {
				Country string `json:"iso_3166_1"`
				Rating  string `json:"rating"`
			} `json:"results"`
		} `json:"content_ratings"`
		AlternativeTitles struct {
			Results []AltTitle `json:"results"`
		} `json:"alternative_titles"`
		ExternalIDs struct {
			IMDB      string `json:"imdb_id"`
			Wikidata  string `json:"wikidata_id"`
			Facebook  string `json:"facebook_id"`
			Instagram string `json:"instagram_id"`
			Twitter   string `json:"twitter_id"`
		} `json:"external_ids"`
		Videos struct {
			Results []struct {
				ID          string `json:"id"`
				Name        string `json:"name"`
				Key         string `json:"key"`
				Site        string `json:"site"`
				Type        string `json:"type"`
				Official    bool   `json:"official"`
				Size        int    `json:"size"`
				PublishedAt string `json:"published_at"`
			} `json:"results"`
		} `json:"videos"`
		WatchProviders struct {
			Results map[string]struct {
				Flatrate []ProviderInfo `json:"flatrate"`
				Rent     []ProviderInfo `json:"rent"`
				Buy      []ProviderInfo `json:"buy"`
				Free     []ProviderInfo `json:"free"`
				Ads      []ProviderInfo `json:"ads"`
			} `json:"results"`
		} `json:"watch/providers"`
	}

	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	t := &TVRecord{
		TMDBID:              id,
		Name:                raw.Name,
		OriginalName:        raw.OriginalName,
		Adult:               raw.Adult,
		Overview:            raw.Overview,
		Tagline:             raw.Tagline,
		FirstAirDate:        raw.FirstAirDate,
		LastAirDate:         raw.LastAirDate,
		Status:              raw.Status,
		Type:                raw.Type,
		InProduction:        raw.InProduction,
		NumberOfSeasons:     raw.NumberOfSeasons,
		NumberOfEpisodes:    raw.NumberOfEpisodes,
		EpisodeRunTime:      raw.EpisodeRunTime,
		Popularity:          raw.Popularity,
		VoteAverage:         raw.VoteAverage,
		VoteCount:           raw.VoteCount,
		OriginalLanguage:    raw.OriginalLanguage,
		OriginCountry:       raw.OriginCountry,
		SpokenLanguages:     raw.SpokenLanguages,
		ProductionCompanies: raw.ProductionCompanies,
		Genres:              raw.Genres,
		Networks:            raw.Networks,
		CreatedBy:           raw.CreatedBy,
		Homepage:            raw.Homepage,
		PosterPath:          raw.PosterPath,
		BackdropPath:        raw.BackdropPath,
		Cast:                parseCast(raw.Credits.Cast),
		Crew:                parseCrew(raw.Credits.Crew),
		Keywords:            raw.Keywords.Results,
		Videos:              parseVideos(raw.Videos.Results),
		WatchProviders:      filterProviders(raw.WatchProviders.Results),
		FetchedAt:           time.Now().UTC().Format(time.RFC3339),
	}

	// Similar (top 10)
	for i, s := range raw.Similar.Results {
		if i >= 10 {
			break
		}
		t.Similar = append(t.Similar, SimilarTitle{ID: s.ID, Title: s.Name})
	}

	// Certifications
	for _, cr := range raw.ContentRatings.Results {
		if targetCountries[cr.Country] && cr.Rating != "" {
			t.Certifications = append(t.Certifications, Certification{
				Country:       cr.Country,
				Certification: cr.Rating,
			})
		}
	}

	// Alternative titles (top 20)
	for i, at := range raw.AlternativeTitles.Results {
		if i >= 20 {
			break
		}
		t.AlternativeTitles = append(t.AlternativeTitles, at)
	}

	// External IDs
	ext := &ExternalIDs{
		IMDB:      raw.ExternalIDs.IMDB,
		Wikidata:  raw.ExternalIDs.Wikidata,
		Facebook:  raw.ExternalIDs.Facebook,
		Instagram: raw.ExternalIDs.Instagram,
		Twitter:   raw.ExternalIDs.Twitter,
	}
	if ext.IMDB != "" || ext.Wikidata != "" || ext.Facebook != "" {
		t.ExternalIDs = ext
	}

	return t, nil
}

// --- Helpers ---

func parseCast(rawCast []json.RawMessage) []CastMember {
	var cast []CastMember
	for i, raw := range rawCast {
		if i >= maxCast {
			break
		}
		var c CastMember
		if json.Unmarshal(raw, &c) == nil && c.ID > 0 {
			cast = append(cast, c)
		}
	}
	return cast
}

func parseCrew(rawCrew []json.RawMessage) []CrewMember {
	var crew []CrewMember
	seen := make(map[string]bool) // dedup by id+job
	for _, raw := range rawCrew {
		var c struct {
			ID          int    `json:"id"`
			Name        string `json:"name"`
			Job         string `json:"job"`
			Department  string `json:"department"`
			ProfilePath string `json:"profile_path"`
			Gender      int    `json:"gender"`
		}
		if json.Unmarshal(raw, &c) != nil || c.ID == 0 {
			continue
		}
		if !keepCrewJobs[c.Job] {
			continue
		}
		key := fmt.Sprintf("%d:%s", c.ID, c.Job)
		if seen[key] {
			continue
		}
		seen[key] = true
		crew = append(crew, CrewMember{
			ID:          c.ID,
			Name:        c.Name,
			Job:         c.Job,
			Department:  c.Department,
			ProfilePath: c.ProfilePath,
			Gender:      c.Gender,
		})
	}
	return crew
}

func parseVideos(rawVideos []struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Key         string `json:"key"`
	Site        string `json:"site"`
	Type        string `json:"type"`
	Official    bool   `json:"official"`
	Size        int    `json:"size"`
	PublishedAt string `json:"published_at"`
}) []Video {
	var videos []Video
	for _, v := range rawVideos {
		vid := Video{
			ID:          v.ID,
			Name:        v.Name,
			Key:         v.Key,
			Site:        v.Site,
			Type:        v.Type,
			Official:    v.Official,
			Size:        v.Size,
			PublishedAt: v.PublishedAt,
		}
		if v.Site == "YouTube" && v.Key != "" {
			vid.URL = "https://www.youtube.com/watch?v=" + v.Key
		}
		videos = append(videos, vid)
	}
	return videos
}

func filterProviders(raw map[string]struct {
	Flatrate []ProviderInfo `json:"flatrate"`
	Rent     []ProviderInfo `json:"rent"`
	Buy      []ProviderInfo `json:"buy"`
	Free     []ProviderInfo `json:"free"`
	Ads      []ProviderInfo `json:"ads"`
}) map[string]CountryProviders {
	if len(raw) == 0 {
		return nil
	}
	result := make(map[string]CountryProviders)
	for country, data := range raw {
		if !targetCountries[country] {
			continue
		}
		result[country] = CountryProviders{
			Flatrate: data.Flatrate,
			Rent:     data.Rent,
			Buy:      data.Buy,
			Free:     data.Free,
			Ads:      data.Ads,
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func fetchIDList(contentType string, minPop float64) []int {
	exportURL := movieDailyExport
	if contentType == "tv" {
		exportURL = tvDailyExport
	}

	for daysBack := range 5 {
		date := time.Now().AddDate(0, 0, -daysBack).Format("01_02_2006")
		url := fmt.Sprintf(exportURL, date)
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
		log.Printf("Loaded %d IDs from daily export (%s), skipped %d below popularity %g",
			len(ids), date, skipped, minPop)
		return ids
	}
	log.Fatal("Failed to fetch daily export")
	return nil
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
		var entry struct {
			TMDBID int `json:"tmdb_id"`
		}
		if json.Unmarshal(scanner.Bytes(), &entry) == nil && entry.TMDBID > 0 {
			existing[entry.TMDBID] = true
		}
	}
	return existing
}

// unused but satisfies the import
var _ = strings.TrimSpace
