// crawler is the interactive CLI for the crawler data pipeline.
// Start it and use / commands to navigate everything.
//
// Usage:
//
//	go run ./cmd/crawler
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	dataDir = "data"
	r2Acct  = "e04ba18afe42f7cd7efb8235e09d7524"
)

var commands = []cmdInfo{
	{"/status", "Show data pipeline status — what's fetched, what's pending"},
	{"/fetch", "Fetch data from sources (TMDB, IMDb, Wikipedia, OTT)"},
	{"/tag", "Run tagger on fetched data (quality tiers, completeness flags)"},
	{"/enrich", "Merge TMDB + IMDb + Wikipedia into enriched dataset"},
	{"/crawl", "Crawl OTT platforms (aha, hotstar, zee5, sunnxt)"},
	{"/cinema", "Fetch cinema showtimes (Cineworld API)"},
	{"/upload", "Upload data to R2"},
	{"/search", "Search titles in local data"},
	{"/inspect", "Inspect a specific title by TMDB ID"},
	{"/r2", "List R2 bucket contents"},
	{"/probe", "Probe an OTT/cinema website for crawlability"},
	{"/registry", "Show OTT & cinema registry"},
	{"/help", "Show this help"},
	{"/quit", "Exit"},
}

type cmdInfo struct {
	Cmd  string
	Desc string
}

func main() {
	fmt.Println("┌─────────────────────────────────────────────┐")
	fmt.Println("│          crawler — data pipeline          │")
	fmt.Println("│                                              │")
	fmt.Println("│  Type /help for commands, /status for state  │")
	fmt.Println("└─────────────────────────────────────────────┘")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("crawler> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])
		args := parts[1:]

		switch cmd {
		case "/help", "help", "?":
			showHelp()
		case "/status", "status":
			showStatus()
		case "/fetch":
			handleFetch(args)
		case "/tag":
			handleTag(args)
		case "/enrich":
			handleEnrich(args)
		case "/crawl":
			handleCrawl(args)
		case "/cinema":
			handleCinema(args)
		case "/upload":
			handleUpload(args)
		case "/search":
			handleSearch(args)
		case "/inspect":
			handleInspect(args)
		case "/r2":
			handleR2()
		case "/probe":
			handleProbe(args)
		case "/registry":
			handleRegistry()
		case "/quit", "/exit", "quit", "exit":
			fmt.Println("Bye!")
			return
		default:
			if strings.HasPrefix(cmd, "/") {
				fmt.Printf("Unknown command: %s. Type /help for commands.\n", cmd)
			} else {
				// Treat as search
				handleSearch(parts)
			}
		}
		fmt.Println()
	}
}

func showHelp() {
	fmt.Println("Commands:")
	for _, c := range commands {
		fmt.Printf("  %-14s %s\n", c.Cmd, c.Desc)
	}
	fmt.Println()
	fmt.Println("Subcommands:")
	fmt.Println("  /fetch movies         Fetch all TMDB movies (v2, full data)")
	fmt.Println("  /fetch tv             Fetch all TMDB TV shows (v2, full data)")
	fmt.Println("  /fetch imdb           Download IMDb datasets")
	fmt.Println("  /fetch wiki           Download Wikipedia dump")
	fmt.Println("  /tag movies           Tag movies with quality tiers")
	fmt.Println("  /tag tv               Tag TV shows with quality tiers")
	fmt.Println("  /enrich all           Run full enrichment pipeline")
	fmt.Println("  /crawl aha            Crawl aha.video")
	fmt.Println("  /crawl hotstar        Crawl Hotstar")
	fmt.Println("  /crawl zee5           Crawl ZEE5")
	fmt.Println("  /crawl sunnxt         Crawl Sun NXT")
	fmt.Println("  /cinema cineworld     Fetch Cineworld showtimes")
	fmt.Println("  /upload movies        Upload movies v2 to R2")
	fmt.Println("  /upload tv            Upload TV v2 to R2")
	fmt.Println("  /search <query>       Search titles")
	fmt.Println("  /inspect <tmdb_id>    Show full record for a title")
	fmt.Println("  /probe <domain>       Probe a website for crawlability")
}

func showStatus() {
	fmt.Println("=== Data Pipeline Status ===")
	fmt.Println()

	// TMDB v2
	fmt.Println("TMDB v2 (primary dataset):")
	showFileStatus("  Movies:", filepath.Join(dataDir, "tmdb", "movies_v2.jsonl"))
	showFileStatus("  TV:    ", filepath.Join(dataDir, "tmdb", "tv_v2.jsonl"))

	// Tagged
	fmt.Println()
	fmt.Println("Tagged data:")
	showFileStatus("  Movies:", filepath.Join(dataDir, "tmdb", "movies_v2_tagged.jsonl"))
	showFileStatus("  TV:    ", filepath.Join(dataDir, "tmdb", "tv_v2_tagged.jsonl"))

	// OTT
	fmt.Println()
	fmt.Println("OTT crawls:")
	showFileStatus("  aha:    ", filepath.Join(dataDir, "ott", "aha.jsonl"))
	showFileStatus("  hotstar:", filepath.Join(dataDir, "ott", "hotstar.jsonl"))

	// Wikipedia
	fmt.Println()
	fmt.Println("Wikipedia:")
	showFileStatus("  Dump:  ", filepath.Join(dataDir, "wiki", "enwiki-latest-pages-articles.xml.bz2"))
	showFileStatus("  Films: ", filepath.Join(dataDir, "movies", "films.jsonl"))
	showFileStatus("  TV:    ", filepath.Join(dataDir, "movies", "tv.jsonl"))

	// IMDb
	fmt.Println()
	fmt.Println("IMDb:")
	for _, f := range []string{"title.ratings.tsv.gz", "title.basics.tsv.gz", "title.principals.tsv.gz", "name.basics.tsv.gz"} {
		showFileStatus("  "+f+":", filepath.Join(dataDir, "imdb", f))
	}

	// Enriched
	fmt.Println()
	fmt.Println("Enriched (v1):")
	showFileStatus("  Movies:", filepath.Join(dataDir, "enriched", "all_movies.jsonl.gz"))
	showFileStatus("  TV:    ", filepath.Join(dataDir, "enriched", "all_tv.jsonl.gz"))

	// TODO
	fmt.Println()
	fmt.Println("Pending actions:")
	if !fileExists(filepath.Join(dataDir, "tmdb", "movies_v2_tagged.jsonl")) {
		fmt.Println("  [ ] Run /tag movies")
	}
	if !fileExists(filepath.Join(dataDir, "tmdb", "tv_v2_tagged.jsonl")) {
		fmt.Println("  [ ] Run /tag tv")
	}
	fmt.Println("  [ ] Run /enrich all (merge Wiki plots + IMDb ratings into v2)")
	fmt.Println("  [ ] Run /crawl zee5, /crawl sunnxt (regional OTTs)")
	fmt.Println("  [ ] Run /cinema cineworld (UK showtimes)")
}

func handleFetch(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /fetch movies|tv|imdb|wiki")
		return
	}
	token := os.Getenv("TMDB_TOKEN")
	if token == "" {
		token = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIwZDZjMWQ3ZTJjMDJjYTkwNDcxZDNiNzU3YjdiY2E1NiIsIm5iZiI6MTcwMjU3MjM1OS45Niwic3ViIjoiNjU3YjMxNDc3YTNjNTIwMGViZmYyNWM4Iiwic2NvcGVzIjpbImFwaV9yZWFkIl0sInZlcnNpb24iOjF9.rMdjgaFbxfbAFPgU8dIm0mZYGbuPnp22GmBBUbD-9L8"
	}

	switch args[0] {
	case "movies":
		fmt.Println("Fetching TMDB movies (v2, full data)...")
		fmt.Println("This will take ~8 hours. Resume-safe.")
		runCmd("go", "run", "./cmd/tmdbfetch/",
			"-token", token, "-type", "movie", "-min-popularity", "0.001",
			"-output", filepath.Join(dataDir, "tmdb", "movies_v2.jsonl"))
	case "tv":
		fmt.Println("Fetching TMDB TV shows (v2, full data)...")
		fmt.Println("This will take ~1.5 hours. Resume-safe.")
		runCmd("go", "run", "./cmd/tmdbfetch/",
			"-token", token, "-type", "tv", "-min-popularity", "0.001",
			"-output", filepath.Join(dataDir, "tmdb", "tv_v2.jsonl"))
	case "imdb":
		fmt.Println("Downloading IMDb datasets...")
		os.MkdirAll(filepath.Join(dataDir, "imdb"), 0o755)
		for _, f := range []string{"title.ratings.tsv.gz", "title.basics.tsv.gz", "title.principals.tsv.gz", "name.basics.tsv.gz", "title.crew.tsv.gz", "title.episode.tsv.gz"} {
			fmt.Printf("  %s...\n", f)
			runCmd("curl", "-sL", "-o", filepath.Join(dataDir, "imdb", f), "https://datasets.imdbws.com/"+f)
		}
		fmt.Println("Done.")
	case "wiki":
		fmt.Println("Downloading Wikipedia dump (~25GB)...")
		os.MkdirAll(filepath.Join(dataDir, "wiki"), 0o755)
		runCmd("curl", "-L", "-C", "-", "-o",
			filepath.Join(dataDir, "wiki", "enwiki-latest-pages-articles.xml.bz2"),
			"https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2")
	default:
		fmt.Printf("Unknown fetch target: %s. Use movies|tv|imdb|wiki\n", args[0])
	}
}

func handleTag(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /tag movies|tv")
		return
	}
	switch args[0] {
	case "movies":
		input := filepath.Join(dataDir, "tmdb", "movies_v2.jsonl")
		output := filepath.Join(dataDir, "tmdb", "movies_v2_tagged.jsonl")
		if !fileExists(input) {
			fmt.Println("No movies data. Run /fetch movies first.")
			return
		}
		fmt.Println("Tagging movies...")
		runCmd("go", "run", "./cmd/tagger/", "-input", input, "-output", output, "-type", "movie")
	case "tv":
		input := filepath.Join(dataDir, "tmdb", "tv_v2.jsonl")
		output := filepath.Join(dataDir, "tmdb", "tv_v2_tagged.jsonl")
		if !fileExists(input) {
			fmt.Println("No TV data. Run /fetch tv first.")
			return
		}
		fmt.Println("Tagging TV shows...")
		runCmd("go", "run", "./cmd/tagger/", "-input", input, "-output", output, "-type", "tv")
	default:
		fmt.Println("Usage: /tag movies|tv")
	}
}

func handleEnrich(args []string) {
	if len(args) == 0 || args[0] == "all" {
		fmt.Println("Running enrichment pipeline...")
		cmdArgs := []string{"run", "./cmd/enrich/",
			"-tmdb-movies", filepath.Join(dataDir, "tmdb", "movies_v2.jsonl"),
			"-tmdb-tv", filepath.Join(dataDir, "tmdb", "tv_v2.jsonl"),
			"-wiki-movies", filepath.Join(dataDir, "movies", "films.jsonl"),
			"-wiki-tv", filepath.Join(dataDir, "movies", "tv.jsonl"),
			"-imdb-ratings", filepath.Join(dataDir, "imdb", "title.ratings.tsv.gz"),
			"-imdb-cast", filepath.Join(dataDir, "imdb", "title.principals.tsv.gz"),
			"-imdb-names", filepath.Join(dataDir, "imdb", "name.basics.tsv.gz"),
			"-imdb-basics", filepath.Join(dataDir, "imdb", "title.basics.tsv.gz"),
			"-output", filepath.Join(dataDir, "enriched"),
		}
		runCmd("go", cmdArgs...)
	} else {
		fmt.Println("Usage: /enrich all")
	}
}

func handleCrawl(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /crawl aha|hotstar|zee5|sunnxt")
		fmt.Println()
		fmt.Println("Status:")
		showFileStatus("  aha:    ", filepath.Join(dataDir, "ott", "aha.jsonl"))
		showFileStatus("  hotstar:", filepath.Join(dataDir, "ott", "hotstar.jsonl"))
		return
	}
	platform := strings.ToLower(args[0])
	switch platform {
	case "aha", "hotstar":
		fmt.Printf("Crawling %s...\n", platform)
		runCmd("go", "run", "./cmd/ottcrawl/", "-platform", platform, "-output", filepath.Join(dataDir, "ott"))
	default:
		fmt.Printf("Platform '%s' not yet configured in ottcrawl. Run /probe %s to investigate.\n", platform, platform)
	}
}

func handleCinema(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /cinema cineworld [date]")
		fmt.Println()
		fmt.Println("Available:")
		fmt.Println("  cineworld  — 87 UK cinemas, open REST API")
		return
	}
	switch args[0] {
	case "cineworld":
		date := time.Now().Format("2006-01-02")
		if len(args) > 1 {
			date = args[1]
		}
		fetchCineworldShowtimes(date)
	default:
		fmt.Printf("Cinema '%s' not yet supported.\n", args[0])
	}
}

func fetchCineworldShowtimes(date string) {
	fmt.Printf("Fetching Cineworld showtimes for %s...\n", date)

	// Get cinemas
	cinemasURL := fmt.Sprintf("https://www.cineworld.co.uk/uk/data-api-service/v1/quickbook/10108/cinemas/with-event/until/%s?attr=&lang=en_GB", date)
	cinemasResp, err := http.Get(cinemasURL)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer cinemasResp.Body.Close()

	var cinemasData struct {
		Body struct {
			Cinemas []struct {
				ID          string `json:"id"`
				DisplayName string `json:"displayName"`
			} `json:"cinemas"`
		} `json:"body"`
	}
	json.NewDecoder(cinemasResp.Body).Decode(&cinemasData)
	cinemas := cinemasData.Body.Cinemas
	fmt.Printf("Found %d cinemas\n", len(cinemas))

	// Fetch showtimes for each cinema
	os.MkdirAll(filepath.Join(dataDir, "cinema"), 0o755)
	outPath := filepath.Join(dataDir, "cinema", fmt.Sprintf("cineworld_%s.jsonl", date))
	f, _ := os.Create(outPath)
	defer f.Close()
	enc := json.NewEncoder(f)

	var totalFilms, totalEvents int
	for i, cinema := range cinemas {
		url := fmt.Sprintf("https://www.cineworld.co.uk/uk/data-api-service/v1/quickbook/10108/film-events/in-cinema/%s/at-date/%s?attr=&lang=en_GB", cinema.ID, date)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}

		var data struct {
			Body struct {
				Films []struct {
					ID       string `json:"id"`
					Name     string `json:"name"`
					Length   int    `json:"length"`
					Rating   string `json:"rating"`
					Poster   string `json:"posterLink"`
					Trailer  string `json:"videoLink"`
				} `json:"films"`
				Events []struct {
					FilmID        string `json:"filmId"`
					DateTime      string `json:"eventDateTime"`
					BookingLink   string `json:"bookingLink"`
					Auditorium    string `json:"auditorium"`
					Attributes    []string `json:"attributeIds"`
				} `json:"events"`
			} `json:"body"`
		}
		json.NewDecoder(resp.Body).Decode(&data)
		resp.Body.Close()

		for _, film := range data.Body.Films {
			var showtimes []map[string]any
			for _, evt := range data.Body.Events {
				if evt.FilmID == film.ID {
					showtimes = append(showtimes, map[string]any{
						"time":        evt.DateTime,
						"booking_url": evt.BookingLink,
						"auditorium":  evt.Auditorium,
						"attributes":  evt.Attributes,
					})
				}
			}
			record := map[string]any{
				"cinema_id":   cinema.ID,
				"cinema_name": cinema.DisplayName,
				"film_name":   film.Name,
				"film_id":     film.ID,
				"runtime":     film.Length,
				"rating":      film.Rating,
				"poster":      film.Poster,
				"trailer":     film.Trailer,
				"date":        date,
				"showtimes":   showtimes,
			}
			enc.Encode(record)
			totalFilms++
		}
		totalEvents += len(data.Body.Events)

		if (i+1)%10 == 0 {
			fmt.Printf("  %d/%d cinemas processed...\n", i+1, len(cinemas))
		}
		time.Sleep(100 * time.Millisecond) // be polite
	}

	fmt.Printf("Done: %d film+cinema combinations, %d total screening events\n", totalFilms, totalEvents)
	fmt.Printf("Output: %s\n", outPath)
}

func handleUpload(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /upload movies|tv|ott|cinema|all")
		return
	}
	fmt.Println("Uploading to R2...")
	switch args[0] {
	case "movies":
		uploadToR2(filepath.Join(dataDir, "tmdb", "movies_v2.jsonl.gz"), "final/movies_v2.jsonl.gz")
	case "tv":
		uploadToR2(filepath.Join(dataDir, "tmdb", "tv_v2.jsonl.gz"), "final/tv_v2.jsonl.gz")
	default:
		fmt.Printf("Unknown upload target: %s\n", args[0])
	}
}

func handleSearch(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /search <query>")
		return
	}
	query := strings.ToLower(strings.Join(args, " "))

	// Search in movies v2
	moviesPath := filepath.Join(dataDir, "tmdb", "movies_v2.jsonl")
	if !fileExists(moviesPath) {
		fmt.Println("No movies data. Run /fetch movies first.")
		return
	}

	fmt.Printf("Searching for \"%s\"...\n\n", query)

	f, _ := os.Open(moviesPath)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 16<<20), 16<<20)

	var found int
	for scanner.Scan() && found < 10 {
		var m map[string]any
		json.Unmarshal(scanner.Bytes(), &m)

		title := strings.ToLower(fmt.Sprintf("%v", m["title"]))
		origTitle := strings.ToLower(fmt.Sprintf("%v", m["original_title"]))

		if strings.Contains(title, query) || strings.Contains(origTitle, query) {
			found++
			year := ""
			if rd, ok := m["release_date"].(string); ok && len(rd) >= 4 {
				year = rd[:4]
			}
			rating := m["vote_average"]
			votes := m["vote_count"]
			castCount := 0
			if c, ok := m["cast"].([]any); ok {
				castCount = len(c)
			}
			providers := 0
			if wp, ok := m["watch_providers"].(map[string]any); ok {
				providers = len(wp)
			}
			videos := 0
			if v, ok := m["videos"].([]any); ok {
				videos = len(v)
			}

			fmt.Printf("  %d. %s (%s)\n", found, m["title"], year)
			fmt.Printf("     TMDB: %v  IMDB: %v  Rating: %v (%v votes)\n",
				m["tmdb_id"], m["imdb_id"], rating, votes)
			fmt.Printf("     Cast: %d  Videos: %d  Available in: %d countries\n",
				castCount, videos, providers)
			fmt.Printf("     Genres: %v\n", m["genres"])
			fmt.Println()
		}
	}

	if found == 0 {
		// Try TV
		tvPath := filepath.Join(dataDir, "tmdb", "tv_v2.jsonl")
		if fileExists(tvPath) {
			f2, _ := os.Open(tvPath)
			defer f2.Close()
			scanner2 := bufio.NewScanner(f2)
			scanner2.Buffer(make([]byte, 16<<20), 16<<20)
			for scanner2.Scan() && found < 10 {
				var m map[string]any
				json.Unmarshal(scanner2.Bytes(), &m)
				name := strings.ToLower(fmt.Sprintf("%v", m["name"]))
				if strings.Contains(name, query) {
					found++
					fmt.Printf("  %d. [TV] %s (%v seasons)\n", found, m["name"], m["number_of_seasons"])
					fmt.Printf("     TMDB: %v  Rating: %v\n", m["tmdb_id"], m["vote_average"])
					fmt.Println()
				}
			}
		}
	}

	if found == 0 {
		fmt.Println("  No results found.")
	}
}

func handleInspect(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /inspect <tmdb_id>")
		return
	}
	targetID, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Invalid TMDB ID")
		return
	}

	// Search both movies and TV
	for _, info := range []struct{ path, idField string }{
		{filepath.Join(dataDir, "tmdb", "movies_v2.jsonl"), "tmdb_id"},
		{filepath.Join(dataDir, "tmdb", "tv_v2.jsonl"), "tmdb_id"},
	} {
		if !fileExists(info.path) {
			continue
		}
		f, _ := os.Open(info.path)
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 16<<20), 16<<20)
		for scanner.Scan() {
			var m map[string]any
			json.Unmarshal(scanner.Bytes(), &m)
			id, _ := m[info.idField].(float64)
			if int(id) == targetID {
				out, _ := json.MarshalIndent(m, "", "  ")
				// Truncate if too long
				s := string(out)
				if len(s) > 5000 {
					fmt.Println(s[:5000])
					fmt.Printf("\n... truncated (%d bytes total)\n", len(s))
				} else {
					fmt.Println(s)
				}
				f.Close()
				return
			}
		}
		f.Close()
	}
	fmt.Printf("TMDB ID %d not found in local data.\n", targetID)
}

func handleR2() {
	fmt.Println("Listing R2 bucket...")
	cmd := exec.Command("python3", "-c", `
import boto3
s3 = boto3.client("s3",
    endpoint_url="https://e04ba18afe42f7cd7efb8235e09d7524.r2.cloudflarestorage.com",
    aws_access_key_id="ea8dc0d27c920ed1aaee5f395faaf1a3",
    aws_secret_access_key="cc1ff71c8a1ca00ceceb511057cf23a5490ff917b4e8a9f9943098dd1dde4257",
    region_name="auto",
)
total = 0
resp = s3.list_objects_v2(Bucket="crawler-data", MaxKeys=1000)
for obj in resp.get("Contents", []):
    sz = obj["Size"]
    total += sz
    mb = sz / 1024 / 1024
    if mb > 0.1:
        print(f"  {obj['Key']:55s} {mb:8.1f} MB")
print(f"\n  Total: {total/1024/1024/1024:.1f} GB")
`)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}

func handleProbe(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: /probe <domain>")
		fmt.Println("Example: /probe www.zee5.com")
		return
	}
	fmt.Printf("Probing %s...\n", args[0])
	runCmd("go", "run", "./cmd/cinemaprobe/", "-domain", args[0])
}

func handleRegistry() {
	regPath := filepath.Join(dataDir, "ott", "registry.json")
	if !fileExists(regPath) {
		fmt.Println("Registry not found.")
		return
	}

	data, _ := os.ReadFile(regPath)
	var reg map[string]any
	json.Unmarshal(data, &reg)

	if platforms, ok := reg["ott_platforms"].(map[string]any); ok {
		fmt.Println("=== OTT Platforms ===")
		for region, list := range platforms {
			if items, ok := list.([]any); ok {
				fmt.Printf("\n  %s (%d platforms):\n", region, len(items))
				for _, item := range items {
					if m, ok := item.(map[string]any); ok {
						status := m["status"]
						if status == nil {
							status = m["strategy"]
						}
						fmt.Printf("    %-25s %v\n", m["name"], status)
					}
				}
			}
		}
	}

	if chains, ok := reg["cinema_chains"].(map[string]any); ok {
		fmt.Println("\n=== Cinema Chains ===")
		for country, list := range chains {
			if items, ok := list.([]any); ok {
				fmt.Printf("\n  %s (%d chains):\n", country, len(items))
				for _, item := range items {
					if m, ok := item.(map[string]any); ok {
						fmt.Printf("    %-25s %v\n", m["name"], m["strategy"])
					}
				}
			}
		}
	}
}

// --- Helpers ---

func showFileStatus(label, path string) {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Printf("%s NOT FOUND\n", label)
		return
	}
	size := float64(info.Size()) / 1024 / 1024
	unit := "MB"
	if size > 1024 {
		size /= 1024
		unit = "GB"
	}

	// Count lines for JSONL
	lines := countLines(path)
	if lines > 0 {
		fmt.Printf("%s %d records (%.1f %s) — %s\n", label, lines, size, unit, info.ModTime().Format("2006-01-02 15:04"))
	} else {
		fmt.Printf("%s %.1f %s — %s\n", label, size, unit, info.ModTime().Format("2006-01-02 15:04"))
	}
}

func countLines(path string) int {
	if strings.HasSuffix(path, ".gz") || strings.HasSuffix(path, ".bz2") {
		return 0 // skip counting compressed files
	}
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return 0
		}
		defer gr.Close()
		r = gr
	}

	count := 0
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		count++
	}
	return count
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func uploadToR2(localPath, r2Key string) {
	if !fileExists(localPath) {
		fmt.Printf("File not found: %s\n", localPath)
		return
	}
	fmt.Printf("Uploading %s → %s\n", localPath, r2Key)
	encodedKey := strings.ReplaceAll(r2Key, "/", "%2F")
	runCmd("curl", "-s", "-X", "PUT",
		fmt.Sprintf("https://api.cloudflare.com/client/v4/accounts/%s/r2/buckets/crawler-data/objects/%s", r2Acct, encodedKey),
		"-H", "X-Auth-Key: efafb8730f6e42084dede2541afc9961898ec",
		"-H", "X-Auth-Email: arun88m@gmail.com",
		"-H", "Content-Type: application/gzip",
		"-T", localPath)
}

func runCmd(name string, args ...string) {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		fmt.Printf("Command failed: %v\n", err)
	}
}
