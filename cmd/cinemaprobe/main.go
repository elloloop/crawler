// cinemaprobe checks cinema chain websites for crawlability:
// robots.txt, sitemaps, JSON-LD structured data, and API endpoints.
//
// Usage:
//
//	cinemaprobe -domain www.cineworld.co.uk
//	cinemaprobe -all
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type CinemaChain struct {
	Name    string `json:"name"`
	Domain  string `json:"domain"`
	Country string `json:"country"`
}

var chains = []CinemaChain{
	// UK
	{"Cineworld", "www.cineworld.co.uk", "UK"},
	{"Odeon", "www.odeon.co.uk", "UK"},
	{"Vue", "www.myvue.com", "UK"},
	{"Curzon", "www.curzon.com", "UK"},
	{"Picturehouse", "www.picturehouses.com", "UK"},
	{"Everyman", "www.everymancinema.com", "UK"},
	{"Showcase", "www.showcasecinemas.co.uk", "UK"},
	// India
	{"PVR INOX", "www.pvrcinemas.com", "IN"},
	{"Cinepolis India", "www.cinepolisindia.com", "IN"},
	{"BookMyShow", "in.bookmyshow.com", "IN"},
	// US
	{"AMC", "www.amctheatres.com", "US"},
	{"Regal", "www.regmovies.com", "US"},
	{"Cinemark", "www.cinemark.com", "US"},
	{"Fandango", "www.fandango.com", "US"},
}

type ProbeResult struct {
	Name           string   `json:"name"`
	Domain         string   `json:"domain"`
	Country        string   `json:"country"`
	RobotsTxt      string   `json:"robots_txt"`      // "found", "not_found", "error"
	HasSitemap     bool     `json:"has_sitemap"`
	SitemapURLs    []string `json:"sitemap_urls,omitempty"`
	HasJSONLD      bool     `json:"has_json_ld"`
	JSONLDTypes    []string `json:"json_ld_types,omitempty"`
	HasScreeningEvent bool  `json:"has_screening_event"`
	SamplePageStatus int   `json:"sample_page_status"`
	Notes          []string `json:"notes,omitempty"`
}

var (
	reLDJSON = regexp.MustCompile(`(?s)<script type="application/ld\+json">\s*(.*?)\s*</script>`)
	client   = &http.Client{Timeout: 15 * time.Second}
)

func main() {
	domain := flag.String("domain", "", "Single domain to probe")
	all := flag.Bool("all", false, "Probe all known cinema chains")
	flag.Parse()

	if !*all && *domain == "" {
		log.Fatal("Usage: cinemaprobe -domain DOMAIN or cinemaprobe -all")
	}

	var toProbe []CinemaChain
	if *all {
		toProbe = chains
	} else {
		toProbe = []CinemaChain{{"Custom", *domain, "?"}}
	}

	for _, chain := range toProbe {
		result := probe(chain)
		out, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(out))
		fmt.Println()
	}
}

func probe(chain CinemaChain) *ProbeResult {
	log.Printf("Probing %s (%s)...", chain.Name, chain.Domain)
	r := &ProbeResult{
		Name:    chain.Name,
		Domain:  chain.Domain,
		Country: chain.Country,
	}

	// 1. Check robots.txt
	robotsBody := fetch("https://" + chain.Domain + "/robots.txt")
	if robotsBody != "" {
		r.RobotsTxt = "found"
		// Extract sitemap URLs
		for _, line := range strings.Split(robotsBody, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(strings.ToLower(line), "sitemap:") {
				url := strings.TrimSpace(line[8:])
				r.SitemapURLs = append(r.SitemapURLs, url)
				r.HasSitemap = true
			}
		}
		// Check for interesting disallow rules
		if strings.Contains(strings.ToLower(robotsBody), "disallow: /") {
			r.Notes = append(r.Notes, "Has Disallow rules in robots.txt")
		}
	} else {
		r.RobotsTxt = "not_found"
	}

	// 2. Check main page for JSON-LD
	mainBody := fetch("https://" + chain.Domain)
	if mainBody != "" {
		r.SamplePageStatus = 200
		ldTypes := extractLDTypes(mainBody)
		if len(ldTypes) > 0 {
			r.HasJSONLD = true
			r.JSONLDTypes = ldTypes
			for _, t := range ldTypes {
				if t == "ScreeningEvent" || t == "MovieTheater" {
					r.HasScreeningEvent = true
				}
			}
		}
	}

	// 3. Try common showtime page patterns
	showtimePaths := []string{"/showtimes", "/whats-on", "/movies", "/now-showing", "/films"}
	for _, path := range showtimePaths {
		body := fetch("https://" + chain.Domain + path)
		if body != "" {
			ldTypes := extractLDTypes(body)
			if len(ldTypes) > 0 {
				r.HasJSONLD = true
				for _, t := range ldTypes {
					if !contains(r.JSONLDTypes, t) {
						r.JSONLDTypes = append(r.JSONLDTypes, t)
					}
					if t == "ScreeningEvent" || t == "MovieTheater" {
						r.HasScreeningEvent = true
					}
				}
				r.Notes = append(r.Notes, fmt.Sprintf("JSON-LD found at %s", path))
			}
			if strings.Contains(strings.ToLower(body), "showtime") || strings.Contains(strings.ToLower(body), "screening") {
				r.Notes = append(r.Notes, fmt.Sprintf("Showtime content at %s", path))
			}
		}
	}

	// 4. Check for API endpoints
	apiPaths := []string{"/api/showtimes", "/api/v1/showtimes", "/api/movies", "/graphql"}
	for _, path := range apiPaths {
		resp, err := client.Get("https://" + chain.Domain + path)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 || resp.StatusCode == 401 || resp.StatusCode == 403 {
				r.Notes = append(r.Notes, fmt.Sprintf("API endpoint %s → %d", path, resp.StatusCode))
			}
		}
	}

	return r
}

func fetch(url string) string {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return ""
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return string(body)
}

func extractLDTypes(html string) []string {
	var types []string
	matches := reLDJSON.FindAllStringSubmatch(html, -1)
	for _, m := range matches {
		// Try as object
		var obj map[string]any
		if err := json.Unmarshal([]byte(m[1]), &obj); err == nil {
			if t, ok := obj["@type"].(string); ok {
				types = append(types, t)
			}
		}
		// Try as array
		var arr []map[string]any
		if err := json.Unmarshal([]byte(m[1]), &arr); err == nil {
			for _, item := range arr {
				if t, ok := item["@type"].(string); ok {
					types = append(types, t)
				}
			}
		}
	}
	return types
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
