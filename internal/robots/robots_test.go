package robots

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// mockFetch creates a FetchFunc that returns the given body and status code.
func mockFetch(body string, statusCode int) FetchFunc {
	return func(ctx context.Context, url string) ([]byte, int, error) {
		return []byte(body), statusCode, nil
	}
}

// mockFetchErr creates a FetchFunc that returns an error.
func mockFetchErr(err error) FetchFunc {
	return func(ctx context.Context, url string) ([]byte, int, error) {
		return nil, 0, err
	}
}

func TestIsAllowed_BasicDisallow(t *testing.T) {
	robotsTxt := `User-agent: *
Disallow: /private/
Disallow: /tmp/
Allow: /private/public/
`
	c := NewChecker("TestBot")

	tests := []struct {
		url     string
		allowed bool
	}{
		{"http://example.com/", true},
		{"http://example.com/about", true},
		{"http://example.com/private/secret", false},
		{"http://example.com/private/public/page", true},
		{"http://example.com/tmp/file", false},
		{"http://example.com/public", true},
	}

	for _, tt := range tests {
		allowed, err := c.IsAllowed(context.Background(), tt.url, mockFetch(robotsTxt, 200))
		if err != nil {
			t.Fatalf("IsAllowed(%q): unexpected error: %v", tt.url, err)
		}
		if allowed != tt.allowed {
			t.Errorf("IsAllowed(%q) = %v, want %v", tt.url, allowed, tt.allowed)
		}
	}
}

func TestIsAllowed_SpecificUserAgent(t *testing.T) {
	robotsTxt := `User-agent: SpecialBot
Disallow: /

User-agent: *
Disallow: /admin/
`
	c := NewChecker("SpecialBot/1.0")
	allowed, err := c.IsAllowed(context.Background(), "http://example.com/page", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Error("SpecialBot should be disallowed from /page")
	}

	// A different bot should be allowed.
	c2 := NewChecker("OtherBot")
	allowed2, err := c2.IsAllowed(context.Background(), "http://example.com/page", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if !allowed2 {
		t.Error("OtherBot should be allowed for /page")
	}

	// OtherBot should be blocked from /admin/.
	allowed3, err := c2.IsAllowed(context.Background(), "http://example.com/admin/settings", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if allowed3 {
		t.Error("OtherBot should be disallowed from /admin/settings")
	}
}

func TestIsAllowed_404AllowsAll(t *testing.T) {
	c := NewChecker("TestBot")
	allowed, err := c.IsAllowed(context.Background(), "http://example.com/anything", mockFetch("", 404))
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Error("404 robots.txt should allow all")
	}
}

func TestIsAllowed_5xxDisallowsAll(t *testing.T) {
	c := NewChecker("TestBot")
	allowed, err := c.IsAllowed(context.Background(), "http://example.com/anything", mockFetch("", 500))
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Error("5xx robots.txt should disallow all")
	}
}

func TestIsAllowed_FetchErrorDisallowsAll(t *testing.T) {
	c := NewChecker("TestBot")
	allowed, err := c.IsAllowed(context.Background(), "http://example.com/anything", mockFetchErr(fmt.Errorf("network error")))
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Error("fetch error should disallow all (conservative)")
	}
}

func TestGetCrawlDelay(t *testing.T) {
	robotsTxt := `User-agent: *
Crawl-delay: 5
Disallow: /private/
`
	c := NewChecker("TestBot")
	_, err := c.IsAllowed(context.Background(), "http://example.com/page", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}

	delay := c.GetCrawlDelay("example.com")
	if delay != 5.0 {
		t.Errorf("GetCrawlDelay = %v, want 5.0", delay)
	}
}

func TestGetCrawlDelay_Float(t *testing.T) {
	robotsTxt := `User-agent: *
Crawl-delay: 0.5
`
	c := NewChecker("TestBot")
	_, err := c.IsAllowed(context.Background(), "http://example.com/", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	delay := c.GetCrawlDelay("example.com")
	if delay != 0.5 {
		t.Errorf("GetCrawlDelay = %v, want 0.5", delay)
	}
}

func TestGetCrawlDelay_Unfetched(t *testing.T) {
	c := NewChecker("TestBot")
	delay := c.GetCrawlDelay("unknown.com")
	if delay != 0 {
		t.Errorf("unfetched domain crawl-delay should be 0, got %v", delay)
	}
}

func TestGetSitemaps(t *testing.T) {
	robotsTxt := `User-agent: *
Disallow: /private/

Sitemap: http://example.com/sitemap.xml
Sitemap: http://example.com/sitemap2.xml
`
	c := NewChecker("TestBot")
	_, err := c.IsAllowed(context.Background(), "http://example.com/", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}

	sitemaps := c.GetSitemaps("example.com")
	if len(sitemaps) != 2 {
		t.Fatalf("expected 2 sitemaps, got %d", len(sitemaps))
	}
	if sitemaps[0] != "http://example.com/sitemap.xml" {
		t.Errorf("sitemap[0] = %q, want http://example.com/sitemap.xml", sitemaps[0])
	}
	if sitemaps[1] != "http://example.com/sitemap2.xml" {
		t.Errorf("sitemap[1] = %q, want http://example.com/sitemap2.xml", sitemaps[1])
	}
}

func TestGetSitemaps_Unfetched(t *testing.T) {
	c := NewChecker("TestBot")
	sitemaps := c.GetSitemaps("unknown.com")
	if sitemaps != nil {
		t.Errorf("unfetched domain sitemaps should be nil, got %v", sitemaps)
	}
}

func TestIsAllowed_WildcardPattern(t *testing.T) {
	robotsTxt := `User-agent: *
Disallow: /*.json$
Disallow: /api/*/internal
Allow: /api/v1/public
`
	c := NewChecker("TestBot")

	tests := []struct {
		url     string
		allowed bool
	}{
		{"http://example.com/data.json", false},
		{"http://example.com/data.json?q=1", true}, // $ anchor means exact end
		{"http://example.com/api/v2/internal", false},
		{"http://example.com/api/v1/public", true},
		{"http://example.com/page.html", true},
	}

	for _, tt := range tests {
		allowed, err := c.IsAllowed(context.Background(), tt.url, mockFetch(robotsTxt, 200))
		if err != nil {
			t.Fatalf("IsAllowed(%q): %v", tt.url, err)
		}
		if allowed != tt.allowed {
			t.Errorf("IsAllowed(%q) = %v, want %v", tt.url, allowed, tt.allowed)
		}
	}
}

func TestIsAllowed_EmptyDisallow(t *testing.T) {
	robotsTxt := `User-agent: *
Disallow:
`
	c := NewChecker("TestBot")
	allowed, err := c.IsAllowed(context.Background(), "http://example.com/anything", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Error("empty Disallow should allow all")
	}
}

func TestIsAllowed_Comments(t *testing.T) {
	robotsTxt := `# This is a comment
User-agent: * # all bots
Disallow: /secret/ # keep out
Allow: /secret/public/ # but this is ok
`
	c := NewChecker("TestBot")

	allowed, err := c.IsAllowed(context.Background(), "http://example.com/secret/page", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Error("/secret/page should be disallowed")
	}

	allowed2, err := c.IsAllowed(context.Background(), "http://example.com/secret/public/page", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if !allowed2 {
		t.Error("/secret/public/page should be allowed")
	}
}

func TestIsAllowed_CachingPerDomain(t *testing.T) {
	callCount := 0
	fetch := func(ctx context.Context, url string) ([]byte, int, error) {
		callCount++
		return []byte("User-agent: *\nDisallow: /admin/\n"), 200, nil
	}

	c := NewChecker("TestBot")

	// First call should fetch.
	_, err := c.IsAllowed(context.Background(), "http://example.com/page1", fetch)
	if err != nil {
		t.Fatal(err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 fetch call, got %d", callCount)
	}

	// Second call same domain should use cache.
	_, err = c.IsAllowed(context.Background(), "http://example.com/page2", fetch)
	if err != nil {
		t.Fatal(err)
	}
	if callCount != 1 {
		t.Errorf("expected cache hit (1 fetch call), got %d", callCount)
	}

	// Different domain should fetch again.
	_, err = c.IsAllowed(context.Background(), "http://other.com/page", fetch)
	if err != nil {
		t.Fatal(err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 fetch calls for different domain, got %d", callCount)
	}
}

func TestIsAllowed_ThreadSafety(t *testing.T) {
	robotsTxt := `User-agent: *
Disallow: /private/
`
	c := NewChecker("TestBot")
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			url := fmt.Sprintf("http://example.com/page%d", i)
			_, err := c.IsAllowed(context.Background(), url, mockFetch(robotsTxt, 200))
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestParseSitemap_URLSet(t *testing.T) {
	sitemap := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/page1</loc></url>
  <url><loc>http://example.com/page2</loc></url>
  <url><loc>http://example.com/page3</loc></url>
</urlset>`

	fetch := mockFetch(sitemap, 200)
	urls, err := ParseSitemap(context.Background(), "http://example.com/sitemap.xml", fetch, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 3 {
		t.Fatalf("expected 3 URLs, got %d", len(urls))
	}
	expected := []string{
		"http://example.com/page1",
		"http://example.com/page2",
		"http://example.com/page3",
	}
	for i, u := range urls {
		if u != expected[i] {
			t.Errorf("url[%d] = %q, want %q", i, u, expected[i])
		}
	}
}

func TestParseSitemap_MaxURLs(t *testing.T) {
	sitemap := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/page1</loc></url>
  <url><loc>http://example.com/page2</loc></url>
  <url><loc>http://example.com/page3</loc></url>
</urlset>`

	urls, err := ParseSitemap(context.Background(), "http://example.com/sitemap.xml", mockFetch(sitemap, 200), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("expected 2 URLs (maxURLs=2), got %d", len(urls))
	}
}

func TestParseSitemap_Index(t *testing.T) {
	sitemapIndex := `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>http://example.com/sitemap1.xml</loc></sitemap>
  <sitemap><loc>http://example.com/sitemap2.xml</loc></sitemap>
</sitemapindex>`

	sitemap1 := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/a</loc></url>
</urlset>`

	sitemap2 := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/b</loc></url>
</urlset>`

	fetch := func(ctx context.Context, url string) ([]byte, int, error) {
		switch url {
		case "http://example.com/sitemap-index.xml":
			return []byte(sitemapIndex), 200, nil
		case "http://example.com/sitemap1.xml":
			return []byte(sitemap1), 200, nil
		case "http://example.com/sitemap2.xml":
			return []byte(sitemap2), 200, nil
		default:
			return nil, 404, nil
		}
	}

	urls, err := ParseSitemap(context.Background(), "http://example.com/sitemap-index.xml", fetch, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 {
		t.Fatalf("expected 2 URLs from index, got %d: %v", len(urls), urls)
	}
}

func TestParseSitemap_Gzipped(t *testing.T) {
	sitemap := `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://example.com/gzpage</loc></url>
</urlset>`

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write([]byte(sitemap))
	gw.Close()
	gzBody := buf.Bytes()

	fetch := func(ctx context.Context, url string) ([]byte, int, error) {
		return gzBody, 200, nil
	}

	urls, err := ParseSitemap(context.Background(), "http://example.com/sitemap.xml.gz", fetch, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 {
		t.Fatalf("expected 1 URL, got %d", len(urls))
	}
	if urls[0] != "http://example.com/gzpage" {
		t.Errorf("url = %q, want http://example.com/gzpage", urls[0])
	}
}

func TestParseSitemap_FetchError(t *testing.T) {
	_, err := ParseSitemap(context.Background(), "http://example.com/sitemap.xml", mockFetchErr(fmt.Errorf("fail")), 100)
	if err == nil {
		t.Error("expected error on fetch failure")
	}
}

func TestParseSitemap_NonSuccessStatus(t *testing.T) {
	_, err := ParseSitemap(context.Background(), "http://example.com/sitemap.xml", mockFetch("", 404), 100)
	if err == nil {
		t.Error("expected error on 404")
	}
}

func TestParseSitemap_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ParseSitemap(ctx, "http://example.com/sitemap.xml", mockFetch("", 200), 100)
	if err == nil {
		t.Error("expected error on cancelled context")
	}
}

func TestIsAllowed_WithHTTPTestServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, "User-agent: *\nDisallow: /secret/\nCrawl-delay: 2\nSitemap: http://test/sitemap.xml\n")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewChecker("TestBot")
	fetch := func(ctx context.Context, url string) ([]byte, int, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, 0, err
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		buf.ReadFrom(resp.Body)
		return buf.Bytes(), resp.StatusCode, nil
	}

	// Rewrite URL to point to test server.
	allowed, err := c.IsAllowed(context.Background(), ts.URL+"/page", fetch)
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Error("/page should be allowed")
	}

	allowed2, err := c.IsAllowed(context.Background(), ts.URL+"/secret/data", fetch)
	if err != nil {
		t.Fatal(err)
	}
	if allowed2 {
		t.Error("/secret/data should be disallowed")
	}

	// Verify crawl delay and sitemaps from parsed domain.
	host := strings.TrimPrefix(ts.URL, "http://")
	delay := c.GetCrawlDelay(host)
	if delay != 2.0 {
		t.Errorf("crawl-delay = %v, want 2.0", delay)
	}
	sitemaps := c.GetSitemaps(host)
	if len(sitemaps) != 1 || sitemaps[0] != "http://test/sitemap.xml" {
		t.Errorf("sitemaps = %v, want [http://test/sitemap.xml]", sitemaps)
	}
}

func TestPathMatches_DollarAnchor(t *testing.T) {
	// $ anchor should only match at end of path.
	if !pathMatches("/data.json", "/*.json$") {
		t.Error("/*.json$ should match /data.json")
	}
	if pathMatches("/data.json?q=1", "/*.json$") {
		t.Error("/*.json$ should NOT match /data.json?q=1")
	}
}

func TestIsAllowed_MultipleUserAgentLines(t *testing.T) {
	robotsTxt := `User-agent: BotA
User-agent: BotB
Disallow: /restricted/

User-agent: *
Disallow:
`
	c := NewChecker("BotB/2.0")
	allowed, err := c.IsAllowed(context.Background(), "http://example.com/restricted/page", mockFetch(robotsTxt, 200))
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Error("BotB should be disallowed from /restricted/")
	}
}
