package polite

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/strategy"
)

// --- Mock implementations ---

type mockQueue struct {
	mu     sync.Mutex
	events []*crawlerv1.URLFrontierEvent
}

func (q *mockQueue) Push(ctx context.Context, event *crawlerv1.URLFrontierEvent) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.events = append(q.events, event)
	return nil
}

func (q *mockQueue) Pop(ctx context.Context) (*crawlerv1.URLFrontierEvent, error) {
	return nil, fmt.Errorf("not implemented")
}

func (q *mockQueue) Len(ctx context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.events)), nil
}

func (q *mockQueue) Close() error { return nil }

func (q *mockQueue) getEvents() []*crawlerv1.URLFrontierEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*crawlerv1.URLFrontierEvent, len(q.events))
	copy(out, q.events)
	return out
}

type mockStorage struct {
	mu    sync.Mutex
	pages []*crawlerv1.Page
}

func (s *mockStorage) WritePage(ctx context.Context, page *crawlerv1.Page) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pages = append(s.pages, page)
	return nil
}

func (s *mockStorage) ListPages(ctx context.Context, crawlID string, limit int, offset int) ([]*crawlerv1.Page, error) {
	return nil, nil
}

func (s *mockStorage) GetPage(ctx context.Context, crawlID string, url string) (*crawlerv1.Page, error) {
	return nil, nil
}

func (s *mockStorage) Export(ctx context.Context, crawlID string, w io.Writer) error { return nil }

func (s *mockStorage) Close() error { return nil }

func (s *mockStorage) getPages() []*crawlerv1.Page {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*crawlerv1.Page, len(s.pages))
	copy(out, s.pages)
	return out
}

type mockState struct {
	mu       sync.Mutex
	seen     map[string]struct{}
	etags    map[string][2]string // url -> [etag, lastModified]
	projects map[string]*crawlerv1.Project
	jobs     map[string]*crawlerv1.CrawlJob
}

func newMockState() *mockState {
	return &mockState{
		seen:     make(map[string]struct{}),
		etags:    make(map[string][2]string),
		projects: make(map[string]*crawlerv1.Project),
		jobs:     make(map[string]*crawlerv1.CrawlJob),
	}
}

func (s *mockState) HasSeen(ctx context.Context, crawlID string, url string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := crawlID + ":" + url
	_, ok := s.seen[key]
	return ok, nil
}

func (s *mockState) MarkSeen(ctx context.Context, crawlID string, url string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := crawlID + ":" + url
	s.seen[key] = struct{}{}
	return nil
}

func (s *mockState) GetETag(ctx context.Context, url string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.etags[url]
	if !ok {
		return "", "", nil
	}
	return v[0], v[1], nil
}

func (s *mockState) SetETag(ctx context.Context, url string, etag string, lastModified string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.etags[url] = [2]string{etag, lastModified}
	return nil
}

func (s *mockState) SaveProject(ctx context.Context, project *crawlerv1.Project) error {
	return nil
}
func (s *mockState) GetProject(ctx context.Context, id string) (*crawlerv1.Project, error) {
	return nil, nil
}
func (s *mockState) ListProjects(ctx context.Context) ([]*crawlerv1.Project, error) {
	return nil, nil
}
func (s *mockState) DeleteProject(ctx context.Context, id string) error { return nil }
func (s *mockState) SaveCrawlJob(ctx context.Context, job *crawlerv1.CrawlJob) error {
	return nil
}
func (s *mockState) GetCrawlJob(ctx context.Context, id string) (*crawlerv1.CrawlJob, error) {
	return nil, nil
}
func (s *mockState) ListCrawlJobs(ctx context.Context, projectID string) ([]*crawlerv1.CrawlJob, error) {
	return nil, nil
}
func (s *mockState) Close() error { return nil }

type mockLimiter struct {
	mu     sync.Mutex
	limits map[string]*crawlerv1.DomainRateLimit
}

func newMockLimiter() *mockLimiter {
	return &mockLimiter{
		limits: make(map[string]*crawlerv1.DomainRateLimit),
	}
}

func (l *mockLimiter) Acquire(ctx context.Context, domain string) error { return nil }

func (l *mockLimiter) SetLimit(domain string, limit *crawlerv1.DomainRateLimit) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limits[domain] = limit
	return nil
}

func (l *mockLimiter) GetLimit(domain string) (*crawlerv1.DomainRateLimit, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limits[domain], nil
}

func (l *mockLimiter) ListLimits() ([]*crawlerv1.DomainRateLimit, error) {
	return nil, nil
}

func (l *mockLimiter) ResetLimit(domain string) error { return nil }

func (l *mockLimiter) Close() error { return nil }

// --- Tests ---

func TestName(t *testing.T) {
	s := New()
	if s.Name() != "polite" {
		t.Errorf("Name() = %q, want %q", s.Name(), "polite")
	}
}

func TestProcessURL_BasicPage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, "User-agent: *\nDisallow:\n")
	})
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>Test Page</title></head><body><p>Hello world</p><a href="/other">link</a></body></html>`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	q := &mockQueue{}
	st := &mockStorage{}
	state := newMockState()
	limiter := newMockLimiter()

	deps := &strategy.Deps{
		Queue:   q,
		Storage: st,
		State:   state,
		Limiter: limiter,
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/page",
		Depth:     0,
		ProjectId: "proj1",
		CrawlId:   "crawl1",
		Strategy:  crawlerv1.CrawlStrategy_CRAWL_STRATEGY_POLITE,
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Check page was stored.
	pages := st.getPages()
	if len(pages) != 1 {
		t.Fatalf("expected 1 stored page, got %d", len(pages))
	}
	if pages[0].Title != "Test Page" {
		t.Errorf("page title = %q, want %q", pages[0].Title, "Test Page")
	}
	if pages[0].Depth != 0 {
		t.Errorf("page depth = %d, want 0", pages[0].Depth)
	}
	if pages[0].ProjectId != "proj1" {
		t.Errorf("page project_id = %q, want %q", pages[0].ProjectId, "proj1")
	}

	// Check link was enqueued.
	events := q.getEvents()
	if len(events) == 0 {
		t.Fatal("expected at least 1 enqueued URL")
	}
	found := false
	for _, e := range events {
		if e.Url == ts.URL+"/other" {
			found = true
			if e.Depth != 1 {
				t.Errorf("enqueued depth = %d, want 1", e.Depth)
			}
			if e.DiscoveredFrom != ts.URL+"/page" {
				t.Errorf("discovered_from = %q, want %q", e.DiscoveredFrom, ts.URL+"/page")
			}
		}
	}
	if !found {
		t.Errorf("expected link %s/other to be enqueued", ts.URL)
	}
}

func TestProcessURL_DisallowedByRobots(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nDisallow: /secret/\n")
	})
	mux.HandleFunc("/secret/data", func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not have fetched /secret/data")
		fmt.Fprint(w, "<html><body>secret</body></html>")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	st := &mockStorage{}

	deps := &strategy.Deps{
		Queue:   &mockQueue{},
		Storage: st,
		State:   newMockState(),
		Limiter: newMockLimiter(),
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/secret/data",
		CrawlId:   "crawl1",
		ProjectId: "proj1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Page should NOT be stored.
	pages := st.getPages()
	if len(pages) != 0 {
		t.Errorf("expected 0 stored pages (disallowed), got %d", len(pages))
	}
}

func TestProcessURL_304NotModified(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nDisallow:\n")
	})
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"abc123"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"abc123"`)
		fmt.Fprint(w, "<html><body>content</body></html>")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	state := newMockState()

	// Pre-set ETag for the URL.
	state.SetETag(context.Background(), ts.URL+"/page", `"abc123"`, "")

	st := &mockStorage{}
	deps := &strategy.Deps{
		Queue:   &mockQueue{},
		Storage: st,
		State:   state,
		Limiter: newMockLimiter(),
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:     ts.URL + "/page",
		CrawlId: "crawl1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Page should NOT be stored (304).
	pages := st.getPages()
	if len(pages) != 0 {
		t.Errorf("expected 0 stored pages (304), got %d", len(pages))
	}
}

func TestProcessURL_MetaNoindex(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nDisallow:\n")
	})
	mux.HandleFunc("/noindex", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><head><meta name="robots" content="noindex, follow"></head><body><a href="/other">link</a></body></html>`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	st := &mockStorage{}
	q := &mockQueue{}

	deps := &strategy.Deps{
		Queue:   q,
		Storage: st,
		State:   newMockState(),
		Limiter: newMockLimiter(),
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:     ts.URL + "/noindex",
		CrawlId: "crawl1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Page should NOT be stored (noindex).
	pages := st.getPages()
	if len(pages) != 0 {
		t.Errorf("expected 0 stored pages (noindex), got %d", len(pages))
	}

	// Links should still be followed (follow is set).
	events := q.getEvents()
	if len(events) == 0 {
		t.Error("expected links to be enqueued despite noindex (follow is set)")
	}
}

func TestProcessURL_MetaNofollow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nDisallow:\n")
	})
	mux.HandleFunc("/nofollow", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><head><meta name="robots" content="index, nofollow"></head><body><a href="/other">link</a></body></html>`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	st := &mockStorage{}
	q := &mockQueue{}

	deps := &strategy.Deps{
		Queue:   q,
		Storage: st,
		State:   newMockState(),
		Limiter: newMockLimiter(),
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:     ts.URL + "/nofollow",
		CrawlId: "crawl1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Page should be stored (index is set).
	pages := st.getPages()
	if len(pages) != 1 {
		t.Errorf("expected 1 stored page (index), got %d", len(pages))
	}

	// Links should NOT be followed (nofollow).
	events := q.getEvents()
	if len(events) != 0 {
		t.Errorf("expected 0 enqueued URLs (nofollow), got %d", len(events))
	}
}

func TestProcessURL_CrawlDelaySetsRateLimit(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nCrawl-delay: 10\nDisallow:\n")
	})
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html><body>hi</body></html>")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	limiter := newMockLimiter()

	deps := &strategy.Deps{
		Queue:   &mockQueue{},
		Storage: &mockStorage{},
		State:   newMockState(),
		Limiter: limiter,
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:     ts.URL + "/page",
		CrawlId: "crawl1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Check that the rate limit was set.
	host := domainFromURL(ts.URL + "/page")
	limit, _ := limiter.GetLimit(host)
	if limit == nil {
		t.Fatal("expected rate limit to be set from crawl-delay")
	}
	expectedRPS := 1.0 / 10.0
	if limit.RequestsPerSecond != expectedRPS {
		t.Errorf("rate limit RPS = %v, want %v", limit.RequestsPerSecond, expectedRPS)
	}
}

func TestProcessURL_DeduplicatesLinks(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nDisallow:\n")
	})
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body><a href="/link1">L1</a><a href="/link1">L1 again</a></body></html>`)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	q := &mockQueue{}
	state := newMockState()

	deps := &strategy.Deps{
		Queue:   q,
		Storage: &mockStorage{},
		State:   state,
		Limiter: newMockLimiter(),
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:     ts.URL + "/page",
		CrawlId: "crawl1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// The parser deduplicates links internally, so we should see at most one enqueue.
	events := q.getEvents()
	linkCount := 0
	for _, e := range events {
		if e.Url == ts.URL+"/link1" {
			linkCount++
		}
	}
	if linkCount > 1 {
		t.Errorf("link /link1 enqueued %d times, expected at most 1", linkCount)
	}
}

func TestProcessURL_SitemapProcessing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "User-agent: *\nDisallow:\nSitemap: %s/sitemap.xml\n", "http://"+r.Host)
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://%s/from-sitemap</loc></url>
</urlset>`, r.Host)
	})
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html><body>content</body></html>")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))
	q := &mockQueue{}

	deps := &strategy.Deps{
		Queue:   q,
		Storage: &mockStorage{},
		State:   newMockState(),
		Limiter: newMockLimiter(),
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/page",
		CrawlId:   "crawl1",
		ProjectId: "proj1",
	}

	err := s.ProcessURL(context.Background(), event, deps)
	if err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Check that the sitemap URL was enqueued.
	events := q.getEvents()
	found := false
	for _, e := range events {
		if e.Url == ts.URL+"/from-sitemap" {
			found = true
			if e.Depth != 0 {
				t.Errorf("sitemap URL depth = %d, want 0", e.Depth)
			}
		}
	}
	if !found {
		t.Error("expected URL from sitemap to be enqueued")
		for _, e := range events {
			t.Logf("  enqueued: %s", e.Url)
		}
	}
}

func TestProcessURL_SitemapOnlyOnce(t *testing.T) {
	sitemapFetches := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "User-agent: *\nDisallow:\nSitemap: %s/sitemap.xml\n", "http://"+r.Host)
	})
	mux.HandleFunc("/sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		sitemapFetches++
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://`+r.Host+`/sm-page</loc></url>
</urlset>`)
	})
	mux.HandleFunc("/page1", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html><body>page1</body></html>")
	})
	mux.HandleFunc("/page2", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html><body>page2</body></html>")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("TestBot"))

	deps := &strategy.Deps{
		Queue:   &mockQueue{},
		Storage: &mockStorage{},
		State:   newMockState(),
		Limiter: newMockLimiter(),
	}

	// Process two URLs from same domain and crawl.
	for _, path := range []string{"/page1", "/page2"} {
		err := s.ProcessURL(context.Background(), &crawlerv1.URLFrontierEvent{
			Url:     ts.URL + path,
			CrawlId: "crawl1",
		}, deps)
		if err != nil {
			t.Fatalf("ProcessURL(%s): %v", path, err)
		}
	}

	// Sitemap should only be fetched once.
	if sitemapFetches != 1 {
		t.Errorf("sitemap fetched %d times, expected 1", sitemapFetches)
	}
}

func TestProcessURL_UserAgentHeader(t *testing.T) {
	var receivedUA string
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "User-agent: *\nDisallow:\n")
	})
	mux.HandleFunc("/page", func(w http.ResponseWriter, r *http.Request) {
		receivedUA = r.Header.Get("User-Agent")
		fmt.Fprint(w, "<html><body>hello</body></html>")
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	s := New(WithUserAgent("MyCustomBot/2.0"))

	deps := &strategy.Deps{
		Queue:   &mockQueue{},
		Storage: &mockStorage{},
		State:   newMockState(),
		Limiter: newMockLimiter(),
	}

	err := s.ProcessURL(context.Background(), &crawlerv1.URLFrontierEvent{
		Url:     ts.URL + "/page",
		CrawlId: "crawl1",
	}, deps)
	if err != nil {
		t.Fatal(err)
	}

	if receivedUA != "MyCustomBot/2.0" {
		t.Errorf("User-Agent = %q, want %q", receivedUA, "MyCustomBot/2.0")
	}
}

func TestStrategy_ImplementsInterface(t *testing.T) {
	var _ strategy.Strategy = (*Strategy)(nil)
}
