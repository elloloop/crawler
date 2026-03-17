package full

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	memqueue "github.com/elloloop/crawler/internal/queue/memory"
	memlimit "github.com/elloloop/crawler/internal/ratelimit/memory"
	"github.com/elloloop/crawler/internal/state/sqlite"
	"github.com/elloloop/crawler/internal/storage/local"
	"github.com/elloloop/crawler/internal/strategy"
)

func TestProcessURL(t *testing.T) {
	// Set up test HTTP server with root, /page1, and /page2.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>Root Page</title></head>
<body>
<h1>Hello World</h1>
<a href="/page1">Page 1</a>
<a href="/page2">Page 2</a>
</body>
</html>`))
	})
	mux.HandleFunc("/page1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>Page One</title></head>
<body><p>Content of page 1</p></body>
</html>`))
	})
	mux.HandleFunc("/page2", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<!DOCTYPE html>
<html>
<head><title>Page Two</title></head>
<body><p>Content of page 2</p></body>
</html>`))
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Create temp dir for storage and state.
	tmpDir := t.TempDir()

	// Set up real dependencies.
	q := memqueue.New(1000)
	defer q.Close()

	store, err := local.New(filepath.Join(tmpDir, "storage"))
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	defer store.Close()

	st, err := sqlite.New(filepath.Join(tmpDir, "state.db"))
	if err != nil {
		t.Fatalf("create sqlite state: %v", err)
	}
	defer st.Close()

	limiter := memlimit.New(100) // high RPS for tests
	defer limiter.Close()

	deps := &strategy.Deps{
		Queue:   q,
		Storage: store,
		State:   st,
		Limiter: limiter,
	}

	// Create strategy and process root URL.
	s := New("test-crawler/1.0")

	if s.Name() != "full" {
		t.Errorf("Name() = %q, want %q", s.Name(), "full")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rootEvent := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/",
		Depth:     0,
		ProjectId: "proj-1",
		CrawlId:   "crawl-1",
		Strategy:  crawlerv1.CrawlStrategy_CRAWL_STRATEGY_FULL,
	}

	if err := s.ProcessURL(ctx, rootEvent, deps); err != nil {
		t.Fatalf("ProcessURL root: %v", err)
	}

	// Verify the page was stored.
	page, err := store.GetPage(ctx, "crawl-1", ts.URL+"/")
	if err != nil {
		t.Fatalf("GetPage root: %v", err)
	}
	if page.Title != "Root Page" {
		t.Errorf("page.Title = %q, want %q", page.Title, "Root Page")
	}
	if page.StatusCode != 200 {
		t.Errorf("page.StatusCode = %d, want 200", page.StatusCode)
	}
	if page.Depth != 0 {
		t.Errorf("page.Depth = %d, want 0", page.Depth)
	}
	if page.ProjectId != "proj-1" {
		t.Errorf("page.ProjectId = %q, want %q", page.ProjectId, "proj-1")
	}
	if page.CrawlId != "crawl-1" {
		t.Errorf("page.CrawlId = %q, want %q", page.CrawlId, "crawl-1")
	}
	if page.ContentHash == "" {
		t.Error("page.ContentHash is empty")
	}

	// Verify links were found in the page.
	if len(page.Links) != 2 {
		t.Fatalf("page.Links has %d entries, want 2", len(page.Links))
	}

	// Verify the two links were enqueued.
	qLen, err := q.Len(ctx)
	if err != nil {
		t.Fatalf("queue Len: %v", err)
	}
	if qLen != 2 {
		t.Fatalf("queue length = %d, want 2", qLen)
	}

	// Pop both events and verify them.
	seen := make(map[string]bool)
	for i := 0; i < 2; i++ {
		popCtx, popCancel := context.WithTimeout(ctx, 2*time.Second)
		evt, err := q.Pop(popCtx)
		popCancel()
		if err != nil {
			t.Fatalf("Pop %d: %v", i, err)
		}
		seen[evt.Url] = true
		if evt.Depth != 1 {
			t.Errorf("enqueued event depth = %d, want 1", evt.Depth)
		}
		if evt.ProjectId != "proj-1" {
			t.Errorf("enqueued event ProjectId = %q, want %q", evt.ProjectId, "proj-1")
		}
		if evt.CrawlId != "crawl-1" {
			t.Errorf("enqueued event CrawlId = %q, want %q", evt.CrawlId, "crawl-1")
		}
		if evt.Strategy != crawlerv1.CrawlStrategy_CRAWL_STRATEGY_FULL {
			t.Errorf("enqueued event Strategy = %v, want CRAWL_STRATEGY_FULL", evt.Strategy)
		}
		if evt.DiscoveredFrom != ts.URL+"/" {
			t.Errorf("enqueued event DiscoveredFrom = %q, want %q", evt.DiscoveredFrom, ts.URL+"/")
		}
	}
	if !seen[ts.URL+"/page1"] {
		t.Error("expected enqueued URL for /page1")
	}
	if !seen[ts.URL+"/page2"] {
		t.Error("expected enqueued URL for /page2")
	}
}

func TestProcessURL_304NotModified(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == `"abc123"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"abc123"`)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<html><head><title>Test</title></head><body>Hello</body></html>`))
	}))
	defer ts.Close()

	tmpDir := t.TempDir()

	q := memqueue.New(1000)
	defer q.Close()
	store, _ := local.New(filepath.Join(tmpDir, "storage"))
	defer store.Close()
	st, _ := sqlite.New(filepath.Join(tmpDir, "state.db"))
	defer st.Close()
	limiter := memlimit.New(100)
	defer limiter.Close()

	deps := &strategy.Deps{Queue: q, Storage: store, State: st, Limiter: limiter}
	s := New("test-crawler/1.0")

	ctx := context.Background()
	event := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/",
		Depth:     0,
		ProjectId: "proj-1",
		CrawlId:   "crawl-1",
		Strategy:  crawlerv1.CrawlStrategy_CRAWL_STRATEGY_FULL,
	}

	// First request: should fetch and store ETag.
	if err := s.ProcessURL(ctx, event, deps); err != nil {
		t.Fatalf("first ProcessURL: %v", err)
	}

	// Verify ETag was saved.
	etag, _, err := st.GetETag(ctx, ts.URL+"/")
	if err != nil {
		t.Fatalf("GetETag: %v", err)
	}
	if etag != `"abc123"` {
		t.Errorf("stored ETag = %q, want %q", etag, `"abc123"`)
	}

	// Second request: should get 304 and return nil with no additional page stored.
	if err := s.ProcessURL(ctx, event, deps); err != nil {
		t.Fatalf("second ProcessURL: %v", err)
	}

	// Only one page should be stored (from the first request).
	pages, err := store.ListPages(ctx, "crawl-1", 100, 0)
	if err != nil {
		t.Fatalf("ListPages: %v", err)
	}
	if len(pages) != 1 {
		t.Errorf("stored pages = %d, want 1", len(pages))
	}
}

func TestProcessURL_NonOKStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	tmpDir := t.TempDir()

	q := memqueue.New(1000)
	defer q.Close()
	store, _ := local.New(filepath.Join(tmpDir, "storage"))
	defer store.Close()
	st, _ := sqlite.New(filepath.Join(tmpDir, "state.db"))
	defer st.Close()
	limiter := memlimit.New(100)
	defer limiter.Close()

	deps := &strategy.Deps{Queue: q, Storage: store, State: st, Limiter: limiter}
	s := New("test-crawler/1.0")

	ctx := context.Background()
	event := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/",
		Depth:     0,
		ProjectId: "proj-1",
		CrawlId:   "crawl-1",
		Strategy:  crawlerv1.CrawlStrategy_CRAWL_STRATEGY_FULL,
	}

	err := s.ProcessURL(ctx, event, deps)
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
}

func TestProcessURL_UserAgent(t *testing.T) {
	var receivedUA string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<html><body>ok</body></html>`))
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	q := memqueue.New(1000)
	defer q.Close()
	store, _ := local.New(filepath.Join(tmpDir, "storage"))
	defer store.Close()
	st, _ := sqlite.New(filepath.Join(tmpDir, "state.db"))
	defer st.Close()
	limiter := memlimit.New(100)
	defer limiter.Close()

	deps := &strategy.Deps{Queue: q, Storage: store, State: st, Limiter: limiter}
	s := New("my-custom-agent/2.0")

	ctx := context.Background()
	event := &crawlerv1.URLFrontierEvent{
		Url:       ts.URL + "/",
		ProjectId: "proj-1",
		CrawlId:   "crawl-1",
	}

	if err := s.ProcessURL(ctx, event, deps); err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}
	if receivedUA != "my-custom-agent/2.0" {
		t.Errorf("User-Agent = %q, want %q", receivedUA, "my-custom-agent/2.0")
	}
}
