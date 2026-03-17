package wiki

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/strategy"
)

const testDump = `<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/">
  <page>
    <title>Test Article</title>
    <ns>0</ns>
    <id>1</id>
    <revision>
      <text>'''Test Article''' is about [[something]]. It has {{a template}} and a &amp; symbol.</text>
    </revision>
  </page>
  <page>
    <title>Redirect Page</title>
    <ns>0</ns>
    <id>2</id>
    <redirect title="Test Article" />
    <revision>
      <text>#REDIRECT [[Test Article]]</text>
    </revision>
  </page>
  <page>
    <title>Talk:Test Article</title>
    <ns>1</ns>
    <id>3</id>
    <revision>
      <text>This is a talk page and should be skipped.</text>
    </revision>
  </page>
</mediawiki>`

// mockStorage captures pages written via WritePage.
type mockStorage struct {
	mu    sync.Mutex
	pages []*crawlerv1.Page
}

func (m *mockStorage) WritePage(_ context.Context, page *crawlerv1.Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pages = append(m.pages, page)
	return nil
}

func (m *mockStorage) ListPages(_ context.Context, _ string, _ int, _ int) ([]*crawlerv1.Page, error) {
	return nil, nil
}

func (m *mockStorage) GetPage(_ context.Context, _ string, _ string) (*crawlerv1.Page, error) {
	return nil, nil
}

func (m *mockStorage) Export(_ context.Context, _ string, _ io.Writer) error {
	return nil
}

func (m *mockStorage) Close() error {
	return nil
}

func TestProcessURL(t *testing.T) {
	// Write the test dump to a temp file.
	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "dump.xml")
	if err := os.WriteFile(dumpPath, []byte(testDump), 0644); err != nil {
		t.Fatalf("write temp dump: %v", err)
	}

	store := &mockStorage{}
	deps := &strategy.Deps{
		Storage: store,
	}

	event := &crawlerv1.URLFrontierEvent{
		Url:       dumpPath,
		ProjectId: "proj-1",
		CrawlId:   "crawl-1",
	}

	s := New()

	if s.Name() != "wiki" {
		t.Fatalf("expected name 'wiki', got %q", s.Name())
	}

	if err := s.ProcessURL(context.Background(), event, deps); err != nil {
		t.Fatalf("ProcessURL: %v", err)
	}

	// Only 1 article should be stored (redirect and ns=1 skipped).
	if len(store.pages) != 1 {
		t.Fatalf("expected 1 page stored, got %d", len(store.pages))
	}

	page := store.pages[0]

	// Verify title.
	if page.Title != "Test Article" {
		t.Errorf("expected title 'Test Article', got %q", page.Title)
	}

	// Verify URL.
	expectedURL := "https://en.wikipedia.org/wiki/Test%20Article"
	if page.Url != expectedURL {
		t.Errorf("expected URL %q, got %q", expectedURL, page.Url)
	}

	// Verify project and crawl IDs.
	if page.ProjectId != "proj-1" {
		t.Errorf("expected project_id 'proj-1', got %q", page.ProjectId)
	}
	if page.CrawlId != "crawl-1" {
		t.Errorf("expected crawl_id 'crawl-1', got %q", page.CrawlId)
	}

	// Verify depth.
	if page.Depth != 0 {
		t.Errorf("expected depth 0, got %d", page.Depth)
	}

	// Verify content hash is non-empty.
	if page.ContentHash == "" {
		t.Error("expected non-empty content hash")
	}

	// Verify text is stripped (no wikitext artifacts).
	if page.Text == "" {
		t.Error("expected non-empty text")
	}
	if containsAny(page.Text, "'''", "{{", "}}") {
		t.Errorf("expected wikitext stripped from text, got %q", page.Text)
	}

	// Verify the & was decoded from &amp;.
	if !containsAny(page.Text, "&") {
		t.Errorf("expected decoded &amp; in text, got %q", page.Text)
	}

	// Verify CrawledAt is set.
	if page.CrawledAt == nil {
		t.Error("expected CrawledAt to be set")
	}
}

func TestProcessURLContextCancellation(t *testing.T) {
	dir := t.TempDir()
	dumpPath := filepath.Join(dir, "dump.xml")
	if err := os.WriteFile(dumpPath, []byte(testDump), 0644); err != nil {
		t.Fatalf("write temp dump: %v", err)
	}

	store := &mockStorage{}
	deps := &strategy.Deps{
		Storage: store,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	event := &crawlerv1.URLFrontierEvent{
		Url:       dumpPath,
		ProjectId: "proj-1",
		CrawlId:   "crawl-1",
	}

	s := New()
	err := s.ProcessURL(ctx, event, deps)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
