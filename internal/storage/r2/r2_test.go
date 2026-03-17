package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestObjectKey(t *testing.T) {
	url := "https://example.com/page"
	key := ObjectKey("proj-1", "crawl-1", url)

	h := sha256.Sum256([]byte(url))
	expected := fmt.Sprintf("proj-1/crawl-1/%x.json", h)
	if key != expected {
		t.Errorf("ObjectKey: got %q, want %q", key, expected)
	}
}

func TestObjectKeyDeterministic(t *testing.T) {
	key1 := ObjectKey("p", "c", "https://example.com")
	key2 := ObjectKey("p", "c", "https://example.com")
	if key1 != key2 {
		t.Error("ObjectKey is not deterministic")
	}
}

func TestObjectKeyDifferentURLs(t *testing.T) {
	key1 := ObjectKey("p", "c", "https://example.com/a")
	key2 := ObjectKey("p", "c", "https://example.com/b")
	if key1 == key2 {
		t.Error("different URLs should produce different keys")
	}
}

func TestPageSerialization(t *testing.T) {
	page := &crawlerv1.Page{
		Url:        "https://example.com/test",
		FinalUrl:   "https://example.com/test",
		StatusCode: 200,
		Title:      "Test Page",
		Text:       "Hello, world!",
		ProjectId:  "proj-1",
		CrawlId:    "crawl-1",
		CrawledAt:  timestamppb.Now(),
	}

	data, err := protojson.Marshal(page)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := &crawlerv1.Page{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Url != page.Url {
		t.Errorf("url: got %q, want %q", decoded.Url, page.Url)
	}
	if decoded.Title != page.Title {
		t.Errorf("title: got %q, want %q", decoded.Title, page.Title)
	}
	if decoded.StatusCode != page.StatusCode {
		t.Errorf("status_code: got %d, want %d", decoded.StatusCode, page.StatusCode)
	}
}

func TestCrawlPrefix(t *testing.T) {
	prefix := crawlPrefix("proj-1", "crawl-1")
	if prefix != "proj-1/crawl-1/" {
		t.Errorf("crawlPrefix: got %q, want %q", prefix, "proj-1/crawl-1/")
	}
}

func TestIntegrationR2Storage(t *testing.T) {
	endpoint := os.Getenv("R2_ENDPOINT")
	if endpoint == "" {
		t.Skip("R2_ENDPOINT not set, skipping integration test")
	}

	accessKey := os.Getenv("R2_ACCESS_KEY_ID")
	secretKey := os.Getenv("R2_SECRET_ACCESS_KEY")
	bucket := os.Getenv("R2_BUCKET")
	region := os.Getenv("R2_REGION")
	if region == "" {
		region = "auto"
	}

	s, err := New(endpoint, accessKey, secretKey, bucket, region)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	page := &crawlerv1.Page{
		Url:        "https://example.com/r2-test-" + t.Name(),
		FinalUrl:   "https://example.com/r2-test-" + t.Name(),
		StatusCode: 200,
		Title:      "R2 Test Page",
		Text:       "Integration test content",
		ProjectId:  "test-proj",
		CrawlId:    "test-crawl-" + t.Name(),
		CrawledAt:  timestamppb.Now(),
	}

	t.Run("WritePage", func(t *testing.T) {
		if err := s.WritePage(ctx, page); err != nil {
			t.Fatalf("WritePage: %v", err)
		}
	})

	t.Run("GetPage", func(t *testing.T) {
		got, err := s.GetPage(ctx, page.CrawlId, page.Url)
		if err != nil {
			t.Fatalf("GetPage: %v", err)
		}
		if got.Title != page.Title {
			t.Errorf("title: got %q, want %q", got.Title, page.Title)
		}
	})

	t.Run("ListPages", func(t *testing.T) {
		pages, err := s.ListPages(ctx, page.CrawlId, 10, 0)
		if err != nil {
			t.Fatalf("ListPages: %v", err)
		}
		if len(pages) == 0 {
			t.Error("expected at least one page")
		}
	})

	t.Run("Export", func(t *testing.T) {
		var buf bytes.Buffer
		if err := s.Export(ctx, page.CrawlId, &buf); err != nil {
			t.Fatalf("Export: %v", err)
		}
		if buf.Len() == 0 {
			t.Error("expected non-empty export")
		}
	})
}
