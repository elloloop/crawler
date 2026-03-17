package redis

import (
	"context"
	"os"
	"testing"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestEventSerialization(t *testing.T) {
	event := &crawlerv1.URLFrontierEvent{
		Url:            "https://example.com/page",
		Depth:          2,
		ProjectId:      "proj-1",
		CrawlId:        "crawl-1",
		Priority:       5,
		RetryCount:     1,
		DiscoveredFrom: "https://example.com",
		Strategy:       crawlerv1.CrawlStrategy_CRAWL_STRATEGY_POLITE,
	}

	data, err := protojson.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := &crawlerv1.URLFrontierEvent{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Url != event.Url {
		t.Errorf("url: got %q, want %q", decoded.Url, event.Url)
	}
	if decoded.Depth != event.Depth {
		t.Errorf("depth: got %d, want %d", decoded.Depth, event.Depth)
	}
	if decoded.ProjectId != event.ProjectId {
		t.Errorf("project_id: got %q, want %q", decoded.ProjectId, event.ProjectId)
	}
	if decoded.CrawlId != event.CrawlId {
		t.Errorf("crawl_id: got %q, want %q", decoded.CrawlId, event.CrawlId)
	}
	if decoded.Priority != event.Priority {
		t.Errorf("priority: got %d, want %d", decoded.Priority, event.Priority)
	}
	if decoded.RetryCount != event.RetryCount {
		t.Errorf("retry_count: got %d, want %d", decoded.RetryCount, event.RetryCount)
	}
	if decoded.DiscoveredFrom != event.DiscoveredFrom {
		t.Errorf("discovered_from: got %q, want %q", decoded.DiscoveredFrom, event.DiscoveredFrom)
	}
	if decoded.Strategy != event.Strategy {
		t.Errorf("strategy: got %v, want %v", decoded.Strategy, event.Strategy)
	}
}

func TestIntegrationQueue(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping integration test")
	}
	password := os.Getenv("REDIS_PASSWORD")

	q, err := New(addr, password, "test:queue:"+t.Name(), "test-group", "test-consumer")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := &crawlerv1.URLFrontierEvent{
		Url:       "https://example.com/test",
		Depth:     1,
		ProjectId: "proj-test",
		CrawlId:   "crawl-test",
	}

	if err := q.Push(ctx, event); err != nil {
		t.Fatalf("Push: %v", err)
	}

	n, err := q.Len(ctx)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	if n < 1 {
		t.Errorf("Len: got %d, want >= 1", n)
	}

	got, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}

	if got.Url != event.Url {
		t.Errorf("Pop url: got %q, want %q", got.Url, event.Url)
	}
	if got.ProjectId != event.ProjectId {
		t.Errorf("Pop project_id: got %q, want %q", got.ProjectId, event.ProjectId)
	}
}
