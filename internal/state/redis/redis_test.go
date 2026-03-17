package redis

import (
	"context"
	"os"
	"testing"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestKeyPatterns(t *testing.T) {
	tests := []struct {
		name string
		fn   func() string
		want string
	}{
		{"projectKey", func() string { return projectKey("abc") }, "project:abc"},
		{"projectsSetKey", func() string { return projectsSetKey() }, "projects"},
		{"crawlKey", func() string { return crawlKey("c1") }, "crawl:c1"},
		{"projectCrawlsKey", func() string { return projectCrawlsKey("p1") }, "project:p1:crawls"},
		{"seenKey", func() string { return seenKey("c1") }, "seen:c1"},
		{"etagKey", func() string { return etagKey("https://example.com") }, "etag:https://example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProjectSerialization(t *testing.T) {
	project := &crawlerv1.Project{
		Id:          "proj-1",
		Name:        "Test Project",
		Description: "A test project",
	}

	data, err := protojson.Marshal(project)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := &crawlerv1.Project{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Id != project.Id {
		t.Errorf("id: got %q, want %q", decoded.Id, project.Id)
	}
	if decoded.Name != project.Name {
		t.Errorf("name: got %q, want %q", decoded.Name, project.Name)
	}
}

func TestCrawlJobSerialization(t *testing.T) {
	job := &crawlerv1.CrawlJob{
		Id:        "crawl-1",
		ProjectId: "proj-1",
		Status:    crawlerv1.CrawlStatus_CRAWL_STATUS_RUNNING,
		Config: &crawlerv1.CrawlConfig{
			SeedUrls: []string{"https://example.com"},
			MaxDepth: 3,
		},
	}

	data, err := protojson.Marshal(job)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := &crawlerv1.CrawlJob{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Id != job.Id {
		t.Errorf("id: got %q, want %q", decoded.Id, job.Id)
	}
	if decoded.ProjectId != job.ProjectId {
		t.Errorf("project_id: got %q, want %q", decoded.ProjectId, job.ProjectId)
	}
	if decoded.Status != job.Status {
		t.Errorf("status: got %v, want %v", decoded.Status, job.Status)
	}
}

func TestIntegrationState(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping integration test")
	}
	password := os.Getenv("REDIS_PASSWORD")

	s, err := New(addr, password)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("ProjectCRUD", func(t *testing.T) {
		project := &crawlerv1.Project{
			Id:   "test-proj-" + t.Name(),
			Name: "Test",
		}

		if err := s.SaveProject(ctx, project); err != nil {
			t.Fatalf("SaveProject: %v", err)
		}

		got, err := s.GetProject(ctx, project.Id)
		if err != nil {
			t.Fatalf("GetProject: %v", err)
		}
		if got.Name != project.Name {
			t.Errorf("name: got %q, want %q", got.Name, project.Name)
		}

		projects, err := s.ListProjects(ctx)
		if err != nil {
			t.Fatalf("ListProjects: %v", err)
		}
		found := false
		for _, p := range projects {
			if p.Id == project.Id {
				found = true
				break
			}
		}
		if !found {
			t.Error("project not found in list")
		}

		if err := s.DeleteProject(ctx, project.Id); err != nil {
			t.Fatalf("DeleteProject: %v", err)
		}

		_, err = s.GetProject(ctx, project.Id)
		if err == nil {
			t.Error("expected error after delete, got nil")
		}
	})

	t.Run("CrawlJob", func(t *testing.T) {
		job := &crawlerv1.CrawlJob{
			Id:        "test-crawl-" + t.Name(),
			ProjectId: "test-proj-" + t.Name(),
			Status:    crawlerv1.CrawlStatus_CRAWL_STATUS_RUNNING,
		}

		if err := s.SaveCrawlJob(ctx, job); err != nil {
			t.Fatalf("SaveCrawlJob: %v", err)
		}

		got, err := s.GetCrawlJob(ctx, job.Id)
		if err != nil {
			t.Fatalf("GetCrawlJob: %v", err)
		}
		if got.Status != job.Status {
			t.Errorf("status: got %v, want %v", got.Status, job.Status)
		}

		jobs, err := s.ListCrawlJobs(ctx, job.ProjectId)
		if err != nil {
			t.Fatalf("ListCrawlJobs: %v", err)
		}
		if len(jobs) == 0 {
			t.Error("expected at least one crawl job")
		}
	})

	t.Run("SeenURLs", func(t *testing.T) {
		crawlID := "test-seen-" + t.Name()
		url := "https://example.com/page"

		seen, err := s.HasSeen(ctx, crawlID, url)
		if err != nil {
			t.Fatalf("HasSeen: %v", err)
		}
		if seen {
			t.Error("expected not seen")
		}

		if err := s.MarkSeen(ctx, crawlID, url); err != nil {
			t.Fatalf("MarkSeen: %v", err)
		}

		seen, err = s.HasSeen(ctx, crawlID, url)
		if err != nil {
			t.Fatalf("HasSeen after mark: %v", err)
		}
		if !seen {
			t.Error("expected seen after mark")
		}
	})

	t.Run("ETag", func(t *testing.T) {
		url := "https://example.com/etag-test-" + t.Name()

		etag, lm, err := s.GetETag(ctx, url)
		if err != nil {
			t.Fatalf("GetETag: %v", err)
		}
		if etag != "" || lm != "" {
			t.Errorf("expected empty etag/lm, got %q/%q", etag, lm)
		}

		if err := s.SetETag(ctx, url, "abc123", "Mon, 01 Jan 2024 00:00:00 GMT"); err != nil {
			t.Fatalf("SetETag: %v", err)
		}

		etag, lm, err = s.GetETag(ctx, url)
		if err != nil {
			t.Fatalf("GetETag after set: %v", err)
		}
		if etag != "abc123" {
			t.Errorf("etag: got %q, want %q", etag, "abc123")
		}
		if lm != "Mon, 01 Jan 2024 00:00:00 GMT" {
			t.Errorf("last_modified: got %q", lm)
		}
	})
}
