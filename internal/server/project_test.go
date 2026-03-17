package server

import (
	"context"
	"path/filepath"
	"testing"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	memoryqueue "github.com/elloloop/crawler/internal/queue/memory"
	memorylimiter "github.com/elloloop/crawler/internal/ratelimit/memory"
	"github.com/elloloop/crawler/internal/state/sqlite"
	localstorage "github.com/elloloop/crawler/internal/storage/local"
	"github.com/elloloop/crawler/internal/strategy"
)

func setupTestServer(t *testing.T) *Server {
	t.Helper()
	tmpDir := t.TempDir()

	st, err := sqlite.New(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("create state: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	stor, err := localstorage.New(filepath.Join(tmpDir, "pages"))
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}
	t.Cleanup(func() { stor.Close() })

	q := memoryqueue.New(1000)
	lim := memorylimiter.New(1.0)
	strats := make(map[string]strategy.Strategy)

	return New(st, stor, q, lim, strats)
}

func TestCreateProject(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	resp, err := srv.CreateProject(ctx, &crawlerv1.CreateProjectRequest{
		Name:        "Test Project",
		Description: "A test project",
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if resp.Project == nil {
		t.Fatal("expected project in response")
	}
	if resp.Project.Id == "" {
		t.Error("expected non-empty project ID")
	}
	if resp.Project.Name != "Test Project" {
		t.Errorf("got name %q, want %q", resp.Project.Name, "Test Project")
	}
	if resp.Project.Description != "A test project" {
		t.Errorf("got description %q, want %q", resp.Project.Description, "A test project")
	}
	if resp.Project.CreatedAt == nil {
		t.Error("expected created_at to be set")
	}
	if resp.Project.UpdatedAt == nil {
		t.Error("expected updated_at to be set")
	}
}

func TestGetProject(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	created, err := srv.CreateProject(ctx, &crawlerv1.CreateProjectRequest{
		Name: "Get Test",
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	got, err := srv.GetProject(ctx, &crawlerv1.GetProjectRequest{
		Id: created.Project.Id,
	})
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if got.Project.Name != "Get Test" {
		t.Errorf("got name %q, want %q", got.Project.Name, "Get Test")
	}
}

func TestGetProjectNotFound(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	_, err := srv.GetProject(ctx, &crawlerv1.GetProjectRequest{
		Id: "nonexistent-id",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent project")
	}
}

func TestListProjects(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	// Empty list
	resp, err := srv.ListProjects(ctx, &crawlerv1.ListProjectsRequest{})
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(resp.Projects) != 0 {
		t.Errorf("expected 0 projects, got %d", len(resp.Projects))
	}

	// Create some projects
	for i := range 3 {
		_, err := srv.CreateProject(ctx, &crawlerv1.CreateProjectRequest{
			Name: "Project " + string(rune('A'+i)),
		})
		if err != nil {
			t.Fatalf("CreateProject: %v", err)
		}
	}

	resp, err = srv.ListProjects(ctx, &crawlerv1.ListProjectsRequest{})
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(resp.Projects) != 3 {
		t.Errorf("expected 3 projects, got %d", len(resp.Projects))
	}
}

func TestUpdateProject(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	created, err := srv.CreateProject(ctx, &crawlerv1.CreateProjectRequest{
		Name:        "Original Name",
		Description: "Original Description",
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	updated, err := srv.UpdateProject(ctx, &crawlerv1.UpdateProjectRequest{
		Id:          created.Project.Id,
		Name:        "Updated Name",
		Description: "Updated Description",
	})
	if err != nil {
		t.Fatalf("UpdateProject: %v", err)
	}
	if updated.Project.Name != "Updated Name" {
		t.Errorf("got name %q, want %q", updated.Project.Name, "Updated Name")
	}
	if updated.Project.Description != "Updated Description" {
		t.Errorf("got description %q, want %q", updated.Project.Description, "Updated Description")
	}

	// Verify via GetProject
	got, err := srv.GetProject(ctx, &crawlerv1.GetProjectRequest{
		Id: created.Project.Id,
	})
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if got.Project.Name != "Updated Name" {
		t.Errorf("got name %q, want %q", got.Project.Name, "Updated Name")
	}
}

func TestUpdateProjectNotFound(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	_, err := srv.UpdateProject(ctx, &crawlerv1.UpdateProjectRequest{
		Id:   "nonexistent-id",
		Name: "Test",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent project")
	}
}

func TestDeleteProject(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	created, err := srv.CreateProject(ctx, &crawlerv1.CreateProjectRequest{
		Name: "To Delete",
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	_, err = srv.DeleteProject(ctx, &crawlerv1.DeleteProjectRequest{
		Id: created.Project.Id,
	})
	if err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}

	// Verify project is gone
	_, err = srv.GetProject(ctx, &crawlerv1.GetProjectRequest{
		Id: created.Project.Id,
	})
	if err == nil {
		t.Fatal("expected error after deleting project")
	}
}

func TestCreateProjectWithConfig(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()

	resp, err := srv.CreateProject(ctx, &crawlerv1.CreateProjectRequest{
		Name: "Config Project",
		DefaultConfig: &crawlerv1.CrawlConfig{
			SeedUrls:    []string{"https://example.com"},
			MaxDepth:    3,
			Concurrency: 2,
		},
		SiteConfigs: []*crawlerv1.SiteConfig{
			{
				Domain:   "example.com",
				Strategy: crawlerv1.CrawlStrategy_CRAWL_STRATEGY_POLITE,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	got, err := srv.GetProject(ctx, &crawlerv1.GetProjectRequest{
		Id: resp.Project.Id,
	})
	if err != nil {
		t.Fatalf("GetProject: %v", err)
	}
	if got.Project.DefaultConfig == nil {
		t.Fatal("expected default config")
	}
	if len(got.Project.DefaultConfig.SeedUrls) != 1 {
		t.Errorf("expected 1 seed URL, got %d", len(got.Project.DefaultConfig.SeedUrls))
	}
	if len(got.Project.SiteConfigs) != 1 {
		t.Errorf("expected 1 site config, got %d", len(got.Project.SiteConfigs))
	}
}
