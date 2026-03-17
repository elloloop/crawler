// Package sqlite provides a SQLite-backed State implementation.
// Uses modernc.org/sqlite (pure Go, no CGO required).
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/state"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type sqliteState struct {
	db *sql.DB
}

// New opens (or creates) a SQLite database at the given path.
func New(dbPath string) (state.State, error) {
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	s := &sqliteState{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *sqliteState) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS projects (
			id TEXT PRIMARY KEY,
			data TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS crawl_jobs (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			data TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_crawl_jobs_project ON crawl_jobs(project_id)`,
		`CREATE TABLE IF NOT EXISTS seen_urls (
			crawl_id TEXT NOT NULL,
			url TEXT NOT NULL,
			PRIMARY KEY (crawl_id, url)
		)`,
		`CREATE TABLE IF NOT EXISTS etags (
			url TEXT PRIMARY KEY,
			etag TEXT NOT NULL DEFAULT '',
			last_modified TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt[:40], err)
		}
	}
	return nil
}

// --- Projects ---

func (s *sqliteState) SaveProject(_ context.Context, project *crawlerv1.Project) error {
	data, err := protojson.Marshal(project)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = s.db.Exec(
		`INSERT INTO projects (id, data, created_at, updated_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at`,
		project.Id, string(data), now, now,
	)
	return err
}

func (s *sqliteState) GetProject(_ context.Context, id string) (*crawlerv1.Project, error) {
	var data string
	err := s.db.QueryRow(`SELECT data FROM projects WHERE id = ?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("project not found: %s", id)
	}
	if err != nil {
		return nil, err
	}
	p := &crawlerv1.Project{}
	if err := protojson.Unmarshal([]byte(data), p); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *sqliteState) ListProjects(_ context.Context) ([]*crawlerv1.Project, error) {
	rows, err := s.db.Query(`SELECT data FROM projects ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []*crawlerv1.Project
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		p := &crawlerv1.Project{}
		if err := protojson.Unmarshal([]byte(data), p); err != nil {
			continue
		}
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

func (s *sqliteState) DeleteProject(_ context.Context, id string) error {
	_, err := s.db.Exec(`DELETE FROM projects WHERE id = ?`, id)
	return err
}

// --- Crawl Jobs ---

func (s *sqliteState) SaveCrawlJob(_ context.Context, job *crawlerv1.CrawlJob) error {
	data, err := protojson.Marshal(job)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = s.db.Exec(
		`INSERT INTO crawl_jobs (id, project_id, data, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET data=excluded.data`,
		job.Id, job.ProjectId, string(data), now,
	)
	return err
}

func (s *sqliteState) GetCrawlJob(_ context.Context, id string) (*crawlerv1.CrawlJob, error) {
	var data string
	err := s.db.QueryRow(`SELECT data FROM crawl_jobs WHERE id = ?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("crawl job not found: %s", id)
	}
	if err != nil {
		return nil, err
	}
	job := &crawlerv1.CrawlJob{}
	if err := protojson.Unmarshal([]byte(data), job); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *sqliteState) ListCrawlJobs(_ context.Context, projectID string) ([]*crawlerv1.CrawlJob, error) {
	rows, err := s.db.Query(`SELECT data FROM crawl_jobs WHERE project_id = ? ORDER BY created_at DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*crawlerv1.CrawlJob
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		job := &crawlerv1.CrawlJob{}
		if err := protojson.Unmarshal([]byte(data), job); err != nil {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// --- URL Dedup ---

func (s *sqliteState) HasSeen(_ context.Context, crawlID, url string) (bool, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM seen_urls WHERE crawl_id = ? AND url = ?`, crawlID, url).Scan(&count)
	return count > 0, err
}

func (s *sqliteState) MarkSeen(_ context.Context, crawlID, url string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO seen_urls (crawl_id, url) VALUES (?, ?)`, crawlID, url)
	return err
}

// --- ETag Cache ---

func (s *sqliteState) GetETag(_ context.Context, url string) (string, string, error) {
	var etag, lastMod string
	err := s.db.QueryRow(`SELECT etag, last_modified FROM etags WHERE url = ?`, url).Scan(&etag, &lastMod)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	return etag, lastMod, err
}

func (s *sqliteState) SetETag(_ context.Context, url, etag, lastModified string) error {
	_, err := s.db.Exec(
		`INSERT INTO etags (url, etag, last_modified) VALUES (?, ?, ?)
		 ON CONFLICT(url) DO UPDATE SET etag=excluded.etag, last_modified=excluded.last_modified`,
		url, etag, lastModified,
	)
	return err
}

func (s *sqliteState) Close() error {
	return s.db.Close()
}

// nowTimestamp returns a protobuf timestamp for the current time.
var _ = nowTimestamp

func nowTimestamp() *timestamppb.Timestamp {
	return timestamppb.Now()
}
