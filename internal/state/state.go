package state

import (
	"context"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

// State manages projects, crawl jobs, and URL deduplication.
type State interface {
	// Projects
	SaveProject(ctx context.Context, project *crawlerv1.Project) error
	GetProject(ctx context.Context, id string) (*crawlerv1.Project, error)
	ListProjects(ctx context.Context) ([]*crawlerv1.Project, error)
	DeleteProject(ctx context.Context, id string) error

	// Crawl jobs
	SaveCrawlJob(ctx context.Context, job *crawlerv1.CrawlJob) error
	GetCrawlJob(ctx context.Context, id string) (*crawlerv1.CrawlJob, error)
	ListCrawlJobs(ctx context.Context, projectID string) ([]*crawlerv1.CrawlJob, error)

	// URL deduplication
	HasSeen(ctx context.Context, crawlID string, url string) (bool, error)
	MarkSeen(ctx context.Context, crawlID string, url string) error

	// ETag/Last-Modified cache for conditional requests
	GetETag(ctx context.Context, url string) (etag string, lastModified string, err error)
	SetETag(ctx context.Context, url string, etag string, lastModified string) error

	// Close releases state resources.
	Close() error
}
