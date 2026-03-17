package storage

import (
	"context"
	"io"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

// Storage persists crawled page data.
type Storage interface {
	// WritePage stores a crawled page.
	WritePage(ctx context.Context, page *crawlerv1.Page) error

	// ListPages returns pages for a crawl job with pagination.
	ListPages(ctx context.Context, crawlID string, limit int, offset int) ([]*crawlerv1.Page, error)

	// GetPage retrieves a single page by crawl ID and URL.
	GetPage(ctx context.Context, crawlID string, url string) (*crawlerv1.Page, error)

	// Export writes all pages for a crawl as JSONL to the writer.
	Export(ctx context.Context, crawlID string, w io.Writer) error

	// Close releases storage resources.
	Close() error
}
