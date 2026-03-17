package queue

import (
	"context"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

// Queue is the URL frontier work queue.
type Queue interface {
	// Push adds a URL event to the queue.
	Push(ctx context.Context, event *crawlerv1.URLFrontierEvent) error

	// Pop retrieves the next URL event. Blocks until available or context is cancelled.
	Pop(ctx context.Context) (*crawlerv1.URLFrontierEvent, error)

	// Len returns the approximate number of items in the queue.
	Len(ctx context.Context) (int64, error)

	// Close releases queue resources.
	Close() error
}
