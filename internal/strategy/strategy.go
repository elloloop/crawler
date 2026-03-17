package strategy

import (
	"context"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/queue"
	"github.com/elloloop/crawler/internal/ratelimit"
	"github.com/elloloop/crawler/internal/state"
	"github.com/elloloop/crawler/internal/storage"
)

// Deps bundles the infrastructure dependencies a strategy needs.
type Deps struct {
	Queue   queue.Queue
	Storage storage.Storage
	State   state.State
	Limiter ratelimit.Limiter
}

// Strategy defines a crawl execution mode.
type Strategy interface {
	// Name returns the strategy identifier (e.g. "polite", "full", "wiki").
	Name() string

	// ProcessURL handles a single URL from the frontier queue.
	// It should fetch, parse, store the page, and enqueue discovered URLs.
	ProcessURL(ctx context.Context, event *crawlerv1.URLFrontierEvent, deps *Deps) error
}
