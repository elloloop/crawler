// Package server implements the gRPC service handlers for the crawler.
package server

import (
	"sync"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/queue"
	"github.com/elloloop/crawler/internal/ratelimit"
	"github.com/elloloop/crawler/internal/state"
	"github.com/elloloop/crawler/internal/storage"
	"github.com/elloloop/crawler/internal/strategy"
	"github.com/elloloop/crawler/internal/worker"
)

// Server implements all four gRPC service interfaces:
// ProjectService, CrawlService, RateLimitService, and RobotsService.
type Server struct {
	crawlerv1.UnimplementedProjectServiceServer
	crawlerv1.UnimplementedCrawlServiceServer
	crawlerv1.UnimplementedRateLimitServiceServer
	crawlerv1.UnimplementedRobotsServiceServer

	state      state.State
	storage    storage.Storage
	queue      queue.Queue
	limiter    ratelimit.Limiter
	strategies map[string]strategy.Strategy
	pool       *worker.Pool
	poolMu     sync.Mutex
}

// New creates a new Server with the given dependencies.
func New(
	st state.State,
	stor storage.Storage,
	q queue.Queue,
	lim ratelimit.Limiter,
	strats map[string]strategy.Strategy,
) *Server {
	return &Server{
		state:      st,
		storage:    stor,
		queue:      q,
		limiter:    lim,
		strategies: strats,
	}
}
