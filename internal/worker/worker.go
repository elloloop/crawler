// Package worker provides the goroutine pool that pulls URLs from the queue
// and delegates to the configured strategy.
package worker

import (
	"context"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/queue"
	"github.com/elloloop/crawler/internal/ratelimit"
	"github.com/elloloop/crawler/internal/state"
	"github.com/elloloop/crawler/internal/storage"
	"github.com/elloloop/crawler/internal/strategy"
)

// Pool manages a set of worker goroutines that process URLs from the queue.
type Pool struct {
	queue      queue.Queue
	storage    storage.Storage
	state      state.State
	limiter    ratelimit.Limiter
	strategies map[string]strategy.Strategy
	numWorkers int

	stats   Stats
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Stats tracks crawl progress. Fields are accessed atomically.
type Stats struct {
	PagesCrawled  atomic.Int64
	PagesFailed   atomic.Int64
	PagesSkipped  atomic.Int64
	URLsDiscovered atomic.Int64
	URLsQueued    atomic.Int64
	StartedAt     time.Time
}

// Snapshot returns a proto-compatible stats snapshot.
func (s *Stats) Snapshot() *crawlerv1.CrawlStats {
	elapsed := time.Since(s.StartedAt).Milliseconds()
	return &crawlerv1.CrawlStats{
		PagesCrawled:  s.PagesCrawled.Load(),
		PagesFailed:   s.PagesFailed.Load(),
		PagesSkipped:  s.PagesSkipped.Load(),
		UrlsDiscovered: s.URLsDiscovered.Load(),
		UrlsQueued:    s.URLsQueued.Load(),
		DurationMs:    elapsed,
	}
}

// New creates a worker pool.
func New(
	q queue.Queue,
	stor storage.Storage,
	st state.State,
	lim ratelimit.Limiter,
	strategies map[string]strategy.Strategy,
	numWorkers int,
) *Pool {
	return &Pool{
		queue:      q,
		storage:    stor,
		state:      st,
		limiter:    lim,
		strategies: strategies,
		numWorkers: numWorkers,
	}
}

// Start launches the worker goroutines. Returns immediately.
// Call Stop() to shut down gracefully.
func (p *Pool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	p.stats.StartedAt = time.Now()

	deps := &strategy.Deps{
		Queue:   p.queue,
		Storage: p.storage,
		State:   p.state,
		Limiter: p.limiter,
	}

	for i := range p.numWorkers {
		p.wg.Add(1)
		go p.worker(ctx, i, deps)
	}
	log.Printf("worker pool started: %d workers", p.numWorkers)
}

// Stop signals all workers to stop and waits for them to finish.
func (p *Pool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	log.Printf("worker pool stopped: %d pages crawled, %d failed",
		p.stats.PagesCrawled.Load(), p.stats.PagesFailed.Load())
}

// GetStats returns current crawl statistics.
func (p *Pool) GetStats() *Stats {
	return &p.stats
}

func (p *Pool) worker(ctx context.Context, id int, deps *strategy.Deps) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		event, err := p.queue.Pop(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker %d: pop error: %v", id, err)
			continue
		}

		// Determine strategy for this URL
		stratName := strategyName(event.Strategy)
		strat, ok := p.strategies[stratName]
		if !ok {
			log.Printf("worker %d: unknown strategy %q for %s, skipping", id, stratName, event.Url)
			p.stats.PagesSkipped.Add(1)
			continue
		}

		// Check dedup
		seen, err := p.state.HasSeen(ctx, event.CrawlId, event.Url)
		if err != nil {
			log.Printf("worker %d: dedup check error: %v", id, err)
		}
		if seen {
			p.stats.PagesSkipped.Add(1)
			continue
		}

		// Mark as seen before processing to prevent duplicates
		if err := p.state.MarkSeen(ctx, event.CrawlId, event.Url); err != nil {
			log.Printf("worker %d: mark seen error: %v", id, err)
		}

		// Rate limit
		domain := extractDomain(event.Url)
		if err := p.limiter.Acquire(ctx, domain); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker %d: rate limit error: %v", id, err)
			continue
		}

		// Process
		if err := strat.ProcessURL(ctx, event, deps); err != nil {
			log.Printf("worker %d: error processing %s: %v", id, event.Url, err)
			p.stats.PagesFailed.Add(1)
			continue
		}

		p.stats.PagesCrawled.Add(1)
		crawled := p.stats.PagesCrawled.Load()
		if crawled%100 == 0 {
			log.Printf("progress: %d pages crawled, %d failed, %d in queue",
				crawled, p.stats.PagesFailed.Load(), mustLen(ctx, p.queue))
		}
	}
}

func strategyName(s crawlerv1.CrawlStrategy) string {
	switch s {
	case crawlerv1.CrawlStrategy_CRAWL_STRATEGY_POLITE:
		return "polite"
	case crawlerv1.CrawlStrategy_CRAWL_STRATEGY_FULL:
		return "full"
	case crawlerv1.CrawlStrategy_CRAWL_STRATEGY_WIKI:
		return "wiki"
	default:
		return "polite"
	}
}

func extractDomain(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host
}

func mustLen(ctx context.Context, q queue.Queue) int64 {
	n, _ := q.Len(ctx)
	return n
}
