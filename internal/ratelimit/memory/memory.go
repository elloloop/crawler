// Package memory provides an in-process token bucket rate limiter.
package memory

import (
	"context"
	"sync"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/ratelimit"
)

type domainState struct {
	limit    *crawlerv1.DomainRateLimit
	lastTick time.Time
	mu       sync.Mutex
}

type memLimiter struct {
	mu       sync.RWMutex
	domains  map[string]*domainState
	defaults *crawlerv1.DomainRateLimit
}

// New creates an in-process rate limiter with default limits.
func New(defaultRPS float64) ratelimit.Limiter {
	return &memLimiter{
		domains: make(map[string]*domainState),
		defaults: &crawlerv1.DomainRateLimit{
			RequestsPerSecond: defaultRPS,
			MaxConcurrent:     1,
		},
	}
}

func (l *memLimiter) Acquire(ctx context.Context, domain string) error {
	ds := l.getOrCreate(domain)
	ds.mu.Lock()
	defer ds.mu.Unlock()

	rps := ds.limit.RequestsPerSecond
	if rps <= 0 {
		rps = l.defaults.RequestsPerSecond
	}
	if rps <= 0 {
		return nil
	}

	interval := time.Duration(float64(time.Second) / rps)
	now := time.Now()
	next := ds.lastTick.Add(interval)

	if now.Before(next) {
		wait := next.Sub(now)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	ds.lastTick = time.Now()
	return nil
}

func (l *memLimiter) SetLimit(domain string, limit *crawlerv1.DomainRateLimit) error {
	ds := l.getOrCreate(domain)
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.limit = limit
	return nil
}

func (l *memLimiter) GetLimit(domain string) (*crawlerv1.DomainRateLimit, error) {
	l.mu.RLock()
	ds, ok := l.domains[domain]
	l.mu.RUnlock()
	if !ok {
		return l.defaults, nil
	}
	return ds.limit, nil
}

func (l *memLimiter) ListLimits() ([]*crawlerv1.DomainRateLimit, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]*crawlerv1.DomainRateLimit, 0, len(l.domains))
	for _, ds := range l.domains {
		result = append(result, ds.limit)
	}
	return result, nil
}

func (l *memLimiter) ResetLimit(domain string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.domains, domain)
	return nil
}

func (l *memLimiter) Close() error {
	return nil
}

func (l *memLimiter) getOrCreate(domain string) *domainState {
	l.mu.RLock()
	ds, ok := l.domains[domain]
	l.mu.RUnlock()
	if ok {
		return ds
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	// Double-check after acquiring write lock.
	if ds, ok = l.domains[domain]; ok {
		return ds
	}
	ds = &domainState{
		limit: &crawlerv1.DomainRateLimit{
			Domain:            domain,
			RequestsPerSecond: l.defaults.RequestsPerSecond,
			MaxConcurrent:     l.defaults.MaxConcurrent,
		},
	}
	l.domains[domain] = ds
	return ds
}
