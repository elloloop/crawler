package ratelimit

import (
	"context"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

// Limiter controls per-domain request rates.
type Limiter interface {
	// Acquire blocks until a request to the domain is allowed, or context is cancelled.
	Acquire(ctx context.Context, domain string) error

	// SetLimit configures the rate limit for a domain.
	SetLimit(domain string, limit *crawlerv1.DomainRateLimit) error

	// GetLimit returns the current rate limit for a domain.
	GetLimit(domain string) (*crawlerv1.DomainRateLimit, error)

	// ListLimits returns all configured rate limits.
	ListLimits() ([]*crawlerv1.DomainRateLimit, error)

	// ResetLimit removes a domain-specific limit, reverting to defaults.
	ResetLimit(domain string) error

	// Close releases limiter resources.
	Close() error
}
