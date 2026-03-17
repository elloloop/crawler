// Package polite implements the polite crawl strategy that respects
// robots.txt rules, crawl-delay directives, conditional requests, and
// meta robots tags.
package polite

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/parser"
	"github.com/elloloop/crawler/internal/robots"
	"github.com/elloloop/crawler/internal/strategy"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultUserAgent     = "CrawlerBot/1.0"
	maxResponseBodyBytes = 10 * 1024 * 1024 // 10 MB
	maxSitemapURLs       = 10000
)

// Strategy implements the polite crawl strategy.
type Strategy struct {
	userAgent string
	client    *http.Client
	checker   *robots.Checker

	// seenDomains tracks domains whose sitemaps have already been processed
	// for a given crawl ID to avoid re-processing on every URL.
	mu          sync.Mutex
	seenDomains map[string]struct{}
}

// Option configures the polite strategy.
type Option func(*Strategy)

// WithUserAgent sets the User-Agent string used for HTTP requests.
func WithUserAgent(ua string) Option {
	return func(s *Strategy) {
		s.userAgent = ua
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(c *http.Client) Option {
	return func(s *Strategy) {
		s.client = c
	}
}

// New creates a new polite strategy with the given options.
func New(opts ...Option) *Strategy {
	s := &Strategy{
		userAgent:   defaultUserAgent,
		seenDomains: make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.client == nil {
		s.client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	s.checker = robots.NewChecker(s.userAgent)
	return s
}

// Name returns the strategy identifier.
func (s *Strategy) Name() string {
	return "polite"
}

// ProcessURL handles a single URL from the frontier queue.
func (s *Strategy) ProcessURL(ctx context.Context, event *crawlerv1.URLFrontierEvent, deps *strategy.Deps) error {
	rawURL := event.GetUrl()
	crawlID := event.GetCrawlId()
	projectID := event.GetProjectId()
	depth := event.GetDepth()

	logger := slog.With("url", rawURL, "crawl_id", crawlID, "depth", depth)

	// 1. Check robots.txt — skip if disallowed.
	allowed, err := s.checker.IsAllowed(ctx, rawURL, s.fetchFunc())
	if err != nil {
		logger.Warn("robots.txt check failed, allowing URL", "error", err)
		// On error checking robots.txt, we proceed optimistically.
		allowed = true
	}
	if !allowed {
		logger.Debug("URL disallowed by robots.txt")
		return nil
	}

	// 2. Update rate limiter with robots.txt crawl-delay.
	domain := domainFromURL(rawURL)
	if domain != "" {
		crawlDelay := s.checker.GetCrawlDelay(domain)
		if crawlDelay > 0 {
			rps := 1.0 / crawlDelay
			if err := deps.Limiter.SetLimit(domain, &crawlerv1.DomainRateLimit{
				Domain:            domain,
				RequestsPerSecond: rps,
			}); err != nil {
				logger.Warn("failed to set rate limit from crawl-delay", "error", err)
			}
		}

		// Acquire rate limiter slot.
		if err := deps.Limiter.Acquire(ctx, domain); err != nil {
			return fmt.Errorf("polite: rate limit acquire for %s: %w", domain, err)
		}
	}

	// 3. Fetch page with conditional requests (ETag/If-Modified-Since).
	etag, lastModified, _ := deps.State.GetETag(ctx, rawURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("polite: create request for %s: %w", rawURL, err)
	}
	req.Header.Set("User-Agent", s.userAgent)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified != "" {
		req.Header.Set("If-Modified-Since", lastModified)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("polite: fetch %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	// 4. On 304: skip processing (content not modified).
	if resp.StatusCode == http.StatusNotModified {
		logger.Debug("content not modified (304)")
		return nil
	}

	// Save ETag/Last-Modified for future conditional requests.
	newETag := resp.Header.Get("ETag")
	newLastModified := resp.Header.Get("Last-Modified")
	if newETag != "" || newLastModified != "" {
		_ = deps.State.SetETag(ctx, rawURL, newETag, newLastModified)
	}

	// Read body.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	if err != nil {
		return fmt.Errorf("polite: read body %s: %w", rawURL, err)
	}

	finalURL := resp.Request.URL.String()

	// Non-success status: do not process further.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Debug("non-success status", "status", resp.StatusCode)
		return nil
	}

	// 5. Parse HTML.
	result := parser.Parse(finalURL, body)

	// 6. Check meta robots.
	shouldIndex, shouldFollow := parser.CheckMetaRobots(result.MetaRobots)

	// 7. If indexable: store page.
	if shouldIndex {
		page := &crawlerv1.Page{
			Url:             rawURL,
			FinalUrl:        finalURL,
			StatusCode:      int32(resp.StatusCode),
			Title:           result.Title,
			Text:            result.Text,
			Links:           result.Links,
			MetaDescription: result.MetaDescription,
			CanonicalUrl:    result.CanonicalURL,
			ContentHash:     result.ContentHash,
			Depth:           depth,
			CrawledAt:       timestamppb.Now(),
			ProjectId:       projectID,
			CrawlId:         crawlID,
		}
		if err := deps.Storage.WritePage(ctx, page); err != nil {
			return fmt.Errorf("polite: store page %s: %w", rawURL, err)
		}
	}

	// 8. If followable: enqueue discovered links.
	if shouldFollow {
		for _, link := range result.Links {
			seen, err := deps.State.HasSeen(ctx, crawlID, link)
			if err != nil {
				logger.Warn("dedup check failed", "link", link, "error", err)
				continue
			}
			if seen {
				continue
			}
			if err := deps.State.MarkSeen(ctx, crawlID, link); err != nil {
				logger.Warn("mark seen failed", "link", link, "error", err)
				continue
			}
			if err := deps.Queue.Push(ctx, &crawlerv1.URLFrontierEvent{
				Url:            link,
				Depth:          depth + 1,
				ProjectId:      projectID,
				CrawlId:        crawlID,
				DiscoveredFrom: rawURL,
				Strategy:       crawlerv1.CrawlStrategy_CRAWL_STRATEGY_POLITE,
			}); err != nil {
				logger.Warn("enqueue failed", "link", link, "error", err)
			}
		}
	}

	// 9. On first domain visit for this crawl: parse sitemaps and enqueue discovered URLs.
	if domain != "" {
		domainKey := crawlID + ":" + domain
		s.mu.Lock()
		_, alreadySeen := s.seenDomains[domainKey]
		if !alreadySeen {
			s.seenDomains[domainKey] = struct{}{}
		}
		s.mu.Unlock()

		if !alreadySeen {
			s.processSitemaps(ctx, domain, rawURL, event, deps, logger)
		}
	}

	return nil
}

// processSitemaps fetches sitemaps listed in robots.txt and enqueues discovered URLs.
func (s *Strategy) processSitemaps(ctx context.Context, domain string, sourceURL string, event *crawlerv1.URLFrontierEvent, deps *strategy.Deps, logger *slog.Logger) {
	sitemapURLs := s.checker.GetSitemaps(domain)
	if len(sitemapURLs) == 0 {
		return
	}

	for _, smURL := range sitemapURLs {
		urls, err := robots.ParseSitemap(ctx, smURL, s.fetchFunc(), maxSitemapURLs)
		if err != nil {
			logger.Debug("sitemap parse failed", "sitemap", smURL, "error", err)
			continue
		}
		for _, u := range urls {
			seen, err := deps.State.HasSeen(ctx, event.GetCrawlId(), u)
			if err != nil || seen {
				continue
			}
			_ = deps.State.MarkSeen(ctx, event.GetCrawlId(), u)
			if err := deps.Queue.Push(ctx, &crawlerv1.URLFrontierEvent{
				Url:            u,
				Depth:          0,
				ProjectId:      event.GetProjectId(),
				CrawlId:        event.GetCrawlId(),
				DiscoveredFrom: "sitemap:" + smURL,
				Strategy:       crawlerv1.CrawlStrategy_CRAWL_STRATEGY_POLITE,
			}); err != nil {
				logger.Debug("sitemap enqueue failed", "url", u, "error", err)
			}
		}
	}
}

// fetchFunc returns a robots.FetchFunc that uses the strategy's HTTP client.
func (s *Strategy) fetchFunc() robots.FetchFunc {
	return func(ctx context.Context, rawURL string) ([]byte, int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("User-Agent", s.userAgent)
		resp, err := s.client.Do(req)
		if err != nil {
			return nil, 0, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
		if err != nil {
			return nil, resp.StatusCode, err
		}
		return body, resp.StatusCode, nil
	}
}

// domainFromURL extracts the host from a URL string. Returns "" on error.
func domainFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Host
}

// Ensure Strategy implements strategy.Strategy at compile time.
var _ strategy.Strategy = (*Strategy)(nil)
