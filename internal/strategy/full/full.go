// Package full implements the unrestricted crawl strategy.
// It ignores robots.txt and crawls all discovered URLs without domain restrictions.
package full

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/parser"
	"github.com/elloloop/crawler/internal/strategy"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxBodySize = 10 << 20 // 10 MB

// userAgentTransport is an http.RoundTripper that sets the User-Agent header.
type userAgentTransport struct {
	base      http.RoundTripper
	userAgent string
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

// Strategy implements strategy.Strategy for unrestricted (full) crawling.
type Strategy struct {
	userAgent string
	client    *http.Client
}

// New creates a new full crawl strategy with the given user agent string.
func New(userAgent string) *Strategy {
	s := &Strategy{
		userAgent: userAgent,
	}
	s.client = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &userAgentTransport{
			base:      http.DefaultTransport,
			userAgent: userAgent,
		},
	}
	return s
}

// Name returns the strategy identifier.
func (s *Strategy) Name() string {
	return "full"
}

// ProcessURL fetches, parses, stores, and enqueues links for a single URL.
// It performs no robots.txt checking.
func (s *Strategy) ProcessURL(ctx context.Context, event *crawlerv1.URLFrontierEvent, deps *strategy.Deps) error {
	// 1. Check ETag cache for conditional request headers.
	etag, lastMod, err := deps.State.GetETag(ctx, event.Url)
	if err != nil {
		return fmt.Errorf("get etag: %w", err)
	}

	// 2. Build the HTTP request.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, event.Url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastMod != "" {
		req.Header.Set("If-Modified-Since", lastMod)
	}

	// 3. Perform the fetch.
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", event.Url, err)
	}
	defer resp.Body.Close()

	// 4. Handle 304 Not Modified — content unchanged.
	if resp.StatusCode == http.StatusNotModified {
		return nil
	}

	// 5. Non-200 is an error.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d for %s", resp.StatusCode, event.Url)
	}

	// 6. Read body (limit to 10 MB).
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	// 7. Save new ETag / Last-Modified to state.
	newETag := resp.Header.Get("ETag")
	newLastMod := resp.Header.Get("Last-Modified")
	if newETag != "" || newLastMod != "" {
		if err := deps.State.SetETag(ctx, event.Url, newETag, newLastMod); err != nil {
			return fmt.Errorf("set etag: %w", err)
		}
	}

	// 8. Parse HTML.
	result := parser.Parse(event.Url, body)

	// 9. Determine the final URL after redirects.
	finalURL := resp.Request.URL.String()

	// 10. Build Page proto.
	page := &crawlerv1.Page{
		Url:             event.Url,
		FinalUrl:        finalURL,
		StatusCode:      int32(resp.StatusCode),
		Title:           result.Title,
		Text:            result.Text,
		Links:           result.Links,
		MetaDescription: result.MetaDescription,
		CanonicalUrl:    result.CanonicalURL,
		ContentHash:     result.ContentHash,
		Depth:           event.Depth,
		CrawledAt:       timestamppb.Now(),
		ProjectId:       event.ProjectId,
		CrawlId:         event.CrawlId,
	}

	// 11. Store the page.
	if err := deps.Storage.WritePage(ctx, page); err != nil {
		return fmt.Errorf("write page: %w", err)
	}

	// 12. Enqueue all discovered links.
	for _, link := range result.Links {
		child := &crawlerv1.URLFrontierEvent{
			Url:            link,
			Depth:          event.Depth + 1,
			ProjectId:      event.ProjectId,
			CrawlId:        event.CrawlId,
			DiscoveredFrom: event.Url,
			Strategy:       crawlerv1.CrawlStrategy_CRAWL_STRATEGY_FULL,
		}
		if err := deps.Queue.Push(ctx, child); err != nil {
			return fmt.Errorf("enqueue %s: %w", link, err)
		}
	}

	return nil
}
