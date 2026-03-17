// Package robots provides robots.txt fetching, parsing, and URL permission checking.
package robots

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// FetchFunc fetches a URL and returns (body, statusCode, error).
type FetchFunc func(ctx context.Context, url string) ([]byte, int, error)

// robotsData holds parsed robots.txt data for a single domain.
type robotsData struct {
	allowRules    []string
	disallowRules []string
	crawlDelay    float64
	sitemaps      []string
	// allowAll is true when robots.txt returned 404 (no restrictions).
	allowAll bool
	// disallowAll is true when robots.txt returned 5xx (conservative).
	disallowAll bool
}

// Checker fetches and caches robots.txt data per domain, providing
// permission checks and crawl-delay/sitemap information.
type Checker struct {
	userAgent string
	mu        sync.RWMutex
	cache     map[string]*robotsData
}

// NewChecker creates a new robots.txt checker for the given user agent string.
func NewChecker(userAgent string) *Checker {
	return &Checker{
		userAgent: userAgent,
		cache:     make(map[string]*robotsData),
	}
}

// domainFromURL extracts the scheme + host from a raw URL string.
func domainFromURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}

// robotsURL returns the robots.txt URL for a given raw URL.
func robotsURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s://%s/robots.txt", u.Scheme, u.Host), nil
}

// ensureFetched ensures that robots.txt for the domain has been fetched and cached.
// It must be called with at least a read lock NOT held (it acquires its own locks).
func (c *Checker) ensureFetched(ctx context.Context, rawURL string, fetch FetchFunc) (*robotsData, error) {
	domain, err := domainFromURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("robots: invalid URL %q: %w", rawURL, err)
	}

	// Fast path: check cache with read lock.
	c.mu.RLock()
	data, ok := c.cache[domain]
	c.mu.RUnlock()
	if ok {
		return data, nil
	}

	// Slow path: fetch and parse.
	rURL, err := robotsURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("robots: invalid URL %q: %w", rawURL, err)
	}

	body, statusCode, fetchErr := fetch(ctx, rURL)

	data = &robotsData{}

	switch {
	case fetchErr != nil || statusCode >= 500:
		// Server error or fetch failure: be conservative, disallow all.
		data.disallowAll = true
	case statusCode == 404 || statusCode == 410:
		// No robots.txt: allow everything.
		data.allowAll = true
	case statusCode >= 400:
		// Other 4xx (401, 403): allow all per convention.
		data.allowAll = true
	default:
		// 2xx/3xx: parse the robots.txt content.
		c.parseRobotsTxt(data, string(body))
	}

	c.mu.Lock()
	// Double-check: another goroutine may have populated it.
	if existing, ok := c.cache[domain]; ok {
		c.mu.Unlock()
		return existing, nil
	}
	c.cache[domain] = data
	c.mu.Unlock()

	return data, nil
}

// parseRobotsTxt parses robots.txt content and populates the robotsData.
// It looks for rules matching our user agent first, then falls back to "*".
func (c *Checker) parseRobotsTxt(data *robotsData, content string) {
	type agentBlock struct {
		agents    []string
		allow     []string
		disallow  []string
		delay     float64
		hasDelay  bool
	}

	var blocks []agentBlock
	var current *agentBlock
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		// Strip comments.
		if idx := strings.IndexByte(line, '#'); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		colonIdx := strings.IndexByte(line, ':')
		if colonIdx < 0 {
			continue
		}

		directive := strings.TrimSpace(line[:colonIdx])
		value := strings.TrimSpace(line[colonIdx+1:])
		directiveLower := strings.ToLower(directive)

		switch directiveLower {
		case "user-agent":
			// If current block has no rules yet and only agents, add to it.
			if current != nil && len(current.allow) == 0 && len(current.disallow) == 0 && !current.hasDelay {
				current.agents = append(current.agents, strings.ToLower(value))
			} else {
				blocks = append(blocks, agentBlock{})
				current = &blocks[len(blocks)-1]
				current.agents = append(current.agents, strings.ToLower(value))
			}
		case "disallow":
			if current == nil {
				continue
			}
			if value != "" {
				current.disallow = append(current.disallow, value)
			}
		case "allow":
			if current == nil {
				continue
			}
			if value != "" {
				current.allow = append(current.allow, value)
			}
		case "crawl-delay":
			if current == nil {
				continue
			}
			if d, err := strconv.ParseFloat(value, 64); err == nil && d >= 0 {
				current.delay = d
				current.hasDelay = true
			}
		case "sitemap":
			// Sitemaps are global, not per user-agent.
			data.sitemaps = append(data.sitemaps, value)
		}
	}

	// Find the best matching block: prefer specific user-agent match, then "*".
	uaLower := strings.ToLower(c.userAgent)
	var bestBlock *agentBlock
	var wildBlock *agentBlock

	for i := range blocks {
		b := &blocks[i]
		for _, agent := range b.agents {
			if agent == "*" {
				if wildBlock == nil {
					wildBlock = b
				}
			} else if strings.Contains(uaLower, agent) {
				if bestBlock == nil {
					bestBlock = b
				}
			}
		}
	}

	// Prefer specific match over wildcard.
	chosen := bestBlock
	if chosen == nil {
		chosen = wildBlock
	}

	if chosen != nil {
		data.allowRules = chosen.allow
		data.disallowRules = chosen.disallow
		if chosen.hasDelay {
			data.crawlDelay = chosen.delay
		}
	}
}

// IsAllowed checks whether the given URL is allowed by robots.txt rules.
// It fetches and caches robots.txt if not already cached.
func (c *Checker) IsAllowed(ctx context.Context, rawURL string, fetch FetchFunc) (bool, error) {
	data, err := c.ensureFetched(ctx, rawURL, fetch)
	if err != nil {
		return false, err
	}

	if data.allowAll {
		return true, nil
	}
	if data.disallowAll {
		return false, nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return false, fmt.Errorf("robots: invalid URL %q: %w", rawURL, err)
	}

	path := u.Path
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path = path + "?" + u.RawQuery
	}

	return isPathAllowed(path, data.allowRules, data.disallowRules), nil
}

// isPathAllowed determines if a path is allowed given allow and disallow rules.
// More specific (longer) rules take precedence. Allow wins ties.
func isPathAllowed(path string, allowRules, disallowRules []string) bool {
	// Find the longest matching allow rule.
	longestAllow := -1
	for _, rule := range allowRules {
		if pathMatches(path, rule) && len(rule) > longestAllow {
			longestAllow = len(rule)
		}
	}

	// Find the longest matching disallow rule.
	longestDisallow := -1
	for _, rule := range disallowRules {
		if pathMatches(path, rule) && len(rule) > longestDisallow {
			longestDisallow = len(rule)
		}
	}

	// No matching rules: allowed.
	if longestDisallow < 0 {
		return true
	}

	// Allow rule is more specific or equal length: allowed.
	if longestAllow >= longestDisallow {
		return true
	}

	return false
}

// pathMatches checks if a URL path matches a robots.txt path pattern.
// Supports * wildcard and $ end-of-string anchor.
func pathMatches(path, pattern string) bool {
	// Handle $ anchor at end.
	if strings.HasSuffix(pattern, "$") {
		pattern = pattern[:len(pattern)-1]
		return matchWildcard(path, pattern) && len(path) == consumeMatch(path, pattern)
	}

	return matchWildcard(path, pattern)
}

// matchWildcard checks if path starts with pattern, supporting * wildcards.
func matchWildcard(path, pattern string) bool {
	// If no wildcards, simple prefix match.
	if !strings.Contains(pattern, "*") {
		return strings.HasPrefix(path, pattern)
	}

	// Split pattern by * and match segments in order.
	parts := strings.Split(pattern, "*")
	pos := 0

	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(path[pos:], part)
		if idx < 0 {
			return false
		}
		if i == 0 && idx != 0 {
			// First segment must match at the beginning.
			return false
		}
		pos += idx + len(part)
	}

	return true
}

// consumeMatch returns how many characters of path are consumed when matching pattern.
// Used for $ anchor matching.
func consumeMatch(path, pattern string) int {
	if !strings.Contains(pattern, "*") {
		return len(pattern)
	}

	parts := strings.Split(pattern, "*")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(path[pos:], part)
		if idx < 0 {
			return pos
		}
		if i == 0 && idx != 0 {
			return 0
		}
		pos += idx + len(part)
	}
	return pos
}

// GetCrawlDelay returns the crawl-delay for a domain. Returns 0 if none is set
// or the domain's robots.txt has not been fetched yet.
func (c *Checker) GetCrawlDelay(domain string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if data, ok := c.cache[domain]; ok {
		return data.crawlDelay
	}
	return 0
}

// GetSitemaps returns the sitemap URLs from robots.txt for a domain.
// Returns nil if the domain's robots.txt has not been fetched yet.
func (c *Checker) GetSitemaps(domain string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if data, ok := c.cache[domain]; ok {
		return data.sitemaps
	}
	return nil
}
