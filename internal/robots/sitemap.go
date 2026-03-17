package robots

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// sitemapIndex represents a <sitemapindex> element.
type sitemapIndex struct {
	Sitemaps []sitemapEntry `xml:"sitemap"`
}

// sitemapEntry represents a <sitemap> within a sitemap index.
type sitemapEntry struct {
	Loc string `xml:"loc"`
}

// urlSet represents a <urlset> element.
type urlSet struct {
	URLs []urlEntry `xml:"url"`
}

// urlEntry represents a <url> within a urlset.
type urlEntry struct {
	Loc string `xml:"loc"`
}

// ParseSitemap fetches and parses a sitemap (or sitemap index) and returns
// the discovered URLs. It handles sitemap index files recursively up to
// maxDepth levels (2 recommended). Gzipped sitemaps (.xml.gz) are
// automatically decompressed.
func ParseSitemap(ctx context.Context, sitemapURL string, fetch FetchFunc, maxURLs int) ([]string, error) {
	return parseSitemapRecursive(ctx, sitemapURL, fetch, maxURLs, 0, 2)
}

func parseSitemapRecursive(ctx context.Context, sitemapURL string, fetch FetchFunc, maxURLs int, depth int, maxDepth int) ([]string, error) {
	if depth > maxDepth {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	body, statusCode, err := fetch(ctx, sitemapURL)
	if err != nil {
		return nil, fmt.Errorf("sitemap: fetch %q: %w", sitemapURL, err)
	}
	if statusCode < 200 || statusCode >= 300 {
		return nil, fmt.Errorf("sitemap: fetch %q returned status %d", sitemapURL, statusCode)
	}

	// Decompress if gzipped.
	if strings.HasSuffix(strings.ToLower(sitemapURL), ".gz") {
		gr, gzErr := gzip.NewReader(bytes.NewReader(body))
		if gzErr != nil {
			return nil, fmt.Errorf("sitemap: gzip decompress %q: %w", sitemapURL, gzErr)
		}
		decompressed, readErr := io.ReadAll(gr)
		gr.Close()
		if readErr != nil {
			return nil, fmt.Errorf("sitemap: gzip read %q: %w", sitemapURL, readErr)
		}
		body = decompressed
	}

	// Try to determine the type by looking for the root element.
	isSitemapIndex := isSitemapIndexDoc(body)

	if isSitemapIndex {
		return parseSitemapIndexDoc(ctx, body, fetch, maxURLs, depth, maxDepth)
	}

	return parseURLSetDoc(body, maxURLs)
}

// isSitemapIndexDoc checks whether the XML document is a sitemapindex.
func isSitemapIndexDoc(body []byte) bool {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := decoder.Token()
		if err != nil {
			return false
		}
		if se, ok := tok.(xml.StartElement); ok {
			return strings.ToLower(se.Name.Local) == "sitemapindex"
		}
	}
}

// parseSitemapIndexDoc parses a sitemapindex document and follows child sitemaps.
func parseSitemapIndexDoc(ctx context.Context, body []byte, fetch FetchFunc, maxURLs int, depth int, maxDepth int) ([]string, error) {
	var idx sitemapIndex
	if err := xml.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("sitemap: parse index: %w", err)
	}

	var urls []string
	for _, entry := range idx.Sitemaps {
		loc := strings.TrimSpace(entry.Loc)
		if loc == "" {
			continue
		}

		childURLs, err := parseSitemapRecursive(ctx, loc, fetch, maxURLs-len(urls), depth+1, maxDepth)
		if err != nil {
			// Continue processing other sitemaps on error.
			continue
		}
		urls = append(urls, childURLs...)
		if len(urls) >= maxURLs {
			urls = urls[:maxURLs]
			break
		}
	}

	return urls, nil
}

// parseURLSetDoc parses a urlset document and extracts URLs.
func parseURLSetDoc(body []byte, maxURLs int) ([]string, error) {
	var us urlSet
	if err := xml.Unmarshal(body, &us); err != nil {
		return nil, fmt.Errorf("sitemap: parse urlset: %w", err)
	}

	var urls []string
	for _, entry := range us.URLs {
		loc := strings.TrimSpace(entry.Loc)
		if loc == "" {
			continue
		}
		urls = append(urls, loc)
		if len(urls) >= maxURLs {
			break
		}
	}

	return urls, nil
}
