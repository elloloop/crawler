package wiki

import (
	"compress/bzip2"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/strategy"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxTextLen = 5000

// XML structures for MediaWiki dump format.
type xmlPage struct {
	Title    string      `xml:"title"`
	NS       string      `xml:"ns"`
	ID       int64       `xml:"id"`
	Redirect *struct{}   `xml:"redirect"`
	Revision xmlRevision `xml:"revision"`
}

type xmlRevision struct {
	Text string `xml:"text"`
}

// Strategy implements strategy.Strategy for Wikipedia dump import.
type Strategy struct{}

// New creates a new wiki dump import strategy.
func New() *Strategy {
	return &Strategy{}
}

// Name returns the strategy identifier.
func (s *Strategy) Name() string {
	return "wiki"
}

// ProcessURL processes a Wikipedia dump file (local path or URL).
// It streams the XML, extracts article pages, strips wikitext markup,
// and stores each page via the provided storage dependency.
func (s *Strategy) ProcessURL(ctx context.Context, event *crawlerv1.URLFrontierEvent, deps *strategy.Deps) error {
	src := event.Url

	reader, closer, err := openSource(src)
	if err != nil {
		return fmt.Errorf("open dump source: %w", err)
	}
	defer closer()

	// Detect bzip2 compression by extension.
	if strings.HasSuffix(src, ".bz2") {
		reader = bzip2.NewReader(reader)
	}

	decoder := xml.NewDecoder(reader)
	var count int64

	for {
		// Check for context cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("xml token: %w", err)
		}

		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "page" {
			continue
		}

		var page xmlPage
		if err := decoder.DecodeElement(&page, &se); err != nil {
			return fmt.Errorf("decode page element: %w", err)
		}

		// Skip non-article namespaces.
		if page.NS != "0" {
			continue
		}

		// Skip redirects.
		if page.Redirect != nil {
			continue
		}

		rawText := page.Revision.Text
		stripped := StripWikitext(rawText)

		// Truncate text to maxTextLen characters.
		text := stripped
		if len(text) > maxTextLen {
			text = text[:maxTextLen]
		}

		// Compute content hash from raw wikitext.
		hash := sha256.Sum256([]byte(rawText))

		pbPage := &crawlerv1.Page{
			Url:         "https://en.wikipedia.org/wiki/" + url.PathEscape(page.Title),
			Title:       page.Title,
			Text:        text,
			ContentHash: fmt.Sprintf("%x", hash),
			ProjectId:   event.ProjectId,
			CrawlId:     event.CrawlId,
			Depth:       0,
			CrawledAt:   timestamppb.Now(),
		}

		if err := deps.Storage.WritePage(ctx, pbPage); err != nil {
			return fmt.Errorf("write page %q: %w", page.Title, err)
		}

		count++
		if count%10000 == 0 {
			log.Printf("wiki: processed %d pages", count)
		}
	}

	log.Printf("wiki: finished, total pages processed: %d", count)
	return nil
}

// openSource returns a reader and a close function for the given source.
// If the source looks like a URL, it downloads it via HTTP GET.
// Otherwise, it opens a local file.
func openSource(src string) (io.Reader, func(), error) {
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		resp, err := http.Get(src)
		if err != nil {
			return nil, nil, fmt.Errorf("http get %s: %w", src, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, nil, fmt.Errorf("http status %d for %s", resp.StatusCode, src)
		}
		return resp.Body, func() { resp.Body.Close() }, nil
	}

	f, err := os.Open(src)
	if err != nil {
		return nil, nil, fmt.Errorf("open file %s: %w", src, err)
	}
	return f, func() { f.Close() }, nil
}
