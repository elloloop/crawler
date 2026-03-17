// Package parser provides high-performance streaming HTML extraction
// using golang.org/x/net/html tokenizer. Single-pass, no DOM construction.
package parser

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// Result holds the extracted data from an HTML page.
type Result struct {
	Title           string
	Text            string
	Links           []string
	MetaDescription string
	MetaRobots      string
	CanonicalURL    string
	ContentHash     string
}

// Parse extracts structured data from HTML using a streaming tokenizer.
// baseURL is used to resolve relative links to absolute URLs.
// This performs a single pass over the HTML with no DOM allocation.
func Parse(baseURL string, body []byte) *Result {
	base, _ := url.Parse(baseURL)

	r := &Result{}
	z := html.NewTokenizer(strings.NewReader(string(body)))

	var textBuf strings.Builder
	var titleBuf strings.Builder
	inTitle := false
	inScript := false
	inStyle := false
	inNoScript := false
	textBuf.Grow(8192)
	seen := make(map[string]struct{}, 128)

	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			r.Text = truncate(textBuf.String(), 5000)
			r.Title = strings.TrimSpace(titleBuf.String())
			r.ContentHash = fmt.Sprintf("%x", sha256.Sum256(body))
			return r

		case html.StartTagToken, html.SelfClosingTagToken:
			tn, hasAttr := z.TagName()
			tag := atom.Lookup(tn)

			// Read all attributes once into a map.
			var attrs map[string]string
			if hasAttr {
				attrs = readAttrs(z)
			}

			switch tag {
			case atom.Title:
				if tt == html.StartTagToken {
					inTitle = true
				}
			case atom.Script:
				inScript = true
			case atom.Style:
				inStyle = true
			case atom.Noscript:
				inNoScript = true
			case atom.A:
				if href := attrs["href"]; href != "" {
					if resolved := resolveURL(base, href); resolved != "" {
						if _, ok := seen[resolved]; !ok {
							seen[resolved] = struct{}{}
							r.Links = append(r.Links, resolved)
						}
					}
				}
			case atom.Meta:
				name := strings.ToLower(attrs["name"])
				content := attrs["content"]
				switch name {
				case "description":
					r.MetaDescription = content
				case "robots":
					r.MetaRobots = content
				}
			case atom.Link:
				if strings.ToLower(attrs["rel"]) == "canonical" {
					r.CanonicalURL = attrs["href"]
				}
			}

		case html.EndTagToken:
			tn, _ := z.TagName()
			tag := atom.Lookup(tn)
			switch tag {
			case atom.Title:
				inTitle = false
			case atom.Script:
				inScript = false
			case atom.Style:
				inStyle = false
			case atom.Noscript:
				inNoScript = false
			}

		case html.TextToken:
			if inScript || inStyle || inNoScript {
				continue
			}
			text := strings.TrimSpace(string(z.Text()))
			if text == "" {
				continue
			}
			if inTitle {
				titleBuf.WriteString(text)
			}
			if textBuf.Len() < 5200 {
				if textBuf.Len() > 0 {
					textBuf.WriteByte(' ')
				}
				textBuf.WriteString(text)
			}
		}
	}
}

// CheckMetaRobots returns (shouldIndex, shouldFollow) based on meta robots content.
func CheckMetaRobots(metaRobots string) (bool, bool) {
	if metaRobots == "" {
		return true, true
	}
	lower := strings.ToLower(metaRobots)
	shouldIndex := !strings.Contains(lower, "noindex")
	shouldFollow := !strings.Contains(lower, "nofollow")
	return shouldIndex, shouldFollow
}

// readAttrs reads all attributes of the current token into a map.
func readAttrs(z *html.Tokenizer) map[string]string {
	attrs := make(map[string]string, 4)
	for {
		key, val, more := z.TagAttr()
		attrs[string(key)] = string(val)
		if !more {
			break
		}
	}
	return attrs
}

func resolveURL(base *url.URL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") {
		return ""
	}
	ref, err := url.Parse(href)
	if err != nil {
		return ""
	}
	resolved := base.ResolveReference(ref)
	resolved.Fragment = ""
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}
	return resolved.String()
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}
