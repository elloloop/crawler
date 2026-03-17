// Package wiki implements the Wikipedia dump import strategy.
package wiki

import (
	"html"
	"regexp"
	"strings"
)

var (
	// Category links: [[Category:...]]
	reCategory = regexp.MustCompile(`\[\[Category:[^\]]*\]\]`)

	// File/Image links: [[File:...|...]] or [[Image:...|...]]
	reFile = regexp.MustCompile(`\[\[(?:File|Image):[^\]]*\]\]`)

	// Internal links with display text: [[target|display]]
	reLinkPiped = regexp.MustCompile(`\[\[[^\[\]]*?\|([^\[\]]*?)\]\]`)

	// Internal links without display text: [[target]]
	reLinkSimple = regexp.MustCompile(`\[\[([^\[\]|]*?)\]\]`)

	// Wiki tables: {| ... |}
	reTable = regexp.MustCompile(`(?s)\{\|.*?\|\}`)

	// Headings: == Heading == (all levels 2-6)
	reHeading = regexp.MustCompile(`(?m)^={2,6}\s*(.+?)\s*={2,6}\s*$`)

	// Bold: '''text'''
	reBold = regexp.MustCompile(`'''(.*?)'''`)

	// Italic: ''text''
	reItalic = regexp.MustCompile(`''(.*?)''`)

	// Ref tags with content: <ref>...</ref> or <ref name="...">...</ref>
	reRefBlock = regexp.MustCompile(`(?s)<ref[^>]*>.*?</ref>`)

	// Self-closing ref tags: <ref ... />
	reRefSelf = regexp.MustCompile(`<ref[^>]*/>`)

	// HTML comments
	reComment = regexp.MustCompile(`(?s)<!--.*?-->`)

	// Remaining HTML tags
	reHTMLTag = regexp.MustCompile(`<[^>]+>`)

	// External links with display text: [http://url display text]
	reExtLink = regexp.MustCompile(`\[https?://[^\s\]]+\s+([^\]]+)\]`)

	// External links without display text: [http://url]
	reExtLinkBare = regexp.MustCompile(`\[https?://[^\s\]]+\]`)

	// Multiple blank lines
	reMultiNewline = regexp.MustCompile(`\n{3,}`)

	// Multiple spaces
	reMultiSpace = regexp.MustCompile(`[^\S\n]{2,}`)

	// Magic words / behavior switches: __TOC__, __NOTOC__, etc.
	reMagicWord = regexp.MustCompile(`__[A-Z]+__`)
)

// StripWikitext removes wikitext markup and returns plain text.
func StripWikitext(wikitext string) string {
	s := wikitext

	// Remove HTML comments first (they can appear anywhere).
	s = reComment.ReplaceAllString(s, "")

	// Remove <ref>...</ref> and <ref .../> before other processing.
	s = reRefBlock.ReplaceAllString(s, "")
	s = reRefSelf.ReplaceAllString(s, "")

	// Remove nested templates {{ ... }} using a counter-based approach.
	s = stripNested(s, "{{", "}}")

	// Remove wiki tables {| ... |}.
	s = reTable.ReplaceAllString(s, "")

	// Remove category links.
	s = reCategory.ReplaceAllString(s, "")

	// Remove file/image links.
	s = reFile.ReplaceAllString(s, "")

	// Convert piped internal links [[target|display]] -> display.
	s = reLinkPiped.ReplaceAllString(s, "$1")

	// Convert simple internal links [[target]] -> target.
	s = reLinkSimple.ReplaceAllString(s, "$1")

	// Convert headings.
	s = reHeading.ReplaceAllString(s, "$1")

	// Strip bold and italic markers.
	s = reBold.ReplaceAllString(s, "$1")
	s = reItalic.ReplaceAllString(s, "$1")

	// Convert external links with display text.
	s = reExtLink.ReplaceAllString(s, "$1")

	// Remove bare external links.
	s = reExtLinkBare.ReplaceAllString(s, "")

	// Remove magic words.
	s = reMagicWord.ReplaceAllString(s, "")

	// Remove remaining HTML tags.
	s = reHTMLTag.ReplaceAllString(s, "")

	// Decode HTML entities.
	s = html.UnescapeString(s)

	// Collapse whitespace.
	s = reMultiSpace.ReplaceAllString(s, " ")
	s = reMultiNewline.ReplaceAllString(s, "\n\n")

	s = strings.TrimSpace(s)

	return s
}

// stripNested removes all occurrences of text between open and close delimiters,
// including nested ones, using a counter-based approach.
func stripNested(s, open, close string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	depth := 0
	i := 0
	for i < len(s) {
		if i+len(open) <= len(s) && s[i:i+len(open)] == open {
			depth++
			i += len(open)
			continue
		}
		if depth > 0 && i+len(close) <= len(s) && s[i:i+len(close)] == close {
			depth--
			i += len(close)
			continue
		}
		if depth == 0 {
			buf.WriteByte(s[i])
		}
		i++
	}
	return buf.String()
}
