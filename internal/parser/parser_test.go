package parser

import (
	"strings"
	"testing"
)

func TestParse_BasicHTML(t *testing.T) {
	html := `<!DOCTYPE html>
<html>
<head>
  <title>Test Page</title>
  <meta name="description" content="A test page">
  <meta name="robots" content="noindex, nofollow">
  <link rel="canonical" href="https://example.com/canonical">
</head>
<body>
  <h1>Hello World</h1>
  <p>Some text content here.</p>
  <a href="/page1">Link 1</a>
  <a href="https://example.com/page2">Link 2</a>
  <a href="#fragment">Fragment</a>
  <a href="javascript:void(0)">JS Link</a>
  <script>var x = 1;</script>
  <style>.foo { color: red; }</style>
</body>
</html>`

	result := Parse("https://example.com/test", []byte(html))

	if result.Title != "Test Page" {
		t.Errorf("title = %q, want %q", result.Title, "Test Page")
	}
	if result.MetaDescription != "A test page" {
		t.Errorf("meta description = %q, want %q", result.MetaDescription, "A test page")
	}
	if result.MetaRobots != "noindex, nofollow" {
		t.Errorf("meta robots = %q, want %q", result.MetaRobots, "noindex, nofollow")
	}
	if result.CanonicalURL != "https://example.com/canonical" {
		t.Errorf("canonical = %q, want %q", result.CanonicalURL, "https://example.com/canonical")
	}

	// Should have 2 valid links (relative resolved, absolute kept; fragment and javascript skipped)
	if len(result.Links) != 2 {
		t.Errorf("links count = %d, want 2, got %v", len(result.Links), result.Links)
	}
	if len(result.Links) >= 2 {
		if result.Links[0] != "https://example.com/page1" {
			t.Errorf("link[0] = %q, want %q", result.Links[0], "https://example.com/page1")
		}
		if result.Links[1] != "https://example.com/page2" {
			t.Errorf("link[1] = %q, want %q", result.Links[1], "https://example.com/page2")
		}
	}

	// Text should not contain script/style content
	if strings.Contains(result.Text, "var x") {
		t.Error("text contains script content")
	}
	if strings.Contains(result.Text, "color: red") {
		t.Error("text contains style content")
	}
	if !strings.Contains(result.Text, "Hello World") {
		t.Error("text missing body content")
	}

	if result.ContentHash == "" {
		t.Error("content hash is empty")
	}
}

func TestParse_EmptyHTML(t *testing.T) {
	result := Parse("https://example.com", []byte(""))
	if result.Title != "" {
		t.Errorf("title = %q, want empty", result.Title)
	}
	if len(result.Links) != 0 {
		t.Errorf("links = %v, want empty", result.Links)
	}
}

func TestParse_DuplicateLinks(t *testing.T) {
	html := `<a href="/a">A</a><a href="/a">A again</a><a href="/b">B</a>`
	result := Parse("https://example.com", []byte(html))
	if len(result.Links) != 2 {
		t.Errorf("links count = %d, want 2 (deduped)", len(result.Links))
	}
}

func TestCheckMetaRobots(t *testing.T) {
	tests := []struct {
		input       string
		wantIndex   bool
		wantFollow  bool
	}{
		{"", true, true},
		{"noindex", false, true},
		{"nofollow", true, false},
		{"noindex, nofollow", false, false},
		{"NOINDEX, NOFOLLOW", false, false},
		{"index, follow", true, true},
	}
	for _, tt := range tests {
		idx, fol := CheckMetaRobots(tt.input)
		if idx != tt.wantIndex || fol != tt.wantFollow {
			t.Errorf("CheckMetaRobots(%q) = (%v, %v), want (%v, %v)",
				tt.input, idx, fol, tt.wantIndex, tt.wantFollow)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	// Simulate a large Wikipedia-like page
	var buf strings.Builder
	buf.WriteString("<html><head><title>Benchmark Page</title></head><body>")
	for i := range 1000 {
		buf.WriteString("<p>This is paragraph ")
		buf.WriteString(strings.Repeat("word ", 50))
		buf.WriteString("</p>")
		if i%10 == 0 {
			buf.WriteString(`<a href="/page/`)
			buf.WriteString(string(rune('a' + i%26)))
			buf.WriteString(`">Link</a>`)
		}
	}
	buf.WriteString("</body></html>")
	data := []byte(buf.String())

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for range b.N {
		Parse("https://example.com/bench", data)
	}
}
