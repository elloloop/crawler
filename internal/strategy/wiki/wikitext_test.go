package wiki

import (
	"strings"
	"testing"
)

func TestStripCategory(t *testing.T) {
	input := "Some text [[Category:Science]] more text"
	got := StripWikitext(input)
	if strings.Contains(got, "Category") {
		t.Errorf("expected category removed, got %q", got)
	}
	if !strings.Contains(got, "Some text") {
		t.Errorf("expected surrounding text preserved, got %q", got)
	}
}

func TestStripFileLink(t *testing.T) {
	input := "Before [[File:Example.png|thumb|A caption]] after"
	got := StripWikitext(input)
	if strings.Contains(got, "File:") {
		t.Errorf("expected file link removed, got %q", got)
	}
	if !strings.Contains(got, "Before") || !strings.Contains(got, "after") {
		t.Errorf("expected surrounding text preserved, got %q", got)
	}
}

func TestStripImageLink(t *testing.T) {
	input := "Before [[Image:Photo.jpg|200px]] after"
	got := StripWikitext(input)
	if strings.Contains(got, "Image:") {
		t.Errorf("expected image link removed, got %q", got)
	}
}

func TestPipedLink(t *testing.T) {
	input := "See [[United States|US]] for details"
	got := StripWikitext(input)
	if !strings.Contains(got, "US") {
		t.Errorf("expected display text 'US', got %q", got)
	}
	if strings.Contains(got, "United States") {
		t.Errorf("expected link target removed, got %q", got)
	}
}

func TestSimpleLink(t *testing.T) {
	input := "Visit [[Paris]] today"
	got := StripWikitext(input)
	if !strings.Contains(got, "Paris") {
		t.Errorf("expected 'Paris' preserved, got %q", got)
	}
	if strings.Contains(got, "[[") || strings.Contains(got, "]]") {
		t.Errorf("expected brackets removed, got %q", got)
	}
}

func TestStripTemplates(t *testing.T) {
	input := "Text {{cite web|url=http://example.com}} after"
	got := StripWikitext(input)
	if strings.Contains(got, "cite") {
		t.Errorf("expected template removed, got %q", got)
	}
	if !strings.Contains(got, "Text") || !strings.Contains(got, "after") {
		t.Errorf("expected surrounding text preserved, got %q", got)
	}
}

func TestStripNestedTemplates(t *testing.T) {
	input := "A {{outer {{inner}} stuff}} B"
	got := StripWikitext(input)
	if strings.Contains(got, "outer") || strings.Contains(got, "inner") {
		t.Errorf("expected nested templates removed, got %q", got)
	}
	if !strings.Contains(got, "A") || !strings.Contains(got, "B") {
		t.Errorf("expected surrounding text preserved, got %q", got)
	}
}

func TestStripTable(t *testing.T) {
	input := "Before\n{| class=\"wikitable\"\n|-\n| cell\n|}\nAfter"
	got := StripWikitext(input)
	if strings.Contains(got, "wikitable") || strings.Contains(got, "cell") {
		t.Errorf("expected table removed, got %q", got)
	}
	if !strings.Contains(got, "Before") || !strings.Contains(got, "After") {
		t.Errorf("expected surrounding text preserved, got %q", got)
	}
}

func TestHeadings(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"== History ==", "History"},
		{"=== Details ===", "Details"},
		{"==== Sub ====", "Sub"},
	}
	for _, tt := range tests {
		got := StripWikitext(tt.input)
		if !strings.Contains(got, tt.want) {
			t.Errorf("StripWikitext(%q) = %q, want to contain %q", tt.input, got, tt.want)
		}
		if strings.Contains(got, "=") {
			t.Errorf("StripWikitext(%q) = %q, expected '=' removed", tt.input, got)
		}
	}
}

func TestBoldAndItalic(t *testing.T) {
	input := "This is '''bold''' and ''italic'' text"
	got := StripWikitext(input)
	if !strings.Contains(got, "bold") || !strings.Contains(got, "italic") {
		t.Errorf("expected text preserved, got %q", got)
	}
	if strings.Contains(got, "'''") || strings.Contains(got, "''") {
		t.Errorf("expected markup removed, got %q", got)
	}
}

func TestStripRefTags(t *testing.T) {
	input := "Fact<ref>Source here</ref> and<ref name=\"a\">Another</ref> more<ref name=\"b\" /> end"
	got := StripWikitext(input)
	if strings.Contains(got, "Source") || strings.Contains(got, "Another") || strings.Contains(got, "ref") {
		t.Errorf("expected ref tags removed, got %q", got)
	}
	if !strings.Contains(got, "Fact") || !strings.Contains(got, "end") {
		t.Errorf("expected surrounding text preserved, got %q", got)
	}
}

func TestStripHTMLTags(t *testing.T) {
	input := "Before <div class=\"box\">inside</div> after"
	got := StripWikitext(input)
	if strings.Contains(got, "<div") || strings.Contains(got, "</div>") {
		t.Errorf("expected HTML tags removed, got %q", got)
	}
	if !strings.Contains(got, "inside") {
		t.Errorf("expected inner text preserved, got %q", got)
	}
}

func TestExternalLink(t *testing.T) {
	input := "See [http://example.com Example Site] for info"
	got := StripWikitext(input)
	if !strings.Contains(got, "Example Site") {
		t.Errorf("expected display text preserved, got %q", got)
	}
	if strings.Contains(got, "http://") {
		t.Errorf("expected URL removed, got %q", got)
	}
}

func TestExternalLinkBare(t *testing.T) {
	input := "See [http://example.com] for info"
	got := StripWikitext(input)
	if strings.Contains(got, "http://") || strings.Contains(got, "[") {
		t.Errorf("expected bare external link removed, got %q", got)
	}
}

func TestHTMLEntities(t *testing.T) {
	input := "A &amp; B &lt; C &gt; D &quot;E&quot;"
	got := StripWikitext(input)
	if !strings.Contains(got, "A & B") {
		t.Errorf("expected &amp; decoded, got %q", got)
	}
	if !strings.Contains(got, "< C >") {
		t.Errorf("expected &lt;/&gt; decoded, got %q", got)
	}
}

func TestCollapseWhitespace(t *testing.T) {
	input := "word1    word2\n\n\n\n\nword3"
	got := StripWikitext(input)
	if strings.Contains(got, "    ") {
		t.Errorf("expected multiple spaces collapsed, got %q", got)
	}
	if strings.Contains(got, "\n\n\n") {
		t.Errorf("expected multiple newlines collapsed, got %q", got)
	}
}

func TestRealisticWikitext(t *testing.T) {
	input := `'''Albert Einstein''' (14 March 1879{{snd}}18 April 1955) was a German-born
[[theoretical physics|theoretical physicist]].{{sfn|Pais|1982|p=301}}

He developed the [[theory of relativity]],<ref>{{cite journal|title=Zur Elektrodynamik bewegter Körper}}</ref>
one of the two pillars of [[modern physics]]<ref name="frs">{{cite journal|last=Whittaker}}</ref>
(alongside [[quantum mechanics]]).

== Early life ==
Einstein was born in the [[German Empire|German]] city of [[Ulm]],
in the [[Kingdom of Württemberg]].{{efn|name=birth}}

[[File:Einstein patridge 1920.jpg|thumb|left|Einstein in 1920]]
[[Category:Nobel laureates in Physics]]
[[Category:German physicists]]

{| class="wikitable"
|-
! Year !! Event
|-
| 1879 || Born
|}

He said &quot;imagination is more important than knowledge&quot;.`

	got := StripWikitext(input)

	// Should contain cleaned text.
	if !strings.Contains(got, "Albert Einstein") {
		t.Error("expected 'Albert Einstein'")
	}
	if !strings.Contains(got, "theoretical physicist") {
		t.Error("expected 'theoretical physicist' from piped link")
	}
	if !strings.Contains(got, "theory of relativity") {
		t.Error("expected 'theory of relativity' from simple link")
	}
	if !strings.Contains(got, "Early life") {
		t.Error("expected 'Early life' heading text")
	}
	if !strings.Contains(got, "Ulm") {
		t.Error("expected 'Ulm' from simple link")
	}
	if !strings.Contains(got, `"imagination is more important than knowledge"`) {
		t.Error("expected decoded HTML entities")
	}

	// Should not contain wikitext artifacts.
	for _, bad := range []string{"[[", "]]", "{{", "}}", "<ref", "Category:", "File:", "{|", "|}", "==", "'''"} {
		if strings.Contains(got, bad) {
			t.Errorf("unexpected wikitext artifact %q in output:\n%s", bad, got)
		}
	}
}

func TestMagicWords(t *testing.T) {
	input := "Before __TOC__ after __NOTOC__ end"
	got := StripWikitext(input)
	if strings.Contains(got, "__TOC__") || strings.Contains(got, "__NOTOC__") {
		t.Errorf("expected magic words removed, got %q", got)
	}
}
