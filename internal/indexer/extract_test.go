package indexer

import (
	"net/url"
	"strings"
	"testing"
)

func TestExtract(t *testing.T) {
	base, _ := url.Parse("http://example.com/dir/page.html")
	html := `<!doctype html><html><head>
		<title>  Hello World  </title>
		<meta name="description" content="A short summary.">
		<style>.x{color:red}</style>
	</head><body>
		<script>var ignore = "not indexed";</script>
		<h1>Heading</h1>
		<p>The quick brown fox.</p>
		<a href="/other">rel-root</a>
		<a href="sub/child.html">rel-dir</a>
		<a href="http://elsewhere.org/z">absolute</a>
		<a href="mailto:a@b.com">skip</a>
		<a href="#frag">skip</a>
	</body></html>`

	ex := extract(base, []byte(html), 4096)

	if ex.title != "Hello World" {
		t.Errorf("title = %q", ex.title)
	}
	if ex.description != "A short summary." {
		t.Errorf("description = %q", ex.description)
	}
	if !strings.Contains(ex.text, "quick brown fox") {
		t.Errorf("text missing body: %q", ex.text)
	}
	if strings.Contains(ex.text, "not indexed") {
		t.Errorf("script content leaked into text: %q", ex.text)
	}
	if strings.Contains(ex.text, "color:red") {
		t.Errorf("style content leaked into text: %q", ex.text)
	}

	want := map[string]bool{
		"http://example.com/other":              true,
		"http://example.com/dir/sub/child.html": true,
		"http://elsewhere.org/z":                true,
	}
	if len(ex.links) != len(want) {
		t.Fatalf("got %d links %v, want %d", len(ex.links), ex.links, len(want))
	}
	for _, l := range ex.links {
		if !want[l] {
			t.Errorf("unexpected link %q", l)
		}
	}
}

func TestExtractTextCap(t *testing.T) {
	base, _ := url.Parse("http://example.com/")
	var sb strings.Builder
	sb.WriteString("<html><body>")
	for i := 0; i < 1000; i++ {
		sb.WriteString("word ")
	}
	sb.WriteString("</body></html>")

	ex := extract(base, []byte(sb.String()), 64)
	if len(ex.text) > 64 {
		t.Errorf("text not capped: len=%d", len(ex.text))
	}
}
