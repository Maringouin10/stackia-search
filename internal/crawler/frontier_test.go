package crawler

import "testing"

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		in   string
		want string // "" means expect nil (uncrawlable)
	}{
		{"http://Example.com/Path", "http://example.com/Path"},
		{"http://example.com:80/", "http://example.com/"},
		{"https://example.com:443/x", "https://example.com/x"},
		{"http://example.com/p#frag", "http://example.com/p"},
		{"http://example.com", "http://example.com/"},
		{"ftp://example.com/x", ""},
		{"mailto:a@b.com", ""},
		{"/relative/only", ""},
		{"", ""},
	}
	for _, c := range cases {
		got := normalizeURL(c.in)
		if c.want == "" {
			if got != nil {
				t.Errorf("normalizeURL(%q) = %q, want nil", c.in, got.String())
			}
			continue
		}
		if got == nil {
			t.Errorf("normalizeURL(%q) = nil, want %q", c.in, c.want)
			continue
		}
		if got.String() != c.want {
			t.Errorf("normalizeURL(%q) = %q, want %q", c.in, got.String(), c.want)
		}
	}
}
