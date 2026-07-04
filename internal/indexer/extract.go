package indexer

import (
	"bytes"
	"net/url"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// extracted is the raw output of a single HTML parse pass.
type extracted struct {
	title       string
	description string
	text        string
	links       []string
}

// extract parses HTML once and pulls out everything downstream needs: the
// title, the meta description, the de-scripted visible text (capped at
// maxText bytes), and absolute outbound links (deduped within the page).
//
// A single DFS does all four jobs — the crawler never parses, so this is the
// only tree walk per page.
func extract(base *url.URL, body []byte, maxText int) extracted {
	node, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return extracted{}
	}

	var out extracted
	var sb strings.Builder
	sb.Grow(min(maxText, 8192))

	seen := make(map[string]struct{}, 32)
	baseHref := base

	var walk func(n *html.Node, skip bool)
	walk = func(n *html.Node, skip bool) {
		switch n.Type {
		case html.ElementNode:
			switch n.DataAtom {
			case atom.Script, atom.Style, atom.Noscript, atom.Template, atom.Svg:
				skip = true // don't descend into non-content subtrees for text
			case atom.Title:
				if out.title == "" {
					out.title = strings.TrimSpace(textOf(n))
				}
			case atom.Base:
				if href := attr(n, "href"); href != "" {
					if u, err := baseHref.Parse(href); err == nil {
						baseHref = u
					}
				}
			case atom.Meta:
				if strings.EqualFold(attr(n, "name"), "description") && out.description == "" {
					out.description = strings.TrimSpace(attr(n, "content"))
				}
			case atom.A:
				if href := attr(n, "href"); href != "" {
					if abs := resolve(baseHref, href); abs != "" {
						if _, ok := seen[abs]; !ok {
							seen[abs] = struct{}{}
							out.links = append(out.links, abs)
						}
					}
				}
			}
		case html.TextNode:
			if !skip && sb.Len() < maxText {
				appendText(&sb, n.Data, maxText)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, skip)
		}
	}
	walk(node, false)

	out.text = sb.String()
	return out
}

// appendText adds whitespace-collapsed text without exceeding the byte cap.
func appendText(sb *strings.Builder, s string, maxText int) {
	fields := strings.Fields(s)
	for _, w := range fields {
		if sb.Len()+len(w)+1 > maxText {
			return
		}
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(w)
	}
}

// textOf returns the concatenated text of a node's descendants (used for
// <title>, which is small).
func textOf(n *html.Node) string {
	var sb strings.Builder
	var rec func(*html.Node)
	rec = func(x *html.Node) {
		if x.Type == html.TextNode {
			sb.WriteString(x.Data)
		}
		for c := x.FirstChild; c != nil; c = c.NextSibling {
			rec(c)
		}
	}
	rec(n)
	return sb.String()
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

// resolve turns a possibly-relative href into an absolute http(s) URL string,
// or "" if it isn't crawlable.
func resolve(base *url.URL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") ||
		strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") ||
		strings.HasPrefix(href, "tel:") || strings.HasPrefix(href, "data:") {
		return ""
	}
	u, err := base.Parse(href)
	if err != nil {
		return ""
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	u.Fragment = ""
	return u.String()
}
