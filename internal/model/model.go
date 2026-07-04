// Package model holds the small, dependency-free data types that flow between
// the crawler, the extraction pipeline, the indexer and the search layer.
//
// Keeping these structs in a leaf package (no imports of our own packages)
// avoids import cycles: crawler -> model <- indexer, etc.
package model

import "time"

// RawPage is what the crawler emits after a successful fetch. It deliberately
// carries the raw body bytes (not a parsed tree) so the crawl goroutines stay
// cheap; all HTML parsing happens once, downstream, in the indexer.
type RawPage struct {
	URL         string
	FinalURL    string // after redirects
	Host        string
	Body        []byte
	ContentType string
	StatusCode  int
	FetchedAt   time.Time
	Depth       int
}

// Doc is the fully-extracted representation of a page: the searchable text
// plus the metadata we persist in the KV store and the outbound links we feed
// back into the frontier.
type Doc struct {
	URL         string
	Host        string
	Title       string
	Description string
	Text        string   // visible, de-scripted body text (already length-capped)
	Snippet     string   // short preview stored for result rendering
	Links       []string // absolute, http(s)-only, ready for the frontier
	FetchedAt   time.Time
	Depth       int
}

// Meta is the compact record stored in BadgerDB keyed by URL. It is everything
// the search layer needs to render a result row without touching the index.
type Meta struct {
	URL         string `json:"u"`
	Title       string `json:"t"`
	Description string `json:"d"`
	Snippet     string `json:"s"`
	Host        string `json:"h"`
	FetchedAt   int64  `json:"f"` // unix seconds
}

// SearchResult is a single ranked hit returned to the API layer.
type SearchResult struct {
	URL     string  `json:"url"`
	Title   string  `json:"title"`
	Snippet string  `json:"snippet"`
	Host    string  `json:"host"`
	Score   float64 `json:"score"`
}

// SearchResponse is the full payload for a query.
type SearchResponse struct {
	Query    string         `json:"query"`
	Total    uint64         `json:"total"`
	TookMS   float64        `json:"took_ms"`
	Results  []SearchResult `json:"results"`
	MaxScore float64        `json:"max_score"`
}
