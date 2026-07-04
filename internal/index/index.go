// Package index wraps Bluge: an embedded, segment-based inverted index with
// BM25 ranking out of the box. We use the English analysis pipeline
// (tokenizer -> lowercase -> stop-word filter -> Snowball stemmer) on both the
// indexed text and the query, so "running" matches "run".
//
// The index deliberately stores almost nothing: only the document _id (the URL)
// is retained. Titles, snippets and hosts live in BadgerDB, keeping index
// segments small enough to memory-map comfortably on a Pi.
package index

import (
	"context"
	"fmt"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/lang/en"
	"github.com/blugelabs/bluge/search"

	"github.com/Maringouin10/stackia-search/internal/config"
	"github.com/Maringouin10/stackia-search/internal/model"
)

// Field names used across the index and the query builder.
const (
	FieldTitle = "title"
	FieldBody  = "body"
	FieldHost  = "host"
	FieldAll   = "_all"
	FieldID    = "_id"
)

type Index struct {
	writer   *bluge.Writer
	analyzer *analysis.Analyzer

	mu     sync.RWMutex
	reader *bluge.Reader // cached snapshot; swapped atomically on Refresh
}

// Open creates/opens the on-disk index and its first reader snapshot.
func Open(cfg config.IndexConfig) (*Index, error) {
	ana := en.NewAnalyzer()

	bcfg := bluge.DefaultConfig(cfg.Path)
	bcfg.DefaultSearchField = FieldAll // bare queries hit title+body
	bcfg.DefaultSearchAnalyzer = ana   // query terms stemmed like the corpus
	// DefaultSimilarity is already BM25 (see bluge.defaultConfig).

	w, err := bluge.OpenWriter(bcfg)
	if err != nil {
		return nil, fmt.Errorf("open index writer: %w", err)
	}
	r, err := w.Reader()
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("open index reader: %w", err)
	}
	return &Index{writer: w, analyzer: ana, reader: r}, nil
}

// Analyzer exposes the shared English analyzer so the query builder can stem
// query terms identically to the corpus.
func (ix *Index) Analyzer() *analysis.Analyzer { return ix.analyzer }

// IndexBatch upserts a slice of documents in a single Bluge batch. Re-crawling
// a URL overwrites the previous doc because the _id is the URL.
func (ix *Index) IndexBatch(docs []*model.Doc) error {
	if len(docs) == 0 {
		return nil
	}
	batch := bluge.NewBatch()
	for _, d := range docs {
		doc := bluge.NewDocument(d.URL)
		// Title: analysed + term positions (so phrase queries work); not stored.
		doc.AddField(bluge.NewTextField(FieldTitle, d.Title).
			WithAnalyzer(ix.analyzer).SearchTermPositions())
		// Body: analysed + term positions; not stored (snippets come from KV).
		doc.AddField(bluge.NewTextField(FieldBody, d.Text).
			WithAnalyzer(ix.analyzer).SearchTermPositions())
		// Host: keyword (exact) for optional site: filtering.
		doc.AddField(bluge.NewKeywordField(FieldHost, d.Host))
		// Composite _all lets a plain query match either title or body.
		doc.AddField(bluge.NewCompositeFieldIncluding(FieldAll, []string{FieldTitle, FieldBody}))

		batch.Update(doc.ID(), doc)
	}
	if err := ix.writer.Batch(batch); err != nil {
		return fmt.Errorf("index batch: %w", err)
	}
	return nil
}

// Refresh publishes recently-written documents to searchers by swapping in a
// fresh reader snapshot and closing the old one. The RWMutex guarantees no
// in-flight search still holds the reader we close (see WithReader).
func (ix *Index) Refresh() error {
	nr, err := ix.writer.Reader()
	if err != nil {
		return err
	}
	ix.mu.Lock()
	old := ix.reader
	ix.reader = nr
	ix.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// WithReader runs fn against the current snapshot under a read lock. Because
// Refresh takes the write lock (which waits for all readers to drain) before
// closing the previous snapshot, the reader passed to fn is always live for the
// duration of the call.
func (ix *Index) WithReader(fn func(*bluge.Reader) error) error {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return fn(ix.reader)
}

// Count returns the number of live documents in the current snapshot.
func (ix *Index) Count() uint64 {
	var n uint64
	_ = ix.WithReader(func(r *bluge.Reader) error {
		c, err := r.Count()
		if err == nil {
			n = c
		}
		return err
	})
	return n
}

// SearchTopN executes a request and returns the raw iterator plus a done fn.
// The search package owns query construction; this keeps reader locking here.
func (ix *Index) SearchTopN(ctx context.Context, req bluge.SearchRequest,
	visit func(hit *search.DocumentMatch) error) (uint64, error) {

	var total uint64
	err := ix.WithReader(func(r *bluge.Reader) error {
		dmi, err := r.Search(ctx, req)
		if err != nil {
			return err
		}
		for {
			match, err := dmi.Next()
			if err != nil {
				return err
			}
			if match == nil {
				break
			}
			if err := visit(match); err != nil {
				return err
			}
		}
		if agg := dmi.Aggregations(); agg != nil {
			total = agg.Count()
		}
		return nil
	})
	return total, err
}

func (ix *Index) Close() error {
	ix.mu.Lock()
	if ix.reader != nil {
		_ = ix.reader.Close()
		ix.reader = nil
	}
	ix.mu.Unlock()
	return ix.writer.Close()
}
