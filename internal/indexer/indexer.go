// Package indexer is the ingest pipeline: it receives raw pages from the
// crawler, parses each once (extracting text, metadata and links), and batches
// the results into the inverted index (Bluge) and the KV store (BadgerDB).
//
// Two stages, both bounded:
//   - extraction workers (CPU-bound HTML parsing), sized to the core count;
//   - a single committer that coalesces documents into large, sequential
//     batch writes — the pattern LSM stores and segment indexes love.
//
// Batching + a single committer keep write amplification and peak RAM low,
// which is what makes 20+ pages/s sustainable on a Pi.
package indexer

import (
	"context"
	"net/url"
	"runtime"
	"sync"
	"time"

	"github.com/Maringouin10/stackia-search/internal/config"
	"github.com/Maringouin10/stackia-search/internal/index"
	"github.com/Maringouin10/stackia-search/internal/metrics"
	"github.com/Maringouin10/stackia-search/internal/model"
	"github.com/Maringouin10/stackia-search/internal/store"
)

type Pipeline struct {
	cfg     config.IndexConfig
	in      <-chan model.RawPage
	ix      *index.Index
	kv      *store.Store
	metrics *metrics.Metrics

	// OnLinks receives discovered links (already absolute) at the next depth.
	// Wired to the crawler's scoped Enqueue in main.
	OnLinks func(links []string, depth int)

	extractWorkers int
	docs           chan *model.Doc

	refreshEvery time.Duration
	lastRefresh  time.Time
}

func New(cfg config.IndexConfig, in <-chan model.RawPage, ix *index.Index,
	kv *store.Store, m *metrics.Metrics) *Pipeline {
	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}
	return &Pipeline{
		cfg:            cfg,
		in:             in,
		ix:             ix,
		kv:             kv,
		metrics:        m,
		extractWorkers: workers,
		docs:           make(chan *model.Doc, cfg.BatchSize*2),
		refreshEvery:   time.Second, // cap searcher-snapshot churn
	}
}

// Run launches the extraction workers and the committer, blocking until ctx is
// cancelled and the final batch is flushed.
func (p *Pipeline) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(p.extractWorkers)
	for i := 0; i < p.extractWorkers; i++ {
		go func() {
			defer wg.Done()
			p.extractLoop(ctx)
		}()
	}

	// Committer runs in its own goroutine; we wait on it so the last batch is
	// durably flushed before Run returns.
	done := make(chan struct{})
	go func() {
		p.commitLoop(ctx)
		close(done)
	}()

	wg.Wait()     // extraction workers stop first (ctx done or `in` closed)
	close(p.docs) // signal committer no more docs are coming
	<-done
}

func (p *Pipeline) extractLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case raw, ok := <-p.in:
			if !ok {
				return
			}
			p.handle(ctx, raw)
		}
	}
}

func (p *Pipeline) handle(ctx context.Context, raw model.RawPage) {
	canonical := raw.FinalURL
	if canonical == "" {
		canonical = raw.URL
	}
	base, err := url.Parse(canonical)
	if err != nil {
		return
	}

	ex := extract(base, raw.Body, p.cfg.MaxTextBytes)
	p.metrics.LinksFound.Add(uint64(len(ex.links)))

	// Feed discovered links back to the crawler at the next depth.
	if p.OnLinks != nil && len(ex.links) > 0 {
		p.OnLinks(ex.links, raw.Depth+1)
	}

	title := ex.title
	if title == "" {
		title = raw.Host
	}
	snippet := ex.description
	if snippet == "" {
		snippet = truncate(ex.text, p.cfg.SnippetBytes)
	} else {
		snippet = truncate(snippet, p.cfg.SnippetBytes)
	}

	doc := &model.Doc{
		URL:         canonical,
		Host:        raw.Host,
		Title:       title,
		Description: ex.description,
		Text:        ex.text,
		Snippet:     snippet,
		FetchedAt:   raw.FetchedAt,
		Depth:       raw.Depth,
	}
	select {
	case p.docs <- doc:
	case <-ctx.Done():
	}
}

func (p *Pipeline) commitLoop(ctx context.Context) {
	interval := time.Duration(p.cfg.FlushInterval) * time.Millisecond
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make([]*model.Doc, 0, p.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.flush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case d, ok := <-p.docs:
			if !ok {
				flush() // channel closed: commit the remainder and exit
				return
			}
			batch = append(batch, d)
			if len(batch) >= p.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flush writes one batch to the index and the KV store, then (throttled)
// publishes a fresh searcher snapshot.
func (p *Pipeline) flush(batch []*model.Doc) {
	if err := p.ix.IndexBatch(batch); err != nil {
		p.metrics.IndexErrors.Add(uint64(len(batch)))
		return
	}

	wb := p.kv.NewBatch()
	ok := true
	for _, d := range batch {
		meta := &model.Meta{
			URL:         d.URL,
			Title:       d.Title,
			Description: d.Description,
			Snippet:     d.Snippet,
			Host:        d.Host,
			FetchedAt:   d.FetchedAt.Unix(),
		}
		if err := wb.PutMeta(meta); err != nil {
			ok = false
			break
		}
	}
	if !ok {
		wb.Cancel()
		p.metrics.IndexErrors.Add(uint64(len(batch)))
		return
	}
	if err := wb.Flush(); err != nil {
		p.metrics.IndexErrors.Add(uint64(len(batch)))
		return
	}

	p.metrics.DocsIndexed.Add(uint64(len(batch)))

	// Publishing a new reader is comparatively costly; throttle it so a burst
	// of small batches doesn't thrash snapshots.
	if time.Since(p.lastRefresh) >= p.refreshEvery {
		if err := p.ix.Refresh(); err == nil {
			p.lastRefresh = time.Now()
		}
	}
}

// truncate cuts s to at most n bytes on a rune boundary, appending an ellipsis.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8Start(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// utf8Start reports whether b is the first byte of a UTF-8 rune.
func utf8Start(b byte) bool { return b&0xC0 != 0x80 }
