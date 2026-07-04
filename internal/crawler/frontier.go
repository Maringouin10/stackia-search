package crawler

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Maringouin10/stackia-search/internal/metrics"
)

// task is one unit of crawl work.
type task struct {
	url   string
	host  string // host:port — the politeness key
	depth int
}

// domainQueue is a per-host FIFO plus its politeness clock. A head index avoids
// re-slicing churn; the backing array is reset once fully drained.
type domainQueue struct {
	items    []task
	head     int
	nextTime time.Time     // earliest time the next fetch may start
	inflight int           // in-progress fetches for this host
	delay    time.Duration // robots.txt crawl-delay override (>= politeness min)
}

func (dq *domainQueue) len() int { return len(dq.items) - dq.head }

func (dq *domainQueue) pop() task {
	t := dq.items[dq.head]
	dq.head++
	if dq.head == len(dq.items) {
		dq.items = dq.items[:0]
		dq.head = 0
	}
	return t
}

// frontier is a bounded, politeness-aware URL scheduler. A single dispatcher
// goroutine round-robins across hosts and pushes ready tasks onto `ready`,
// which the crawl workers consume. De-duplication is delegated to the mmap
// Bloom filter so a URL is enqueued at most once.
type frontier struct {
	mu      sync.Mutex
	domains map[string]*domainQueue
	order   []string // round-robin host ring
	cursor  int

	perHostDelay    time.Duration
	perHostInflight int
	queueCap        int64
	maxDepth        int

	pending atomic.Int64
	ready   chan task
	bloom   *Bloom
	metrics *metrics.Metrics
}

func newFrontier(b *Bloom, m *metrics.Metrics, perHostDelay time.Duration,
	perHostInflight, queueCap, maxDepth int) *frontier {
	if perHostInflight < 1 {
		perHostInflight = 1
	}
	return &frontier{
		domains:         make(map[string]*domainQueue),
		perHostDelay:    perHostDelay,
		perHostInflight: perHostInflight,
		queueCap:        int64(queueCap),
		maxDepth:        maxDepth,
		ready:           make(chan task, perHostInflight*64),
		bloom:           b,
		metrics:         m,
	}
}

// Add normalises, dedups and enqueues a URL at the given depth. It is safe for
// many goroutines. When the frontier is at capacity the URL is dropped without
// being marked seen, so it can be rediscovered once the queue drains
// (backpressure rather than permanent loss).
func (f *frontier) Add(raw string, depth int) {
	if depth > f.maxDepth {
		return
	}
	u := normalizeURL(raw)
	if u == nil {
		return
	}
	f.metrics.URLsSeen.Add(1)
	norm := u.String()

	f.mu.Lock()
	if f.pending.Load() >= f.queueCap {
		f.mu.Unlock()
		return
	}
	if f.bloom.TestAndAdd([]byte(norm)) {
		f.mu.Unlock()
		f.metrics.URLsDeduped.Add(1)
		return
	}
	host := u.Host
	dq := f.domains[host]
	if dq == nil {
		dq = &domainQueue{}
		f.domains[host] = dq
		f.order = append(f.order, host)
	}
	dq.items = append(dq.items, task{url: norm, host: host, depth: depth})
	f.mu.Unlock()

	n := f.pending.Add(1)
	f.metrics.QueueDepth.Store(n)
}

// Done releases an in-flight slot for a host after its fetch completes.
func (f *frontier) Done(host string) {
	f.mu.Lock()
	if dq := f.domains[host]; dq != nil && dq.inflight > 0 {
		dq.inflight--
	}
	f.mu.Unlock()
}

// NoteCrawlDelay records a robots.txt crawl-delay so future dispatches for the
// host respect it (never shorter than the configured politeness minimum).
func (f *frontier) NoteCrawlDelay(host string, d time.Duration) {
	if d <= 0 {
		return
	}
	f.mu.Lock()
	if dq := f.domains[host]; dq != nil && d > dq.delay {
		dq.delay = d
	}
	f.mu.Unlock()
}

// Next is the channel crawl workers read ready tasks from.
func (f *frontier) Next() <-chan task { return f.ready }

// run is the dispatcher loop. It scans the host ring; for each host that has
// work, a free in-flight slot and a satisfied politeness clock, it pops one
// task and hands it to a worker.
func (f *frontier) run(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		dispatched := f.sweep(ctx)
		if dispatched == 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

// sweep does one full pass over the host ring, dispatching at most one task per
// eligible host. It returns how many tasks it handed off.
func (f *frontier) sweep(ctx context.Context) int {
	dispatched := 0
	f.mu.Lock()
	n := len(f.order)
	now := time.Now()
	for scanned := 0; scanned < n; scanned++ {
		if len(f.order) == 0 {
			break
		}
		f.cursor = (f.cursor + 1) % len(f.order)
		dom := f.order[f.cursor]
		dq := f.domains[dom]
		if dq == nil || dq.len() == 0 || dq.inflight >= f.perHostInflight || now.Before(dq.nextTime) {
			continue
		}
		delay := f.perHostDelay
		if dq.delay > delay {
			delay = dq.delay
		}
		t := dq.pop()
		dq.inflight++
		dq.nextTime = now.Add(delay)

		f.mu.Unlock()
		select {
		case f.ready <- t:
			n := f.pending.Add(-1)
			f.metrics.QueueDepth.Store(n)
			dispatched++
		case <-ctx.Done():
			return dispatched
		}
		f.mu.Lock()
		now = time.Now()
	}
	f.mu.Unlock()
	return dispatched
}

// normalizeURL canonicalises a URL for de-duplication: http(s) only, lowercased
// host, no fragment, no default port. Returns nil for anything uncrawlable.
func normalizeURL(raw string) *url.URL {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 2048 {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil
	}
	if u.Host == "" {
		return nil
	}
	u.Fragment = ""
	u.Host = strings.ToLower(u.Host)
	// Drop the default port so http://x:80 and http://x are one key.
	if (u.Scheme == "http" && strings.HasSuffix(u.Host, ":80")) ||
		(u.Scheme == "https" && strings.HasSuffix(u.Host, ":443")) {
		u.Host = u.Host[:strings.LastIndex(u.Host, ":")]
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u
}
