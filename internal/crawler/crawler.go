// Package crawler is the asynchronous fetch engine: a bounded worker pool that
// pulls URLs from a politeness-aware frontier, fetches them over a connection-
// reusing HTTP client, and streams raw pages to the indexing pipeline. URL
// de-duplication is O(1) via a memory-mapped Bloom filter; per-host rate limits
// and robots.txt are enforced before every request.
package crawler

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/publicsuffix"

	"github.com/Maringouin10/stackia-search/internal/config"
	"github.com/Maringouin10/stackia-search/internal/metrics"
	"github.com/Maringouin10/stackia-search/internal/model"
)

type Crawler struct {
	cfg      config.CrawlerConfig
	client   *http.Client
	frontier *frontier
	robots   *robotsCache
	bloom    *Bloom
	metrics  *metrics.Metrics

	out chan<- model.RawPage // hand-off to the indexer pipeline

	// scope: hosts / registered domains we are allowed to crawl.
	scopeHosts map[string]struct{}
	scopeETLD  map[string]struct{}
	allowSub   bool
}

// New builds a crawler. `out` is the channel raw pages are pushed onto; the
// indexer owns the receiving end.
func New(cfg config.CrawlerConfig, bloom *Bloom, m *metrics.Metrics, out chan<- model.RawPage) *Crawler {
	client := newHTTPClient(cfg)
	fr := newFrontier(bloom, m, time.Duration(cfg.PerHostDelayMS)*time.Millisecond,
		cfg.PerHostInflight, cfg.QueueCapacity, cfg.MaxDepth)

	c := &Crawler{
		cfg:        cfg,
		client:     client,
		frontier:   fr,
		robots:     newRobotsCache(client, cfg.UserAgent, 30*time.Minute),
		bloom:      bloom,
		metrics:    m,
		out:        out,
		scopeHosts: make(map[string]struct{}),
		scopeETLD:  make(map[string]struct{}),
		allowSub:   cfg.AllowSubdomains,
	}
	c.buildScope()
	return c
}

// newHTTPClient returns a client tuned for high-concurrency crawling: generous
// connection pooling and keep-alive reuse, aggressive but bounded timeouts, and
// HTTP/2 enabled. Connection reuse is the single biggest throughput lever.
func newHTTPClient(cfg config.CrawlerConfig) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.Workers * 2,
		MaxIdleConnsPerHost:   cfg.PerHostInflight + 2, // reuse per-host sockets
		MaxConnsPerHost:       cfg.PerHostInflight * 2,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   6 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: time.Duration(cfg.RequestTimeout) * time.Millisecond,
		WriteBufferSize:       16 << 10,
		ReadBufferSize:        16 << 10,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(cfg.RequestTimeout)*time.Millisecond + 5*time.Second,
		// Cap redirect chains; too many usually signals a trap.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// buildScope seeds the crawl scope from the configured seed URLs.
func (c *Crawler) buildScope() {
	for _, s := range c.cfg.Seeds {
		u := normalizeURL(s)
		if u == nil {
			continue
		}
		c.scopeHosts[u.Hostname()] = struct{}{}
		if etld, err := publicsuffix.EffectiveTLDPlusOne(u.Hostname()); err == nil {
			c.scopeETLD[etld] = struct{}{}
		}
	}
}

// inScope decides whether a discovered host may be crawled. With no seeds
// configured the crawl is unrestricted (open-web mode); otherwise it stays on
// the seed hosts, optionally including their subdomains.
func (c *Crawler) inScope(host string) bool {
	if len(c.scopeHosts) == 0 {
		return true
	}
	if _, ok := c.scopeHosts[host]; ok {
		return true
	}
	if c.allowSub {
		if etld, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
			if _, ok := c.scopeETLD[etld]; ok {
				return true
			}
		}
	}
	return false
}

// Enqueue is the sink for links discovered by the indexer. It applies scope
// before handing off to the frontier (which dedups and rate-limits).
func (c *Crawler) Enqueue(links []string, depth int) {
	for _, l := range links {
		u := normalizeURL(l)
		if u == nil || !c.inScope(u.Hostname()) {
			continue
		}
		c.frontier.Add(u.String(), depth)
	}
}

// Seed injects the configured seed URLs at depth 0.
func (c *Crawler) Seed() {
	for _, s := range c.cfg.Seeds {
		c.frontier.Add(s, 0)
	}
}

// Run starts the dispatcher and the worker pool, blocking until ctx is done.
func (c *Crawler) Run(ctx context.Context) {
	go c.frontier.run(ctx)

	var wg sync.WaitGroup
	wg.Add(c.cfg.Workers)
	for i := 0; i < c.cfg.Workers; i++ {
		go func() {
			defer wg.Done()
			c.worker(ctx)
		}()
	}
	wg.Wait()
}

func (c *Crawler) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-c.frontier.Next():
			c.metrics.ActiveWorker.Add(1)
			c.process(ctx, t)
			c.frontier.Done(t.host)
			c.metrics.ActiveWorker.Add(-1)
		}
	}
}

func (c *Crawler) process(ctx context.Context, t task) {
	u, err := url.Parse(t.url)
	if err != nil {
		return
	}

	// robots.txt gate + crawl-delay.
	if c.cfg.RespectRobots {
		allowed, delay := c.robots.Allowed(ctx, u)
		if !allowed {
			return
		}
		if delay > 0 {
			c.frontier.NoteCrawlDelay(t.host, delay)
		}
	}

	body, finalURL, ctype, status, err := c.fetch(ctx, t.url)
	if err != nil {
		c.metrics.PagesFailed.Add(1)
		return
	}
	c.metrics.PagesFetched.Add(1)
	c.metrics.BytesFetched.Add(uint64(len(body)))

	// Only HTML is worth extracting; other content types are counted but not
	// indexed.
	if !isHTML(ctype) {
		return
	}

	fu := u
	if pu, err := url.Parse(finalURL); err == nil {
		fu = pu
	}
	page := model.RawPage{
		URL:         t.url,
		FinalURL:    finalURL,
		Host:        fu.Hostname(),
		Body:        body,
		ContentType: ctype,
		StatusCode:  status,
		FetchedAt:   time.Now(),
		Depth:       t.depth,
	}
	// Backpressure: block (respecting ctx) if the indexer is behind, so we
	// never balloon memory with un-indexed pages.
	select {
	case c.out <- page:
	case <-ctx.Done():
	}
}

// fetch performs one GET with a hard body-size cap (OOM guard) and returns the
// (bounded) body, final URL, content type and status.
func (c *Crawler) fetch(ctx context.Context, target string) (body []byte, finalURL, ctype string, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, "", "", 0, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Encoding", "gzip") // Go transport auto-decompresses

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, "", "", 0, err
	}
	defer func() {
		// Drain a little so the connection can be reused, then close.
		_, _ = io.CopyN(io.Discard, resp.Body, 4<<10)
		_ = resp.Body.Close()
	}()

	finalURL = resp.Request.URL.String()
	ctype = resp.Header.Get("Content-Type")
	status = resp.StatusCode
	if status < 200 || status >= 300 {
		return nil, finalURL, ctype, status, errNonOK
	}

	limited := io.LimitReader(resp.Body, c.cfg.MaxBodyBytes)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, finalURL, ctype, status, err
	}
	return b, finalURL, ctype, status, nil
}

func isHTML(ctype string) bool {
	if ctype == "" {
		return true // many servers omit it; let the extractor decide
	}
	ct := strings.ToLower(ctype)
	return strings.Contains(ct, "text/html") || strings.Contains(ct, "application/xhtml")
}

// errNonOK is a sentinel for non-2xx responses (kept package-local to avoid an
// allocation per failed fetch).
var errNonOK = errStr("non-2xx response")

type errStr string

func (e errStr) Error() string { return string(e) }
