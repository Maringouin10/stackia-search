package crawler

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

// robotsCache fetches and caches per-host robots.txt rules. Entries expire so a
// long-running crawl eventually re-reads changed policies. A negative/failed
// fetch caches an "allow all" group so we don't hammer a host that has no
// robots.txt.
type robotsCache struct {
	client    *http.Client
	userAgent string
	ttl       time.Duration

	mu      sync.Mutex
	entries map[string]*robotsEntry
	// single-flight: hosts currently being fetched, so N workers hitting a new
	// host issue exactly one robots.txt request.
	inflight map[string]*sync.WaitGroup
}

type robotsEntry struct {
	group   *robotstxt.Group
	fetched time.Time
}

func newRobotsCache(client *http.Client, ua string, ttl time.Duration) *robotsCache {
	return &robotsCache{
		client:    client,
		userAgent: ua,
		ttl:       ttl,
		entries:   make(map[string]*robotsEntry),
		inflight:  make(map[string]*sync.WaitGroup),
	}
}

// Allowed reports whether the target URL may be fetched, plus the host's
// crawl-delay (zero if unspecified).
func (rc *robotsCache) Allowed(ctx context.Context, u *url.URL) (bool, time.Duration) {
	g := rc.groupFor(ctx, u.Scheme+"://"+u.Host)
	if g == nil {
		return true, 0
	}
	return g.Test(u.Path), g.CrawlDelay
}

func (rc *robotsCache) groupFor(ctx context.Context, origin string) *robotstxt.Group {
	rc.mu.Lock()
	if e, ok := rc.entries[origin]; ok && time.Since(e.fetched) < rc.ttl {
		rc.mu.Unlock()
		return e.group
	}
	// If another goroutine is already fetching this origin, wait for it.
	if wg, ok := rc.inflight[origin]; ok {
		rc.mu.Unlock()
		wg.Wait()
		rc.mu.Lock()
		e := rc.entries[origin]
		rc.mu.Unlock()
		if e != nil {
			return e.group
		}
		return nil
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	rc.inflight[origin] = wg
	rc.mu.Unlock()

	group := rc.fetch(ctx, origin)

	rc.mu.Lock()
	rc.entries[origin] = &robotsEntry{group: group, fetched: time.Now()}
	delete(rc.inflight, origin)
	rc.mu.Unlock()
	wg.Done()
	return group
}

func (rc *robotsCache) fetch(ctx context.Context, origin string) *robotstxt.Group {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/robots.txt", nil)
	if err != nil {
		return allowAll()
	}
	req.Header.Set("User-Agent", rc.userAgent)
	resp, err := rc.client.Do(req)
	if err != nil {
		return allowAll()
	}
	defer resp.Body.Close()

	// Per RFC 9309: 4xx (incl. 404) means "no restrictions".
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return allowAll()
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512<<10)) // 512 KiB cap
	if err != nil {
		return allowAll()
	}
	data, err := robotstxt.FromBytes(body)
	if err != nil {
		return allowAll()
	}
	return data.FindGroup(rc.userAgent)
}

func allowAll() *robotstxt.Group {
	data, _ := robotstxt.FromBytes([]byte(""))
	return data.FindGroup("*")
}
