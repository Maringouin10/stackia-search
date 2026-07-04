// Package metrics collects the live counters and system gauges that drive the
// admin dashboard. Hot-path updates are lock-free atomics; the expensive
// samples (CPU%, RSS, index size on disk) are taken on a background ticker so
// the crawl loop never blocks on syscalls.
package metrics

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/process"
)

// Metrics is safe for concurrent use. Counters are updated with atomic ops on
// the crawl/index hot paths; the gauges are refreshed by Sample().
type Metrics struct {
	// crawl counters
	PagesFetched atomic.Uint64
	PagesFailed  atomic.Uint64
	BytesFetched atomic.Uint64
	LinksFound   atomic.Uint64
	URLsSeen     atomic.Uint64
	URLsDeduped  atomic.Uint64

	// index counters
	DocsIndexed atomic.Uint64
	IndexErrors atomic.Uint64

	// queue gauges (set by the frontier)
	QueueDepth   atomic.Int64
	ActiveWorker atomic.Int64

	// derived rate
	crawlRate atomic.Uint64 // pages/sec * 1000, fixed-point

	// system gauges (refreshed by Sample)
	cpuPct    atomic.Uint64 // *100
	rssBytes  atomic.Uint64
	heapBytes atomic.Uint64
	sysBytes  atomic.Uint64
	numGC     atomic.Uint64
	goroutine atomic.Int64
	indexSize atomic.Uint64
	storeSize atomic.Uint64

	startedAt time.Time

	proc  *process.Process
	lastN uint64
	lastT time.Time
	mu    sync.Mutex
}

func New() *Metrics {
	m := &Metrics{startedAt: time.Now(), lastT: time.Now()}
	if p, err := process.NewProcess(int32(os.Getpid())); err == nil {
		m.proc = p
	}
	return m
}

// Snapshot is a flat, JSON-friendly view for the API/WebSocket layer.
type Snapshot struct {
	UptimeSec    float64 `json:"uptime_sec"`
	PagesFetched uint64  `json:"pages_fetched"`
	PagesFailed  uint64  `json:"pages_failed"`
	CrawlRate    float64 `json:"crawl_rate"` // pages/sec
	BytesFetched uint64  `json:"bytes_fetched"`
	LinksFound   uint64  `json:"links_found"`
	URLsSeen     uint64  `json:"urls_seen"`
	URLsDeduped  uint64  `json:"urls_deduped"`
	DocsIndexed  uint64  `json:"docs_indexed"`
	IndexErrors  uint64  `json:"index_errors"`
	QueueDepth   int64   `json:"queue_depth"`
	Workers      int64   `json:"active_workers"`
	CPUPercent   float64 `json:"cpu_percent"`
	RSSBytes     uint64  `json:"rss_bytes"`
	HeapBytes    uint64  `json:"heap_bytes"`
	SysBytes     uint64  `json:"sys_bytes"`
	NumGC        uint64  `json:"num_gc"`
	Goroutines   int64   `json:"goroutines"`
	IndexBytes   uint64  `json:"index_bytes"`
	StoreBytes   uint64  `json:"store_bytes"`
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		UptimeSec:    time.Since(m.startedAt).Seconds(),
		PagesFetched: m.PagesFetched.Load(),
		PagesFailed:  m.PagesFailed.Load(),
		CrawlRate:    float64(m.crawlRate.Load()) / 1000.0,
		BytesFetched: m.BytesFetched.Load(),
		LinksFound:   m.LinksFound.Load(),
		URLsSeen:     m.URLsSeen.Load(),
		URLsDeduped:  m.URLsDeduped.Load(),
		DocsIndexed:  m.DocsIndexed.Load(),
		IndexErrors:  m.IndexErrors.Load(),
		QueueDepth:   m.QueueDepth.Load(),
		Workers:      m.ActiveWorker.Load(),
		CPUPercent:   float64(m.cpuPct.Load()) / 100.0,
		RSSBytes:     m.rssBytes.Load(),
		HeapBytes:    m.heapBytes.Load(),
		SysBytes:     m.sysBytes.Load(),
		NumGC:        m.numGC.Load(),
		Goroutines:   m.goroutine.Load(),
		IndexBytes:   m.indexSize.Load(),
		StoreBytes:   m.storeSize.Load(),
	}
}

// StartSampler refreshes system gauges every `every` until ctx-like stop chan
// is closed. It is cheap: one CPU read, one RSS read, one MemStats read, and a
// throttled directory walk for on-disk index/store sizes.
func (m *Metrics) StartSampler(stop <-chan struct{}, every time.Duration, indexPath, storePath string) {
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		var diskEvery int
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				m.sampleRate()
				m.sampleCPU()
				m.sampleMem()
				// Directory walks are comparatively expensive; do them at 1/5
				// the cadence of the cheap gauges.
				diskEvery++
				if diskEvery%5 == 0 {
					m.indexSize.Store(dirSize(indexPath))
					m.storeSize.Store(dirSize(storePath))
				}
			}
		}
	}()
}

func (m *Metrics) sampleRate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	n := m.PagesFetched.Load()
	dt := now.Sub(m.lastT).Seconds()
	if dt > 0 {
		inst := float64(n-m.lastN) / dt
		// Exponential moving average smooths the jitter from bursty fetches.
		prev := float64(m.crawlRate.Load()) / 1000.0
		ema := prev*0.6 + inst*0.4
		m.crawlRate.Store(uint64(ema * 1000))
	}
	m.lastN = n
	m.lastT = now
}

func (m *Metrics) sampleCPU() {
	// Non-blocking read (interval 0) returns usage since the previous call.
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		m.cpuPct.Store(uint64(pcts[0] * 100))
	}
}

func (m *Metrics) sampleMem() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.heapBytes.Store(ms.HeapAlloc)
	m.sysBytes.Store(ms.Sys)
	m.numGC.Store(uint64(ms.NumGC))
	m.goroutine.Store(int64(runtime.NumGoroutine()))
	if m.proc != nil {
		if mi, err := m.proc.MemoryInfo(); err == nil && mi != nil {
			m.rssBytes.Store(mi.RSS)
		}
	}
}

func dirSize(root string) uint64 {
	var total uint64
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // best-effort; skip unreadable entries
		}
		if d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}
