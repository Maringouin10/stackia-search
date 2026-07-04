// Command stackia is the single-binary web search engine: crawler + indexer +
// search API + live dashboard, wired together and tuned for constrained
// hardware (Raspberry Pi class). Everything runs in one process; state lives in
// a memory-mapped Bloom filter, a Bluge index and a BadgerDB store on disk.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Maringouin10/stackia-search/internal/api"
	"github.com/Maringouin10/stackia-search/internal/config"
	"github.com/Maringouin10/stackia-search/internal/crawler"
	"github.com/Maringouin10/stackia-search/internal/index"
	"github.com/Maringouin10/stackia-search/internal/indexer"
	"github.com/Maringouin10/stackia-search/internal/metrics"
	"github.com/Maringouin10/stackia-search/internal/model"
	"github.com/Maringouin10/stackia-search/internal/search"
	"github.com/Maringouin10/stackia-search/internal/store"
)

func main() {
	var (
		configPath = flag.String("config", "config.yaml", "path to YAML config (optional)")
		addr       = flag.String("addr", "", "override server listen address, e.g. :8080")
		seedsFlag  = flag.String("seeds", "", "comma-separated seed URLs (overrides config)")
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *addr != "" {
		cfg.Server.Addr = *addr
	}
	if *seedsFlag != "" {
		cfg.Crawler.Seeds = splitCSV(*seedsFlag)
	}

	applyRuntimeTuning(cfg.Runtime)

	log.Printf("stackia starting: workers=%d mem_limit=%dMB gc=%d%% seeds=%d",
		cfg.Crawler.Workers, cfg.Runtime.MemLimitMB, cfg.Runtime.GCPercent, len(cfg.Crawler.Seeds))

	// --- storage layer ---
	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Fatalf("data dir: %v", err)
	}
	bloom, err := crawler.OpenBloom(cfg.Bloom.Path, cfg.Bloom.Capacity, cfg.Bloom.FalsePositive)
	if err != nil {
		log.Fatalf("bloom: %v", err)
	}
	kv, err := store.Open(cfg.Store)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	ix, err := index.Open(cfg.Index)
	if err != nil {
		log.Fatalf("index: %v", err)
	}

	// --- observability ---
	m := metrics.New()
	stop := make(chan struct{})
	m.StartSampler(stop, 2*time.Second, cfg.Index.Path, cfg.Store.Path)

	// --- pipeline wiring ---
	// Bounded hand-off channel: the crawler blocks when the indexer falls
	// behind, which is exactly the backpressure that keeps RAM flat.
	raw := make(chan model.RawPage, cfg.Crawler.QueueCapacity)

	cr := crawler.New(cfg.Crawler, bloom, m, raw)
	pipe := indexer.New(cfg.Index, raw, ix, kv, m)
	pipe.OnLinks = cr.Enqueue // discovered links flow back into the scoped frontier

	searcher := search.New(ix, kv)
	srv, err := api.New(searcher, m)
	if err != nil {
		log.Fatalf("api: %v", err)
	}

	// --- lifecycle ---
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		log.Printf("http listening on %s  (UI: /  dashboard: /dashboard)", cfg.Server.Addr)
		if err := srv.Listen(cfg.Server.Addr); err != nil {
			log.Printf("http server stopped: %v", err)
			cancel()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pipe.Run(ctx) }()
	go func() { defer wg.Done(); cr.Run(ctx) }()

	cr.Seed()
	go maintenance(ctx, cfg.Runtime, kv, bloom)

	<-ctx.Done()
	log.Printf("signal received, draining…")

	// Shutdown order: stop accepting HTTP, let crawler+indexer drain and flush
	// the final batch, then close storage cleanly.
	_ = srv.Shutdown()
	wg.Wait()
	close(stop)

	if err := ix.Close(); err != nil {
		log.Printf("index close: %v", err)
	}
	if err := kv.Close(); err != nil {
		log.Printf("store close: %v", err)
	}
	if err := bloom.Close(); err != nil {
		log.Printf("bloom close: %v", err)
	}
	log.Printf("stopped cleanly")
}

// applyRuntimeTuning sets the memory guards that keep the process from OOMing a
// Pi: a soft heap limit (GOMEMLIMIT), an aggressive GC target, and an optional
// GOMAXPROCS cap so the crawler doesn't starve the indexer/searcher.
func applyRuntimeTuning(rt config.RuntimeConfig) {
	if rt.MemLimitMB > 0 {
		// Soft limit: the GC runs more frequently as the heap approaches it,
		// trading CPU for a bounded footprint — the right trade on a Pi.
		debug.SetMemoryLimit(int64(rt.MemLimitMB) << 20)
	}
	if rt.GCPercent > 0 {
		debug.SetGCPercent(rt.GCPercent)
	}
	if rt.MaxProcs > 0 {
		runtime.GOMAXPROCS(rt.MaxProcs)
	}
}

// maintenance periodically returns freed memory to the OS (so RSS actually
// shrinks), checkpoints the Bloom filter to disk, and reclaims BadgerDB value
// log space.
func maintenance(ctx context.Context, rt config.RuntimeConfig, kv *store.Store, bloom *crawler.Bloom) {
	scavenge := time.Duration(rt.FreeOSMemorySecs) * time.Second
	if scavenge <= 0 {
		scavenge = time.Minute
	}
	freeTick := time.NewTicker(scavenge)
	gcTick := time.NewTicker(5 * time.Minute)
	syncTick := time.NewTicker(30 * time.Second)
	defer freeTick.Stop()
	defer gcTick.Stop()
	defer syncTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-freeTick.C:
			debug.FreeOSMemory() // hand unused heap pages back to the kernel
		case <-syncTick.C:
			_ = bloom.Sync() // checkpoint dedup bits (mmap MS_ASYNC)
		case <-gcTick.C:
			kv.RunGC() // reclaim BadgerDB value-log space
		}
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
