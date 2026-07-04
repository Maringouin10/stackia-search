# Stackia Search

A production-lean **web search engine in a single Go binary**, engineered to run
on a Raspberry Pi. Crawler, inverted index, KV store, search API and a live
analytics dashboard are all wired into one process with a hard focus on
**bounded memory, high I/O concurrency and sub-100 ms queries**.

**Targets:** ~10M pages · 20+ pages/second crawl · < 100 ms search · runs in a few
hundred MB of RAM.

Verified end-to-end locally: crawl → extract → index → search all pass, queries
resolve in **well under 1 ms** on a small corpus, resident memory sits at ~20 MB,
and the memory-mapped Bloom filter persists de-dup state across restarts.

---

## Architecture at a glance

```
                      ┌──────────────────────────────────────────────────────────┐
                      │                     stackia (one binary)                   │
                      │                                                            │
  seeds ──▶  ┌────────┴─────────┐   RawPage    ┌──────────────────┐               │
             │     CRAWLER       │  (channel)   │     INDEXER      │               │
             │  worker pool      ├─────────────▶│  extract (1 pass)│               │
             │  HTTP keep-alive  │              │  → text/meta     │               │
             │  robots.txt       │◀─────────────┤  → links (sink)  │               │
             │  per-host limits  │  discovered  │  batch writer    │               │
             └───────┬───────────┘   links      └───────┬──────────┘               │
                     │                                   │                          │
             ┌───────▼────────┐                  ┌───────▼────────┐  ┌───────────┐  │
             │  FRONTIER       │                  │  Bluge INDEX  │  │ BadgerDB  │  │
             │  bounded queue  │                  │  inverted +   │  │ meta /    │  │
             │  round-robin    │                  │  BM25 + EN    │  │ snippets  │  │
             └───────┬────────┘                  │  analyzer     │  │ (LSM)     │  │
                     │                            └───────┬───────┘  └─────┬─────┘  │
          ┌──────────▼───────────┐                       │  cached          │        │
          │  mmap BLOOM FILTER   │                ┌──────▼──────────────────▼─────┐  │
          │  O(1) URL de-dup     │                │           SEARCH             │  │
          │  MAP_SHARED, atomic  │                │  parse → BM25 → hydrate KV   │  │
          └──────────────────────┘                └──────────────┬───────────────┘  │
                                                                  │                  │
                              ┌───────────────────────────────────▼───────────────┐ │
                              │  FIBER HTTP  ·  /  /dashboard  /api/search         │ │
                              │  /ws/stats (live crawl-rate, RAM/CPU, index size)  │ │
                              └────────────────────────────────────────────────────┘ │
                      └──────────────────────────────────────────────────────────────┘
```

Pick of components, and why each suits constrained hardware:

| Concern | Choice | Why on a Pi |
|---|---|---|
| Language | **Go** | Cheap goroutines for massive I/O concurrency; one static binary; GC with a soft memory cap. |
| Inverted index | **Bluge** | Embedded, segment-based, **BM25 by default**, English analyzer (tokenize → stop-words → Snowball stem). Segments memory-map cleanly. |
| KV / doc store | **BadgerDB** (LSM) | Sequential writes (kind to SD cards), explicitly **bounded caches**, ZSTD compression. |
| URL de-dup | **Memory-mapped Bloom filter** | `MAP_SHARED` so the OS page cache owns it — resident memory tracks the working set, not the full 11 MB filter; lock-free atomic bit-sets; persists across restarts with zero serialization. |
| Politeness | Per-host FIFO + delay + robots.txt | Round-robin dispatcher enforces crawl-delay and per-host in-flight caps. |
| API / UI | **Fiber** + `go:embed` | Tiny HTTP layer; the SPA + dashboard are baked into the binary. |
| Live metrics | WebSocket + gopsutil | Real-time crawl rate, CPU%, RSS, Go heap, index/store size. |

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the design deep-dive
(memory model, backpressure, latency budget, scaling to 10M).

---

## Directory layout

```
stackia-search/
├── cmd/
│   └── stackia/
│       └── main.go            # entrypoint: config, runtime tuning, wiring, lifecycle
├── internal/
│   ├── config/                # one struct of tunables, YAML-loaded, Pi-sized defaults
│   │   └── config.go
│   ├── model/                 # dependency-free shared types (RawPage, Doc, Meta, results)
│   │   └── model.go
│   ├── crawler/               # the async fetch engine
│   │   ├── crawler.go         #   worker pool + tuned HTTP transport + scope
│   │   ├── frontier.go        #   bounded, politeness-aware URL scheduler
│   │   ├── robots.go          #   cached, single-flight robots.txt
│   │   └── bloom.go           #   memory-mapped Bloom filter (mmap, atomic CAS)
│   ├── indexer/               # ingest pipeline
│   │   ├── indexer.go         #   extract workers + single batching committer
│   │   └── extract.go         #   single-pass HTML → text / meta / links
│   ├── index/                 # Bluge wrapper: BM25, EN analyzer, cached reader
│   │   └── index.go
│   ├── store/                 # BadgerDB wrapper (bounded caches, batch writes)
│   │   └── store.go
│   ├── search/                # query parser + execution (sub-100 ms)
│   │   └── search.go
│   ├── metrics/               # atomic counters + gopsutil system gauges
│   │   └── metrics.go
│   └── api/                   # Fiber server
│       ├── server.go          #   routes + embedded HTML
│       ├── handlers.go        #   /api/search, /api/stats
│       └── ws.go              #   /ws/stats live stream
├── web/
│   ├── embed.go               # go:embed of the two pages
│   └── static/
│       ├── index.html         # search SPA (Tailwind)
│       └── dashboard.html     # live admin dashboard (WebSocket + canvas chart)
├── config.yaml                # commented, Pi-tuned configuration
├── Dockerfile                 # multi-arch (arm64) distroless image
├── Makefile                   # build / run / cross-compile (pi, pi32)
├── go.mod / go.sum
└── docs/
    └── ARCHITECTURE.md
```

---

## Quick start

```bash
# 1. Build
make build            # -> ./stackia

# 2. Point it at some seeds (edit config.yaml, or pass -seeds)
./stackia -seeds "https://en.wikipedia.org/wiki/Search_engine"

# 3. Open the UI
#    http://localhost:8080/            search
#    http://localhost:8080/dashboard   live crawl analytics
```

Cross-compile for the Pi and copy it over:

```bash
make pi                          # -> ./stackia-arm64 (64-bit Pi 3/4/5)
scp stackia-arm64 config.yaml pi@raspberrypi:~/
ssh pi@raspberrypi './stackia-arm64 -config config.yaml'
```

Or run in Docker:

```bash
docker buildx build --platform linux/arm64 -t stackia:pi .
docker run -p 8080:8080 -v stackia-data:/app/data stackia:pi
```

---

## HTTP API

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Search UI (single page) |
| `GET` | `/dashboard` | Live analytics dashboard |
| `GET` | `/api/search?q=<query>&n=<count>` | JSON search. Supports `"quoted phrases"` and `site:host` filters. |
| `GET` | `/api/stats` | One-shot metrics snapshot (JSON) |
| `GET` | `/ws/stats` | WebSocket stream of the same snapshot, 1 Hz |
| `GET` | `/healthz` | Liveness probe |

Example:

```bash
curl 'http://localhost:8080/api/search?q=inverted+index&n=10'
# {"query":"inverted index","total":4,"took_ms":0.18,"results":[ ... ]}
```

---

## Memory & performance tuning

Everything that matters for staying inside a Pi's RAM budget is in
`config.yaml` under `runtime`, `store`, `index` and `bloom`:

- **`runtime.mem_limit_mb`** → `GOMEMLIMIT`, a *soft* heap cap; the GC works
  harder as the heap approaches it instead of OOMing.
- **`runtime.gc_percent`** → `GOGC` (default here 40, vs Go's 100) for a smaller
  live heap at the cost of more frequent collections.
- **`runtime.free_os_memory_secs`** → periodic `debug.FreeOSMemory()` so freed
  pages are actually returned to the kernel and RSS shrinks.
- **`store.*_cache_mb`** → BadgerDB block/index caches are the biggest RAM lever;
  they are explicitly small here.
- **`crawler.max_body_bytes`** → hard per-page body cap (OOM guard against a
  multi-GB response).
- **`crawler.queue_capacity`** → bounded crawler→indexer hand-off; when the
  indexer falls behind, the crawler *blocks* (backpressure) rather than buffering
  unbounded pages in memory.

## License

MIT — see [`LICENSE`](LICENSE).
