# Architecture & Design Notes

This document explains *why* Stackia is built the way it is, with the Raspberry
Pi constraints (a few hundred MB of usable RAM, a slow-ish SD card / USB SSD,
4 cores) driving nearly every decision.

## 1. One process, three subsystems, shared backpressure

```
crawler  ──RawPage channel──▶  indexer  ──batched writes──▶  index + store
   ▲                               │
   └──────── discovered links ◀────┘
                                    ▲
                          search ───┘ (cached reader snapshot)
```

The crawler and indexer are decoupled by a **single bounded channel** of
`RawPage` values (`crawler.queue_capacity`). This is the load-bearing design
choice for memory safety:

- When the indexer keeps up, pages flow straight through.
- When the indexer falls behind (segment merge, GC, disk stall), the channel
  fills and **crawler goroutines block on send**. Fetching throttles itself to
  the rate the indexer can absorb. Peak memory is therefore
  `≈ queue_capacity × max_body_bytes` plus fixed overhead — a number you can
  compute up front, not a mystery that OOMs at 3 a.m.

## 2. Crawler

**Worker pool.** `crawler.workers` goroutines each pull one task from the
frontier's `ready` channel, fetch, and push the result downstream. Because
crawling is I/O-bound, workers ≫ cores is correct; 64 workers idle at almost no
CPU while saturating the network.

**HTTP client.** A single shared `http.Transport` with a large idle-connection
pool and keep-alive is the biggest throughput lever — TLS + TCP handshakes
dominate per-request cost otherwise. HTTP/2 is enabled, buffers are sized down
(16 KiB) to save RAM across many concurrent conns, and every timeout is bounded
so a slow host can't pin a worker forever.

**Politeness (frontier).** URLs are bucketed per host. A single dispatcher
goroutine round-robins the host ring and releases a task only when that host's
(a) politeness clock has elapsed (`per_host_delay_ms`, raised to any robots.txt
`Crawl-delay`) and (b) in-flight count is below `per_host_inflight`. This gives
global concurrency *and* per-host courtesy without a lock per URL.

**robots.txt** is fetched once per host, cached with a TTL, and guarded by a
single-flight so N workers hitting a fresh host issue exactly one probe. A 4xx
(including 404) is treated as "allow all" per RFC 9309.

**De-duplication** is delegated to the Bloom filter (below), so a URL is
enqueued at most once and the frontier never balloons with duplicates.

## 3. Memory-mapped Bloom filter

The de-dup structure is the clearest example of designing for the Pi:

- The bit array is a file mapped `MAP_SHARED`. The **OS page cache owns the
  pages**; resident memory grows with the *working set of touched pages*, not
  the full filter. Cold regions get evicted under pressure and paged back on
  demand.
- **Persistence is free** — the mapping *is* the file. On restart we re-`mmap`
  and de-dup state is intact (verified: a restarted crawl re-seeds, sees the
  seed already-marked, and fetches nothing).
- **Lock-free writes**: each probe sets one bit via a `CompareAndSwapUint64`
  loop on the 8-byte-aligned mapped word. No mutex on the hottest path in the
  crawler.
- **Sizing**: standard formulas. 10M URLs at p=1% → m ≈ 95.8 Mbit ≈ **11.4 MiB**,
  k = 7 probes. Two independent 64-bit FNV-1a hashes feed
  Kirsch–Mitzenmacher double hashing (`h1 + i·h2`) to synthesize all k probes in
  one pass — and, crucially, the hashes are **seedless/deterministic** so the
  persisted bits stay meaningful.

Trade-off accepted: Bloom filters have false positives (a URL wrongly judged
"seen" and skipped). At the configured 1% that's negligible for a crawler and
buys enormous memory savings over an exact set of 10M URL strings (which would
be hundreds of MB).

## 4. Indexer pipeline

Two bounded stages:

1. **Extraction workers** (`= NumCPU`): HTML parsing is CPU-bound, so this stage
   is sized to cores. Each page is parsed **exactly once** (`golang.org/x/net/html`)
   and that single DFS yields title, meta-description, de-scripted visible text
   (capped at `index.max_text_bytes`) **and** outbound links. The crawler never
   parses; links flow back to it via a callback sink.
2. **A single committer**: it coalesces documents into batches
   (`index.batch_size`, or every `flush_interval_ms`) and issues one Bluge batch
   + one BadgerDB `WriteBatch` per flush. Batching turns many tiny random writes
   into few large sequential ones — exactly what LSM stores and segment indexes
   want, and what keeps an SD card alive.

**Split storage.** The inverted index stores *almost nothing* — only the
document `_id` (the URL). Titles, snippets and hosts live in BadgerDB. This
keeps index segments small (memory-map-friendly) and puts the bulky, rarely-hot
result-rendering data in the LSM store where it compresses well.

## 5. Search & the latency budget

- **BM25** is Bluge's default similarity. Queries run against a title-boosted
  `should(title^2.5, body^1.0)`; a quoted query becomes a phrase match; a
  `site:host` token is ANDed as an exact keyword filter.
- **Analyzer symmetry**: the same English analyzer (lowercase → stop-words →
  Snowball stem) is applied at index time *and* to the query, so `document`
  matches `documents` (verified).
- **Cached reader snapshot**: opening a reader per query would re-load segment
  metadata every time. Instead one reader snapshot is cached and swapped
  atomically after each flush (throttled to ≥1 s). Searches run under a read
  lock; the swap takes the write lock (which drains in-flight searches) before
  closing the old snapshot — so a reader is never closed mid-query.
- **Hard caps**: result count is capped (≤50) and the search carries a 200 ms
  context deadline, so tail latency can't blow the budget. Measured latency on a
  small corpus is < 1 ms; the join to BadgerDB for hydration is the main cost at
  scale and stays comfortably under 100 ms with the configured caches.

## 6. Memory management summary

| Lever | Mechanism | Effect |
|---|---|---|
| `GOMEMLIMIT` | `debug.SetMemoryLimit` | Soft heap ceiling; GC intensifies near it instead of OOMing. |
| `GOGC=40` | `debug.SetGCPercent` | Smaller live heap. |
| Scavenger | periodic `debug.FreeOSMemory()` | Returns freed pages to the kernel → RSS actually drops. |
| Backpressure | bounded `RawPage` channel | Un-indexed pages can't accumulate. |
| Body cap | `io.LimitReader` per fetch | No single response can blow the heap. |
| Bounded caches | Badger block/index cache MB | Store RAM is a fixed, chosen number. |
| mmap | Bloom filter + Bluge segments | OS page cache absorbs cold data. |
| Text cap | per-doc `max_text_bytes` | Bounds per-page work and index growth. |

## 7. Scaling toward 10M pages

The current build runs the whole frontier in RAM. That's fine to a few million
URLs; beyond that the natural extensions are:

- **Disk-spill frontier**: persist per-host queues to BadgerDB and keep only a
  hot window in memory. (The interface is already isolated in `frontier.go`.)
- **Index sharding / periodic merge tuning**: expose Bluge merge-plan options
  for larger segment counts.
- **Snapshot backups**: `Reader.Backup` (Bluge) + Badger's stream backup for
  point-in-time copies.
- **Distributed crawl**: shard the frontier by host hash across multiple Pis,
  each owning a slice of hosts, with a shared/merged index.

None of these change the core loop; they swap a component behind an existing
seam.
