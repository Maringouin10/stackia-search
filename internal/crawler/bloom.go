package crawler

import (
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Bloom is a persistent, memory-mapped Bloom filter used for O(1) URL
// de-duplication. The bit array is a file mapped MAP_SHARED, so:
//   - the OS page cache owns the pages — resident memory grows only with the
//     working set, not the full filter size (crucial on a Pi);
//   - the filter survives restarts with no serialisation step;
//   - bit sets are lock-free via 64-bit atomic CAS on the mapped words.
//
// It is a classic space/accuracy trade: at 10M URLs and a 1% target false
// positive rate the filter is ~11.4 MiB and uses 7 hash probes.
type Bloom struct {
	file  *os.File
	data  []byte // mmap'd bit array, len is a multiple of 8
	m     uint64 // number of bits (multiple of 64)
	k     uint64 // number of hash functions
	count atomic.Uint64
}

// OpenBloom maps (creating if needed) a filter sized for `capacity` items at
// the given false-positive rate. Re-opening an existing file of the right size
// reuses its bits.
func OpenBloom(path string, capacity uint64, fp float64) (*Bloom, error) {
	if capacity == 0 {
		capacity = 1
	}
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	m, k := optimalParams(capacity, fp)
	bytes := int64(m / 8)

	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open bloom file: %w", err)
	}
	if err := f.Truncate(bytes); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("size bloom file: %w", err)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(bytes),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("mmap bloom: %w", err)
	}
	// Advise the kernel we touch the map randomly so it doesn't aggressively
	// read-ahead the whole file.
	_ = unix.Madvise(data, unix.MADV_RANDOM)

	return &Bloom{file: f, data: data, m: m, k: k}, nil
}

// TestAndAdd reports whether key was (probably) already present and inserts it.
// This is the crawler's dedup gate: a false return means "new, enqueue it".
func (b *Bloom) TestAndAdd(key []byte) bool {
	h1, h2 := hash2(key)
	if h2 == 0 {
		h2 = 1 // keep the probe sequence non-degenerate
	}
	allSet := true
	for i := uint64(0); i < b.k; i++ {
		pos := (h1 + i*h2) % b.m
		if !b.setBit(pos) {
			allSet = false
		}
	}
	if !allSet {
		b.count.Add(1)
	}
	return allSet
}

// Test reports probable membership without inserting.
func (b *Bloom) Test(key []byte) bool {
	h1, h2 := hash2(key)
	if h2 == 0 {
		h2 = 1
	}
	for i := uint64(0); i < b.k; i++ {
		if !b.testBit((h1 + i*h2) % b.m) {
			return false
		}
	}
	return true
}

// Count is the approximate number of distinct inserts observed this run.
func (b *Bloom) Count() uint64 { return b.count.Load() }

// FillRatio estimates the fraction of set bits (drives FP-rate reporting).
func (b *Bloom) FillRatio() float64 {
	// Sample the first 1<<16 words to stay cheap regardless of filter size.
	words := len(b.data) / 8
	sample := words
	if sample > 1<<16 {
		sample = 1 << 16
	}
	var set uint64
	for i := 0; i < sample; i++ {
		w := *(*uint64)(unsafe.Pointer(&b.data[i*8]))
		set += uint64(popcount(w))
	}
	if sample == 0 {
		return 0
	}
	return float64(set) / float64(sample*64)
}

// Sync flushes dirty pages to disk (best-effort durability checkpoint).
func (b *Bloom) Sync() error { return unix.Msync(b.data, unix.MS_ASYNC) }

// Close syncs and unmaps.
func (b *Bloom) Close() error {
	_ = unix.Msync(b.data, unix.MS_SYNC)
	err := unix.Munmap(b.data)
	if cerr := b.file.Close(); err == nil {
		err = cerr
	}
	return err
}

// --- internals ---

func (b *Bloom) setBit(pos uint64) (wasSet bool) {
	word := pos >> 6
	mask := uint64(1) << (pos & 63)
	addr := (*uint64)(unsafe.Pointer(&b.data[word*8]))
	for {
		old := atomic.LoadUint64(addr)
		if old&mask != 0 {
			return true
		}
		if atomic.CompareAndSwapUint64(addr, old, old|mask) {
			return false
		}
	}
}

func (b *Bloom) testBit(pos uint64) bool {
	word := pos >> 6
	mask := uint64(1) << (pos & 63)
	addr := (*uint64)(unsafe.Pointer(&b.data[word*8]))
	return atomic.LoadUint64(addr)&mask != 0
}

// optimalParams returns bit count m (rounded up to a multiple of 64) and probe
// count k for the standard Bloom sizing formulas.
func optimalParams(n uint64, p float64) (m, k uint64) {
	mf := math.Ceil(-(float64(n) * math.Log(p)) / (math.Ln2 * math.Ln2))
	m = uint64(mf)
	if r := m % 64; r != 0 {
		m += 64 - r
	}
	if m == 0 {
		m = 64
	}
	kf := math.Round((float64(m) / float64(n)) * math.Ln2)
	k = uint64(kf)
	if k < 1 {
		k = 1
	}
	return m, k
}

// hash2 derives two independent 64-bit hashes in a single pass (two FNV-1a
// variants). Deterministic — no per-process seed — so the mapped bits stay
// meaningful across restarts. Kirsch–Mitzenmacher double hashing then yields
// all k probes as h1 + i*h2.
func hash2(data []byte) (uint64, uint64) {
	const (
		off1  = 14695981039346656037
		off2  = 1099511628211
		prime = 1099511628211
	)
	h1 := uint64(off1)
	h2 := uint64(off2)
	for _, c := range data {
		h1 = (h1 ^ uint64(c)) * prime
		h2 = (h2 + uint64(c)) * prime
	}
	return h1, h2
}

func popcount(x uint64) int {
	// math/bits.OnesCount64 without the import churn.
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((x * 0x0101010101010101) >> 56)
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
