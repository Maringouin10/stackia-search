package crawler

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestBloomBasicDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.bloom")
	b, err := OpenBloom(path, 10000, 0.01)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer b.Close()

	key := []byte("http://example.com/a")
	if b.TestAndAdd(key) {
		t.Fatal("first insert should report not-present")
	}
	if !b.TestAndAdd(key) {
		t.Fatal("second insert should report present")
	}
	if !b.Test(key) {
		t.Fatal("Test should report present after add")
	}
}

// A Bloom filter must never produce false negatives: everything inserted must
// subsequently test as present.
func TestBloomNoFalseNegatives(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.bloom")
	b, err := OpenBloom(path, 5000, 0.01)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer b.Close()

	for i := 0; i < 5000; i++ {
		b.TestAndAdd([]byte(fmt.Sprintf("url-%d", i)))
	}
	for i := 0; i < 5000; i++ {
		if !b.Test([]byte(fmt.Sprintf("url-%d", i))) {
			t.Fatalf("false negative for url-%d", i)
		}
	}
}

// The mmap must persist bits across close/reopen — the property that lets a
// crawl resume de-dup state after a restart.
func TestBloomPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.bloom")
	key := []byte("http://persist.test/x")

	b1, err := OpenBloom(path, 10000, 0.01)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	b1.TestAndAdd(key)
	if err := b1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	b2, err := OpenBloom(path, 10000, 0.01)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer b2.Close()
	if !b2.Test(key) {
		t.Fatal("bit did not persist across reopen")
	}
}

func TestOptimalParams(t *testing.T) {
	m, k := optimalParams(10_000_000, 0.01)
	if m%64 != 0 {
		t.Fatalf("m must be a multiple of 64, got %d", m)
	}
	// ~9.585 bits/item → ~95.85 Mbit; k ~ 7.
	if k < 6 || k > 8 {
		t.Fatalf("unexpected k=%d", k)
	}
	if m < 90_000_000 || m > 100_000_000 {
		t.Fatalf("unexpected m=%d", m)
	}
}
