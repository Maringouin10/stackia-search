// Package store is a thin BadgerDB wrapper holding compact page metadata keyed
// by URL. BadgerDB is an LSM-tree KV store: writes are sequential (great for an
// SD card / cheap SSD), and its caches are explicitly bounded here so the
// process footprint stays predictable on a Pi.
package store

import (
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"

	"github.com/Maringouin10/stackia-search/internal/config"
	"github.com/Maringouin10/stackia-search/internal/model"
)

// ErrNotFound is returned when a key is absent.
var ErrNotFound = errors.New("store: not found")

const metaPrefix = "m:" // key = "m:" + url

type Store struct {
	db *badger.DB
}

// Open configures BadgerDB for low, bounded RAM usage. The knobs that matter
// most on constrained hardware are the block/index cache sizes and the memtable
// budget — left at defaults they can balloon to hundreds of MB.
func Open(cfg config.StoreConfig) (*Store, error) {
	opts := badger.DefaultOptions(cfg.Path).
		WithLogger(nil). // silence Badger's chatty INFO logging
		WithMemTableSize(int64(cfg.MemTableMB) << 20).
		WithNumMemtables(cfg.NumMemtables).
		WithBlockCacheSize(int64(cfg.BlockCacheMB) << 20).
		WithIndexCacheSize(int64(cfg.IndexCacheMB) << 20).
		WithValueLogFileSize(int64(cfg.ValueLogFileMB) << 20).
		WithValueThreshold(int64(cfg.ValueThreshold)).
		WithNumCompactors(cfg.NumCompactors).
		WithNumLevelZeroTables(2).
		WithSyncWrites(false) // batch durability; we flush explicitly

	if cfg.CompressionZSTD {
		opts = opts.WithCompression(options.ZSTD)
	} else {
		opts = opts.WithCompression(options.Snappy)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	return &Store{db: db}, nil
}

// Batch accumulates writes and flushes them together. BadgerDB's WriteBatch
// pipelines commits, which is far cheaper than one txn per document.
type Batch struct {
	wb *badger.WriteBatch
}

func (s *Store) NewBatch() *Batch { return &Batch{wb: s.db.NewWriteBatch()} }

func (b *Batch) PutMeta(m *model.Meta) error {
	v, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return b.wb.Set(append([]byte(metaPrefix), m.URL...), v)
}

// Flush commits and closes the batch; the Batch must not be reused after this.
func (b *Batch) Flush() error { return b.wb.Flush() }

// Cancel discards an unflushed batch.
func (b *Batch) Cancel() { b.wb.Cancel() }

// GetMeta fetches a single record. Used by the search layer to hydrate results.
func (s *Store) GetMeta(url string) (*model.Meta, error) {
	var out model.Meta
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(append([]byte(metaPrefix), url...))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotFound
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &out)
		})
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Count returns an approximate number of stored records (LSM key estimate).
func (s *Store) Count() uint64 {
	var n uint64
	_ = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false, Prefix: []byte(metaPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	return n
}

// RunGC reclaims space in the value log; call periodically (e.g. hourly).
func (s *Store) RunGC() {
	// Reclaim only when at least 50% of a value-log file is discardable.
	for s.db.RunValueLogGC(0.5) == nil {
	}
}

func (s *Store) Close() error { return s.db.Close() }
