// Package config centralises every tunable in one struct so the whole engine
// can be sized for the host it runs on. The defaults target a Raspberry Pi 4
// (4 cores, 2-4 GB RAM): conservative memory ceilings, bounded queues and
// batch-oriented writes.
package config

import (
	"fmt"
	"os"
	"runtime"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration document (see config.yaml).
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Crawler CrawlerConfig `yaml:"crawler"`
	Index   IndexConfig   `yaml:"index"`
	Store   StoreConfig   `yaml:"store"`
	Bloom   BloomConfig   `yaml:"bloom"`
	Runtime RuntimeConfig `yaml:"runtime"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":8080"
}

type CrawlerConfig struct {
	Seeds           []string `yaml:"seeds"`
	Workers         int      `yaml:"workers"`            // concurrent fetchers
	PerHostDelayMS  int      `yaml:"per_host_delay_ms"`  // politeness delay per host
	PerHostInflight int      `yaml:"per_host_inflight"`  // max simultaneous reqs / host
	MaxDepth        int      `yaml:"max_depth"`          // link-following depth cap
	MaxBodyBytes    int64    `yaml:"max_body_bytes"`     // hard cap per page (OOM guard)
	RequestTimeout  int      `yaml:"request_timeout_ms"` // per-request timeout
	UserAgent       string   `yaml:"user_agent"`
	RespectRobots   bool     `yaml:"respect_robots"`
	AllowSubdomains bool     `yaml:"allow_subdomains"`
	QueueCapacity   int      `yaml:"queue_capacity"` // bounded frontier hand-off channel
}

type IndexConfig struct {
	Path          string `yaml:"path"`
	BatchSize     int    `yaml:"batch_size"` // docs per index batch
	FlushInterval int    `yaml:"flush_interval_ms"`
	MaxTextBytes  int    `yaml:"max_text_bytes"` // per-doc text cap before indexing
	SnippetBytes  int    `yaml:"snippet_bytes"`  // stored preview length
}

type StoreConfig struct {
	Path            string `yaml:"path"`
	MemTableMB      int    `yaml:"mem_table_mb"`
	NumMemtables    int    `yaml:"num_memtables"`
	BlockCacheMB    int    `yaml:"block_cache_mb"`
	IndexCacheMB    int    `yaml:"index_cache_mb"`
	ValueLogFileMB  int    `yaml:"value_log_file_mb"`
	ValueThreshold  int    `yaml:"value_threshold"`
	NumCompactors   int    `yaml:"num_compactors"`
	CompressionZSTD bool   `yaml:"compression_zstd"`
}

type BloomConfig struct {
	Path          string  `yaml:"path"`
	Capacity      uint64  `yaml:"capacity"`       // expected number of URLs
	FalsePositive float64 `yaml:"false_positive"` // target FP rate, e.g. 0.01
}

type RuntimeConfig struct {
	MemLimitMB       int `yaml:"mem_limit_mb"`        // soft heap cap -> GOMEMLIMIT
	GCPercent        int `yaml:"gc_percent"`          // GOGC; lower = more aggressive
	MaxProcs         int `yaml:"max_procs"`           // 0 = leave as runtime default
	FreeOSMemorySecs int `yaml:"free_os_memory_secs"` // periodic scavenge, 0 = off
}

// Default returns a configuration sized for a Raspberry Pi 4.
func Default() Config {
	return Config{
		Server: ServerConfig{Addr: ":8080"},
		Crawler: CrawlerConfig{
			Seeds:           []string{},
			Workers:         64,
			PerHostDelayMS:  1000,
			PerHostInflight: 2,
			MaxDepth:        6,
			MaxBodyBytes:    2 << 20, // 2 MiB
			RequestTimeout:  10000,
			UserAgent:       "StackiaBot/1.0 (+https://github.com/Maringouin10/stackia-search)",
			RespectRobots:   true,
			AllowSubdomains: true,
			QueueCapacity:   4096,
		},
		Index: IndexConfig{
			Path:          "data/index",
			BatchSize:     256,
			FlushInterval: 2000,
			MaxTextBytes:  64 << 10, // 64 KiB of text per doc is plenty
			SnippetBytes:  240,
		},
		Store: StoreConfig{
			Path:            "data/store",
			MemTableMB:      16,
			NumMemtables:    2,
			BlockCacheMB:    64,
			IndexCacheMB:    32,
			ValueLogFileMB:  64,
			ValueThreshold:  1024,
			NumCompactors:   2,
			CompressionZSTD: true,
		},
		Bloom: BloomConfig{
			Path:          "data/bloom/urls.bloom",
			Capacity:      10_000_000, // 10M pages target
			FalsePositive: 0.01,
		},
		Runtime: RuntimeConfig{
			MemLimitMB:       768,
			GCPercent:        40,
			MaxProcs:         0,
			FreeOSMemorySecs: 30,
		},
	}
}

// Load reads a YAML file over the defaults. A missing path yields the defaults
// unchanged, so the engine can boot with zero configuration.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	cfg.normalise()
	return cfg, nil
}

func (c *Config) normalise() {
	if c.Crawler.Workers <= 0 {
		c.Crawler.Workers = runtime.NumCPU() * 8
	}
	if c.Crawler.QueueCapacity < c.Crawler.Workers {
		c.Crawler.QueueCapacity = c.Crawler.Workers * 4
	}
	if c.Index.BatchSize <= 0 {
		c.Index.BatchSize = 128
	}
	if c.Bloom.Capacity == 0 {
		c.Bloom.Capacity = 1_000_000
	}
	if c.Bloom.FalsePositive <= 0 || c.Bloom.FalsePositive >= 1 {
		c.Bloom.FalsePositive = 0.01
	}
}
