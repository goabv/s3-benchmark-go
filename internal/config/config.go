// Package config loads and normalizes the benchmark configuration. The JSON
// schema and key-naming are intentionally identical to the JS project so both
// benchmarks target the exact same seeded S3 objects.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// SizeSpec is one entry of the "sizes" array: a human size label (e.g. "30GiB")
// and how many files of that size exist.
type SizeSpec struct {
	Size  string `json:"size"`
	Count int    `json:"count"`
}

// Download holds the download-benchmark knobs.
type Download struct {
	Workers           int  `json:"workers"`
	Concurrency       int  `json:"concurrency"`
	Iterations        int  `json:"iterations"`
	Warmup            int  `json:"warmup"`
	ValidateChecksum  bool `json:"validateChecksum"`
	SpreadConnections bool `json:"spreadConnections"`
	TLS               bool `json:"tls"`
	// DeliveryMode selects the sink each downloaded part is written to:
	//   "discard"        - drain + count bytes, no sink (default; lowest memory)
	//   "ordered-stream" - reassemble parts in ascending order via a buffer pool,
	//                      bounded by MaxBufferedBytes (models in-order streaming)
	//   "file"           - write each part to disk at its byte offset under DeliveryPath
	DeliveryMode string `json:"deliveryMode"`
	// DeliveryPath is the directory for "file" delivery mode.
	DeliveryPath string `json:"deliveryPath"`
	// MaxBufferedBytes caps the ordered-stream reorder buffer (0 = bounded only by
	// parallelism x partSize).
	MaxBufferedBytes int64 `json:"maxBufferedBytes"`
	// MaxRetries is the number of extra attempts per part on transient failure.
	MaxRetries int `json:"maxRetries"`
	// StallTimeoutMs trips the watchdog when no bytes move for this long (0 = default).
	StallTimeoutMs int `json:"stallTimeoutMs"`
}

// Upload holds the multipart-PUT benchmark knobs.
type Upload struct {
	Workers        int    `json:"workers"`
	Concurrency    int    `json:"concurrency"`
	Iterations     int    `json:"iterations"`
	Warmup         int    `json:"warmup"`
	KeyPrefix      string `json:"keyPrefix"`
	MaxRetries     int    `json:"maxRetries"`
	StallTimeoutMs int    `json:"stallTimeoutMs"`
}

// Parallelism mirrors the workers x concurrency-per-worker model.
func (u Upload) Parallelism() int {
	p := u.Workers * u.Concurrency
	if p < 1 {
		return 1
	}
	return p
}

// Sampling controls the background time-series resource sampler.
type Sampling struct {
	Enabled    bool `json:"enabled"`
	IntervalMs int  `json:"intervalMs"`
}

// Results controls run capture: where to write the JSON report + SVG plot and
// whether to upload them to S3.
type Results struct {
	Dir    string `json:"dir"`
	Plot   bool   `json:"plot"`
	Upload bool   `json:"upload"`
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
}

// Config is the whole bench.config.json.
type Config struct {
	Bucket     string     `json:"bucket"`
	Region     string     `json:"region"`
	DataPrefix string     `json:"dataPrefix"`
	CodePrefix string     `json:"codePrefix"`
	Sizes      []SizeSpec `json:"sizes"`
	PartSize   string     `json:"partSize"`
	Checksum   string     `json:"checksum"`
	Download   Download   `json:"download"`
	Upload     Upload     `json:"upload"`
	Sampling   Sampling   `json:"sampling"`
	Results    Results    `json:"results"`
}

// Load reads and parses a bench.config.json file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	c.applyDefaults()
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.PartSize == "" {
		c.PartSize = "16MiB"
	}
	if c.CodePrefix == "" {
		c.CodePrefix = "code/"
	}

	if c.Download.Workers <= 0 {
		c.Download.Workers = 32
	}
	if c.Download.Concurrency <= 0 {
		c.Download.Concurrency = 1
	}
	if c.Download.Iterations <= 0 {
		c.Download.Iterations = 1
	}
	if c.Download.MaxRetries <= 0 {
		c.Download.MaxRetries = 2
	}
	if c.Download.StallTimeoutMs <= 0 {
		c.Download.StallTimeoutMs = 30000
	}
	if c.Download.DeliveryMode == "" {
		c.Download.DeliveryMode = "discard"
	}
	if c.Download.DeliveryPath == "" {
		c.Download.DeliveryPath = os.TempDir()
	}

	if c.Upload.Workers <= 0 {
		c.Upload.Workers = 32
	}
	if c.Upload.Concurrency <= 0 {
		c.Upload.Concurrency = 1
	}
	if c.Upload.Iterations <= 0 {
		c.Upload.Iterations = 1
	}
	if c.Upload.MaxRetries <= 0 {
		c.Upload.MaxRetries = 2
	}
	if c.Upload.StallTimeoutMs <= 0 {
		c.Upload.StallTimeoutMs = 30000
	}
	if c.Upload.KeyPrefix == "" {
		c.Upload.KeyPrefix = "bench-upload/"
	}

	if c.Sampling.IntervalMs <= 0 {
		c.Sampling.IntervalMs = 250
	}

	if c.Results.Dir == "" {
		c.Results.Dir = "results"
	}
	if c.Results.Prefix == "" {
		c.Results.Prefix = "results/"
	}
}

// StallTimeout returns the download stall-watchdog timeout.
func (d Download) StallTimeout() time.Duration {
	return time.Duration(d.StallTimeoutMs) * time.Millisecond
}

// StallTimeout returns the upload stall-watchdog timeout.
func (u Upload) StallTimeout() time.Duration {
	return time.Duration(u.StallTimeoutMs) * time.Millisecond
}

// SampleInterval returns the resource-sampler tick interval.
func (s Sampling) SampleInterval() time.Duration {
	return time.Duration(s.IntervalMs) * time.Millisecond
}

// Parallelism is the number of concurrent in-flight requests to drive: it mirrors
// the JS model of workers x concurrency-per-worker.
func (d Download) Parallelism() int { return d.Workers * d.Concurrency }

// KeysFor expands a SizeSpec into its object keys, matching the JS keyForSize:
//
//	count == 1 -> "<prefix><size>.bin"
//	count  > 1 -> "<prefix><size>-<i>.bin"   (0-indexed)
//
// with the size label lowercased and whitespace stripped.
func (c *Config) KeysFor(s SizeSpec) []string {
	clean := strings.ToLower(strings.ReplaceAll(s.Size, " ", ""))
	count := s.Count
	if count <= 1 {
		return []string{fmt.Sprintf("%s%s.bin", c.DataPrefix, clean)}
	}
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("%s%s-%d.bin", c.DataPrefix, clean, i)
	}
	return keys
}

// ParseSize converts size labels like "32MiB", "30GiB", "512KiB", "1024" into a
// byte count. Bare numbers are treated as bytes.
func ParseSize(label string) (int64, error) {
	s := strings.TrimSpace(label)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mults := []struct {
		suffix string
		mult   int64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1},
	}
	for _, m := range mults {
		if strings.HasSuffix(s, m.suffix) {
			n, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(s, m.suffix)), 64)
			if err != nil {
				return 0, fmt.Errorf("parse size %q: %w", label, err)
			}
			return int64(n * float64(m.mult)), nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", label, err)
	}
	return n, nil
}
