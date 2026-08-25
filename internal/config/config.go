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

// TMDownload holds the Transfer Manager (v2 baseline) download knobs, expressed
// directly rather than derived from a workers x concurrency product. Consumed by
// cmd/tmbench; the optimized/custom runner (cmd/bench) uses Download instead.
type TMDownload struct {
	API               string `json:"api"`               // "get" (GetObject stream) | "download-object" (WriterAt, parallel)
	GetObjectType     string `json:"getObjectType"`     // "parts" (PartNumber) | "ranges" (byte ranges of partSize)
	Sink              string `json:"sink"`              // "discard" | "file" (download-object only)
	DeliveryPath      string `json:"deliveryPath"`      // directory for sink=file
	ObjectConcurrency int    `json:"objectConcurrency"` // objects in parallel (0 = object count)
	Concurrency       int    `json:"concurrency"`       // per-object part concurrency (0 = SDK default)
	Iterations        int    `json:"iterations"`
	Warmup            int    `json:"warmup"`
	ValidateChecksum  bool   `json:"validateChecksum"`
	MaxBufferedBytes  int64  `json:"maxBufferedBytes"` // GetObject read-ahead ("get" API only)
	StallTimeoutMs    int    `json:"stallTimeoutMs"`
	// DirectIO (optimized profile only) writes the file sink with O_DIRECT,
	// bypassing the page cache so concurrent part-writers hit the disk in parallel
	// instead of serializing on the buffered single-inode write path. Baseline
	// leaves this false.
	DirectIO bool `json:"directIO"`
}

// StallTimeout returns the TM download stall-watchdog timeout.
func (d TMDownload) StallTimeout() time.Duration {
	return time.Duration(d.StallTimeoutMs) * time.Millisecond
}

// TMUpload holds the Transfer Manager (v2 baseline) upload knobs.
type TMUpload struct {
	ObjectConcurrency int    `json:"objectConcurrency"` // objects in parallel (0 = object count)
	Concurrency       int    `json:"concurrency"`       // per-object part concurrency (0 = SDK default)
	Iterations        int    `json:"iterations"`
	Warmup            int    `json:"warmup"`
	KeyPrefix         string `json:"keyPrefix"`
	StallTimeoutMs    int    `json:"stallTimeoutMs"`
}

// StallTimeout returns the TM upload stall-watchdog timeout.
func (u TMUpload) StallTimeout() time.Duration {
	return time.Duration(u.StallTimeoutMs) * time.Millisecond
}

// TransferManager is the TM v2 baseline section (cmd/tmbench), kept separate from
// the optimized custom runner's Download/Upload sections so the two never share
// meaning. Concurrency is set directly here rather than derived.
type TransferManager struct {
	Download TMDownload `json:"download"`
	Upload   TMUpload   `json:"upload"`
}

// applyTMDefaults fills unset fields of a Transfer Manager section (baseline or
// optimized) with defaults. ObjectConcurrency/Concurrency are intentionally left
// at 0 ("auto"), and DirectIO defaults to false.
func applyTMDefaults(tm *TransferManager) {
	d := &tm.Download
	if d.API == "" {
		d.API = "get"
	}
	if d.GetObjectType == "" {
		d.GetObjectType = "parts"
	}
	if d.Sink == "" {
		d.Sink = "discard"
	}
	if d.DeliveryPath == "" {
		d.DeliveryPath = os.TempDir()
	}
	if d.Iterations <= 0 {
		d.Iterations = 1
	}
	if d.StallTimeoutMs <= 0 {
		d.StallTimeoutMs = 30000
	}
	u := &tm.Upload
	if u.Iterations <= 0 {
		u.Iterations = 1
	}
	if u.KeyPrefix == "" {
		u.KeyPrefix = "tm-upload/"
	}
	if u.StallTimeoutMs <= 0 {
		u.StallTimeoutMs = 30000
	}
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
	Bucket          string          `json:"bucket"`
	Region          string          `json:"region"`
	DataPrefix      string          `json:"dataPrefix"`
	CodePrefix      string          `json:"codePrefix"`
	Sizes           []SizeSpec      `json:"sizes"`
	PartSize        string          `json:"partSize"`
	Checksum        string          `json:"checksum"`
	Download        Download        `json:"download"`
	Upload          Upload          `json:"upload"`
	TransferManager TransferManager `json:"transferManager"`
	// TMOptimized is the "optimized" TM profile (cmd/tmbench -profile optimized):
	// same shape as TransferManager but carries the optimization knobs (e.g.
	// DirectIO). transferManager stays the pristine baseline; all mods live here.
	TMOptimized TransferManager `json:"tmOptimized"`
	Sampling    Sampling        `json:"sampling"`
	Results     Results         `json:"results"`
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

	// Transfer Manager defaults, applied to both the baseline (transferManager)
	// and optimized (tmOptimized) sections. ObjectConcurrency/Concurrency stay
	// 0 = "auto" and are resolved by cmd/tmbench (0 objects -> object count; 0
	// per-object -> SDK default).
	applyTMDefaults(&c.TransferManager)
	applyTMDefaults(&c.TMOptimized)

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
