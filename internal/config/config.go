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

// TMDownload holds the Transfer Manager download knobs, expressed directly rather
// than derived from a workers x concurrency product. Consumed by cmd/tmbench.
type TMDownload struct {
	API               string `json:"api"`               // "get" (GetObject stream) | "download-object" (WriterAt, parallel)
	GetObjectType     string `json:"getObjectType"`     // "parts" (PartNumber) | "ranges" (byte ranges of partSize)
	Sink              string `json:"sink"`              // "discard" | "file" (download-object only)
	DeliveryPath      string `json:"deliveryPath"`      // directory for sink=file
	ObjectConcurrency int    `json:"objectConcurrency"` // objects in parallel (0 = object count)
	Concurrency       int    `json:"concurrency"`       // per-object part concurrency (0 = SDK default)
	// PartSize is the download range/part size, e.g. "8MiB"; for download-file it is
	// the ranged-GET size the SDK reads and coalesces. Independent of the upload
	// part size (they live in their own sections).
	PartSize         string `json:"partSize"`
	Iterations       int    `json:"iterations"`
	Warmup           int    `json:"warmup"`
	ValidateChecksum bool   `json:"validateChecksum"`
	MaxBufferedBytes int64  `json:"maxBufferedBytes"` // GetObject read-ahead ("get" API only)
	StallTimeoutMs    int    `json:"stallTimeoutMs"`
	// MultiNIC (optimized profile only) round-robins outbound connections across
	// all of the host's ENI source IPs to spread load over multiple network cards.
	// Requires host-side policy routing (scripts/setup-multinic.sh). Baseline
	// leaves this false.
	MultiNIC bool `json:"multiNIC"`
	// DirectIOThreshold (api=download-file only) is the object-size threshold, e.g.
	// "100MiB", above which the SDK's DownloadFile writes with O_DIRECT; smaller
	// objects use a buffered writer. Empty uses the SDK default (100 MiB).
	DirectIOThreshold string `json:"directIOThreshold"`

	// DisableDirectIO (api=download-file only) forces the SDK's DownloadFile to use
	// the buffered writer regardless of object size (for A/B vs O_DIRECT).
	DisableDirectIO bool `json:"disableDirectIO"`

	// WriteChunkSize (api=download-file only) is the fixed disk-write chunk size,
	// e.g. "8MiB", the SDK coalesces part/range data into, independent of the
	// download range/part size. Empty uses the SDK default (8 MiB).
	WriteChunkSize string `json:"writeChunkSize"`

	// WriteFlushWorkers (api=download-file only) is the number of write-behind flush
	// goroutines per file in the SDK's DownloadFile sink. 0 uses the SDK default (16).
	WriteFlushWorkers int `json:"writeFlushWorkers"`

	// WriteFlushQueueDepth (api=download-file only) is the depth of the bounded queue
	// feeding the DownloadFile flush workers. 0 uses the SDK default (64).
	WriteFlushQueueDepth int `json:"writeFlushQueueDepth"`
}

// StallTimeout returns the TM download stall-watchdog timeout.
func (d TMDownload) StallTimeout() time.Duration {
	return time.Duration(d.StallTimeoutMs) * time.Millisecond
}

// TMUpload holds the Transfer Manager (v2 baseline) upload knobs.
type TMUpload struct {
	ObjectConcurrency int `json:"objectConcurrency"` // objects in parallel (0 = object count)
	Concurrency       int `json:"concurrency"`       // per-object part concurrency (0 = SDK default)
	// PartSize is the multipart upload part size, e.g. "128MiB". It must be large
	// enough that objectSize/partSize <= 10000 (S3's max parts) or the upload fails;
	// the SDK only auto-upsizes when it knows the body length, which streamed
	// uploads do not provide. Independent of the download part size.
	PartSize       string `json:"partSize"`
	Iterations     int    `json:"iterations"`
	Warmup         int    `json:"warmup"`
	KeyPrefix      string `json:"keyPrefix"`
	StallTimeoutMs int    `json:"stallTimeoutMs"`
}

// StallTimeout returns the TM upload stall-watchdog timeout.
func (u TMUpload) StallTimeout() time.Duration {
	return time.Duration(u.StallTimeoutMs) * time.Millisecond
}

// TransferManager is the TM config section (cmd/tmbench). Concurrency is set
// directly here rather than derived from a workers x concurrency product.
type TransferManager struct {
	Download TMDownload `json:"download"`
	Upload   TMUpload   `json:"upload"`
}

// applyTMDefaults fills unset fields of the Transfer Manager section with
// defaults. ObjectConcurrency/Concurrency are intentionally left at 0 ("auto").
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
	if d.PartSize == "" {
		d.PartSize = "8MiB"
	}
	if d.Iterations <= 0 {
		d.Iterations = 1
	}
	if d.StallTimeoutMs <= 0 {
		d.StallTimeoutMs = 30000
	}
	u := &tm.Upload
	if u.PartSize == "" {
		// Large enough that a 1 TiB object stays under S3's 10000-part cap.
		u.PartSize = "128MiB"
	}
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
	Bucket     string     `json:"bucket"`
	Region     string     `json:"region"`
	DataPrefix string     `json:"dataPrefix"`
	CodePrefix string     `json:"codePrefix"`
	Sizes      []SizeSpec `json:"sizes"`
	Checksum   string     `json:"checksum"`
	// MemoryLimit sets Go's soft memory limit (GOMEMLIMIT equivalent) as an
	// absolute value, e.g. "40GiB". Empty = default (80% of system RAM). Lower it
	// to force the GC to collect harder and hold peak RSS down; keep it above the
	// live working set (objectConcurrency x concurrency x region size) or the GC
	// will thrash.
	MemoryLimit     string          `json:"memoryLimit"`
	TransferManager TransferManager `json:"transferManager"`
	Sampling        Sampling        `json:"sampling"`
	Results         Results         `json:"results"`
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
	if c.CodePrefix == "" {
		c.CodePrefix = "code/"
	}

	// Transfer Manager defaults. ObjectConcurrency/Concurrency stay 0 = "auto" and
	// are resolved by cmd/tmbench (0 objects -> object count; 0 per-object -> SDK
	// default).
	applyTMDefaults(&c.TransferManager)

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

// SampleInterval returns the resource-sampler tick interval.
func (s Sampling) SampleInterval() time.Duration {
	return time.Duration(s.IntervalMs) * time.Millisecond
}

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
