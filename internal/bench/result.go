package bench

import (
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// RunResult is the full outcome of a benchmark invocation across all size groups.
type RunResult struct {
	Mode   string
	Groups []GroupResult
}

// Sample3 is one iteration's throughput, matching the JS runner's {secs,mibps,gbps}.
type Sample3 struct {
	Secs  float64 `json:"secs"`
	Mibps float64 `json:"mibps"`
	Gbps  float64 `json:"gbps"`
}

// PartTimeStats is the per-part latency distribution in milliseconds.
type PartTimeStats struct {
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}

// Resources summarizes process resource usage over the sampled window.
type Resources struct {
	PeakRssBytes       int64   `json:"peakRssBytes"`
	AvgRssBytes        float64 `json:"avgRssBytes"`
	PeakCpuPercent     float64 `json:"peakCpuPercent"`
	AvgCpuPercent      float64 `json:"avgCpuPercent"`
	PeakMemUtilPercent float64 `json:"peakMemUtilPercent"`
	CpuCount           int     `json:"cpuCount"`
	TotalMemBytes      int64   `json:"totalMemBytes"`
	Samples            int     `json:"samples"`
}

// TLSInfo is the negotiated TLS protocol and cipher.
type TLSInfo struct {
	Protocol string `json:"protocol"`
	Cipher   string `json:"cipher"`
}

// PartRecord is one per-part timing row for the parttimes CSV.
type PartRecord struct {
	Iter   int
	Key    string
	Part   int32
	Bytes  int64
	Ms     float64
	VIP    string
	ConnID string
}

// GroupResult is the measured outcome for one size group. It is a superset of the
// fields both modes emit; the report package projects it onto the mode-specific
// sweep JSON shape.
type GroupResult struct {
	Label       string
	Files       int
	PerFileSize int64
	Size        int64
	Parts       int
	Workers     int
	Concurrency int
	InFlight    int
	Iterations  int

	// download-specific
	ChecksumAlgo           string
	ChecksumValidated      bool
	DeliveryMode           string
	PartsChecksummedPerRun int

	// upload-specific
	PartSize     int64
	Checksum     string
	UploadSource string

	Samples   []Sample3
	Median    Sample3
	Best      Sample3
	Resources Resources
	PartTime  PartTimeStats
	TTFB      PartTimeStats // download time-to-first-byte (console only; not in sweep JSON)
	TLS       TLSInfo

	// captured detail
	Parts_ []PartRecord

	// human-summary extras
	DistinctIPs int
	ReuseRatio  float64
	Retries     int
}

// iterResult is one iteration's raw measurement over a size group.
type iterResult struct {
	Bytes   int64
	Parts   int
	Elapsed time.Duration
}

func (r iterResult) Gbps() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Bytes) * 8 / r.Elapsed.Seconds() / 1e9
}

func (r iterResult) MiBps() float64 {
	if r.Elapsed <= 0 {
		return 0
	}
	return float64(r.Bytes) / (1 << 20) / r.Elapsed.Seconds()
}

func sample3(r iterResult) Sample3 {
	return Sample3{Secs: r.Elapsed.Seconds(), Mibps: r.MiBps(), Gbps: r.Gbps()}
}

func toSamples(rs []iterResult) []Sample3 {
	out := make([]Sample3, len(rs))
	for i, r := range rs {
		out[i] = sample3(r)
	}
	return out
}

// median returns the median-by-throughput sample.
func median(ss []Sample3) Sample3 {
	if len(ss) == 0 {
		return Sample3{}
	}
	sorted := append([]Sample3(nil), ss...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Gbps < sorted[j].Gbps })
	return sorted[len(sorted)/2]
}

// best returns the highest-throughput sample.
func best(ss []Sample3) Sample3 {
	var b Sample3
	for _, s := range ss {
		if s.Gbps > b.Gbps {
			b = s
		}
	}
	return b
}

func toPartTimeStats(p metrics.Percentiles) PartTimeStats {
	return PartTimeStats{
		Count: p.Count, Min: p.Min, P50: p.P50, P90: p.P90,
		P99: p.P99, P999: p.P999, Max: p.Max, Mean: p.Mean,
	}
}

// computeResources derives peak/avg RSS + CPU and peak memory utilization from the
// sampled series. It reports usage over the full sampled window.
func computeResources(samples []metrics.Sample) Resources {
	res := Resources{
		CpuCount:      runtime.NumCPU(),
		TotalMemBytes: metrics.TotalMemBytes(),
		Samples:       len(samples),
	}
	if len(samples) == 0 {
		return res
	}
	var sumRss, sumCpu float64
	for _, s := range samples {
		if s.RSSBytes > res.PeakRssBytes {
			res.PeakRssBytes = s.RSSBytes
		}
		if s.CPUPct > res.PeakCpuPercent {
			res.PeakCpuPercent = s.CPUPct
		}
		sumRss += float64(s.RSSBytes)
		sumCpu += s.CPUPct
		if res.TotalMemBytes > 0 {
			if util := float64(s.RSSBytes) / float64(res.TotalMemBytes) * 100; util > res.PeakMemUtilPercent {
				res.PeakMemUtilPercent = util
			}
		}
	}
	res.AvgRssBytes = sumRss / float64(len(samples))
	res.AvgCpuPercent = sumCpu / float64(len(samples))
	return res
}

// ResourcesFromSamples aggregates a sampled series into a Resources summary. It is
// exposed for the report package.
func ResourcesFromSamples(samples []metrics.Sample) Resources { return computeResources(samples) }

// partRecorder is a concurrency-safe collector of per-part rows.
type partRecorder struct {
	mu   sync.Mutex
	recs []PartRecord
}

func (p *partRecorder) add(r PartRecord) {
	p.mu.Lock()
	p.recs = append(p.recs, r)
	p.mu.Unlock()
}

func (p *partRecorder) snapshot() []PartRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PartRecord, len(p.recs))
	copy(out, p.recs)
	return out
}
