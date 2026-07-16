package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Recorder is a concurrency-safe collector of durations that computes latency
// percentiles on demand.
type Recorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

// Add records one observation.
func (r *Recorder) Add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.mu.Unlock()
}

// Percentiles snapshots the current distribution.
func (r *Recorder) Percentiles() Percentiles {
	r.mu.Lock()
	s := make([]time.Duration, len(r.samples))
	copy(s, r.samples)
	r.mu.Unlock()
	return computePercentiles(s)
}

// Percentiles summarizes a latency distribution. All values are milliseconds.
type Percentiles struct {
	Count int     `json:"count"`
	Min   float64 `json:"minMs"`
	Mean  float64 `json:"meanMs"`
	P50   float64 `json:"p50Ms"`
	P90   float64 `json:"p90Ms"`
	P99   float64 `json:"p99Ms"`
	P999  float64 `json:"p999Ms"`
	Max   float64 `json:"maxMs"`
}

func computePercentiles(s []time.Duration) Percentiles {
	if len(s) == 0 {
		return Percentiles{}
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return Percentiles{
		Count: len(s),
		Min:   ms(s[0]),
		Mean:  ms(sum) / float64(len(s)),
		P50:   ms(quantile(s, 0.50)),
		P90:   ms(quantile(s, 0.90)),
		P99:   ms(quantile(s, 0.99)),
		P999:  ms(quantile(s, 0.999)),
		Max:   ms(s[len(s)-1]),
	}
}

// quantile does linear interpolation between closest ranks on a sorted slice.
func quantile(sorted []time.Duration, q float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return time.Duration(float64(sorted[lo])*(1-frac) + float64(sorted[hi])*frac)
}
