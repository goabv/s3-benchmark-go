package metrics

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Sample is one time-series observation of process + progress state.
type Sample struct {
	TMillis  int64   `json:"tMs"`      // elapsed since sampler start
	RSSBytes int64   `json:"rssBytes"` // resident set size
	CPUPct   float64 `json:"cpuPct"`   // process CPU % since previous sample (can exceed 100 on multicore)
	InFlight int64   `json:"inFlight"` // in-flight requests
	BytesCum int64   `json:"bytesCum"` // cumulative bytes transferred
	Gbps     float64 `json:"gbps"`     // instantaneous throughput since previous sample
}

// Sampler records resource + progress samples on a background goroutine at a
// fixed interval until Stop is called.
type Sampler struct {
	interval time.Duration
	prog     *Progress
	stop     chan struct{}
	done     chan struct{}
	samples  []Sample
}

// NewSampler builds a sampler bound to the given progress counters.
func NewSampler(interval time.Duration, prog *Progress) *Sampler {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	return &Sampler{
		interval: interval,
		prog:     prog,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the sampling goroutine.
func (s *Sampler) Start() { go s.loop() }

func (s *Sampler) loop() {
	defer close(s.done)
	start := time.Now()
	tick := time.NewTicker(s.interval)
	defer tick.Stop()

	prevT := start
	prevCPU := readProcStat().CPUTime
	prevBytes := atomic.LoadInt64(&s.prog.Bytes)
	cores := runtime.NumCPU()
	if cores < 1 {
		cores = 1
	}

	for {
		select {
		case <-s.stop:
			return
		case now := <-tick.C:
			ps := readProcStat()
			bytes := atomic.LoadInt64(&s.prog.Bytes)
			inflight := atomic.LoadInt64(&s.prog.InFlight)
			dt := now.Sub(prevT).Seconds()

			var cpuPct, gbps float64
			if dt > 0 {
				// CPU time consumed per wall second, normalized to "% of all cores"
				// so 100% means every core fully busy (matches the JS runner).
				cpuPct = (ps.CPUTime - prevCPU).Seconds() / dt / float64(cores) * 100
				gbps = float64(bytes-prevBytes) * 8 / dt / 1e9
			}

			s.samples = append(s.samples, Sample{
				TMillis:  now.Sub(start).Milliseconds(),
				RSSBytes: ps.RSSBytes,
				CPUPct:   cpuPct,
				InFlight: inflight,
				BytesCum: bytes,
				Gbps:     gbps,
			})
			prevT, prevCPU, prevBytes = now, ps.CPUTime, bytes
		}
	}
}

// Stop halts sampling and returns the collected samples.
func (s *Sampler) Stop() []Sample {
	close(s.stop)
	<-s.done
	return s.samples
}
