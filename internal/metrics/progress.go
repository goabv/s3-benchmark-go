// Package metrics collects benchmark measurements: latency percentiles, a
// background time-series sampler for process resource usage, and an SVG plotter.
package metrics

// Progress holds the live counters shared between the benchmark workers, the
// time-series sampler, and the stall watchdog. All fields must be accessed with
// sync/atomic.
type Progress struct {
	InFlight int64 // requests currently awaiting or streaming a response
	Bytes    int64 // cumulative bytes transferred across the whole run
}
