package metrics

import "time"

// ProcStat is a point-in-time snapshot of this process's resource usage. It is
// populated by the platform-specific readProcStat.
type ProcStat struct {
	RSSBytes int64         // resident set size
	CPUTime  time.Duration // cumulative user+system CPU consumed by the process
}

// TotalMemBytes returns total system RAM in bytes (0 if unknown on this platform).
func TotalMemBytes() int64 { return totalMemBytes() }
