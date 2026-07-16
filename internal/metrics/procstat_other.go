//go:build !linux

package metrics

import "runtime"

// readProcStat is the portable fallback for non-Linux dev machines. There is no
// dependency-free way to read true RSS/CPU, so it approximates RSS with the
// memory obtained from the OS and leaves CPU unset. On the Linux benchmark host
// the build-tagged reader reports real values.
func readProcStat() ProcStat {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return ProcStat{RSSBytes: int64(m.Sys)}
}

// totalMemBytes has no portable source without extra deps; report unknown.
func totalMemBytes() int64 { return 0 }
