//go:build linux

package metrics

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// clockTicks is the standard Linux USER_HZ. Reading it precisely requires cgo
// (sysconf(_SC_CLK_TCK)); 100 is the near-universal value and is correct for the
// EC2 benchmark hosts.
const clockTicks = 100

// readProcStat reads real RSS and CPU time from /proc on Linux.
func readProcStat() ProcStat {
	var ps ProcStat

	// /proc/self/statm field 2 (0-indexed 1) is resident pages.
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) >= 2 {
			if resident, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				ps.RSSBytes = resident * int64(os.Getpagesize())
			}
		}
	}

	// /proc/self/stat: the comm field (2) is wrapped in parens and may contain
	// spaces, so split after the closing ") ". The remaining fields then start at
	// field 3 (state), making utime (field 14) index 11 and stime (field 15) index 12.
	if data, err := os.ReadFile("/proc/self/stat"); err == nil {
		s := string(data)
		if i := strings.LastIndex(s, ") "); i >= 0 {
			rest := strings.Fields(s[i+2:])
			if len(rest) >= 13 {
				utime, _ := strconv.ParseInt(rest[11], 10, 64)
				stime, _ := strconv.ParseInt(rest[12], 10, 64)
				ticks := utime + stime
				ps.CPUTime = time.Duration(ticks) * time.Second / clockTicks
			}
		}
	}
	return ps
}

// totalMemBytes reads MemTotal (kB) from /proc/meminfo.
func totalMemBytes() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}
