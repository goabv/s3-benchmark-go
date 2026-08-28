package metrics

import "runtime/debug"

// ApplyMemoryLimit sets Go's soft memory limit (GOMEMLIMIT equivalent) to the
// given fraction of total system RAM, so the garbage collector works harder as it
// approaches the cap and the process avoids the kernel OOM killer under large
// read-ahead buffers. It returns the limit applied (0 if total RAM is unknown or
// the fraction is out of range, e.g. on non-Linux dev machines).
func ApplyMemoryLimit(fraction float64) int64 {
	total := TotalMemBytes()
	if total <= 0 || fraction <= 0 || fraction >= 1 {
		return 0
	}
	limit := int64(float64(total) * fraction)
	debug.SetMemoryLimit(limit)
	return limit
}

// SetMemoryLimit sets Go's soft memory limit (GOMEMLIMIT equivalent) to an
// absolute number of bytes and returns the limit applied (0 if limit <= 0, in
// which case the runtime default / any prior limit is left untouched). Use this
// for a fixed ceiling independent of machine RAM (e.g. to hold RSS near the live
// working set and make the GC collect harder). ApplyMemoryLimit is the
// RAM-fraction variant; a caller should use one or the other.
func SetMemoryLimit(limit int64) int64 {
	if limit <= 0 {
		return 0
	}
	debug.SetMemoryLimit(limit)
	return limit
}
