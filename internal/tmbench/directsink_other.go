//go:build !linux

package tmbench

import (
	"fmt"

	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// newDirectWriterAt is unsupported off Linux: O_DIRECT and its alignment handling
// are Linux-specific. The optimized profile's directIO sink therefore only runs
// on the (Linux) benchmark host; the dev build still compiles via this stub.
func newDirectWriterAt(path string, bufSz int64, prog *metrics.Progress) (closableSink, error) {
	return nil, fmt.Errorf("directIO (O_DIRECT sink) is only supported on Linux")
}
