//go:build linux

package tmbench

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// directBlockSize is the alignment (offset, length, and buffer address) required
// for O_DIRECT I/O. 4096 is a safe superset of the 512/4096 logical block sizes
// used by EBS/XFS.
const directBlockSize = 4096

// directWriterAt is an O_DIRECT-backed io.WriterAt for the optimized file sink.
//
// The Transfer Manager streams each object part through io.Copy, so WriteAt is
// called with many small, arbitrarily-sized, unaligned chunks — which O_DIRECT
// rejects (EINVAL). This sink buffers incoming bytes into fixed-size aligned
// regions (bufSz, a multiple of the block size) and flushes each region in a
// single aligned, cache-bypassing WriteAt once it is full. Because XFS takes only
// a shared inode lock for O_DIRECT writes, the concurrent part-writers hit the
// device in parallel instead of serializing on the buffered single-inode path.
//
// The object's final sub-block tail (when the size is not a block multiple) is
// padded up to the block size on the last flush, then chopped back with ftruncate
// in Close. Safe for concurrent WriteAt across disjoint regions.
type directWriterAt struct {
	f     *os.File
	bufSz int64
	prog  *metrics.Progress

	total  int64 // bytes accepted (for throughput accounting)
	maxEnd int64 // highest offset+len seen (exact final file size)

	mu      sync.Mutex
	regions map[int64]*directRegion
}

type directRegion struct {
	mu     sync.Mutex
	buf    []byte // block-aligned, len == bufSz
	filled int64  // bytes written into this region so far
}

// newDirectWriterAt opens path with O_DIRECT and returns a sink that flushes in
// aligned regions of bufSz (rounded up to the block size).
func newDirectWriterAt(path string, bufSz int64, prog *metrics.Progress) (closableSink, error) {
	if bufSz <= 0 {
		bufSz = 32 << 20
	}
	if r := bufSz % directBlockSize; r != 0 {
		bufSz += directBlockSize - r
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open O_DIRECT %q: %w", path, err)
	}
	return &directWriterAt{
		f:       f,
		bufSz:   bufSz,
		prog:    prog,
		regions: make(map[int64]*directRegion),
	}, nil
}

func (d *directWriterAt) WriteAt(p []byte, off int64) (int, error) {
	total := len(p)
	end := off + int64(total)
	atomic.AddInt64(&d.total, int64(total))
	for {
		cur := atomic.LoadInt64(&d.maxEnd)
		if end <= cur || atomic.CompareAndSwapInt64(&d.maxEnd, cur, end) {
			break
		}
	}
	if d.prog != nil {
		atomic.AddInt64(&d.prog.Bytes, int64(total))
	}

	pos := off
	rem := p
	for len(rem) > 0 {
		ridx := pos / d.bufSz
		intra := pos % d.bufSz
		take := d.bufSz - intra
		if take > int64(len(rem)) {
			take = int64(len(rem))
		}
		r := d.getRegion(ridx)
		r.mu.Lock()
		copy(r.buf[intra:intra+take], rem[:take])
		r.filled += take
		full := r.filled >= d.bufSz
		r.mu.Unlock()
		if full {
			// Region complete (all bufSz bytes received); no further writes will
			// target it, so flushing outside the lock is safe.
			if err := d.flushRegion(ridx, r, d.bufSz); err != nil {
				return 0, err
			}
		}
		pos += take
		rem = rem[take:]
	}
	return total, nil
}

func (d *directWriterAt) getRegion(ridx int64) *directRegion {
	d.mu.Lock()
	r := d.regions[ridx]
	if r == nil {
		r = &directRegion{buf: alignedBuf(d.bufSz, directBlockSize)}
		d.regions[ridx] = r
	}
	d.mu.Unlock()
	return r
}

// flushRegion writes length (block-aligned) bytes of the region to its file
// offset via a single O_DIRECT write, then drops it from the map.
func (d *directWriterAt) flushRegion(ridx int64, r *directRegion, length int64) error {
	if _, err := d.f.WriteAt(r.buf[:length], ridx*d.bufSz); err != nil {
		return fmt.Errorf("O_DIRECT write region %d: %w", ridx, err)
	}
	d.mu.Lock()
	delete(d.regions, ridx)
	d.mu.Unlock()
	return nil
}

func (d *directWriterAt) written() int64 { return atomic.LoadInt64(&d.total) }

// Close flushes the remaining (final, partial) region padded up to the block
// size, truncates the file to the exact object size, and closes the fd. Must be
// called after the DownloadObject call returns (no concurrent WriteAt).
func (d *directWriterAt) Close() error {
	d.mu.Lock()
	idxs := make([]int64, 0, len(d.regions))
	for i := range d.regions {
		idxs = append(idxs, i)
	}
	regs := d.regions
	d.mu.Unlock()

	for _, i := range idxs {
		r := regs[i]
		writeLen := r.filled
		if rem := writeLen % directBlockSize; rem != 0 {
			// Pad to the next block; bytes beyond filled are zero (fresh buffer)
			// and get chopped by the truncate below.
			writeLen += directBlockSize - rem
		}
		if writeLen > 0 {
			if err := d.flushRegion(i, r, writeLen); err != nil {
				d.f.Close()
				return err
			}
		}
	}
	if err := d.f.Truncate(atomic.LoadInt64(&d.maxEnd)); err != nil {
		d.f.Close()
		return fmt.Errorf("truncate: %w", err)
	}
	return d.f.Close()
}

// alignedBuf returns a byte slice of length size whose backing-array start is
// aligned to align bytes. Relies on the Go runtime not moving heap allocations
// (true for the current non-moving GC), which O_DIRECT requires for the buffer
// address.
func alignedBuf(size, align int64) []byte {
	b := make([]byte, size+align)
	off := int64(uintptr(unsafe.Pointer(&b[0])) % uintptr(align))
	if off != 0 {
		off = align - off
	}
	return b[off : off+size]
}
