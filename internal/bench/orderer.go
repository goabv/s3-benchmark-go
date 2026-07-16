package bench

import (
	"io"
	"sync"

	"github.com/goabv/s3-benchmark-go/internal/bufpool"
)

// orderer reassembles an object's parts into ascending order before writing them
// to a sink, modeling in-order stream delivery while parts download in parallel.
// Buffers are recycled to the pool once flushed.
type orderer struct {
	mu      sync.Mutex
	next    int32
	pending map[int32]*[]byte
	sink    io.Writer
	pool    *bufpool.Pool
}

func newOrderer(sink io.Writer, pool *bufpool.Pool) *orderer {
	return &orderer{next: 1, pending: map[int32]*[]byte{}, sink: sink, pool: pool}
}

// deliver hands one part's bytes to the orderer, which flushes as many contiguous
// parts as possible to the sink in ascending order, recycling each buffer. It
// returns how many parts were flushed (for buffered-bytes budget accounting).
func (o *orderer) deliver(part int32, buf *[]byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[part] = buf
	flushed := 0
	for {
		b, ok := o.pending[o.next]
		if !ok {
			return flushed, nil
		}
		delete(o.pending, o.next)
		if _, err := o.sink.Write(*b); err != nil {
			o.pool.Put(b)
			return flushed, err
		}
		o.pool.Put(b)
		o.next++
		flushed++
	}
}

// streamInto reads r fully into a pooled buffer, growing beyond the pool size only
// if a part exceeds the configured capacity. It returns the (possibly grown)
// buffer, the byte count, and any read error.
func streamInto(buf *[]byte, r io.Reader) (*[]byte, int64, error) {
	b := (*buf)[:0]
	for {
		if len(b) == cap(b) {
			grown := make([]byte, len(b), cap(b)*2)
			copy(grown, b)
			b = grown
		}
		n, err := r.Read(b[len(b):cap(b)])
		if n > 0 {
			b = b[:len(b)+n]
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			nb := b
			return &nb, int64(len(b)), err
		}
	}
	nb := b
	return &nb, int64(len(b)), nil
}
