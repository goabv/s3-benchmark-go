package bench

import (
	"io"
	"sync"

	"github.com/goabv/s3-benchmark-go/internal/bufpool"
)

// orderedReader presents one object's parts as a single in-order io.Reader.
// Fetch workers push parts (possibly out of order) via push(); the reader emits
// them to its consumer in ascending part order through a bounded channel (which
// provides backpressure). It implements io.WriterTo so io.Copy drains it with no
// intermediate copy — the pooled part buffer is handed straight to the
// destination writer and then recycled — while Read remains available for
// consumers that need the pull interface. This makes the custom runner's ordered
// output a drop-in, directly-consumable stream, as ergonomic as the SDK Transfer
// Manager's GetObject Body.
type orderedReader struct {
	pool *bufpool.Pool
	ch   chan *[]byte
	done chan struct{}
	once sync.Once

	mu      sync.Mutex
	next    int32
	nparts  int32
	pending map[int32]*[]byte
	closed  bool

	// Read-path (single consumer) state.
	cur *[]byte
	off int
}

func newOrderedReader(nparts int32, capBuffers int, pool *bufpool.Pool) *orderedReader {
	if capBuffers < 1 {
		capBuffers = 1
	}
	return &orderedReader{
		pool:    pool,
		ch:      make(chan *[]byte, capBuffers),
		done:    make(chan struct{}),
		next:    1,
		nparts:  nparts,
		pending: make(map[int32]*[]byte, capBuffers),
	}
}

// push hands one part's buffer to the reader, transferring ownership: contiguous
// parts are emitted to the consumer in order (the consumer recycles their
// buffers), and the stream is closed once the final part is emitted. It blocks
// when the consumer is behind (backpressure) and returns io.ErrClosedPipe if the
// reader was aborted early.
func (r *orderedReader) push(part int32, buf *[]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		r.pool.Put(buf)
		return io.ErrClosedPipe
	}
	r.pending[part] = buf
	for {
		b, ok := r.pending[r.next]
		if !ok {
			return nil
		}
		delete(r.pending, r.next)
		select {
		case r.ch <- b:
		case <-r.done:
			r.pool.Put(b)
			r.closed = true
			return io.ErrClosedPipe
		}
		r.next++
		if r.next > r.nparts {
			close(r.ch)
			r.closed = true
			return nil
		}
	}
}

// WriteTo drains the ordered stream to w with no intermediate copy: each pooled
// part buffer is written directly and then recycled. This is the fast path taken
// by io.Copy.
func (r *orderedReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		select {
		case b, ok := <-r.ch:
			if !ok {
				return total, nil
			}
			n, err := w.Write(*b)
			total += int64(n)
			r.pool.Put(b)
			if err != nil {
				r.abort()
				return total, err
			}
		case <-r.done:
			return total, nil
		}
	}
}

// Read is the pull-interface fallback (copies into the caller's p, like a typical
// reader). Provided for consumers that don't use io.Copy/WriteTo.
func (r *orderedReader) Read(p []byte) (int, error) {
	for r.cur == nil {
		select {
		case b, ok := <-r.ch:
			if !ok {
				return 0, io.EOF
			}
			if len(*b) == 0 {
				r.pool.Put(b)
				continue
			}
			r.cur, r.off = b, 0
		case <-r.done:
			return 0, io.EOF
		}
	}
	n := copy(p, (*r.cur)[r.off:])
	r.off += n
	if r.off >= len(*r.cur) {
		r.pool.Put(r.cur)
		r.cur = nil
	}
	return n, nil
}

// Close aborts the stream and releases any buffered parts.
func (r *orderedReader) Close() error {
	r.abort()
	return nil
}

// abort unblocks any pending push or drain (used on consumer error or run abort).
func (r *orderedReader) abort() {
	r.once.Do(func() { close(r.done) })
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
