// Package bufpool hands out fixed-capacity byte buffers and recycles them to cut
// allocation and GC pressure during high-throughput streaming.
package bufpool

import "sync"

// Pool recycles []byte buffers of a fixed capacity.
type Pool struct {
	pool sync.Pool
	size int
}

// New returns a pool whose buffers have the given capacity.
func New(size int) *Pool {
	if size <= 0 {
		size = 8 << 20
	}
	p := &Pool{size: size}
	p.pool.New = func() any {
		b := make([]byte, 0, size)
		return &b
	}
	return p
}

// Get returns a buffer sliced to length 0 with capacity >= the pool size.
// A pointer is used so the value stored in sync.Pool does not itself allocate.
func (p *Pool) Get() *[]byte {
	b := p.pool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

// Put recycles a buffer. Buffers smaller than the pool size are dropped.
func (p *Pool) Put(b *[]byte) {
	if b == nil || cap(*b) < p.size {
		return
	}
	p.pool.Put(b)
}

// Size is the configured buffer capacity.
func (p *Pool) Size() int { return p.size }
