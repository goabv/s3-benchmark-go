package metrics

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Watchdog watches cumulative progress and logs a warning whenever no bytes move
// for longer than timeout while requests are still in flight. It counts stall
// events; it does not cancel work.
type Watchdog struct {
	timeout time.Duration
	prog    *Progress
	stalls  int64
	stop    chan struct{}
	done    chan struct{}
}

// NewWatchdog builds a watchdog bound to the given progress counters.
func NewWatchdog(timeout time.Duration, prog *Progress) *Watchdog {
	return &Watchdog{
		timeout: timeout,
		prog:    prog,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Start launches the watchdog goroutine.
func (w *Watchdog) Start(ctx context.Context) {
	go func() {
		defer close(w.done)
		if w.timeout <= 0 {
			return
		}
		check := w.timeout / 2
		if check <= 0 {
			check = w.timeout
		}
		tick := time.NewTicker(check)
		defer tick.Stop()

		last := atomic.LoadInt64(&w.prog.Bytes)
		lastMove := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stop:
				return
			case now := <-tick.C:
				cur := atomic.LoadInt64(&w.prog.Bytes)
				if cur != last {
					last, lastMove = cur, now
					continue
				}
				if now.Sub(lastMove) >= w.timeout && atomic.LoadInt64(&w.prog.InFlight) > 0 {
					atomic.AddInt64(&w.stalls, 1)
					fmt.Fprintf(os.Stderr, "[watchdog] stall: no progress for %s (in-flight=%d)\n",
						now.Sub(lastMove).Round(time.Millisecond), atomic.LoadInt64(&w.prog.InFlight))
					lastMove = now // warn at most once per timeout window
				}
			}
		}
	}()
}

// Stop halts the watchdog and returns the number of stall events observed.
func (w *Watchdog) Stop() int {
	close(w.stop)
	<-w.done
	return int(atomic.LoadInt64(&w.stalls))
}
