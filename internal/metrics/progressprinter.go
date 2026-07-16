package metrics

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// ProgressPrinter prints a live, single-line progress indicator (cumulative
// bytes, instantaneous + average throughput, in-flight) to stderr on a timer.
// It only reads the shared atomic counters, so it never touches the transfer hot
// path — enabling it has no measurable effect on throughput. Output goes to
// stderr so it stays out of the captured stdout summary.
type ProgressPrinter struct {
	interval time.Duration
	prog     *Progress
	label    string
	stop     chan struct{}
	done     chan struct{}
}

// NewProgressPrinter builds a printer bound to the given counters.
func NewProgressPrinter(interval time.Duration, prog *Progress, label string) *ProgressPrinter {
	if interval <= 0 {
		interval = time.Second
	}
	return &ProgressPrinter{
		interval: interval,
		prog:     prog,
		label:    label,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the printer goroutine.
func (p *ProgressPrinter) Start() { go p.loop() }

func (p *ProgressPrinter) loop() {
	defer close(p.done)
	start := time.Now()
	tick := time.NewTicker(p.interval)
	defer tick.Stop()

	prevT := start
	prevBytes := atomic.LoadInt64(&p.prog.Bytes)
	for {
		select {
		case <-p.stop:
			return
		case now := <-tick.C:
			cur := atomic.LoadInt64(&p.prog.Bytes)
			inflight := atomic.LoadInt64(&p.prog.InFlight)
			dt := now.Sub(prevT).Seconds()
			elapsed := now.Sub(start).Seconds()
			var inst, avg float64
			if dt > 0 {
				inst = float64(cur-prevBytes) * 8 / dt / 1e9
			}
			if elapsed > 0 {
				avg = float64(cur) * 8 / elapsed / 1e9
			}
			fmt.Fprintf(os.Stderr, "\r[%s] %6.1fs  cum=%7.2f GiB  inst=%7.2f Gbps  avg=%7.2f Gbps  in-flight=%-5d ",
				p.label, elapsed, float64(cur)/(1<<30), inst, avg, inflight)
			prevT, prevBytes = now, cur
		}
	}
}

// Stop halts the printer and terminates the progress line.
func (p *ProgressPrinter) Stop() {
	close(p.stop)
	<-p.done
	fmt.Fprintln(os.Stderr)
}
