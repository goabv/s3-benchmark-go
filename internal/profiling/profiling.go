// Package profiling wires up optional pprof instrumentation for tmbench runs:
// a live HTTP endpoint for interactive inspection, and/or file-based CPU,
// block, and mutex profiles spanning the whole benchmark invocation.
package profiling

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	pprofruntime "runtime/pprof"
)

// Options controls which profiling instrumentation is enabled for a run.
type Options struct {
	// HTTPAddr, if non-empty, starts a pprof HTTP server on this address
	// (e.g. "127.0.0.1:6060"). Reach it via SSM port forwarding or an SSH
	// tunnel; binding to localhost keeps it off the network by default.
	HTTPAddr string

	// CPUProfilePath, if non-empty, writes a CPU profile covering the full
	// run to this path.
	CPUProfilePath string

	// BlockProfilePath, if non-empty, writes a blocking profile (time spent
	// blocked on channel ops, syscalls the runtime tracks as blocking, etc.)
	// covering the full run to this path.
	BlockProfilePath string

	// MutexProfilePath, if non-empty, writes a contended-mutex profile
	// covering the full run to this path.
	MutexProfilePath string

	// BlockProfileRate sets runtime.SetBlockProfileRate when block or mutex
	// profiling is requested. 1 = sample every blocking event (most detail,
	// most overhead). Defaults to 1 if left at 0 and either path is set.
	BlockProfileRate int

	// MutexProfileFraction sets runtime.SetMutexProfileFraction. 1 = sample
	// every contention event. Defaults to 1 if left at 0 and MutexProfilePath
	// is set.
	MutexProfileFraction int
}

// Stop, returned by Start, finalizes and writes any enabled file-based
// profiles. Call it via defer immediately after a successful Start.
type Stop func()

// Start applies Options: launches the HTTP server (if configured) and begins
// CPU/block/mutex profiling (if configured). The returned Stop must be called
// before the process exits for file-based profiles to be flushed and valid.
func Start(opts Options) (Stop, error) {
	if opts.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		srv := &http.Server{Addr: opts.HTTPAddr, Handler: mux}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "pprof http server error: %v\n", err)
			}
		}()
		fmt.Fprintf(os.Stderr, ">> pprof http endpoint: http://%s/debug/pprof/\n", opts.HTTPAddr)
	}

	var cpuFile *os.File
	if opts.CPUProfilePath != "" {
		f, err := os.Create(opts.CPUProfilePath)
		if err != nil {
			return nil, fmt.Errorf("create cpu profile %q: %w", opts.CPUProfilePath, err)
		}
		if err := pprofruntime.StartCPUProfile(f); err != nil {
			f.Close()
			return nil, fmt.Errorf("start cpu profile: %w", err)
		}
		cpuFile = f
		fmt.Fprintf(os.Stderr, ">> cpu profile: %s\n", opts.CPUProfilePath)
	}

	if opts.BlockProfilePath != "" {
		rate := opts.BlockProfileRate
		if rate == 0 {
			rate = 1
		}
		runtime.SetBlockProfileRate(rate)
		fmt.Fprintf(os.Stderr, ">> block profile: %s (rate=%d)\n", opts.BlockProfilePath, rate)
	}

	if opts.MutexProfilePath != "" {
		frac := opts.MutexProfileFraction
		if frac == 0 {
			frac = 1
		}
		runtime.SetMutexProfileFraction(frac)
		fmt.Fprintf(os.Stderr, ">> mutex profile: %s (fraction=%d)\n", opts.MutexProfilePath, frac)
	}

	return func() {
		if cpuFile != nil {
			pprofruntime.StopCPUProfile()
			cpuFile.Close()
		}
		if opts.BlockProfilePath != "" {
			writeProfile("block", opts.BlockProfilePath)
			runtime.SetBlockProfileRate(0)
		}
		if opts.MutexProfilePath != "" {
			writeProfile("mutex", opts.MutexProfilePath)
			runtime.SetMutexProfileFraction(0)
		}
	}, nil
}

func writeProfile(name, path string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s profile %q: %v\n", name, path, err)
		return
	}
	defer f.Close()
	if err := pprofruntime.Lookup(name).WriteTo(f, 0); err != nil {
		fmt.Fprintf(os.Stderr, "write %s profile %q: %v\n", name, path, err)
	}
}
