// Command bench runs the S3 throughput benchmark (AWS SDK for Go v2).
//
// Usage:
//
//	go run ./cmd/bench -config bench.config.json -mode download
//	go run ./cmd/bench -mode upload -label spread-arm64
//	go run ./cmd/bench -mode seed                       # seed download data
//	go run ./cmd/bench -mode download -out results/download-sweep.json  # scratch
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/goabv/s3-benchmark-go/internal/bench"
	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
	"github.com/goabv/s3-benchmark-go/internal/report"
	"github.com/goabv/s3-benchmark-go/internal/s3client"
)

// overrides are per-run CLI knobs that take precedence over the config file,
// mirroring the JS sweep scripts' WORKERS/CONCURRENCY/ITERATIONS/WARMUP/PART_SIZE.
type overrides struct {
	workers      int
	concurrency  int
	iterations   int
	warmup       int
	partSize     string
	delivery     string
	deliveryPath string
	maxBuffered  string // size label, e.g. "64GiB"; "" = use config
	noChecksum   bool   // disable per-part checksum validation (overrides config)
	noTLS        bool   // use the plaintext HTTP endpoint (overrides config)
	out          string
}

func main() {
	cfgPath := flag.String("config", "bench.config.json", "path to bench config JSON")
	mode := flag.String("mode", "download", "benchmark mode: download | upload | seed (upload/download run separately)")
	region := flag.String("region", "", "override region")
	bucket := flag.String("bucket", "", "override bucket")
	label := flag.String("label", "", "optional run label (appended to the run directory name)")

	ov := overrides{}
	flag.IntVar(&ov.workers, "workers", 0, "override workers (0 = use config)")
	flag.IntVar(&ov.concurrency, "concurrency", 0, "override concurrency/worker (0 = use config)")
	flag.IntVar(&ov.iterations, "iterations", 0, "override measured iterations (0 = use config)")
	flag.IntVar(&ov.warmup, "warmup", -1, "override warmup iterations (-1 = use config)")
	flag.StringVar(&ov.partSize, "part-size", "", "override part size, e.g. 32MiB (empty = use config)")
	flag.StringVar(&ov.delivery, "delivery", "", "override download delivery mode: discard | ordered-stream | file")
	flag.StringVar(&ov.deliveryPath, "delivery-path", "", "override delivery path for file mode")
	flag.StringVar(&ov.maxBuffered, "max-buffered", "", "override ordered-stream buffer cap, e.g. 64GiB (0 = unbounded)")
	flag.BoolVar(&ov.noChecksum, "no-checksum", false, "disable per-part checksum validation on download (overrides config)")
	flag.BoolVar(&ov.noTLS, "no-tls", false, "use S3's plaintext HTTP endpoint to measure TLS overhead (overrides config)")
	flag.StringVar(&ov.out, "out", "", "scratch mode: write a single sweep JSON to this path (no run dir / S3)")
	progress := flag.Bool("progress", true, "print a live progress indicator to stderr")
	flag.Parse()

	if err := run(*cfgPath, *mode, *region, *bucket, *label, ov, *progress); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath, mode, region, bucket, label string, ov overrides, progress bool) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	if region != "" {
		cfg.Region = region
	}
	if bucket != "" {
		cfg.Bucket = bucket
	}
	if err := applyOverrides(cfg, mode, ov); err != nil {
		return err
	}

	// Bound RSS so a large ordered-stream buffer can't get the process OOM-killed.
	if lim := metrics.ApplyMemoryLimit(0.80); lim > 0 {
		fmt.Fprintf(os.Stderr, "soft memory limit: %.0f GiB (80%% of RAM)\n", float64(lim)/(1<<30))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	maxConns := cfg.Download.Parallelism()
	if mode != "download" {
		if p := cfg.Upload.Parallelism(); p > maxConns {
			maxConns = p
		}
	}
	client, err := s3client.New(ctx, s3client.Options{
		Region:            cfg.Region,
		MaxConns:          maxConns,
		SpreadConnections: cfg.Download.SpreadConnections,
		TLS:               cfg.Download.TLS,
	})
	if err != nil {
		return err
	}

	// Seeding is a data-prep step, not a benchmark: no sampler, watchdog, or report.
	if mode == "seed" {
		return bench.RunSeed(ctx, client, cfg, &metrics.Progress{})
	}

	// Build the ordered list of phases. "both" runs upload first, then download —
	// but each phase is sampled and reported independently so the numbers never mix.
	phases, err := phasesFor(mode, ctx, client, cfg)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	var runs []*bench.RunResult
	var allSamples []metrics.Sample
	var summary strings.Builder
	totalStalls := 0
	var tOffset int64

	for _, ph := range phases {
		rr, samples, stalls, phaseSummary, err := runPhase(ctx, cfg, ph, progress)
		if err != nil {
			return err
		}
		runs = append(runs, rr)
		totalStalls += stalls
		summary.WriteString(phaseSummary)
		// Offset samples so the combined time-series plot is a continuous timeline.
		for _, s := range samples {
			s.TMillis += tOffset
			allSamples = append(allSamples, s)
		}
		if len(samples) > 0 {
			tOffset = allSamples[len(allSamples)-1].TMillis + cfg.Sampling.SampleInterval().Milliseconds()
		}
		debug.FreeOSMemory() // release this phase's buffers before the next
	}

	// Scratch mode: write standalone sweep JSON(s) and stop (no run dir / S3).
	if ov.out != "" {
		for _, r := range runs {
			path := scratchPath(ov.out, r.Mode, len(runs) > 1)
			if err := report.WriteSweep(path, cfg, r); err != nil {
				return err
			}
			fmt.Printf("wrote sweep: %s\n", path)
		}
		return nil
	}

	art, err := report.Write(ctx, client, report.Options{
		Config:    cfg,
		Mode:      mode,
		Runs:      runs,
		Samples:   allSamples,
		Stalls:    totalStalls,
		StartedAt: startedAt,
		Label:     label,
		Summary:   summary.String(),
	})
	if err != nil {
		return fmt.Errorf("write report: %w", err)
	}

	fmt.Printf("\nrun dir: %s\n", art.Dir)
	if art.Uploaded {
		fmt.Printf("uploaded to: s3://%s/%s\n", art.S3Bucket, art.S3Prefix)
	}
	return nil
}

// phase is one independently-sampled benchmark phase.
type phase struct {
	name string
	fn   func(prog *metrics.Progress) (*bench.RunResult, error)
}

// phasesFor expands a mode into its ordered phases. "both" is upload then download.
func phasesFor(mode string, ctx context.Context, client *s3.Client, cfg *config.Config) ([]phase, error) {
	download := phase{"download", func(p *metrics.Progress) (*bench.RunResult, error) {
		return bench.RunDownload(ctx, client, cfg, p)
	}}
	upload := phase{"upload", func(p *metrics.Progress) (*bench.RunResult, error) {
		return bench.RunUpload(ctx, client, cfg, p)
	}}
	switch mode {
	case "download":
		return []phase{download}, nil
	case "upload":
		return []phase{upload}, nil
	case "both":
		return []phase{upload, download}, nil
	default:
		return nil, fmt.Errorf("unknown mode %q (supported: download, upload, both, seed)", mode)
	}
}

// runPhase executes one phase with its own progress counters, sampler, watchdog,
// and progress printer, then applies that phase's resource usage to its groups and
// renders its tables — keeping every phase's numbers distinct.
func runPhase(ctx context.Context, cfg *config.Config, ph phase, progress bool) (*bench.RunResult, []metrics.Sample, int, string, error) {
	prog := &metrics.Progress{}

	var sampler *metrics.Sampler
	if cfg.Sampling.Enabled {
		sampler = metrics.NewSampler(cfg.Sampling.SampleInterval(), prog)
		sampler.Start()
	}
	stall := cfg.Download.StallTimeout()
	if ph.name == "upload" {
		stall = cfg.Upload.StallTimeout()
	}
	wd := metrics.NewWatchdog(stall, prog)
	wd.Start(ctx)
	var pp *metrics.ProgressPrinter
	if progress {
		pp = metrics.NewProgressPrinter(time.Second, prog, ph.name)
		pp.Start()
	}

	var rr *bench.RunResult
	console, runErr := captureStdout(func() error {
		r, e := ph.fn(prog)
		rr = r
		return e
	})

	if pp != nil {
		pp.Stop()
	}
	stalls := wd.Stop()
	var samples []metrics.Sample
	if sampler != nil {
		samples = sampler.Stop()
	}
	if runErr != nil {
		return nil, nil, 0, "", runErr
	}

	res := bench.ResourcesFromSamples(samples)
	for i := range rr.Groups {
		rr.Groups[i].Resources = res
	}
	tables := report.RenderTables(rr, res)
	fmt.Print(tables)
	if stalls > 0 {
		fmt.Printf("\nwarning: %d stall event(s) during %s\n", stalls, ph.name)
	}
	return rr, samples, stalls, console + tables, nil
}

// applyOverrides layers CLI knobs over the config for the section(s) the mode uses.
func applyOverrides(cfg *config.Config, mode string, ov overrides) error {
	if ov.partSize != "" {
		cfg.PartSize = ov.partSize
	}
	setDownload := func() {
		if ov.workers > 0 {
			cfg.Download.Workers = ov.workers
		}
		if ov.concurrency > 0 {
			cfg.Download.Concurrency = ov.concurrency
		}
		if ov.iterations > 0 {
			cfg.Download.Iterations = ov.iterations
		}
		if ov.warmup >= 0 {
			cfg.Download.Warmup = ov.warmup
		}
		if ov.delivery != "" {
			cfg.Download.DeliveryMode = ov.delivery
		}
		if ov.deliveryPath != "" {
			cfg.Download.DeliveryPath = ov.deliveryPath
		}
		if ov.noChecksum {
			cfg.Download.ValidateChecksum = false
		}
	}
	setUpload := func() {
		if ov.workers > 0 {
			cfg.Upload.Workers = ov.workers
		}
		if ov.concurrency > 0 {
			cfg.Upload.Concurrency = ov.concurrency
		}
		if ov.iterations > 0 {
			cfg.Upload.Iterations = ov.iterations
		}
		if ov.warmup >= 0 {
			cfg.Upload.Warmup = ov.warmup
		}
	}
	switch mode {
	case "download":
		setDownload()
	case "upload", "seed":
		setUpload()
	case "both":
		setDownload()
		setUpload()
	}

	if ov.maxBuffered != "" && (mode == "download" || mode == "both") {
		n, err := config.ParseSize(ov.maxBuffered)
		if err != nil {
			return fmt.Errorf("parse -max-buffered %q: %w", ov.maxBuffered, err)
		}
		cfg.Download.MaxBufferedBytes = n
	}

	// TLS affects the shared client, so apply it regardless of mode.
	if ov.noTLS {
		cfg.Download.TLS = false
	}
	return nil
}

// scratchPath returns the -out path, disambiguating by mode when a single run
// produced multiple sweeps (mode "both").
func scratchPath(out, mode string, multi bool) string {
	if !multi {
		return out
	}
	if i := strings.LastIndex(out, "."); i >= 0 {
		return out[:i] + "-" + mode + out[i:]
	}
	return out + "-" + mode
}

// captureStdout redirects os.Stdout through a pipe for the duration of fn, teeing
// everything both to the real stdout and to a buffer that is returned.
func captureStdout(fn func() error) (string, error) {
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", fn() // fall back to no capture
	}
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.MultiWriter(orig, &buf), r)
		close(done)
	}()

	runErr := fn()

	w.Close()
	os.Stdout = orig
	<-done
	return buf.String(), runErr
}
