// Command tmbench benchmarks the AWS SDK for Go v2 S3 Transfer Manager
// (feature/s3/transfermanager) with fully in-memory data — no local files.
// Upload streams from a repeating in-memory buffer via UploadObject; download
// drains GetObject bodies to io.Discard. It targets the same seeded objects as
// the main benchmark and captures results into results/runs/ for comparison.
//
// Usage:
//
//	go run ./cmd/tmbench -mode download
//	go run ./cmd/tmbench -mode upload -concurrency 128 -label tm-baseline
//	go run ./cmd/tmbench -mode both
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

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"

	"github.com/goabv/s3-benchmark-go/internal/bench"
	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
	"github.com/goabv/s3-benchmark-go/internal/report"
	"github.com/goabv/s3-benchmark-go/internal/s3client"
	"github.com/goabv/s3-benchmark-go/internal/tmbench"
)

func main() {
	cfgPath := flag.String("config", "bench.config.json", "path to bench config JSON")
	mode := flag.String("mode", "download", "benchmark mode: upload | download | both (both = upload then download, reported separately)")
	region := flag.String("region", "", "override region")
	bucket := flag.String("bucket", "", "override bucket")
	label := flag.String("label", "", "optional run label (appended to the run directory name)")
	concurrency := flag.Int("concurrency", 0, "TM part concurrency per object (0 = auto: total/object-concurrency)")
	objectConc := flag.Int("object-concurrency", 0, "objects transferred in parallel (0 = auto: object count)")
	partSize := flag.String("part-size", "", "Transfer Manager part size, e.g. 32MiB (empty = config partSize)")
	maxBuffered := flag.String("max-buffered", "", "GetObject read-ahead buffer, e.g. 64GiB (empty = config maxBufferedBytes)")
	prefix := flag.String("prefix", "tm-upload/", "key prefix for uploaded objects")
	progress := flag.Bool("progress", true, "print a live progress indicator to stderr")
	flag.Parse()

	if err := run(*cfgPath, *mode, *region, *bucket, *label, *concurrency, *objectConc, *partSize, *maxBuffered, *prefix, *progress); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath, mode, region, bucket, label string, concurrency, objectConc int, partSize, maxBuffered, prefix string, progress bool) error {
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
	if partSize != "" {
		cfg.PartSize = partSize
	}
	if maxBuffered != "" {
		n, err := config.ParseSize(maxBuffered)
		if err != nil {
			return fmt.Errorf("parse -max-buffered %q: %w", maxBuffered, err)
		}
		cfg.Download.MaxBufferedBytes = n
	}

	// Bound RSS so a large GetObject read-ahead buffer can't get the process
	// OOM-killed; the GC reclaims harder as it nears the limit.
	if lim := metrics.ApplyMemoryLimit(0.80); lim > 0 {
		fmt.Fprintf(os.Stderr, "soft memory limit: %s (80%% of RAM)\n", humanBytes(lim))
	}

	switch mode {
	case "upload", "download", "both":
	default:
		return fmt.Errorf("unknown mode %q (supported: upload, download, both)", mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Resolve the two parallelism dimensions so the total parts in flight
	// (objectConcurrency x perObjectConcurrency) defaults to the custom runner's
	// workers x concurrency for an apples-to-apples comparison.
	targetTotal := cfg.Download.Parallelism()
	if mode == "upload" {
		targetTotal = cfg.Upload.Parallelism()
	}
	if objectConc <= 0 {
		objectConc = maxObjectCount(cfg)
	}
	perObjectConc := concurrency
	if perObjectConc <= 0 {
		perObjectConc = targetTotal / objectConc
		if perObjectConc < 1 {
			perObjectConc = 1
		}
	}

	// Size the keep-alive transport to the total parts in flight so connections
	// aren't the bottleneck; no connection-spreading (isolate the TM's behavior).
	maxConns := objectConc * perObjectConc
	if maxConns < 64 {
		maxConns = 64
	}
	s3c, err := s3client.New(ctx, s3client.Options{
		Region:   cfg.Region,
		MaxConns: maxConns,
		TLS:      true,
	})
	if err != nil {
		return err
	}

	tmPartSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return err
	}
	// GetObject only fetches GetObjectBufferSize/partSize parts ahead of the
	// consumer (default 50MiB => ~1 part for 32MiB parts), which throttles download
	// regardless of Concurrency. download.maxBufferedBytes (the same key/value the
	// JS runner uses, 64GiB) is a TOTAL budget split across the concurrent objects,
	// since GetObjectBufferSize is per-GetObject-call.
	getBuffer := resolveGetBuffer(cfg.Download.MaxBufferedBytes, objectConc, perObjectConc, tmPartSize)
	tm := transfermanager.New(s3c, func(o *transfermanager.Options) {
		o.Concurrency = perObjectConc
		if tmPartSize > 0 {
			o.PartSizeBytes = tmPartSize
		}
		o.GetObjectBufferSize = getBuffer
	})
	if cfg.Download.MaxBufferedBytes > 0 {
		fmt.Printf("tm read-ahead: total-budget=%s across %d objects -> %s/object (%d parts/object)\n\n",
			humanBytes(cfg.Download.MaxBufferedBytes), objectConc, humanBytes(getBuffer), getBuffer/max64(tmPartSize, 1))
	}

	// "both" runs upload first, then download — each sampled and reported
	// independently so the numbers never mix.
	phases := phasesFor(mode, ctx, tm, cfg, prefix, objectConc, perObjectConc)

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
		for _, s := range samples {
			s.TMillis += tOffset
			allSamples = append(allSamples, s)
		}
		if len(samples) > 0 {
			tOffset = allSamples[len(allSamples)-1].TMillis + cfg.Sampling.SampleInterval().Milliseconds()
		}
		debug.FreeOSMemory() // release this phase's buffers before the next
	}

	art, err := report.Write(ctx, s3c, report.Options{
		Config:    cfg,
		Mode:      "tm-" + mode,
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

// phase is one independently-sampled Transfer Manager phase.
type phase struct {
	name string
	fn   func(prog *metrics.Progress) (*bench.RunResult, error)
}

// phasesFor expands a mode into ordered phases. "both" is upload then download.
func phasesFor(mode string, ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prefix string, objectConc, perObjectConc int) []phase {
	upload := phase{"tm-upload", func(p *metrics.Progress) (*bench.RunResult, error) {
		return tmbench.RunUpload(ctx, tm, cfg, p, prefix, objectConc, perObjectConc)
	}}
	download := phase{"tm-download", func(p *metrics.Progress) (*bench.RunResult, error) {
		return tmbench.RunDownload(ctx, tm, cfg, p, objectConc, perObjectConc)
	}}
	switch mode {
	case "upload":
		return []phase{upload}
	case "download":
		return []phase{download}
	default: // both
		return []phase{upload, download}
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
	if ph.name == "tm-upload" {
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

// resolveGetBuffer chooses the per-object TM GetObject read-ahead buffer. When
// maxBufferedBytes is set it is treated as a TOTAL budget (matching the JS
// runner's value) and divided across the concurrent objects, floored at one part
// per object so read-ahead never stalls. Otherwise it falls back to enough to
// hold perObjectConc parts, floored at the SDK's 50MiB default.
func resolveGetBuffer(maxBufferedBytes int64, objectConc, perObjectConc int, partSize int64) int64 {
	if objectConc < 1 {
		objectConc = 1
	}
	if maxBufferedBytes > 0 {
		perObject := maxBufferedBytes / int64(objectConc)
		if perObject < partSize {
			perObject = partSize // at least one part of read-ahead per object
		}
		return perObject
	}
	buf := int64(perObjectConc) * partSize
	if buf < 50<<20 {
		buf = 50 << 20
	}
	return buf
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// maxObjectCount returns the largest object count across configured size groups.
func maxObjectCount(cfg *config.Config) int {
	max := 1
	for _, s := range cfg.Sizes {
		if s.Count > max {
			max = s.Count
		}
	}
	return max
}

func captureStdout(fn func() error) (string, error) {
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", fn()
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
