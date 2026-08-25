// Command tmbench benchmarks the AWS SDK for Go v2 S3 Transfer Manager
// (feature/s3/transfermanager) with fully in-memory data — no local files.
// Upload streams from a repeating in-memory buffer via UploadObject; download
// drains GetObject bodies to io.Discard. It targets the same seeded objects as
// the main benchmark and captures results into results/runs/ for comparison.
//
// All Transfer Manager knobs (api, sink, deliveryPath, getObjectType,
// objectConcurrency, concurrency, iterations, warmup, validateChecksum,
// maxBufferedBytes) live in bench.config.json under "transferManager". The flags
// below are optional per-run overrides (empty/0 = use the config value); -mode
// selects what to run.
//
// -profile selects the config section: baseline (transferManager, the pristine TM
// baseline) or optimized (tmOptimized, which carries mods like directIO / O_DIRECT).
//
// Usage:
//
//	go run ./cmd/tmbench -mode download                 # profile=baseline (transferManager)
//	go run ./cmd/tmbench -mode download -profile optimized   # tmOptimized (e.g. O_DIRECT sink)
//	go run ./cmd/tmbench -mode both -label tm-baseline
//	go run ./cmd/tmbench -mode download -concurrency 128 -get-object-type ranges  # one-off overrides
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
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"

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
	// All TM knobs live in bench.config.json under "transferManager". These flags
	// are optional per-run overrides: empty string / 0 means "use the config value".
	concurrency := flag.Int("concurrency", 0, "override per-object part concurrency (0 = config transferManager.*.concurrency)")
	objectConc := flag.Int("object-concurrency", 0, "override objects in parallel (0 = config transferManager.*.objectConcurrency)")
	partSize := flag.String("part-size", "", "override part size, e.g. 32MiB (empty = config partSize)")
	maxBuffered := flag.String("max-buffered", "", "override GetObject read-ahead, e.g. 64GiB (empty = config transferManager.download.maxBufferedBytes)")
	prefix := flag.String("prefix", "", "override upload key prefix (empty = config transferManager.upload.keyPrefix)")
	downloadAPI := flag.String("download-api", "", "override TM download API: get | download-object (empty = config transferManager.download.api)")
	downloadSink := flag.String("download-sink", "", "override download-object sink: discard | file (empty = config transferManager.download.sink)")
	deliveryPath := flag.String("delivery-path", "", "override sink=file directory (empty = config transferManager.download.deliveryPath)")
	getObjectType := flag.String("get-object-type", "", "override TM download strategy: parts | ranges (empty = config transferManager.download.getObjectType)")
	profile := flag.String("profile", "baseline", "config profile: baseline (transferManager) | optimized (tmOptimized, e.g. O_DIRECT)")
	progress := flag.Bool("progress", true, "print a live progress indicator to stderr")
	flag.Parse()

	if err := run(*cfgPath, *mode, *region, *bucket, *label, *concurrency, *objectConc, *partSize, *maxBuffered, *prefix, *downloadAPI, *downloadSink, *deliveryPath, *getObjectType, *profile, *progress); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath, mode, region, bucket, label string, concurrency, objectConc int, partSize, maxBuffered, prefix, downloadAPI, downloadSink, deliveryPath, getObjectType, profile string, progress bool) error {
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

	// Select the profile's config section: baseline (pristine TM) or optimized
	// (carries mods like directIO). The section is the source of truth; flags
	// override in place (empty/0 = keep the config value), so the captured report
	// reflects what ran.
	var sel *config.TransferManager
	switch profile {
	case "baseline":
		sel = &cfg.TransferManager
	case "optimized":
		sel = &cfg.TMOptimized
	default:
		return fmt.Errorf("unknown -profile %q (want baseline | optimized)", profile)
	}
	tmd := &sel.Download
	tmu := &sel.Upload
	if maxBuffered != "" {
		n, err := config.ParseSize(maxBuffered)
		if err != nil {
			return fmt.Errorf("parse -max-buffered %q: %w", maxBuffered, err)
		}
		tmd.MaxBufferedBytes = n
	}
	if downloadAPI != "" {
		tmd.API = downloadAPI
	}
	if downloadSink != "" {
		tmd.Sink = downloadSink
	}
	if getObjectType != "" {
		tmd.GetObjectType = getObjectType
	}
	if deliveryPath != "" {
		tmd.DeliveryPath = deliveryPath
	}
	upKeyPrefix := tmu.KeyPrefix
	if prefix != "" {
		upKeyPrefix = prefix
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
	switch tmd.API {
	case "get", "download-object":
	default:
		return fmt.Errorf("unknown transferManager.download.api %q (want get | download-object)", tmd.API)
	}
	switch tmd.Sink {
	case "discard", "file":
	default:
		return fmt.Errorf("unknown transferManager.download.sink %q (want discard | file)", tmd.Sink)
	}
	if tmd.Sink == "file" && tmd.API != "download-object" {
		return fmt.Errorf("transferManager.download.sink=file requires api=download-object")
	}
	var tmGetObjType types.GetObjectType
	switch tmd.GetObjectType {
	case "parts":
		tmGetObjType = types.GetObjectParts
	case "ranges":
		tmGetObjType = types.GetObjectRanges
	default:
		return fmt.Errorf("unknown transferManager.download.getObjectType %q (want parts | ranges)", tmd.GetObjectType)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tmPartSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return err
	}

	// Resolve the two parallelism dimensions directly from the transferManager
	// config; -object-concurrency / -concurrency override when > 0. objectConc 0 =
	// object count; per-object concurrency 0 = the SDK default.
	maxObj := maxObjectCount(cfg)
	dlObjConc := resolveObjConc(objectConc, tmd.ObjectConcurrency, maxObj)
	dlPerObjConc := resolvePerObjConc(concurrency, tmd.Concurrency)
	upObjConc := resolveObjConc(objectConc, tmu.ObjectConcurrency, maxObj)
	upPerObjConc := resolvePerObjConc(concurrency, tmu.Concurrency)
	// GetObject read-ahead is a TOTAL budget (transferManager.download.maxBufferedBytes)
	// split across the concurrent objects.
	dlGetBuffer := resolveGetBuffer(tmd.MaxBufferedBytes, dlObjConc, dlPerObjConc, tmPartSize)

	// Size the keep-alive transport to the larger phase's total parts in flight.
	maxConns := upObjConc * upPerObjConc
	if d := dlObjConc * dlPerObjConc; d > maxConns {
		maxConns = d
	}
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

	// One client; per-phase Concurrency and GetObjectBufferSize are applied per
	// call inside RunUpload/RunDownload via functional options.
	tm := transfermanager.New(s3c, func(o *transfermanager.Options) {
		if tmPartSize > 0 {
			o.PartSizeBytes = tmPartSize
		}
	})
	// GetObjectBufferSize only applies to the GetObject (streaming) download API;
	// DownloadObject writes parts to offsets and has no such read-ahead budget.
	if tmd.MaxBufferedBytes > 0 && mode != "upload" && tmd.API == "get" {
		fmt.Printf("tm read-ahead: total-budget=%s across %d objects -> %s/object (%d parts/object)\n\n",
			humanBytes(tmd.MaxBufferedBytes), dlObjConc, humanBytes(dlGetBuffer), dlGetBuffer/max64(tmPartSize, 1))
	}

	// "both" runs upload first, then download — each sampled and reported
	// independently so the numbers never mix.
	phases := phasesFor(mode, ctx, tm, cfg, *sel, upKeyPrefix, tmGetObjType, upObjConc, upPerObjConc, dlObjConc, dlPerObjConc, dlGetBuffer, tmPartSize)

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
// Upload and download carry their own (object, per-object) concurrency, resolved
// per config section.
func phasesFor(mode string, ctx context.Context, tm *transfermanager.Client, cfg *config.Config, sec config.TransferManager, upKeyPrefix string, getObjType types.GetObjectType,
	upObjConc, upPerObjConc, dlObjConc, dlPerObjConc int, dlGetBuffer, bufSz int64) []phase {
	upload := phase{"tm-upload", func(p *metrics.Progress) (*bench.RunResult, error) {
		return tmbench.RunUpload(ctx, tm, cfg, p, sec, upKeyPrefix, upObjConc, upPerObjConc)
	}}
	download := phase{"tm-download", func(p *metrics.Progress) (*bench.RunResult, error) {
		if sec.Download.API == "download-object" {
			return tmbench.RunDownloadObject(ctx, tm, cfg, p, sec, dlObjConc, dlPerObjConc, getObjType, bufSz)
		}
		return tmbench.RunDownload(ctx, tm, cfg, p, sec, dlObjConc, dlPerObjConc, dlGetBuffer, getObjType)
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
	stall := cfg.TransferManager.Download.StallTimeout()
	if ph.name == "tm-upload" {
		stall = cfg.TransferManager.Upload.StallTimeout()
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

// defaultTMConcurrency mirrors the Transfer Manager's own default per-object
// concurrency (feature/s3/transfermanager defaultTransferConcurrency), used when
// neither the config nor the -concurrency flag sets one.
const defaultTMConcurrency = 5

// resolveObjConc picks the number of objects to transfer in parallel: the
// -object-concurrency flag when > 0, else the config value, else the object count.
func resolveObjConc(flagVal, cfgVal, objCount int) int {
	v := cfgVal
	if flagVal > 0 {
		v = flagVal
	}
	if v <= 0 {
		v = objCount
	}
	if v < 1 {
		v = 1
	}
	return v
}

// resolvePerObjConc picks the per-object part concurrency: the -concurrency flag
// when > 0, else the config value, else the SDK default. Always concrete so the
// header and maxConns sizing are meaningful.
func resolvePerObjConc(flagVal, cfgVal int) int {
	v := cfgVal
	if flagVal > 0 {
		v = flagVal
	}
	if v <= 0 {
		v = defaultTMConcurrency
	}
	return v
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
