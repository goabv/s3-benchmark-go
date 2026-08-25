// Command tmbench benchmarks the AWS SDK for Go v2 S3 Transfer Manager
// (feature/s3/transfermanager) with fully in-memory data — no local files.
// Upload streams from a repeating in-memory buffer via UploadObject; download
// drains GetObject bodies to io.Discard. It targets the same seeded objects as
// the main benchmark and captures results into results/runs/ for comparison.
//
// Usage:
//
//	go run ./cmd/tmbench -mode download
//	go run ./cmd/tmbench -mode download -download-api download-object   # WriterAt, parallel (discard)
//	go run ./cmd/tmbench -mode download -download-api download-object -download-sink file -delivery-path /mnt/scratch
//	go run ./cmd/tmbench -mode download -download-api download-object -get-object-type ranges -part-size 32MiB
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
	concurrency := flag.Int("concurrency", 0, "TM part concurrency per object (0 = auto: total/object-concurrency)")
	objectConc := flag.Int("object-concurrency", 0, "objects transferred in parallel (0 = auto: object count)")
	partSize := flag.String("part-size", "", "Transfer Manager part size, e.g. 32MiB (empty = config partSize)")
	maxBuffered := flag.String("max-buffered", "", "GetObject read-ahead buffer, e.g. 64GiB (empty = config maxBufferedBytes)")
	prefix := flag.String("prefix", "tm-upload/", "key prefix for uploaded objects")
	downloadAPI := flag.String("download-api", "get", "TM download API: get (GetObject stream) | download-object (DownloadObject WriterAt, parallel)")
	downloadSink := flag.String("download-sink", "discard", "download-object sink: discard (WriterAt bit-bucket) | file (WriterAt -> *os.File under -delivery-path)")
	deliveryPath := flag.String("delivery-path", "", "directory for -download-sink file (empty = config download.deliveryPath)")
	getObjectType := flag.String("get-object-type", "parts", "TM multipart download strategy: parts (PartNumber, follows upload boundaries) | ranges (byte ranges of part-size)")
	progress := flag.Bool("progress", true, "print a live progress indicator to stderr")
	flag.Parse()

	if err := run(*cfgPath, *mode, *region, *bucket, *label, *concurrency, *objectConc, *partSize, *maxBuffered, *prefix, *downloadAPI, *downloadSink, *deliveryPath, *getObjectType, *progress); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgPath, mode, region, bucket, label string, concurrency, objectConc int, partSize, maxBuffered, prefix, downloadAPI, downloadSink, deliveryPath, getObjectType string, progress bool) error {
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
	switch downloadAPI {
	case "get", "download-object":
	default:
		return fmt.Errorf("unknown -download-api %q (want get | download-object)", downloadAPI)
	}
	switch downloadSink {
	case "discard", "file":
	default:
		return fmt.Errorf("unknown -download-sink %q (want discard | file)", downloadSink)
	}
	if downloadSink == "file" && downloadAPI != "download-object" {
		return fmt.Errorf("-download-sink file requires -download-api download-object")
	}
	// Map -get-object-type to the TM's multipart download strategy. Default
	// "parts" matches the SDK default (types.GetObjectParts).
	var tmGetObjType types.GetObjectType
	switch getObjectType {
	case "parts":
		tmGetObjType = types.GetObjectParts
	case "ranges":
		tmGetObjType = types.GetObjectRanges
	default:
		return fmt.Errorf("unknown -get-object-type %q (want parts | ranges)", getObjectType)
	}
	// Resolve the file sink directory (flag overrides config download.deliveryPath).
	dlPath := deliveryPath
	if dlPath == "" {
		dlPath = cfg.Download.DeliveryPath
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tmPartSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return err
	}

	// Resolve the two parallelism dimensions PER PHASE so upload reads the upload
	// section and download reads the download section (the -object-concurrency /
	// -concurrency flags, when set, override both). Total parts in flight defaults
	// to that section's workers x concurrency for an apples-to-apples comparison.
	maxObj := maxObjectCount(cfg)
	upObjConc, upPerObjConc := resolvePhase(cfg.Upload.Parallelism(), objectConc, concurrency, maxObj)
	dlObjConc, dlPerObjConc := resolvePhase(cfg.Download.Parallelism(), objectConc, concurrency, maxObj)
	// GetObject read-ahead is a TOTAL budget (download.maxBufferedBytes, matching
	// the JS runner) split across the concurrent objects.
	dlGetBuffer := resolveGetBuffer(cfg.Download.MaxBufferedBytes, dlObjConc, dlPerObjConc, tmPartSize)

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
	if cfg.Download.MaxBufferedBytes > 0 && mode != "upload" && downloadAPI == "get" {
		fmt.Printf("tm read-ahead: total-budget=%s across %d objects -> %s/object (%d parts/object)\n\n",
			humanBytes(cfg.Download.MaxBufferedBytes), dlObjConc, humanBytes(dlGetBuffer), dlGetBuffer/max64(tmPartSize, 1))
	}

	// "both" runs upload first, then download — each sampled and reported
	// independently so the numbers never mix.
	phases := phasesFor(mode, ctx, tm, cfg, prefix, downloadAPI, downloadSink, dlPath, tmGetObjType, upObjConc, upPerObjConc, dlObjConc, dlPerObjConc, dlGetBuffer)

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
func phasesFor(mode string, ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prefix, downloadAPI, downloadSink, deliveryPath string, getObjType types.GetObjectType,
	upObjConc, upPerObjConc, dlObjConc, dlPerObjConc int, dlGetBuffer int64) []phase {
	upload := phase{"tm-upload", func(p *metrics.Progress) (*bench.RunResult, error) {
		return tmbench.RunUpload(ctx, tm, cfg, p, prefix, upObjConc, upPerObjConc)
	}}
	download := phase{"tm-download", func(p *metrics.Progress) (*bench.RunResult, error) {
		if downloadAPI == "download-object" {
			return tmbench.RunDownloadObject(ctx, tm, cfg, p, dlObjConc, dlPerObjConc, downloadSink, deliveryPath, getObjType)
		}
		return tmbench.RunDownload(ctx, tm, cfg, p, dlObjConc, dlPerObjConc, dlGetBuffer, getObjType)
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

// resolvePhase derives (objectConcurrency, perObjectConcurrency) for a phase.
// The flags (objFlag/concFlag) override when > 0; otherwise object-concurrency
// defaults to the object count and per-object-concurrency to total/objects so the
// total parts in flight matches that section's workers x concurrency.
func resolvePhase(total, objFlag, concFlag, maxObjects int) (objectConc, perObjectConc int) {
	objectConc = objFlag
	if objectConc <= 0 {
		objectConc = maxObjects
	}
	if objectConc < 1 {
		objectConc = 1
	}
	perObjectConc = concFlag
	if perObjectConc <= 0 {
		perObjectConc = total / objectConc
		if perObjectConc < 1 {
			perObjectConc = 1
		}
	}
	return objectConc, perObjectConc
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
