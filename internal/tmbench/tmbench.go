// Package tmbench benchmarks the AWS SDK for Go v2 S3 Transfer Manager
// (feature/s3/transfermanager) using its single-object UploadObject / GetObject
// operations with fully in-memory data — upload streams from a small repeating
// buffer (no whole-object allocation, no local files) and download drains to
// io.Discard. It targets the same seeded objects as the main benchmark so the
// two stacks are directly comparable.
//
// Parallelism has two dimensions, mirroring the custom runner's total in-flight:
//   - objectConcurrency:    how many objects transfer at once (object pool)
//   - perObjectConcurrency: the TM's own Concurrency (parts in flight per object)
//
// Total parts in flight is approximately objectConcurrency x perObjectConcurrency.
package tmbench

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/goabv/s3-benchmark-go/internal/bench"
	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// sizedReader yields exactly `remaining` bytes drawn from a small repeating
// pattern, so an arbitrarily large object body can be produced without holding
// the whole object in memory. It reports bytes read into prog for live sampling.
type sizedReader struct {
	remaining int64
	pattern   []byte
	off       int
	prog      *metrics.Progress
}

func (r *sizedReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	total := 0
	for total < len(p) {
		n := copy(p[total:], r.pattern[r.off:])
		r.off = (r.off + n) % len(r.pattern)
		total += n
	}
	r.remaining -= int64(total)
	if r.prog != nil {
		atomic.AddInt64(&r.prog.Bytes, int64(total))
	}
	return total, nil
}

// progSink discards bytes while counting them into prog for live sampling.
type progSink struct{ prog *metrics.Progress }

func (w progSink) Write(p []byte) (int, error) {
	if w.prog != nil {
		atomic.AddInt64(&w.prog.Bytes, int64(len(p)))
	}
	return len(p), nil
}

// discardWriterAt is an io.WriterAt that discards data while counting bytes into
// prog (for live sampling) and its own total. The Transfer Manager's
// DownloadObject writes parts concurrently at their byte offsets, so WriteAt must
// be safe for concurrent calls — the atomic counters make it so, and offsets are
// ignored since the sink is a bit bucket.
type discardWriterAt struct {
	prog  *metrics.Progress
	total int64
}

func (w *discardWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	atomic.AddInt64(&w.total, int64(len(p)))
	if w.prog != nil {
		atomic.AddInt64(&w.prog.Bytes, int64(len(p)))
	}
	return len(p), nil
}

func (w *discardWriterAt) written() int64 { return atomic.LoadInt64(&w.total) }

// countingWriterAt wraps a real io.WriterAt (e.g. an *os.File) and counts the
// bytes actually written into prog (live sampling) and its own total. Used for
// the "file" download sink so DownloadObject writes to disk at part offsets while
// we still measure throughput. Concurrency-safe as long as the wrapped WriterAt
// is (an *os.File is, via positional pwrite).
type countingWriterAt struct {
	w     io.WriterAt
	prog  *metrics.Progress
	total int64
}

func (c *countingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := c.w.WriteAt(p, off)
	if n > 0 {
		atomic.AddInt64(&c.total, int64(n))
		if c.prog != nil {
			atomic.AddInt64(&c.prog.Bytes, int64(n))
		}
	}
	return n, err
}

func (c *countingWriterAt) written() int64 { return atomic.LoadInt64(&c.total) }

// countingSink is a byte-counting io.WriterAt (discard or file-backed).
type countingSink interface {
	io.WriterAt
	written() int64
}

// RunSeed uploads the configured object sizes to the download data prefix
// (cfg.KeysFor keys — the exact keys the download reads), skipping any object that
// already exists, so the download benchmark has data to read. It uses HeadObject
// (via the raw S3 client) to skip existing objects and the Transfer Manager's
// UploadObject to write missing ones from a small repeating in-memory pattern (no
// whole-object allocation). Idempotent and safe to re-run; it neither samples nor
// produces a report.
func RunSeed(ctx context.Context, tm *transfermanager.Client, s3c *s3.Client, cfg *config.Config, prog *metrics.Progress) error {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	pattern := makePattern()
	fmt.Printf("=== Seeding download data to s3://%s/%s ===\n", cfg.Bucket, cfg.DataPrefix)
	for _, spec := range cfg.Sizes {
		sizeBytes, err := config.ParseSize(spec.Size)
		if err != nil {
			return fmt.Errorf("parse size %q: %w", spec.Size, err)
		}
		for _, key := range cfg.KeysFor(spec) {
			exists, err := objectExists(ctx, s3c, cfg.Bucket, key)
			if err != nil {
				return fmt.Errorf("head %q: %w", key, err)
			}
			if exists {
				fmt.Printf("  skip (exists): %s\n", key)
				continue
			}
			fmt.Printf("  seeding: %s (%s)\n", key, spec.Size)
			atomic.AddInt64(&prog.InFlight, 1)
			_, err = tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
				Bucket: aws.String(cfg.Bucket),
				Key:    aws.String(key),
				Body:   &sizedReader{remaining: sizeBytes, pattern: pattern, prog: prog},
			})
			atomic.AddInt64(&prog.InFlight, -1)
			if err != nil {
				return fmt.Errorf("seed %q: %w", key, err)
			}
		}
	}
	fmt.Printf("=== Seeding complete ===\n")
	return nil
}

// objectExists reports whether an object is present via HeadObject.
func objectExists(ctx context.Context, s3c *s3.Client, bucket, key string) (bool, error) {
	_, err := s3c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return false, nil
		}
	}
	return false, err
}

// RunUpload uploads `count` in-memory objects per size group via the Transfer
// Manager's UploadObject (which multiparts automatically). Objects transfer
// concurrently through a pool of objectConcurrency; each object's parts run at
// the TM's configured concurrency (reported as perObjectConcurrency).
func RunUpload(ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prog *metrics.Progress, sec config.TransferManager, keyPrefix string, objectConcurrency, perObjectConcurrency int) (*bench.RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	partSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return nil, fmt.Errorf("parse partSize %q: %w", cfg.PartSize, err)
	}
	pattern := makePattern()

	fmt.Printf("=== S3 Transfer Manager UPLOAD benchmark (feature/s3/transfermanager) ===\n")
	// Upload writes the SAME keys the download reads (cfg.KeysFor under dataPrefix),
	// so an upload run seeds/round-trips the exact objects a later download fetches.
	// The keyPrefix arg is retained for signature compatibility but no longer used.
	_ = keyPrefix
	fmt.Printf("region=%s  bucket=%s  keyPrefix=%s  source=memory\n", cfg.Region, cfg.Bucket, cfg.DataPrefix)
	fmt.Printf("object-concurrency=%d  per-object part-concurrency=%d  ~parts-in-flight=%d  iterations=%d (warmup=%d)\n\n",
		objectConcurrency, perObjectConcurrency, objectConcurrency*perObjectConcurrency, sec.Upload.Iterations, sec.Upload.Warmup)

	result := &bench.RunResult{Mode: "tm-upload"}
	for _, spec := range cfg.Sizes {
		sizeBytes, err := config.ParseSize(spec.Size)
		if err != nil {
			return nil, fmt.Errorf("parse size %q: %w", spec.Size, err)
		}
		count := spec.Count
		if count < 1 {
			count = 1
		}
		keys := cfg.KeysFor(spec) // upload to the exact keys the download will read

		var samples []bench.Sample3
		iters := sec.Upload.Warmup + sec.Upload.Iterations
		for it := 0; it < iters; it++ {
			var bytes int64
			start := time.Now()
			err := runObjectsParallel(ctx, objectConcurrency, count, func(i int) error {
				key := keys[i] // matches cfg.KeysFor -> what RunDownload/RunDownloadObject read
				atomic.AddInt64(&prog.InFlight, 1)
				_, e := tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
					Bucket: aws.String(cfg.Bucket),
					Key:    aws.String(key),
					Body:   &sizedReader{remaining: sizeBytes, pattern: pattern, prog: prog},
				}, func(o *transfermanager.Options) {
					o.Concurrency = perObjectConcurrency
				})
				atomic.AddInt64(&prog.InFlight, -1)
				if e != nil {
					return fmt.Errorf("UploadObject %q: %w", key, e)
				}
				atomic.AddInt64(&bytes, sizeBytes)
				return nil
			})
			if err != nil {
				return nil, err
			}
			if it >= sec.Upload.Warmup {
				samples = append(samples, sample3(bytes, time.Since(start)))
			}
		}

		nparts := int((sizeBytes+partSize-1)/partSize) * count
		// The Transfer Manager computes a CRC32 checksum on upload by default.
		result.Groups = append(result.Groups, group(spec, sizeBytes, count, nparts, objectConcurrency, perObjectConcurrency, samples, true))
	}
	return result, nil
}

// RunDownload downloads the seeded objects (dataPrefix keys) per size group via
// the Transfer Manager's GetObject, draining each body to memory (io.Discard).
// Objects transfer concurrently through a pool of objectConcurrency.
func RunDownload(ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prog *metrics.Progress, sec config.TransferManager, objectConcurrency, perObjectConcurrency int, getBuffer int64, getObjType types.GetObjectType) (*bench.RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}

	fmt.Printf("=== S3 Transfer Manager DOWNLOAD benchmark (feature/s3/transfermanager) ===\n")
	fmt.Printf("region=%s  bucket=%s  sink=memory(discard)  get-object-type=%s\n", cfg.Region, cfg.Bucket, getObjType)
	fmt.Printf("object-concurrency=%d  per-object part-concurrency=%d  ~parts-in-flight=%d  iterations=%d (warmup=%d)\n\n",
		objectConcurrency, perObjectConcurrency, objectConcurrency*perObjectConcurrency, sec.Download.Iterations, sec.Download.Warmup)

	result := &bench.RunResult{Mode: "tm-download"}
	for _, spec := range cfg.Sizes {
		keys := cfg.KeysFor(spec)
		perFileSize, _ := config.ParseSize(spec.Size)

		var samples []bench.Sample3
		iters := sec.Download.Warmup + sec.Download.Iterations
		for it := 0; it < iters; it++ {
			var bytes int64
			start := time.Now()
			err := runObjectsParallel(ctx, objectConcurrency, len(keys), func(i int) error {
				key := keys[i]
				atomic.AddInt64(&prog.InFlight, 1)
				defer atomic.AddInt64(&prog.InFlight, -1)
				out, e := tm.GetObject(ctx, &transfermanager.GetObjectInput{
					Bucket: aws.String(cfg.Bucket),
					Key:    aws.String(key),
				}, func(o *transfermanager.Options) {
					o.Concurrency = perObjectConcurrency
					o.DisableChecksumValidation = !sec.Download.ValidateChecksum
					if getObjType != "" {
						o.GetObjectType = getObjType
					}
					if getBuffer > 0 {
						o.GetObjectBufferSize = getBuffer
					}
				})
				if e != nil {
					return fmt.Errorf("GetObject %q: %w", key, e)
				}
				n, ce := io.Copy(progSink{prog}, out.Body)
				if c, ok := out.Body.(io.Closer); ok {
					_ = c.Close()
				}
				if ce != nil {
					return fmt.Errorf("drain %q: %w", key, ce)
				}
				atomic.AddInt64(&bytes, n)
				return nil
			})
			if err != nil {
				return nil, err
			}
			if it >= sec.Download.Warmup {
				samples = append(samples, sample3(bytes, time.Since(start)))
			}
		}

		result.Groups = append(result.Groups, group(spec, perFileSize, len(keys), 0, objectConcurrency, perObjectConcurrency, samples, sec.Download.ValidateChecksum))
	}
	return result, nil
}

// RunDownloadObject downloads the seeded objects (dataPrefix keys) per size group
// via the Transfer Manager's DownloadObject, which writes each object's parts to an
// io.WriterAt at their byte offsets fully in parallel — no single ordered-reader
// funnel and no GetObjectBufferSize read-ahead. The sink is a discarding WriterAt
// (bytes are counted for throughput, then dropped), so this measures the TM's
// parallel-write ceiling. Objects transfer concurrently through a pool of
// objectConcurrency; each object's parts run at perObjectConcurrency.
func RunDownloadObject(ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prog *metrics.Progress, sec config.TransferManager, objectConcurrency, perObjectConcurrency int, getObjType types.GetObjectType) (*bench.RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	toFile := sec.Download.Sink == "file"
	deliveryPath := sec.Download.DeliveryPath
	sinkDesc := "memory(discard WriterAt, parallel offsets)"
	if toFile {
		if deliveryPath == "" {
			deliveryPath = "."
		}
		if err := os.MkdirAll(deliveryPath, 0o755); err != nil {
			return nil, fmt.Errorf("create deliveryPath %q: %w", deliveryPath, err)
		}
		// Start-of-run cleanup: remove any files left by a previous run so we begin
		// from a clean disk. Downloaded files are intentionally NOT deleted at the
		// end of a run — they persist for optional out-of-band CRC32 verification
		// (see cmd/tmbench runPhase) and are cleared here on the next run.
		for _, spec := range cfg.Sizes {
			for _, key := range cfg.KeysFor(spec) {
				_ = os.Remove(filepath.Join(deliveryPath, sanitize(key)))
			}
		}
		sinkDesc = fmt.Sprintf("disk buffered (WriterAt -> *os.File under %s, cleaned at start of run)", deliveryPath)
	}

	fmt.Printf("=== S3 Transfer Manager DOWNLOAD benchmark (DownloadObject, WriterAt) ===\n")
	fmt.Printf("region=%s  bucket=%s  sink=%s  get-object-type=%s\n", cfg.Region, cfg.Bucket, sinkDesc, getObjType)
	fmt.Printf("object-concurrency=%d  per-object part-concurrency=%d  ~parts-in-flight=%d  iterations=%d (warmup=%d)\n\n",
		objectConcurrency, perObjectConcurrency, objectConcurrency*perObjectConcurrency, sec.Download.Iterations, sec.Download.Warmup)

	result := &bench.RunResult{Mode: "tm-download"}
	for _, spec := range cfg.Sizes {
		keys := cfg.KeysFor(spec)
		perFileSize, _ := config.ParseSize(spec.Size)

		var samples []bench.Sample3
		iters := sec.Download.Warmup + sec.Download.Iterations
		for it := 0; it < iters; it++ {
			var bytes int64
			start := time.Now()
			err := runObjectsParallel(ctx, objectConcurrency, len(keys), func(i int) error {
				key := keys[i]
				atomic.AddInt64(&prog.InFlight, 1)
				defer atomic.AddInt64(&prog.InFlight, -1)

				var w countingSink
				finalize := func() error { return nil }
				if toFile {
					path := filepath.Join(deliveryPath, sanitize(key))
					f, ferr := os.Create(path)
					if ferr != nil {
						return fmt.Errorf("create %q: %w", path, ferr)
					}
					w = &countingWriterAt{w: f, prog: prog}
					finalize = func() error { return f.Close() }
				} else {
					w = &discardWriterAt{prog: prog}
				}

				_, e := tm.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
					Bucket:   aws.String(cfg.Bucket),
					Key:      aws.String(key),
					WriterAt: w,
				}, func(o *transfermanager.Options) {
					o.Concurrency = perObjectConcurrency
					o.DisableChecksumValidation = !sec.Download.ValidateChecksum
					if getObjType != "" {
						o.GetObjectType = getObjType
					}
				})
				// finalize() closes the file and must be inside the timed region so
				// the to-disk cost is measured.
				ferr := finalize()
				if e != nil {
					return fmt.Errorf("DownloadObject %q: %w", key, e)
				}
				if ferr != nil {
					return fmt.Errorf("finalize %q: %w", key, ferr)
				}
				atomic.AddInt64(&bytes, w.written())
				return nil
			})
			if err != nil {
				return nil, err
			}
			if it >= sec.Download.Warmup {
				samples = append(samples, sample3(bytes, time.Since(start)))
			}
		}

		result.Groups = append(result.Groups, group(spec, perFileSize, len(keys), 0, objectConcurrency, perObjectConcurrency, samples, sec.Download.ValidateChecksum))
	}
	return result, nil
}

// RunDownloadFile benchmarks the SDK's DownloadFile API, where the transfer
// manager owns the destination writer: each object is written to a file under
// deliveryPath using O_DIRECT when larger than DirectIOThreshold (else a buffered
// writer), coalescing part/range data into fixed WriteChunkSize disk writes.
// Objects transfer concurrently through a pool of objectConcurrency; each object's
// parts run at perObjectConcurrency. Unlike RunDownloadObject, the sink is internal
// to the SDK, so live bytes are tracked via a progress listener.
func RunDownloadFile(ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prog *metrics.Progress, sec config.TransferManager, objectConcurrency, perObjectConcurrency int, getObjType types.GetObjectType, partSize int64) (*bench.RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	deliveryPath := sec.Download.DeliveryPath
	if deliveryPath == "" {
		deliveryPath = "."
	}
	if err := os.MkdirAll(deliveryPath, 0o755); err != nil {
		return nil, fmt.Errorf("create deliveryPath %q: %w", deliveryPath, err)
	}
	// Start-of-run cleanup: clear files left by a previous run (see RunDownloadObject).
	for _, spec := range cfg.Sizes {
		for _, key := range cfg.KeysFor(spec) {
			_ = os.Remove(filepath.Join(deliveryPath, sanitize(key)))
		}
	}

	var directIOThreshold int64
	if s := sec.Download.DirectIOThreshold; s != "" {
		n, err := config.ParseSize(s)
		if err != nil {
			return nil, fmt.Errorf("parse directIOThreshold %q: %w", s, err)
		}
		directIOThreshold = n
	}
	var writeChunkSize int64
	if s := sec.Download.WriteChunkSize; s != "" {
		n, err := config.ParseSize(s)
		if err != nil {
			return nil, fmt.Errorf("parse writeChunkSize %q: %w", s, err)
		}
		writeChunkSize = n
	}
	// Effective write-behind flush pool sizing (mirrors the SDK defaults so the
	// header shows what actually runs).
	flushWorkers := sec.Download.WriteFlushWorkers
	if flushWorkers <= 0 {
		flushWorkers = 16
	}
	flushQueue := sec.Download.WriteFlushQueueDepth
	if flushQueue <= 0 {
		flushQueue = 64
	}

	writeMode := "O_DIRECT > threshold, buffered otherwise (SDK-managed)"
	if sec.Download.DisableDirectIO {
		writeMode = "buffered (O_DIRECT disabled)"
	}
	pooling := "pooled region buffers"
	if sec.Download.DisableWriteBufferPool {
		pooling = "non-pooled region buffers"
	}
	fmt.Printf("=== S3 Transfer Manager DOWNLOAD benchmark (DownloadFile, SDK-owned file sink) ===\n")
	fmt.Printf("region=%s  bucket=%s  deliveryPath=%s  get-object-type=%s  write=%s  %s\n", cfg.Region, cfg.Bucket, deliveryPath, getObjType, writeMode, pooling)
	fmt.Printf("object-concurrency=%d  per-object part-concurrency=%d  ~parts-in-flight=%d  flush-workers=%d  flush-queue=%d  iterations=%d (warmup=%d)\n\n",
		objectConcurrency, perObjectConcurrency, objectConcurrency*perObjectConcurrency, flushWorkers, flushQueue, sec.Download.Iterations, sec.Download.Warmup)

	result := &bench.RunResult{Mode: "tm-download"}
	for _, spec := range cfg.Sizes {
		keys := cfg.KeysFor(spec)
		perFileSize, _ := config.ParseSize(spec.Size)

		var samples []bench.Sample3
		iters := sec.Download.Warmup + sec.Download.Iterations
		for it := 0; it < iters; it++ {
			var bytes int64
			start := time.Now()
			err := runObjectsParallel(ctx, objectConcurrency, len(keys), func(i int) error {
				key := keys[i]
				atomic.AddInt64(&prog.InFlight, 1)
				defer atomic.AddInt64(&prog.InFlight, -1)

				listener := &progBytesListener{prog: prog}
				out, e := tm.DownloadFile(ctx, &transfermanager.DownloadFileInput{
					Bucket:   aws.String(cfg.Bucket),
					Key:      aws.String(key),
					FilePath: filepath.Join(deliveryPath, sanitize(key)),
				}, func(o *transfermanager.Options) {
					o.Concurrency = perObjectConcurrency
					o.DisableChecksumValidation = !sec.Download.ValidateChecksum
					if partSize > 0 {
						o.PartSizeBytes = partSize
					}
					if getObjType != "" {
						o.GetObjectType = getObjType
					}
					o.DisableDirectIO = sec.Download.DisableDirectIO
					if directIOThreshold > 0 {
						o.DirectIOThreshold = directIOThreshold
					}
					if writeChunkSize > 0 {
						o.WriteChunkSizeBytes = writeChunkSize
					}
					o.WriteFlushWorkers = flushWorkers
					o.WriteFlushQueueDepth = flushQueue
					o.DisableWriteBufferPool = sec.Download.DisableWriteBufferPool
					o.ObjectProgressListeners.Register(listener)
				})
				if e != nil {
					return fmt.Errorf("DownloadFile %q: %w", key, e)
				}
				atomic.AddInt64(&bytes, aws.ToInt64(out.ContentLength))
				return nil
			})
			if err != nil {
				return nil, err
			}
			if it >= sec.Download.Warmup {
				samples = append(samples, sample3(bytes, time.Since(start)))
			}
		}

		result.Groups = append(result.Groups, group(spec, perFileSize, len(keys), 0, objectConcurrency, perObjectConcurrency, samples, sec.Download.ValidateChecksum))
	}
	return result, nil
}

// progBytesListener feeds the SDK's per-object cumulative byte-transfer events into
// the benchmark's live prog.Bytes counter. It tracks the max cumulative seen for its
// object and adds only positive deltas, so concurrent/out-of-order part events never
// double-count or move backwards. One instance is registered per object.
type progBytesListener struct {
	prog *metrics.Progress
	last atomic.Int64
}

func (l *progBytesListener) OnObjectBytesTransferred(ctx context.Context, e *transfermanager.ObjectBytesTransferredEvent) {
	for {
		old := l.last.Load()
		if e.BytesTransferred <= old {
			return
		}
		if l.last.CompareAndSwap(old, e.BytesTransferred) {
			atomic.AddInt64(&l.prog.Bytes, e.BytesTransferred-old)
			return
		}
	}
}

// runObjectsParallel runs fn(0..n-1) across a pool of `workers` goroutines,
// stopping early on the first error.
func runObjectsParallel(ctx context.Context, workers, n int, fn func(i int) error) error {
	if workers < 1 {
		workers = 1
	}
	if workers > n {
		workers = n
	}
	idx := make(chan int)
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if firstErr.Load() != nil || ctx.Err() != nil {
					continue
				}
				if err := fn(i); err != nil && firstErr.Load() == nil {
					firstErr.Store(err)
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		if firstErr.Load() != nil {
			break
		}
		idx <- i
	}
	close(idx)
	wg.Wait()
	if e := firstErr.Load(); e != nil {
		return e.(error)
	}
	return nil
}

func group(spec config.SizeSpec, perFileSize int64, files, nparts, objectConc, perObjectConc int, samples []bench.Sample3, checksum bool) bench.GroupResult {
	return bench.GroupResult{
		Label:             spec.Size,
		Files:             files,
		PerFileSize:       perFileSize,
		Size:              perFileSize * int64(files),
		Parts:             nparts,
		Workers:           objectConc,    // objects in parallel
		Concurrency:       perObjectConc, // TM parts per object
		InFlight:          objectConc * perObjectConc,
		Iterations:        len(samples),
		ChecksumValidated: checksum,
		DeliveryMode:      "transfer-manager",
		UploadSource:      "memory",
		Samples:           samples,
		Median:            medianSample(samples),
		Best:              bestSample(samples),
	}
}

func makePattern() []byte {
	p := make([]byte, 1<<20) // 1 MiB
	for i := range p {
		p[i] = byte(i * 31)
	}
	return p
}

func sample3(bytes int64, d time.Duration) bench.Sample3 {
	secs := d.Seconds()
	var gbps, mibps float64
	if secs > 0 {
		gbps = float64(bytes) * 8 / secs / 1e9
		mibps = float64(bytes) / (1 << 20) / secs
	}
	return bench.Sample3{Secs: secs, Mibps: mibps, Gbps: gbps}
}

func medianSample(ss []bench.Sample3) bench.Sample3 {
	if len(ss) == 0 {
		return bench.Sample3{}
	}
	s := append([]bench.Sample3(nil), ss...)
	sort.Slice(s, func(i, j int) bool { return s[i].Gbps < s[j].Gbps })
	return s[len(s)/2]
}

func bestSample(ss []bench.Sample3) bench.Sample3 {
	var b bench.Sample3
	for _, s := range ss {
		if s.Gbps > b.Gbps {
			b = s
		}
	}
	return b
}

// VerifyDownloadedFiles re-reads each downloaded file (cfg.KeysFor under
// deliveryPath) and checks its full-object CRC-32 against the expected data
// pattern. It is meant to run OUTSIDE the sampled/timed region (see
// cmd/tmbench runPhase), so its cost is never counted in throughput or resource
// stats. It does NOT delete the files — start-of-run cleanup owns deletion.
//
// Verification is serial and reads every byte back from disk, so it can take a
// while for large objects; that is why it defaults off and is opt-in via the
// download.verifyFullChecksum config flag.
func VerifyDownloadedFiles(cfg *config.Config, deliveryPath string) {
	if deliveryPath == "" {
		deliveryPath = "."
	}
	for _, spec := range cfg.Sizes {
		size, err := config.ParseSize(spec.Size)
		if err != nil {
			fmt.Printf("  [verify] size %q: parse error: %v\n", spec.Size, err)
			continue
		}
		want := expectedPatternCRC32(size)
		for _, key := range cfg.KeysFor(spec) {
			path := filepath.Join(deliveryPath, sanitize(key))
			fmt.Printf("  [verify] %s: hashing %d bytes ...\n", key, size)
			got, cerr := fileCRC32IEEE(path)
			switch {
			case cerr != nil:
				fmt.Printf("  [verify] %s: read error: %v\n", key, cerr)
			case got == want:
				fmt.Printf("  [verify] %s: CRC32 OK (0x%08x over %d bytes)\n", key, got, size)
			default:
				fmt.Printf("  [verify] %s: CRC32 MISMATCH got=0x%08x want=0x%08x\n", key, got, want)
			}
		}
	}
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '.':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c+('a'-'A'))
		}
	}
	return string(out)
}

// fileCRC32IEEE reads the whole file at path and returns its CRC-32 (IEEE)
// checksum over the entire contents. Used by VerifyDownloadedFiles (the untimed,
// out-of-phase full-object verification) to confirm the object landed on disk intact.
func fileCRC32IEEE(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	h := crc32.NewIEEE()
	buf := make([]byte, 8<<20)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return 0, rerr
		}
	}
	return h.Sum32(), nil
}

// expectedPatternCRC32 returns the CRC-32 (IEEE) of `size` bytes of the upload
// data pattern (byte(offset*31)), matching makePattern and the seeder. Because
// the pattern is continuous across the whole object (its 256-byte period divides
// every part/tile size), this reproduces the exact bytes a correct download
// should have written — without a second download.
func expectedPatternCRC32(size int64) uint32 {
	tile := make([]byte, 1<<20) // 1 MiB = whole number of 256-byte pattern periods
	for i := range tile {
		tile[i] = byte(i * 31)
	}
	h := crc32.NewIEEE()
	remaining := size
	for remaining > 0 {
		n := int64(len(tile))
		if n > remaining {
			n = remaining
		}
		h.Write(tile[:n])
		remaining -= n
	}
	return h.Sum32()
}
