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
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"

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

// RunUpload uploads `count` in-memory objects per size group via the Transfer
// Manager's UploadObject (which multiparts automatically). Objects transfer
// concurrently through a pool of objectConcurrency; each object's parts run at
// the TM's configured concurrency (reported as perObjectConcurrency).
func RunUpload(ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prog *metrics.Progress, keyPrefix string, objectConcurrency, perObjectConcurrency int) (*bench.RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	partSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return nil, fmt.Errorf("parse partSize %q: %w", cfg.PartSize, err)
	}
	pattern := makePattern()

	fmt.Printf("=== S3 Transfer Manager UPLOAD benchmark (feature/s3/transfermanager) ===\n")
	fmt.Printf("region=%s  bucket=%s  keyPrefix=%s  source=memory\n", cfg.Region, cfg.Bucket, keyPrefix)
	fmt.Printf("object-concurrency=%d  per-object part-concurrency=%d  ~parts-in-flight=%d  iterations=%d (warmup=%d)\n\n",
		objectConcurrency, perObjectConcurrency, objectConcurrency*perObjectConcurrency, cfg.Upload.Iterations, cfg.Upload.Warmup)
	printHeader()

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

		var samples []bench.Sample3
		iters := cfg.Upload.Warmup + cfg.Upload.Iterations
		for it := 0; it < iters; it++ {
			var bytes int64
			start := time.Now()
			err := runObjectsParallel(ctx, objectConcurrency, count, func(i int) error {
				key := fmt.Sprintf("%s%s-i%d-%d.bin", keyPrefix, sanitize(spec.Size), it, i)
				atomic.AddInt64(&prog.InFlight, 1)
				_, e := tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
					Bucket: aws.String(cfg.Bucket),
					Key:    aws.String(key),
					Body:   &sizedReader{remaining: sizeBytes, pattern: pattern, prog: prog},
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
			if it >= cfg.Upload.Warmup {
				samples = append(samples, sample3(bytes, time.Since(start)))
			}
		}

		nparts := int((sizeBytes+partSize-1)/partSize) * count
		// The Transfer Manager computes a CRC32 checksum on upload by default.
		result.Groups = append(result.Groups, group(spec, sizeBytes, count, nparts, objectConcurrency, perObjectConcurrency, samples, true))
		printRow(result.Groups[len(result.Groups)-1])
	}
	return result, nil
}

// RunDownload downloads the seeded objects (dataPrefix keys) per size group via
// the Transfer Manager's GetObject, draining each body to memory (io.Discard).
// Objects transfer concurrently through a pool of objectConcurrency.
func RunDownload(ctx context.Context, tm *transfermanager.Client, cfg *config.Config, prog *metrics.Progress, objectConcurrency, perObjectConcurrency int) (*bench.RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}

	fmt.Printf("=== S3 Transfer Manager DOWNLOAD benchmark (feature/s3/transfermanager) ===\n")
	fmt.Printf("region=%s  bucket=%s  sink=memory(discard)\n", cfg.Region, cfg.Bucket)
	fmt.Printf("object-concurrency=%d  per-object part-concurrency=%d  ~parts-in-flight=%d  iterations=%d (warmup=%d)\n\n",
		objectConcurrency, perObjectConcurrency, objectConcurrency*perObjectConcurrency, cfg.Download.Iterations, cfg.Download.Warmup)
	printHeader()

	result := &bench.RunResult{Mode: "tm-download"}
	for _, spec := range cfg.Sizes {
		keys := cfg.KeysFor(spec)
		perFileSize, _ := config.ParseSize(spec.Size)

		var samples []bench.Sample3
		iters := cfg.Download.Warmup + cfg.Download.Iterations
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
			if it >= cfg.Download.Warmup {
				samples = append(samples, sample3(bytes, time.Since(start)))
			}
		}

		result.Groups = append(result.Groups, group(spec, perFileSize, len(keys), 0, objectConcurrency, perObjectConcurrency, samples, cfg.Download.ValidateChecksum))
		printRow(result.Groups[len(result.Groups)-1])
	}
	return result, nil
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

func printHeader() {
	fmt.Printf("%-12s %6s %7s %10s %10s\n", "size", "parts", "files", "med Gbps", "med MiB/s")
	fmt.Printf("--------------------------------------------------------\n")
}

func printRow(g bench.GroupResult) {
	fmt.Printf("%-12s %6d %7d %10.3f %10.1f\n", g.Label, g.Parts, g.Files, g.Median.Gbps, g.Median.Mibps)
}
