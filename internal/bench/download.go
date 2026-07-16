// Package bench runs the S3 throughput benchmarks.
package bench

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/goabv/s3-benchmark-go/internal/bufpool"
	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// delivery modes.
const (
	deliveryDiscard = "discard"
	deliveryOrdered = "ordered-stream"
	deliveryFile    = "file"
)

type job struct {
	key  string
	part int32
}

// downloadRun carries the shared state for one size group's download passes.
type downloadRun struct {
	client   *s3.Client
	cfg      *config.Config
	prog     *metrics.Progress
	pool     *bufpool.Pool
	ips      *ipStats
	timing   *metrics.Recorder // total per-part duration
	ttfb     *metrics.Recorder // time to first byte
	parts    *partRecorder
	retries  int64
	mode     string    // delivery mode
	partSize int64     // configured part size (byte offsets, channel sizing)
	files    *fileSink // file mode only
	iter     int32     // current measured-iteration index; <0 during warmup (no CSV rows)
}

// RunDownload benchmarks parallel ranged (PartNumber) GETs for every configured
// size group. Warmup iterations run first and are discarded; the reported number
// is the median throughput over the measured iterations.
func RunDownload(ctx context.Context, client *s3.Client, cfg *config.Config, prog *metrics.Progress) (*RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	mode := cfg.Download.DeliveryMode
	switch mode {
	case deliveryDiscard, deliveryOrdered, deliveryFile:
	default:
		return nil, fmt.Errorf("unknown deliveryMode %q (want discard | ordered-stream | file)", mode)
	}

	partSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return nil, fmt.Errorf("parse partSize %q: %w", cfg.PartSize, err)
	}
	pool := bufpool.New(int(partSize))
	parallelism := cfg.Download.Parallelism()
	checksumAlgo := cfg.Checksum
	if checksumAlgo == "" {
		checksumAlgo = "CRC32"
	}

	fmt.Printf("=== S3 part-boundary download benchmark (AWS SDK Go v2) ===\n")
	fmt.Printf("region=%s  bucket=%s\n", cfg.Region, cfg.Bucket)
	fmt.Printf("unit=PartNumber  delivery=%s%s  checksum-validation=%v  spread-conns=%v  workers=%d  concurrency/worker=%d  iterations=%d (warmup=%d)\n\n",
		mode, deliveryDetail(cfg, partSize), cfg.Download.ValidateChecksum, cfg.Download.SpreadConnections,
		cfg.Download.Workers, cfg.Download.Concurrency, cfg.Download.Iterations, cfg.Download.Warmup)

	var files *fileSink
	if mode == deliveryFile {
		files = newFileSink(cfg.Download.DeliveryPath)
		defer files.closeAll()
	}

	result := &RunResult{Mode: "download"}
	for _, spec := range cfg.Sizes {
		keys := cfg.KeysFor(spec)
		perFileSize, _ := config.ParseSize(spec.Size)

		// Enumerate parts once; the layout is fixed by how the object was uploaded.
		var allJobs []job
		for _, key := range keys {
			pc, err := partsCount(ctx, client, cfg.Bucket, key)
			if err != nil {
				return nil, fmt.Errorf("head %q: %w", key, err)
			}
			for p := int32(1); p <= pc; p++ {
				allJobs = append(allJobs, job{key: key, part: p})
			}
		}

		run := &downloadRun{
			client:   client,
			cfg:      cfg,
			prog:     prog,
			pool:     pool,
			ips:      newIPStats(),
			timing:   &metrics.Recorder{},
			ttfb:     &metrics.Recorder{},
			parts:    &partRecorder{},
			mode:     mode,
			partSize: partSize,
			files:    files,
		}

		var measured []iterResult
		total := cfg.Download.Warmup + cfg.Download.Iterations
		for it := 0; it < total; it++ {
			if it < cfg.Download.Warmup {
				atomic.StoreInt32(&run.iter, -1)
			} else {
				atomic.StoreInt32(&run.iter, int32(it-cfg.Download.Warmup))
			}
			res, err := run.runOnce(ctx, allJobs, parallelism)
			if err != nil {
				return nil, err
			}
			if it >= cfg.Download.Warmup {
				measured = append(measured, res)
			}
		}

		samples := toSamples(measured)
		distinct, reuse := run.ips.summary()
		proto, cipher := run.ips.tls()
		gr := GroupResult{
			Label:                  spec.Size,
			Files:                  len(keys),
			PerFileSize:            perFileSize,
			Size:                   perFileSize * int64(len(keys)),
			Parts:                  len(allJobs),
			Workers:                cfg.Download.Workers,
			Concurrency:            cfg.Download.Concurrency,
			InFlight:               parallelism,
			Iterations:             cfg.Download.Iterations,
			ChecksumAlgo:           checksumAlgo,
			ChecksumValidated:      cfg.Download.ValidateChecksum,
			DeliveryMode:           mode,
			PartsChecksummedPerRun: checksummedParts(cfg.Download.ValidateChecksum, len(allJobs)),
			Samples:                samples,
			Median:                 median(samples),
			Best:                   best(samples),
			PartTime:               toPartTimeStats(run.timing.Percentiles()),
			TTFB:                   toPartTimeStats(run.ttfb.Percentiles()),
			TLS:                    TLSInfo{Protocol: proto, Cipher: cipher},
			Parts_:                 run.parts.snapshot(),
			DistinctIPs:            distinct,
			ReuseRatio:             reuse,
			Retries:                int(atomic.LoadInt64(&run.retries)),
		}
		result.Groups = append(result.Groups, gr)
	}
	return result, nil
}

func deliveryDetail(cfg *config.Config, partSize int64) string {
	switch cfg.Download.DeliveryMode {
	case deliveryFile:
		return fmt.Sprintf(" (path=%s)", cfg.Download.DeliveryPath)
	case deliveryOrdered:
		if cfg.Download.MaxBufferedBytes > 0 {
			return fmt.Sprintf(" (buffer-pool, max-buffered=%s)", humanBytes(cfg.Download.MaxBufferedBytes))
		}
		return " (buffer-pool)"
	default:
		return ""
	}
}

func checksummedParts(validate bool, parts int) int {
	if validate {
		return parts
	}
	return 0
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
	return fmt.Sprintf("%.0f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// runOnce drives one full pass over the job set. ordered-stream uses a per-object
// streaming reader; discard/file drain each part independently.
func (r *downloadRun) runOnce(ctx context.Context, jobs []job, parallelism int) (iterResult, error) {
	if r.mode == deliveryOrdered {
		return r.runOnceOrdered(ctx, jobs, parallelism)
	}

	jobCh := make(chan job, parallelism*2)
	var totalBytes int64
	var firstErr atomic.Value
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				n, err := r.fetchPart(ctx, j)
				if err != nil {
					if firstErr.Load() == nil {
						firstErr.Store(err)
					}
					continue
				}
				atomic.AddInt64(&totalBytes, n)
			}
		}()
	}

	for _, j := range jobs {
		if firstErr.Load() != nil {
			break
		}
		jobCh <- j
	}
	close(jobCh)
	wg.Wait()
	elapsed := time.Since(start)

	if e := firstErr.Load(); e != nil {
		return iterResult{}, e.(error)
	}
	return iterResult{Bytes: totalBytes, Parts: len(jobs), Elapsed: elapsed}, nil
}

// runOnceOrdered runs a pass where each object is delivered as one in-order
// stream (orderedReader). A consumer goroutine per object drains its reader via
// io.Copy (the zero-copy WriteTo path) to io.Discard — standing in for a real
// consumer that would receive the object's bytes in order. Fetch workers pull
// parts from a shared pool and push them to the owning object's reader.
func (r *downloadRun) runOnceOrdered(ctx context.Context, jobs []job, parallelism int) (iterResult, error) {
	npartsByKey := map[string]int32{}
	for _, j := range jobs {
		if j.part > npartsByKey[j.key] {
			npartsByKey[j.key] = j.part
		}
	}
	capBuffers := r.channelCap(len(npartsByKey))

	readers := make(map[string]*orderedReader, len(npartsByKey))
	var totalBytes int64
	var firstErr atomic.Value
	var consumers sync.WaitGroup
	for key, np := range npartsByKey {
		rd := newOrderedReader(np, capBuffers, r.pool)
		readers[key] = rd
		consumers.Add(1)
		go func(rd *orderedReader) {
			defer consumers.Done()
			n, err := io.Copy(io.Discard, rd) // takes WriteTo -> zero extra copy
			atomic.AddInt64(&totalBytes, n)
			if err != nil && firstErr.Load() == nil {
				firstErr.Store(err)
			}
		}(rd)
	}

	jobCh := make(chan job, parallelism*2)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				if err := r.fetchPartTo(ctx, j, readers[j.key]); err != nil {
					if firstErr.Load() == nil {
						firstErr.Store(err)
					}
				}
			}
		}()
	}
	for _, j := range jobs {
		if firstErr.Load() != nil {
			break
		}
		jobCh <- j
	}
	close(jobCh)
	wg.Wait()

	// On error some readers may never see their final part; unblock their consumers.
	if firstErr.Load() != nil {
		for _, rd := range readers {
			rd.abort()
		}
	}
	consumers.Wait()
	elapsed := time.Since(start)

	if e := firstErr.Load(); e != nil {
		return iterResult{}, e.(error)
	}
	return iterResult{Bytes: totalBytes, Parts: len(jobs), Elapsed: elapsed}, nil
}

// channelCap sizes each object's delivery channel from the buffered-bytes budget,
// split across the objects in flight (at least a few buffers).
func (r *downloadRun) channelCap(nkeys int) int {
	if nkeys < 1 {
		nkeys = 1
	}
	if r.cfg.Download.MaxBufferedBytes <= 0 || r.partSize <= 0 {
		return 4
	}
	per := int(r.cfg.Download.MaxBufferedBytes/r.partSize) / nkeys
	if per < 1 {
		per = 1
	}
	return per
}

// doGet performs one retried, traced, timed ranged GET and hands the body to
// consume, which delivers/drains it and returns the byte count. It records the
// per-part timing, TTFB, connection, and CSV row on success.
func (r *downloadRun) doGet(ctx context.Context, j job, consume func(body io.Reader) (int64, error)) (int64, error) {
	var ci connInfo
	var ttfb, total time.Duration
	var nbytes int64
	attempts := r.cfg.Download.MaxRetries + 1

	tries, rerr := retry(ctx, attempts, func() error {
		ci = connInfo{}
		t0 := time.Now()
		atomic.AddInt64(&r.prog.InFlight, 1)
		defer atomic.AddInt64(&r.prog.InFlight, -1)

		in := &s3.GetObjectInput{
			Bucket:     aws.String(r.cfg.Bucket),
			Key:        aws.String(j.key),
			PartNumber: aws.Int32(j.part),
		}
		if r.cfg.Download.ValidateChecksum {
			in.ChecksumMode = types.ChecksumModeEnabled
		}
		out, e := r.client.GetObject(withConnTrace(ctx, &ci), in)
		if e != nil {
			return e
		}
		ttfb = time.Since(t0)
		defer out.Body.Close()

		n, ce := consume(out.Body)
		if ce != nil {
			return ce
		}
		nbytes = n
		total = time.Since(t0)
		atomic.AddInt64(&r.prog.Bytes, nbytes)
		return nil
	})

	if tries > 1 {
		atomic.AddInt64(&r.retries, int64(tries-1))
	}
	if rerr != nil {
		return 0, rerr
	}

	r.timing.Add(total)
	r.ttfb.Add(ttfb)
	r.ips.record(ci)
	if iter := atomic.LoadInt32(&r.iter); iter >= 0 {
		r.parts.add(PartRecord{
			Iter:  int(iter),
			Key:   j.key,
			Part:  j.part,
			Bytes: nbytes,
			Ms:    float64(total) / float64(time.Millisecond),
			VIP:   ci.remoteIP,
		})
	}
	return nbytes, nil
}

// fetchPart handles discard and file delivery (each part drained independently).
func (r *downloadRun) fetchPart(ctx context.Context, j job) (int64, error) {
	return r.doGet(ctx, j, func(body io.Reader) (int64, error) {
		if r.mode == deliveryFile {
			buf := r.pool.Get()
			nb, n, se := streamInto(buf, body)
			if se != nil {
				r.pool.Put(nb)
				return 0, se
			}
			off := int64(j.part-1) * r.partSize
			if we := r.files.writeAt(j.key, off, *nb); we != nil {
				r.pool.Put(nb)
				return 0, we
			}
			r.pool.Put(nb)
			return n, nil
		}
		return io.Copy(io.Discard, body) // discard
	})
}

// fetchPartTo handles ordered-stream delivery: read the part into a pooled buffer
// and push it (in-order-transferring ownership) to the object's reader.
func (r *downloadRun) fetchPartTo(ctx context.Context, j job, rd *orderedReader) error {
	_, err := r.doGet(ctx, j, func(body io.Reader) (int64, error) {
		buf := r.pool.Get()
		nb, n, se := streamInto(buf, body)
		if se != nil {
			r.pool.Put(nb)
			return 0, se
		}
		if pe := rd.push(j.part, nb); pe != nil { // ownership -> reader/consumer
			return 0, pe
		}
		return n, nil
	})
	return err
}

// partsCount returns how many parts an object was uploaded with. A HeadObject
// with PartNumber=1 echoes the total part count; objects that weren't multipart
// uploaded report a single part.
func partsCount(ctx context.Context, client *s3.Client, bucket, key string) (int32, error) {
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		PartNumber: aws.Int32(1),
	})
	if err != nil {
		return 0, err
	}
	if out.PartsCount != nil && *out.PartsCount > 0 {
		return *out.PartsCount, nil
	}
	return 1, nil
}
