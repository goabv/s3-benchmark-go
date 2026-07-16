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
	mode     string        // delivery mode
	partSize int64         // configured part size (byte offsets, budget accounting)
	files    *fileSink     // file mode only
	sem      chan struct{} // ordered-stream buffered-bytes budget (nil = unbounded)
	orderers map[string]*orderer
	ordMu    sync.Mutex
	iter     int32 // current measured-iteration index; <0 during warmup (no CSV rows)
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
			sem:      newBudget(cfg.Download.MaxBufferedBytes, partSize, mode),
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

// newBudget builds the ordered-stream buffered-bytes semaphore, or nil when the
// mode isn't ordered-stream or no cap is configured.
func newBudget(maxBufferedBytes, partSize int64, mode string) chan struct{} {
	if mode != deliveryOrdered || maxBufferedBytes <= 0 || partSize <= 0 {
		return nil
	}
	slots := int(maxBufferedBytes / partSize)
	if slots < 1 {
		slots = 1
	}
	return make(chan struct{}, slots)
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

// runOnce drives one full pass over the job set with a fixed-size goroutine pool.
func (r *downloadRun) runOnce(ctx context.Context, jobs []job, parallelism int) (iterResult, error) {
	if r.mode == deliveryOrdered {
		r.ordMu.Lock()
		r.orderers = map[string]*orderer{}
		r.ordMu.Unlock()
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

// fetchPart downloads one part (ranged GET by PartNumber) with retry, per-part
// timing, connection-trace capture, and delivery to the configured sink.
func (r *downloadRun) fetchPart(ctx context.Context, j job) (nbytes int64, err error) {
	// ordered-stream budget: hold at most maxBufferedBytes worth of parts at once.
	if r.sem != nil {
		r.sem <- struct{}{}
	}

	var ci connInfo
	var ttfb, total time.Duration
	var delivered *[]byte // ordered mode: the buffer to hand to the orderer on success
	attempts := r.cfg.Download.MaxRetries + 1

	tries, rerr := retry(ctx, attempts, func() error {
		ci = connInfo{}
		delivered = nil
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

		switch r.mode {
		case deliveryFile:
			buf := r.pool.Get()
			nb, n, se := streamInto(buf, out.Body)
			if se != nil {
				r.pool.Put(nb)
				return se
			}
			off := int64(j.part-1) * r.partSize
			if we := r.files.writeAt(j.key, off, *nb); we != nil {
				r.pool.Put(nb)
				return we
			}
			r.pool.Put(nb)
			nbytes = n

		case deliveryOrdered:
			buf := r.pool.Get()
			nb, n, se := streamInto(buf, out.Body)
			if se != nil {
				r.pool.Put(nb)
				return se
			}
			delivered = nb // handed to the orderer after the retry loop succeeds
			nbytes = n

		default: // discard
			n, ce := io.Copy(io.Discard, out.Body)
			if ce != nil {
				return ce
			}
			nbytes = n
		}
		total = time.Since(t0)
		atomic.AddInt64(&r.prog.Bytes, nbytes)
		return nil
	})

	if tries > 1 {
		atomic.AddInt64(&r.retries, int64(tries-1))
	}
	if rerr != nil {
		if delivered != nil {
			r.pool.Put(delivered)
		}
		if r.sem != nil {
			<-r.sem // never delivered; free the slot
		}
		return 0, rerr
	}

	// Ordered delivery + budget release happen once, after a successful fetch.
	if r.mode == deliveryOrdered {
		flushed, de := r.ordererFor(j.key).deliver(j.part, delivered)
		if de != nil {
			if r.sem != nil {
				<-r.sem
			}
			return 0, de
		}
		if r.sem != nil {
			for k := 0; k < flushed; k++ {
				<-r.sem
			}
		}
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

func (r *downloadRun) ordererFor(key string) *orderer {
	r.ordMu.Lock()
	defer r.ordMu.Unlock()
	o := r.orderers[key]
	if o == nil {
		o = newOrderer(io.Discard, r.pool)
		r.orderers[key] = o
	}
	return o
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
