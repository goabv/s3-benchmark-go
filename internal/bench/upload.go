package bench

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/goabv/s3-benchmark-go/internal/config"
	"github.com/goabv/s3-benchmark-go/internal/metrics"
)

// uploadRun carries shared state for one size group's multipart-upload passes.
type uploadRun struct {
	client   *s3.Client
	cfg      *config.Config
	prog     *metrics.Progress
	partSize int64
	data     []byte // read-only source buffer of len == partSize, reused for every part
	ips      *ipStats
	timing   *metrics.Recorder
	parts    *partRecorder
	retries  int64
	iter     int32
}

// RunUpload benchmarks multipart PUT throughput. For each size group it uploads
// `count` objects (sequentially), each as a parallel multipart upload, then
// completes them. Uploaded keys use cfg.Upload.KeyPrefix so seeded download data
// is never clobbered.
func RunUpload(ctx context.Context, client *s3.Client, cfg *config.Config, prog *metrics.Progress) (*RunResult, error) {
	if prog == nil {
		prog = &metrics.Progress{}
	}
	partSize, err := config.ParseSize(cfg.PartSize)
	if err != nil {
		return nil, fmt.Errorf("parse partSize %q: %w", cfg.PartSize, err)
	}
	if partSize < 5<<20 {
		return nil, fmt.Errorf("partSize %s is below the S3 multipart minimum of 5MiB", cfg.PartSize)
	}
	parallelism := cfg.Upload.Parallelism()
	checksum := cfg.Checksum
	if checksum == "" {
		checksum = "CRC32"
	}

	// A single read-only source buffer, reused (via fresh readers) for every part.
	data := make([]byte, partSize)
	for i := range data {
		data[i] = byte(i * 31)
	}

	fmt.Printf("=== S3 multipart UPLOAD benchmark (AWS SDK Go v2) ===\n")
	fmt.Printf("region=%s  bucket=%s  keyPrefix=%s\n", cfg.Region, cfg.Bucket, cfg.Upload.KeyPrefix)
	fmt.Printf("source=memory  part-size=%s  checksum=%s  spread-conns=%v  workers=%d  concurrency/worker=%d  iterations=%d (warmup=%d)\n\n",
		cfg.PartSize, checksum, cfg.Download.SpreadConnections, cfg.Upload.Workers, cfg.Upload.Concurrency,
		cfg.Upload.Iterations, cfg.Upload.Warmup)

	result := &RunResult{Mode: "upload"}
	for _, spec := range cfg.Sizes {
		sizeBytes, err := config.ParseSize(spec.Size)
		if err != nil {
			return nil, fmt.Errorf("parse size %q: %w", spec.Size, err)
		}
		count := spec.Count
		if count < 1 {
			count = 1
		}
		nparts := int((sizeBytes + partSize - 1) / partSize)

		run := &uploadRun{
			client:   client,
			cfg:      cfg,
			prog:     prog,
			partSize: partSize,
			data:     data,
			ips:      newIPStats(),
			timing:   &metrics.Recorder{},
			parts:    &partRecorder{},
		}

		var measured []iterResult
		totalIters := cfg.Upload.Warmup + cfg.Upload.Iterations
		for it := 0; it < totalIters; it++ {
			if it < cfg.Upload.Warmup {
				atomic.StoreInt32(&run.iter, -1)
			} else {
				atomic.StoreInt32(&run.iter, int32(it-cfg.Upload.Warmup))
			}
			res, err := run.runOnce(ctx, spec, sizeBytes, count, parallelism, it)
			if err != nil {
				return nil, err
			}
			if it >= cfg.Upload.Warmup {
				measured = append(measured, res)
			}
		}

		samples := toSamples(measured)
		distinct, reuse := run.ips.summary()
		proto, cipher := run.ips.tls()
		gr := GroupResult{
			Label:        spec.Size,
			Files:        count,
			PerFileSize:  sizeBytes,
			Size:         sizeBytes * int64(count),
			Parts:        nparts * count,
			Workers:      cfg.Upload.Workers,
			Concurrency:  cfg.Upload.Concurrency,
			InFlight:     parallelism,
			Iterations:   cfg.Upload.Iterations,
			PartSize:     partSize,
			Checksum:     checksum,
			UploadSource: "memory",
			Samples:      samples,
			Median:       median(samples),
			Best:         best(samples),
			PartTime:     toPartTimeStats(run.timing.Percentiles()),
			TLS:          TLSInfo{Protocol: proto, Cipher: cipher},
			Parts_:       run.parts.snapshot(),
			DistinctIPs:  distinct,
			ReuseRatio:   reuse,
			Retries:      int(atomic.LoadInt64(&run.retries)),
		}
		result.Groups = append(result.Groups, gr)
	}
	return result, nil
}

// runOnce uploads `count` objects for one size group and returns the aggregate
// throughput measurement. The pass index keeps keys unique across passes.
func (r *uploadRun) runOnce(ctx context.Context, spec config.SizeSpec, sizeBytes int64, count, parallelism, pass int) (iterResult, error) {
	clean := sanitizeLabel(spec.Size)
	start := time.Now()
	var totalBytes int64

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s%s-i%d-%d.bin", r.cfg.Upload.KeyPrefix, clean, pass, i)
		n, err := r.uploadObject(ctx, key, sizeBytes, parallelism)
		if err != nil {
			return iterResult{}, fmt.Errorf("upload %q: %w", key, err)
		}
		totalBytes += n
	}
	return iterResult{Bytes: totalBytes, Elapsed: time.Since(start)}, nil
}

// uploadObject performs one object's multipart upload: create, parallel UploadPart,
// complete. On any error it aborts the upload to avoid leaving orphaned parts.
func (r *uploadRun) uploadObject(ctx context.Context, key string, sizeBytes int64, parallelism int) (int64, error) {
	create, err := r.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(r.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("create multipart: %w", err)
	}
	uploadID := aws.ToString(create.UploadId)

	nparts := int32((sizeBytes + r.partSize - 1) / r.partSize)
	completed := make([]types.CompletedPart, nparts)

	partCh := make(chan int32, parallelism*2)
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < parallelism; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range partCh {
				contentLen := r.partSize
				if int64(p) == int64(nparts) { // last part
					if rem := sizeBytes - int64(p-1)*r.partSize; rem < contentLen {
						contentLen = rem
					}
				}
				etag, err := r.uploadPart(ctx, key, uploadID, p, contentLen)
				if err != nil {
					if firstErr.Load() == nil {
						firstErr.Store(err)
					}
					continue
				}
				completed[p-1] = types.CompletedPart{
					ETag:       aws.String(etag),
					PartNumber: aws.Int32(p),
				}
			}
		}()
	}
	for p := int32(1); p <= nparts; p++ {
		if firstErr.Load() != nil {
			break
		}
		partCh <- p
	}
	close(partCh)
	wg.Wait()

	if e := firstErr.Load(); e != nil {
		r.abort(key, uploadID)
		return 0, e.(error)
	}

	_, err = r.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(r.cfg.Bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		r.abort(key, uploadID)
		return 0, fmt.Errorf("complete multipart: %w", err)
	}
	return sizeBytes, nil
}

// uploadPart uploads a single part with retry, per-part timing, and conn-trace
// capture, recording the CSV row during measured iterations.
func (r *uploadRun) uploadPart(ctx context.Context, key, uploadID string, part int32, contentLen int64) (etag string, err error) {
	var ci connInfo
	var dur time.Duration
	attempts := r.cfg.Upload.MaxRetries + 1

	tries, rerr := retry(ctx, attempts, func() error {
		ci = connInfo{}
		t0 := time.Now()
		atomic.AddInt64(&r.prog.InFlight, 1)
		defer atomic.AddInt64(&r.prog.InFlight, -1)

		body := bytes.NewReader(r.data[:contentLen])
		out, e := r.client.UploadPart(withConnTrace(ctx, &ci), &s3.UploadPartInput{
			Bucket:        aws.String(r.cfg.Bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(part),
			ContentLength: aws.Int64(contentLen),
			Body:          body,
		})
		if e != nil {
			return e
		}
		etag = aws.ToString(out.ETag)
		dur = time.Since(t0)
		atomic.AddInt64(&r.prog.Bytes, contentLen)
		return nil
	})

	if tries > 1 {
		atomic.AddInt64(&r.retries, int64(tries-1))
	}
	if rerr != nil {
		return "", rerr
	}

	// Record stats for measured iterations only (exclude warmup).
	if iter := atomic.LoadInt32(&r.iter); iter >= 0 {
		r.timing.Add(dur)
		r.ips.record(ci)
		r.parts.add(PartRecord{
			Iter:  int(iter),
			Key:   key,
			Part:  part,
			Bytes: contentLen,
			Ms:    float64(dur) / float64(time.Millisecond),
			VIP:   ci.remoteIP,
		})
	}
	return etag, nil
}

func (r *uploadRun) abort(key, uploadID string) {
	// Best-effort cleanup on a detached context so a cancelled run still aborts.
	actx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = r.client.AbortMultipartUpload(actx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(r.cfg.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
}

func sanitizeLabel(s string) string {
	out := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '.':
			out = append(out, c)
		default:
			// drop spaces and other punctuation
		}
	}
	return string(bytes.ToLower(out))
}
