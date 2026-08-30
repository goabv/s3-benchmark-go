# S3 Transfer Manager Benchmark (Go)

A high-throughput benchmark for the **AWS SDK for Go v2 S3 Transfer Manager**
(`feature/s3/transfermanager`). It runs upload and download workloads against
seeded S3 objects and reports throughput, latency percentiles, and process
resource usage, so you can compare the SDK's out-of-the-box behavior (the
**baseline** profile) against tuned variants (the **optimized** profile).

The optimized profile pulls in a **fork** of the Transfer Manager
(`github.com/goabv/aws-sdk-go-v2`, branch `transfermanager-downloadfile`) that adds
a new **`DownloadFile`** operation: the SDK owns the destination file and writes it
with O_DIRECT for large objects. The fork is wired in via a `replace` directive in
`go.mod`.

## Runner

There is a single runner, `cmd/tmbench`. It is a thin wrapper that drives the
Transfer Manager operations and provides the sink/consumer each download API needs:

| download `api` | SDK operation | what the runner provides |
|---|---|---|
| `get` | `GetObject` | drains the returned stream to a counting discard sink |
| `download-object` + `sink: discard` | `DownloadObject` | a `discardWriterAt` (WriterAt bit-bucket, counts bytes) |
| `download-object` + `sink: file` | `DownloadObject` | a buffered `*os.File` WriterAt under `deliveryPath` |
| `download-file` | `DownloadFile` (fork) | nothing — the SDK owns the file sink (O_DIRECT / buffered) |

Upload uses `UploadObject`, streaming each object from a small repeating in-memory
pattern (no whole-object allocation). Both directions transfer objects in parallel
(`objectConcurrency`), and each object's parts run at the TM's own `concurrency`, so
parts in flight ≈ `objectConcurrency × concurrency`.

## Modes and profiles

Modes (`-mode`): `seed | upload | download | both`.

- `seed` — idempotent data-prep: upload the configured sizes to the download keys,
  skipping any that already exist (HeadObject check). No sampling or report.
- `upload` / `download` — one direction, sampled and reported.
- `both` — upload then download, each sampled and reported independently (never mixed).

Profiles (`-profile`): `baseline` (config section `transferManager`) or `optimized`
(config section `tmOptimized`). `transferManager` is the pristine out-of-the-box
baseline (standard `GetObject` / `DownloadObject` / `UploadObject`, unchanged by the
fork). `tmOptimized` carries the optimization knobs — `multiNIC` and the
`DownloadFile` O_DIRECT controls. The config section is the source of truth; flags
override in place.

## Layout

```
cmd/tmbench/main.go            entrypoint: config, flags, sampler/watchdog, dispatch, report
internal/tmbench/tmbench.go    the runner: RunSeed / RunUpload / RunDownload (get) /
                               RunDownloadObject (WriterAt) / RunDownloadFile
internal/config/config.go      config struct, JSON load, size parsing, key naming
internal/s3client/client.go    tuned HTTP/1.1 transport + optional multi-NIC / conn spreading
internal/bench/result.go       shared result/sample/resource types (consumed by tmbench + report)
internal/metrics/*.go          latency percentiles, resource sampler, stall watchdog, SVG plot
internal/report/*.go           JSON run capture + SVG plot, optional S3 upload
scripts/push.ps1               Windows dev -> S3: sync source to the code prefix
scripts/pull.sh                EC2: sync source from S3, install Go, build tmbench
scripts/tm-run.sh              EC2: build (if needed) + run + capture, upload run to S3
scripts/pull-results.ps1       Windows dev <- S3: sync captured runs back to commit
scripts/tune-network.sh        EC2: one-time network tuning (BBR, buffers, initcwnd), with --revert
scripts/setup-multinic.sh      EC2: per-ENI source-based policy routing for multiNIC
```

## Config (`bench.config.json`)

Top-level keys: `bucket`, `region`, `dataPrefix` (download data key prefix),
`codePrefix` (S3 staging prefix for source), `sizes` (e.g.
`[{ "size": "1024GiB", "count": 5 }]`), `partSize` (TM part / range size),
`checksum`, `memoryLimit` (absolute Go soft memory limit, e.g. `"40GiB"`; empty =
80% of RAM), `sampling`, `results`.

Two Transfer Manager sections share the same shape: `transferManager` (baseline) and
`tmOptimized` (optimized). Download knobs:

```json
"tmOptimized": {
  "download": {
    "api": "download-file",         // get | download-object | download-file
    "getObjectType": "ranges",      // parts (PartNumber) | ranges (byte ranges of partSize)
    "sink": "file",                 // discard | file  (download-object only)
    "deliveryPath": "/mnt/stripe/output",
    "objectConcurrency": 5,         // objects in parallel (0 = object count)
    "concurrency": 640,             // per-object part concurrency (0 = SDK default)
    "iterations": 1, "warmup": 0,
    "validateChecksum": true,       // false -> DisableChecksumValidation (skip the CRC pass)
    "maxBufferedBytes": 68719476736,// GetObject read-ahead total ("get" API only)
    "multiNIC": true,               // round-robin source IP across ENIs (optimized only)
    "verifyFullChecksum": false,    // untimed CRC32 re-read of written files (file sinks)
    "directIOThreshold": "100MiB",  // download-file: use O_DIRECT above this object size
    "disableDirectIO": false,       // download-file: force the buffered writer
    "writeChunkSize": "8MiB",       // download-file: fixed disk-write chunk size
    "writeFlushWorkers": 16,        // download-file: write-behind flush workers per file
    "writeFlushQueueDepth": 64,     // download-file: write-behind queue depth
    "disableWriteBufferPool": false // download-file: A/B pooled vs raw-malloc O_DIRECT buffers
  },
  "upload": { "objectConcurrency": 0, "concurrency": 640, "iterations": 1, "warmup": 0 }
}
```

Most knobs have a `-flag` override for one-off runs: `-download-api`,
`-get-object-type`, `-part-size`, `-concurrency`, `-object-concurrency`,
`-delivery-path`, `-profile`.

### The `DownloadFile` fork (optimized profile)

O_DIRECT lives in the fork's `DownloadFile`, not in the benchmark. `DownloadFile`
owns the destination writer: it downloads each object straight to a file under
`deliveryPath`, using **O_DIRECT** for objects larger than `directIOThreshold`
(Linux) and a buffered writer otherwise, and coalesces part/range data into fixed
`writeChunkSize` writes behind a pool of `writeFlushWorkers` draining a
`writeFlushQueueDepth` queue (write-behind — network receive is decoupled from the
disk write). O_DIRECT bypasses the page cache; on XFS the shared inode lock lets the
flush workers hit the stripe in parallel instead of serializing on the buffered
single-inode write lock. Linux only; elsewhere it falls back to the buffered writer.

The fork is a sibling checkout referenced by a `replace` in `go.mod`:

```
replace github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager => ../aws-sdk-go-v2/feature/s3/transfermanager
```

So on both your dev machine and the instance, check the fork out next to this repo
(`../aws-sdk-go-v2`).

### `download-file` code flow

The path a `download-file` run takes, from the benchmark runner into the fork and
down to disk. The network side (`GetObject` → `io.Copy`) and the disk side
(`WriteAt` → flush pool → `pwrite`) run concurrently and decoupled through the
bounded flush queue; the two repos meet at the single `tm.DownloadFile` call
(wired by the `replace` directive above).

**A. Benchmark runner sets up** (this repo, `s3-benchmark-go`):

1. `scripts/tm-run.sh download <label> -profile optimized` → `./tmbench -mode download -profile optimized`.
2. `cmd/tmbench/main.go run()` — `-profile optimized` selects the `tmOptimized` config section, validates `api=download-file` (requires `deliveryPath`), applies `memoryLimit`, builds the tuned S3 client (`s3client.New`, multi-NIC if set) and the TM client (`transfermanager.New`, `PartSizeBytes=partSize`).
3. `phasesFor()` dispatches the download phase on `api` → `tmbench.RunDownloadFile(...)`.
4. `runPhase()` wraps it with the sampler / stall watchdog / progress printer (they read `prog.Bytes`/`InFlight`) and times it.
5. `internal/tmbench/tmbench.go RunDownloadFile()` — mkdir + start-of-run cleanup, then per iteration `runObjectsParallel(objectConcurrency, keys)`: each object goroutine registers a `progBytesListener` and calls **`tm.DownloadFile(&DownloadFileInput{Bucket, Key, FilePath}, opts...)`**, where `opts` set `Concurrency`, `PartSizeBytes`, `GetObjectType`, and the O_DIRECT knobs (`DirectIOThreshold`, `WriteChunkSizeBytes`, `WriteFlushWorkers`, `WriteFlushQueueDepth`, `DisableWriteBufferPool`). Adds `out.ContentLength` to the sample.

**B. Fork sets up the sink** (`aws-sdk-go-v2`, via the `replace`):

6. `api_op_DownloadFile.go Client.DownloadFile()` — copies+applies options, `downloadFileObjectSize()` does a `HeadObject` to learn the size, then `newFileSink(path, size, opts)`.
7. `downloadfile_sink.go newFileSink()` — size > `DirectIOThreshold` and Linux → `newDirectChunkSink()` (`downloadfile_directsink_linux.go`): opens `O_DIRECT`, builds `directBackend`, wraps in `newChunkedWriterAt(backend, chunkSize, flushWorkers, queueDepth)` which **spawns the flush-worker goroutines**.
8. `DownloadFile` builds a `DownloadObjectInput` with `WriterAt: sink` and runs the shared engine `downloader.download(ctx)`.

**C. Shared download engine** (the same code `DownloadObject` uses):

9. `api_op_DownloadObject.go downloader.download()` — RANGE mode: fetches the first range to learn total size, spawns `Concurrency` part-worker goroutines, feeds `dlChunk{w: sink, withRange}` advancing by `PartSizeBytes` (8 MiB ranges).
10. `downloadPart → tryDownloadChunk`: `GetObject` with a `Range` header → `io.Copy(chunk, out.Body)` (stdlib 32 KiB buffer; TLS delivers ~16 KiB reads) → `(*dlChunk).Write` → `sink.WriteAt(p, off)`. Bytes-transferred events fire the `progBytesListener` → live `prog.Bytes`.

**D. Write-behind sink** (fork):

11. `downloadfile_sink.go chunkedWriterAt.WriteAt()` — coalesces the ~16 KiB writes into fixed `writeChunkSize` (8 MiB) regions, keyed by `offset/chunkSize` in a 256-shard map. When a region fills it hands a `flushJob` to the bounded `jobs` channel (depth `writeFlushQueueDepth`), blocking if full (backpressure); it never does the disk write on the part-worker goroutine.
12. `flushLoop()` (`writeFlushWorkers` goroutines) drains `jobs` → `directBackend.writeRegion()`: block-round the length (zero-pad the tail) → `pwriteFull()` = `syscall.Pwrite` on the raw O_DIRECT fd (bypasses `os.File`'s per-fd lock so concurrent aligned writes to disjoint offsets run in parallel, DMA straight to the stripe). Then recycles the buffer to the pool.

**E. Finalize:**

13. After `download()` returns, `DownloadFile` calls `sink.Close()` (`chunkedWriterAt.Close`): enqueue the trailing partial region, close `jobs`, `wg.Wait()` the flush workers, then `directBackend.finalize()`: `Truncate(size)` (chop O_DIRECT block padding to the exact object size) → `syscall.Fdatasync(fd)` (durability flush) → `Close`.
14. `DownloadFile` returns `out` (`ContentLength` = bytes written); `RunDownloadFile` records the sample; `runPhase` renders the tables; `main.go` writes `results/runs/<stamp>-<label>/`.

Compact call chain:

```
tm-run.sh
└─ tmbench/main.go run() → phasesFor() → RunDownloadFile()            [benchmark]
   └─ tm.DownloadFile()                                              [fork ── via go.mod replace]
      ├─ downloadFileObjectSize() → HeadObject
      ├─ newFileSink() → newDirectChunkSink() → newChunkedWriterAt() [spawns flush workers]
      └─ downloader.download()                                       [shared engine]
         └─ N× downloadPart → GetObject(Range) → io.Copy → dlChunk.Write → sink.WriteAt()
            └─ chunkedWriterAt: coalesce → 8MiB region → jobs chan
               └─ flushLoop ×W → directBackend.writeRegion → syscall.Pwrite (O_DIRECT)
      └─ sink.Close() → drain → Truncate → Fdatasync → Close
```

### multiNIC (optimized profile)

When `true`, the client round-robins each outbound connection's source IP across all
of the host's ENI addresses, spreading load over multiple network cards. It requires
host-side source policy routing — run `sudo ./scripts/setup-multinic.sh` first. On a
single-NIC box it resolves to one IP and is a no-op.

### Download read-ahead ("get" API only)

The TM's `GetObject` reader fetches `GetObjectBufferSize / partSize` parts ahead of
the consumer, defaulting to only 50 MiB. `tmbench` drives it from `maxBufferedBytes`
(a total budget split across objects in flight). Only the `get` API uses this;
`download-object` and `download-file` write parts to their offsets and have no such
read-ahead budget.

### Memory limit

`memoryLimit` sets Go's soft memory limit (`GOMEMLIMIT`) to an absolute value; empty
defaults to 80% of system RAM. Lower it to make the GC collect harder and hold peak
RSS down (keep it above the live working set or the GC thrashes). `GOGC` is also
honored from the environment.

## Build and run

Clone this repo **and the fork side-by-side**. The `go.mod` `replace` points at
`../aws-sdk-go-v2/feature/s3/transfermanager`, so the fork must sit next to this repo
for the build to resolve:

```sh
git clone https://github.com/goabv/s3-benchmark-go.git
git clone --depth 1 -b transfermanager-downloadfile https://github.com/goabv/aws-sdk-go-v2.git

# resulting layout (siblings):
#   ./s3-benchmark-go     <- this repo
#   ./aws-sdk-go-v2       <- the DownloadFile fork (branch transfermanager-downloadfile)

cd s3-benchmark-go
go build -o tmbench ./cmd/tmbench      # requires Go 1.26+
```

Credentials resolve via the default AWS chain (env, shared config, SSO, or the EC2
instance role). Edit `bench.config.json` for your `bucket` / `region` / `sizes`,
then seed and run:

```sh
./tmbench -mode seed                                    # idempotent data-prep (skips existing)
./tmbench -mode both -profile baseline                  # upload then download, pristine TM
./tmbench -mode download -profile optimized -download-api download-file -part-size 8MiB
```

Or use the helper `./scripts/tm-run.sh <mode> [label] [extra tmbench flags...]`,
which builds if needed, raises `ulimit -n`, runs, and captures the run under
`results/runs/<stamp>[-label]/` (mirroring it to S3 when `results.upload` is set).

On an EC2 host, one-time network tuning helps at 100+ Gbps:

```sh
sudo ./scripts/tune-network.sh          # BBR, big buffers, initcwnd (--revert to undo)
```

The AWS identity needs `s3:GetObject`/`HeadObject` (download + the seed skip-existing
check), `s3:PutObject` + multipart actions (upload / seed), and `s3:PutObject` on the
results prefix if `results.upload` is set.

<details>
<summary>Alternative: S3-staged workflow (develop on Windows, build on EC2)</summary>

If you'd rather not push code over git, the `scripts/push.ps1` / `scripts/pull.sh` /
`scripts/pull-results.ps1` helpers stage source and results through S3 instead:
`push.ps1` syncs the workspace to `s3://<bucket>/<codePrefix>`, `pull.sh` syncs it
onto the instance (installing Go and building `tmbench`), and `pull-results.ps1`
pulls captured runs back. Note `pull.sh` builds only `tmbench`, so the fork still has
to be checked out next to the benchmark (`../aws-sdk-go-v2`) for the `replace` to
resolve.
</details>

## Output

Each run creates `results/runs/<YYYYMMDDThhmmss>[-label]/`:

| file | contents |
|------|----------|
| `config.json` | snapshot of the config used for the run |
| `env.txt` | host, Go/OS, CPU/mem, EC2 instance-type/AZ, key network sysctls |
| `tm-<mode>-sweep.json` | results: `goVersion`/`sdkVersion`, resolved config, per-group `samples`/`median`/`best`/`resources` |
| `summary.txt` | captured console output (throughput + resource tables) |
| `<mode>.svg` | stacked time-series plot (throughput, RSS, in-flight, CPU) |

A live one-line progress indicator prints to stderr during a run (`-progress`,
default on): cumulative bytes, instantaneous/average Gbps, and in-flight count. It
only reads shared atomic counters, so it has no measurable effect on throughput and
stays out of the captured `summary.txt`.
