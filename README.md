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

## Run on EC2

You develop on Windows and build **on the instance**; S3 is the transport for the
source, so no SSH key handling is needed for the code.

1. **Push source from Windows** (syncs to `s3://<bucket>/<codePrefix>`):

   ```powershell
   ./scripts/push.ps1
   ```

2. **On the instance** — pull, build, tune once, seed, and run:

   ```sh
   ./scripts/pull.sh                       # sync source from S3, install Go if absent, build tmbench
   sudo ./scripts/tune-network.sh          # one-time: BBR, big buffers, initcwnd (--revert to undo)
   ./scripts/tm-run.sh seed                # idempotent data-prep (skips existing objects)
   ./scripts/tm-run.sh both baseline       # upload then download (baseline profile)
   ./scripts/tm-run.sh download df -profile optimized -download-api download-file -part-size 8MiB
   ```

   `tm-run.sh <mode> [label] [extra tmbench flags...]` builds `./tmbench`, raises
   `ulimit -n`, runs, and — with `results.upload` set — mirrors the whole run
   directory to `s3://<bucket>/results/runs/<stamp>[-label]/`.

3. **Pull results back to Windows and commit:**

   ```powershell
   ./scripts/pull-results.ps1               # sync S3 -> results/runs/
   ./scripts/pull-results.ps1 -Commit -Push # sync, then commit only results/runs/
   ```

The instance role must allow `s3:GetObject`/`HeadObject` (download + the seed
skip-existing check), `s3:PutObject` + multipart actions (upload / seed),
`s3:GetObject`/`ListBucket` on the code prefix (pull), and `s3:PutObject` on the
results prefix (`results.upload`).

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
