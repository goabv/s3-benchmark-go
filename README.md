# S3 Benchmark Runner (Go)

A high-throughput S3 benchmark built on the AWS SDK for Go v2. It has two runners
that target the **same seeded S3 objects** (same `bench.config.json`), on the same
host and config, so their numbers are directly comparable:

- **custom runner** (`cmd/bench`) — a hand-tuned parallel ranged-GET-by-PartNumber
  downloader and multipart-PUT uploader: shared worker pool, delivery modes
  (discard / ordered-stream / file), per-part timing, retries, stall watchdog, and
  connection spreading.
- **Transfer Manager runner** (`cmd/tmbench`) — the same workload run through the
  **latest AWS SDK for Go v2 S3 Transfer Manager** (`feature/s3/transfermanager`)
  as an out-of-the-box baseline, using its `UploadObject` / `GetObject` operations.

The point of the project is to measure the custom runner against the SDK's Transfer
Manager on identical objects, hardware, and configuration. The Transfer Manager
dependency is pinned in `go.mod`; bump it to track the latest release.

## Layout

```
cmd/bench/main.go            entrypoint: load config, flags, sampler/watchdog, dispatch, report
cmd/tmbench/main.go          Transfer Manager baseline runner (in-memory, default SDK TM)
internal/tmbench/tmbench.go  TM UploadObject/GetObject benchmark, in-memory bodies
internal/config/config.go    config struct, JSON load, size parsing, key naming
internal/s3client/client.go  tuned HTTP/1.1 transport + connection spreading
internal/bench/download.go   ranged GET by PartNumber, goroutine pool, retry, ordered delivery
internal/bench/upload.go     multipart PUT: create, parallel UploadPart, complete/abort
internal/bench/orderer.go    in-order stream reassembly over a buffer pool
internal/bench/trace.go      httptrace capture of served front-end IP + conn reuse
internal/bench/retry.go      per-part retry with capped exponential backoff
internal/bufpool/pool.go     recycled fixed-size byte buffers
internal/metrics/*.go        latency percentiles, resource sampler, stall watchdog, SVG plot
internal/report/report.go    JSON run capture + SVG plot, optional S3 upload
scripts/push.ps1             Windows dev -> S3: sync source to the code prefix
scripts/pull.sh              EC2: sync source from S3 + install Go + build
scripts/run.sh               EC2: build (if needed) + run + capture, upload run to S3
scripts/tm-run.sh            EC2: Transfer Manager baseline runner (in-memory)
scripts/sweep-download.sh    EC2: seed + download sweep across the size curve (scratch JSON)
scripts/sweep-upload.sh      EC2: upload sweep across the size curve (scratch JSON)
scripts/sweep-download.ps1   Windows: local download sweep (smoke; use the .sh on EC2 for real numbers)
scripts/sweep-upload.ps1     Windows: local upload sweep (smoke)
scripts/tune-network.sh      EC2: one-time network tuning (BBR, buffers, initcwnd), with --revert
scripts/pull-results.ps1     Windows dev <- S3: sync captured runs back to commit
```

## Run (local)

```sh
go run ./cmd/bench -mode seed         # upload the configured sizes to dataPrefix (skips existing)
go run ./cmd/bench -mode download     # ranged-GET throughput across the size curve
go run ./cmd/bench -mode upload       # multipart-PUT throughput
go run ./cmd/bench -mode both         # upload then download, reported separately
```

Per-run overrides (take precedence over the config):

```sh
go run ./cmd/bench -mode download -workers 60 -concurrency 5 -iterations 3 -warmup 1 -part-size 32MiB
go run ./cmd/bench -mode download -delivery ordered-stream -max-buffered 64GiB
go run ./cmd/bench -mode download -delivery file -delivery-path /mnt/data
go run ./cmd/bench -mode download -no-checksum      # disable per-part CRC32 validation
go run ./cmd/bench -mode download -no-tls           # plaintext HTTP endpoint (measure TLS overhead)
go run ./cmd/bench -mode download -out results/download-sweep.json   # scratch JSON, no run dir
```

Credentials resolve via the default AWS chain (env, shared config, SSO, or the
EC2 instance role). On the benchmark EC2 host the instance role is used.

## Transfer Manager baseline (`tmbench`)

A separate runner benchmarks the **latest AWS SDK for Go v2 S3 Transfer Manager**
(`feature/s3/transfermanager`) as an out-of-the-box baseline, using its
single-object `UploadObject` / `GetObject` operations with **fully in-memory**
data — no local files:

- **upload** streams each object from a small repeating in-memory buffer (no
  whole-object allocation), letting the TM multipart it automatically
- **download** drains each `GetObject` body straight to `io.Discard`

It targets the same seeded objects (same `bench.config.json`) as the main
benchmark, and captures into the same `results/runs/` layout (`tm-upload-sweep.json`
/ `tm-download-sweep.json`) so the TM baseline and the custom runner are directly
comparable.

Both upload and download transfer **objects in parallel**, and each object's parts
run at the TM's own concurrency. Total parts in flight is therefore
`object-concurrency × per-object-concurrency`, which **defaults to the custom
runner's `workers × concurrency`** so the two are comparable out of the box:

- `-object-concurrency` — objects transferred at once (default: the object count)
- `-concurrency` — TM part concurrency per object (default: `target_total / object-concurrency`)

```sh
go run ./cmd/tmbench -mode download                        # auto: ~workers*concurrency in flight
go run ./cmd/tmbench -mode both -object-concurrency 10 -concurrency 25
go run ./cmd/tmbench -mode upload -part-size 32MiB -label tm-baseline
```

For example, with the default config (10 objects, download `workers 64 × concurrency 4` = 256),
`tmbench` defaults to `object-concurrency=10 × per-object-concurrency=25 ≈ 250` parts in
flight — matching the custom runner. Set `-object-concurrency 1 -concurrency 256` to
push all parallelism into a single object at a time instead.

On EC2: `./scripts/tm-run.sh both [label]` (extra flags pass through, e.g.
`./scripts/tm-run.sh download lbl -concurrency 64`). `-part-size` defaults to config
`partSize`; `-prefix` is the upload key prefix (default `tm-upload/`).

**Two download APIs (`-download-api`, default `get`):** the TM offers two ways to
download, and `tmbench` can exercise either:

- `get` (default) — `GetObject`, which returns one ordered `io.Reader` per object.
  Parts are fetched concurrently but delivered strictly in order through a single
  reader, bounded by `GetObjectBufferSize` (the read-ahead note above). This is the
  streaming API.
- `download-object` — `DownloadObject`, which writes each object's parts to an
  `io.WriterAt` at their byte offsets, fully in parallel with no single-reader
  funnel and no read-ahead budget. `tmbench` uses a discarding `WriterAt` (bytes are
  counted, then dropped), so it measures the TM's parallel-write ceiling — the
  closest TM analog to the custom runner's `discard`/`file` modes.

```sh
go run ./cmd/tmbench -mode download -download-api download-object
./scripts/tm-run.sh download tm-dlobj -download-api download-object
```

Both produce the same `tm-download-sweep.json` output; only the SDK code path
differs. `-download-api` has no effect on upload.

**Download read-ahead (important):** the TM's `GetObject` reader only fetches
`GetObjectBufferSize / partSize` parts ahead of the consumer, and
`GetObjectBufferSize` defaults to just **50 MiB** — so with 32 MiB parts it fetches
only ~1 part at a time regardless of `Concurrency`, which throttles download hard.
`tmbench` drives `GetObjectBufferSize` from the `download.maxBufferedBytes` config
key (default 64 GiB). Because `GetObjectBufferSize` is
per-`GetObject`-call, that value is treated as a **total** budget and split across
the objects in flight (`maxBufferedBytes / object-concurrency`, floored at one part
per object), so peak read-ahead memory stays ≈ the configured total rather than
×object-concurrency. If `maxBufferedBytes` is unset it falls back to
`per-object-concurrency × partSize` (floored at the SDK's 50 MiB default). The
custom runner streams straight to the sink and has no such client buffer.

**Memory / OOM:** a large read-ahead budget (e.g. `maxBufferedBytes = 64GiB`) can
be held resident during download, and in `both` mode it stacks on top of upload
buffers. Both runners set Go's soft memory limit to 80% of system RAM (so the GC
reclaims before the kernel OOM-kills the process) and free memory to the OS between
phases. Even so, a 64 GiB budget is aggressive on a 128 GiB box — if you see the
throughput collapse then a `Killed`, lower `maxBufferedBytes` (16–24 GiB still gives
tens-to-hundreds of parts of read-ahead per object, far more than needed to
saturate the link).

Both runners accept `-progress` (default on): a live one-line indicator on stderr
(cumulative bytes, instantaneous/average Gbps, in-flight). It only reads the shared
atomic counters, so it has no measurable effect on throughput and stays out of the
captured `summary.txt`. Disable with `-progress=false`.

## Sweeps vs. captured runs

In `both` mode (either runner) the phases run **upload first, then download**, but
each phase is sampled and reported **independently** — its own throughput, its own
resource table, and its own `*-sweep.json`. Nothing is averaged across phases.

Two ways to run:

- `run.sh <download|upload|both> [label]` — a **committable** run: writes the full
  `results/runs/<stamp>[-label]/` directory (config, env, sweep JSON, summary, CSV,
  SVG) and uploads it to S3. This is what you pull back and commit.
- `sweep-download.sh` / `sweep-upload.sh` — a **scratch** run for quick iteration:
  seeds (download) then benchmarks the whole size curve and drops a single
  `results/<mode>-sweep-<stamp>.json` (git-ignored). Override tunables per run:

  ```sh
  WORKERS=16 CONCURRENCY=8 ITERATIONS=3 ./scripts/sweep-download.sh
  DELIVERY=ordered-stream MAX_BUFFERED=64GiB ./scripts/sweep-download.sh   # compare modes
  DELIVERY=file DELIVERY_PATH=/mnt/data ./scripts/sweep-download.sh
  NO_CHECKSUM=1 ./scripts/sweep-download.sh    # checksum-validation off
  NO_TLS=1 ./scripts/sweep-download.sh         # plaintext HTTP
  PART_SIZE=64MiB ./scripts/sweep-upload.sh
  ```

## Run on EC2 c7gn.16xlarge (Graviton3, arm64, ~200 Gbps)

You develop on Windows and build **on the instance**. S3 is the transport, so no
SSH key handling is needed for the code itself.

**1. Push source from Windows** (uses your AWS creds; syncs to `s3://<bucket>/<codePrefix>`):

```powershell
./scripts/push.ps1
```

**2. On the EC2 instance**, pull, build, tune once, and run:

```sh
# first time: fetch pull.sh however you like (git clone, scp, or aws s3 cp),
# then it self-syncs the rest from S3 and builds with Go:
./scripts/pull.sh                      # sync source from S3, install Go if absent, go build
sudo ./scripts/tune-network.sh         # one-time: BBR, big buffers, initcwnd (persists; --revert to undo)
./scripts/run.sh both spread-arm64     # download + upload sweeps -> results/runs/<stamp>-spread-arm64/
```

`run.sh` builds `./bench` on the host (Graviton/arm64), raises `ulimit -n`, runs
the sweep, and — with `results.upload` set — the binary mirrors the whole run
directory to `s3://<bucket>/results/runs/<stamp>[-label]/`.

**3. Pull results back to Windows and commit:**

```powershell
./scripts/pull-results.ps1                      # just sync S3 -> results/runs/
./scripts/pull-results.ps1 -Commit -Push        # sync, then git add/commit/push results/runs
./scripts/pull-results.ps1 -Commit -Message "tm vs custom, aes128"
```

Both runners land under the same `results/runs/` prefix (custom runs contain
`download-sweep.json` / `upload-sweep.json`; TM runs contain
`tm-download-sweep.json` / `tm-upload-sweep.json`), so one `pull-results.ps1`
syncs — and commits — everything. `-Commit` stages and commits only `results/runs/`,
leaving your working changes untouched.

Iterate by editing on Windows, re-running `push.ps1`, then `pull.sh && run.sh` on
the host.

The instance role must allow `s3:GetObject`/`HeadObject` (download),
`s3:PutObject` + multipart actions (upload), `s3:GetObject`/`ListBucket` on the
code prefix (pull), and `s3:PutObject` on the results prefix (`results.upload`).

## Config (`bench.config.json`)

| key | meaning |
|-----|---------|
| `bucket` / `region` | target bucket and region |
| `dataPrefix` | key prefix; keys are `<prefix><size>.bin` or `<prefix><size>-<i>.bin` |
| `sizes` | `[{ "size": "30GiB", "count": 10 }]` |
| `partSize` | part size for the buffer pool and multipart uploads (min 5MiB for upload) |
| `download.workers` x `download.concurrency` | total parallel in-flight GETs |
| `download.iterations` / `download.warmup` | measured vs. discarded passes |
| `download.validateChecksum` | enable per-part CRC32 validation |
| `download.spreadConnections` | fan sockets across all resolved S3 IPs |
| `download.tls` | false = plaintext HTTP endpoint |
| `download.deliveryMode` | where each part goes: `discard`, `ordered-stream`, or `file` (see below) |
| `download.deliveryPath` | directory for `file` mode |
| `download.maxBufferedBytes` | ordered-stream reorder-buffer cap (0 = bounded by parallelism x partSize) |
| `download.maxRetries` | extra attempts per part on transient failure |
| `download.stallTimeoutMs` | watchdog trips when no bytes move this long |
| `upload.workers` x `upload.concurrency` | total parallel in-flight UploadParts |
| `upload.keyPrefix` | prefix for uploaded objects (keeps seeded data safe) |
| `upload.iterations` / `upload.warmup` / `upload.maxRetries` / `upload.stallTimeoutMs` | as above |
| `sampling.enabled` / `sampling.intervalMs` | background RSS/CPU/in-flight/throughput sampling |
| `results.dir` | local directory for the JSON report + SVG plot |
| `results.plot` | render the time-series SVG |
| `results.upload` | also upload report + plot to S3 |
| `results.bucket` / `results.prefix` | destination for uploaded results (bucket defaults to `bucket`) |

## Output

Each run creates a run directory `results/runs/<YYYYMMDDThhmmss>[-label]/`. The
custom and Transfer Manager runners use the same layout so their runs are directly
comparable:

| file | contents |
|------|----------|
| `config.json` | snapshot of the config used for the run |
| `env.txt` | host, Go/OS, CPU/mem, EC2 instance-type/AZ, key network sysctls |
| `<mode>-sweep.json` | full results: `goVersion`/`sdkVersion`, resolved config, and per-group `samples`/`median`/`best`/`resources`/`partTimeStats`/`tlsInfo` |
| `summary.txt` | captured console output (throughput + resource + per-part-time tables) |
| `parttimes-<size>-<stamp>.csv` | per-part rows: `iter,key,part_number,bytes,<mode>_ms,vip,conn_id` |
| `<mode>.svg` | stacked time-series plot (throughput, RSS, in-flight, CPU) |

Pass a label to group/compare runs:

```sh
./run.sh download spread-arm64      # -> results/runs/<stamp>-spread-arm64/
go run ./cmd/bench -mode upload -label baseline
```

With `results.upload` the whole run directory is mirrored to
`s3://<bucket>/results/runs/<stamp>[-label]/`.

## Delivery modes and what they measure

`download.deliveryMode` selects the sink each downloaded part is written to:

| mode | what it does | memory profile |
|------|--------------|----------------|
| `discard` (default) | drain each part to `io.Discard`, count bytes; no ordering | low, flat (~100-200 MB) — nothing is buffered |
| `ordered-stream` | reassemble parts in ascending order and deliver them as one consumable stream per object (see below), bounded by `maxBufferedBytes` | rises with the reorder window (roughly `maxBufferedBytes`) |
| `file` | write each part to disk at its byte offset under `deliveryPath` (positional `WriteAt`) | low RSS, but adds disk-write throughput as the bottleneck/sink |

This is why the resource table's memory % depends on the mode: in the default
`discard` mode the process buffers almost nothing, so RSS (and MEM%) stay low even
at full network throughput. Switch to `ordered-stream` to hold parts in memory for
in-order delivery, or `file` to measure with a real disk sink.

### ordered-stream is a real, consumable stream

`ordered-stream` delivers each object as one in-order `io.Reader` (an
`orderedReader`), exactly like the Transfer Manager's `GetObject` `Body` — so it's
equally ergonomic for a real consumer (drop it into `io.Copy`, a decoder, a hasher,
etc.). It also implements `io.WriterTo`, so `io.Copy` hands each pooled part buffer
straight to the destination and recycles it with **no intermediate copy**. The TM's
reader is `Read`-only, so `io.Copy` from it always copies through a temp buffer; the
custom `ordered-stream` avoids that copy on the `io.Copy`/`WriterTo` path, so it can
match or beat the TM while doing the same ordered-delivery work. The benchmark drains
each object's reader with `io.Copy(io.Discard, reader)` — the same path a real
consumer would drive.

`CPU%` is normalized to "% of all cores" (100% = every vCPU saturated).

## Feature status

- [x] Upload benchmark (multipart PUT) with abort-on-error cleanup
- [x] Ordered-stream delivery as a consumable `io.Reader`/`io.WriterTo` (`deliveryMode: ordered-stream`)
- [x] Per-part timing percentiles (p50/p90/p99/p99.9) + front-end IP / conn-reuse capture
- [x] Time-series RSS/CPU/in-flight sampling + SVG plot
- [x] Stall watchdog + per-part retry
- [x] Run capture + S3 upload of results
