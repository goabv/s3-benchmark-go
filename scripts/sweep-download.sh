#!/usr/bin/env bash
# DOWNLOAD sweep: seed the configured object sizes (skipping any that already
# exist), then benchmark download throughput across the whole size curve.
# Run ON THE EC2 INSTANCE, in the same region as the bucket.
#
# Settings come from bench.config.json (shared keys + the "download" section).
# Override per-run with env vars: WORKERS, CONCURRENCY, ITERATIONS, WARMUP,
# PART_SIZE, DELIVERY (discard|ordered-stream|file), DELIVERY_PATH, MAX_BUFFERED,
# NO_CHECKSUM=1 (disable per-part validation), NO_TLS=1 (plaintext HTTP).
# e.g. DELIVERY=ordered-stream MAX_BUFFERED=64GiB ./scripts/sweep-download.sh
#      NO_CHECKSUM=1 ./scripts/sweep-download.sh
#
# Writes a scratch results/download-sweep-<stamp>.json (git-ignored). Use
# ./scripts/run.sh for a committable, S3-uploaded run directory.
#
# Usage: ./scripts/sweep-download.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="$PATH:/usr/local/go/bin"
if [[ ! -x ./bench ]]; then
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bench ./cmd/bench
fi

STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="results/download-sweep-${STAMP}.json"

PART_ARG=()
[[ -n "${PART_SIZE:-}" ]] && PART_ARG=(-part-size "$PART_SIZE")

BENCH_ARGS=()
[[ -n "${WORKERS:-}" ]]       && BENCH_ARGS+=(-workers "$WORKERS")
[[ -n "${CONCURRENCY:-}" ]]   && BENCH_ARGS+=(-concurrency "$CONCURRENCY")
[[ -n "${ITERATIONS:-}" ]]    && BENCH_ARGS+=(-iterations "$ITERATIONS")
[[ -n "${WARMUP:-}" ]]        && BENCH_ARGS+=(-warmup "$WARMUP")
[[ -n "${DELIVERY:-}" ]]      && BENCH_ARGS+=(-delivery "$DELIVERY")
[[ -n "${DELIVERY_PATH:-}" ]] && BENCH_ARGS+=(-delivery-path "$DELIVERY_PATH")
[[ -n "${MAX_BUFFERED:-}" ]]  && BENCH_ARGS+=(-max-buffered "$MAX_BUFFERED")
[[ -n "${NO_CHECKSUM:-}" ]]   && BENCH_ARGS+=(-no-checksum)
[[ -n "${NO_TLS:-}" ]]        && BENCH_ARGS+=(-no-tls)

echo ">> Seeding configured sizes (bench.config.json), skipping existing objects"
./bench -mode seed "${PART_ARG[@]}"

echo ">> DOWNLOAD benchmarking configured sizes -> ${OUT}"
./bench -mode download "${BENCH_ARGS[@]}" "${PART_ARG[@]}" -out "$OUT"

echo ">> Done. JSON: ${OUT}"
