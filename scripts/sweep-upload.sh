#!/usr/bin/env bash
# UPLOAD sweep: benchmark multipart upload throughput across the whole configured
# size curve. Run ON THE EC2 INSTANCE, in-region.
#
# Settings come from bench.config.json (shared keys + the "upload" section).
# Override per-run with env vars: WORKERS, CONCURRENCY, ITERATIONS, WARMUP, PART_SIZE.
#
# Uploads go to the "upload.keyPrefix" prefix and are re-uploaded every run.
# WARNING: this re-uploads every configured size each iteration (e.g. 30 GiB x
# iterations x count) — that is real S3 write traffic and cost.
#
# Writes a scratch results/upload-sweep-<stamp>.json (git-ignored). Use
# ./scripts/run.sh for a committable, S3-uploaded run directory.
#
# Usage: ./scripts/sweep-upload.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="$PATH:/usr/local/go/bin"
if [[ ! -x ./bench ]]; then
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bench ./cmd/bench
fi

STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="results/upload-sweep-${STAMP}.json"

BENCH_ARGS=()
[[ -n "${WORKERS:-}" ]]     && BENCH_ARGS+=(-workers "$WORKERS")
[[ -n "${CONCURRENCY:-}" ]] && BENCH_ARGS+=(-concurrency "$CONCURRENCY")
[[ -n "${ITERATIONS:-}" ]]  && BENCH_ARGS+=(-iterations "$ITERATIONS")
[[ -n "${WARMUP:-}" ]]      && BENCH_ARGS+=(-warmup "$WARMUP")
[[ -n "${PART_SIZE:-}" ]]   && BENCH_ARGS+=(-part-size "$PART_SIZE")

echo ">> UPLOAD benchmarking configured sizes -> ${OUT}"
./bench -mode upload "${BENCH_ARGS[@]}" -out "$OUT"

echo ">> Done. JSON: ${OUT}"
