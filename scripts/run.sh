#!/usr/bin/env bash
# Run a benchmark ON THE EC2 INSTANCE and capture a self-contained, committable
# run under results/runs/<timestamp>[-label]/. The bench binary writes the run
# directory and, when results.upload is set in bench.config.json, mirrors it to
# S3 so you can pull it down and commit it from your dev machine.
#
# Each run directory contains:
#   config.json          - exact bench.config.json used (snapshot)
#   env.txt              - instance type, Go/SDK versions, kernel, key sysctls
#   download-sweep.json  - download results  (modes: both, download)
#   upload-sweep.json    - upload results    (modes: both, upload)
#   summary.txt          - the formatted console output (throughput + resources)
#   *.csv / *.svg        - per-part times / time-series artifacts
#
# Usage:
#   ./scripts/run.sh [both]     [label]   # DEFAULT: download sweep, then upload sweep
#   ./scripts/run.sh download   [label]   # download sweep only
#   ./scripts/run.sh upload     [label]   # upload sweep only
#
# Example: ./scripts/run.sh both aes128-spread
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-both}"
LABEL="${2:-}"

case "$MODE" in
  both | download | upload) ;;
  *)
    echo "unknown mode '${MODE}' (use both | download | upload)" >&2
    exit 1
    ;;
esac

# Build if the binary is missing or older than the sources (fast, cached).
export PATH="$PATH:/usr/local/go/bin"
if [[ ! -x ./bench ]]; then
  echo ">> building bench ..."
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bench ./cmd/bench
fi

# Raise the fd ceiling for this process; many parallel sockets each need one.
CUR="$(ulimit -n)"
if [[ "$CUR" != "unlimited" && "$CUR" -lt 1048576 ]]; then
  ulimit -n 1048576 2>/dev/null || echo "note: could not raise ulimit -n (run: sudo ./scripts/tune-network.sh is separate; use 'ulimit -n' check)" >&2
fi

LABEL_ARGS=()
if [[ -n "$LABEL" ]]; then LABEL_ARGS=(-label "$LABEL"); fi

echo ">> host:  $(hostname)  cpus=$(nproc)  ulimit -n=$(ulimit -n)"
echo ">> mode:  $MODE  label:${LABEL:-<none>}"

START="$(date +%s)"
./bench -config bench.config.json -mode "$MODE" "${LABEL_ARGS[@]}"
END="$(date +%s)"

echo
echo ">> Done in $((END - START))s. Latest run directory:"
ls -1dt results/runs/*/ 2>/dev/null | head -n 1 || true
echo ">> On your dev machine: .\\scripts\\pull-results.ps1   then commit results/runs/"
