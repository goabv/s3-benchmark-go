#!/usr/bin/env bash
# Run the AWS SDK for Go v2 Transfer Manager baseline ON THE EC2 INSTANCE and
# capture a committable run under results/runs/<timestamp>[-label]/ (uploaded to
# S3 when results.upload is set). Fully in-memory: upload streams from a repeating
# buffer, download drains to /dev/null — no local files.
#
# Usage:
#   ./scripts/tm-run.sh [both]     [label]   # DEFAULT: upload then download
#   ./scripts/tm-run.sh download   [label]
#   ./scripts/tm-run.sh upload     [label]
#   ./scripts/tm-run.sh download my-label -concurrency 128   # extra flags pass through
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-both}"
LABEL="${2:-}"
shift || true
shift || true # drop mode + optional label; remaining args pass through to tmbench

case "$MODE" in
  both | download | upload) ;;
  *)
    echo "unknown mode '${MODE}' (use both | download | upload)" >&2
    exit 1
    ;;
esac

export PATH="$PATH:/usr/local/go/bin"
if [[ ! -x ./tmbench ]]; then
  echo ">> building tmbench ..."
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o tmbench ./cmd/tmbench
fi

CUR="$(ulimit -n)"
if [[ "$CUR" != "unlimited" && "$CUR" -lt 1048576 ]]; then
  ulimit -n 1048576 2>/dev/null || true
fi

LABEL_ARGS=()
if [[ -n "$LABEL" ]]; then LABEL_ARGS=(-label "$LABEL"); fi

echo ">> host:  $(hostname)  cpus=$(nproc)  ulimit -n=$(ulimit -n)"
echo ">> Transfer Manager baseline  mode:$MODE  label:${LABEL:-<none>}"

START="$(date +%s)"
./tmbench -config bench.config.json -mode "$MODE" "${LABEL_ARGS[@]}" "$@"
END="$(date +%s)"

echo
echo ">> Done in $((END - START))s. Latest run directory:"
ls -1dt results/runs/*/ 2>/dev/null | head -n 1 || true
