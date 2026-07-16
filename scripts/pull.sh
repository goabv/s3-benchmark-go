#!/usr/bin/env bash
# Pull the Go project from the S3 staging prefix -> $BENCH_DIR and build it
# (run ON the EC2 instance). Defaults are baked in below, so `./pull.sh` with no
# args just works. Override positionally: ./pull.sh <bucket> [prefix] [region]
set -euo pipefail

# --- defaults (edit here) --------------------------------------------------
DEFAULT_BUCKET="s3dl-bench-usw2-801400661003"
DEFAULT_PREFIX="code-go/"
DEFAULT_REGION="us-west-2"
# ---------------------------------------------------------------------------

BUCKET="${1:-$DEFAULT_BUCKET}"
PREFIX="${2:-$DEFAULT_PREFIX}"
REGION="${3:-$DEFAULT_REGION}"
REGION_ARG=()
if [[ -n "$REGION" ]]; then REGION_ARG=(--region "$REGION"); fi

DEST="${BENCH_DIR:-$HOME/s3-bench-go}"
mkdir -p "$DEST"

echo "Syncing s3://${BUCKET}/${PREFIX} -> ${DEST}"
aws s3 sync "s3://${BUCKET}/${PREFIX}" "$DEST" \
  --delete \
  --exclude ".git/*" \
  --exclude "results/*" \
  --exclude "dist/*" \
  "${REGION_ARG[@]}"

cd "$DEST"

# Install Go if it's not already on the box (Amazon Linux 2023, arm64/Graviton).
if ! command -v go >/dev/null 2>&1; then
  echo "Go not found; installing the latest stable for linux/arm64 ..."
  GO_VER="$(curl -fsSL https://go.dev/VERSION?m=text | head -n1)"
  curl -fsSL "https://go.dev/dl/${GO_VER}.linux-arm64.tar.gz" -o /tmp/go.tgz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
  export PATH="$PATH:/usr/local/go/bin"
  if ! grep -q '/usr/local/go/bin' "$HOME/.bashrc" 2>/dev/null; then
    echo 'export PATH="$PATH:/usr/local/go/bin"' >> "$HOME/.bashrc"
  fi
fi

echo "Building with $(go version) ..."
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bench ./cmd/bench
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o tmbench ./cmd/tmbench

# S3 sync doesn't preserve executable bits; restore them so ./scripts/*.sh run.
chmod +x scripts/*.sh 2>/dev/null || true

echo "Ready in ${DEST}. All settings live in bench.config.json. Examples:"
echo "  sudo ./scripts/tune-network.sh      # one-time network tuning (persists)"
echo "  ./scripts/run.sh both               # custom runner: download + upload sweeps"
echo "  ./scripts/run.sh download spread    # custom download sweep, labelled 'spread'"
echo "  ./scripts/tm-run.sh both            # Transfer Manager baseline (in-memory)"
echo "  ./scripts/tm-run.sh download -concurrency 128"
