# UPLOAD sweep (Windows): benchmark multipart upload throughput across the size
# curve. For real numbers run scripts/sweep-upload.sh on EC2.
# Settings come from bench.config.json (shared + "upload" section); params override.
#
# WARNING: this re-uploads every configured size each iteration (e.g. 30 GiB x
# iterations x count) — real S3 write traffic and cost.
#
# Usage: .\scripts\sweep-upload.ps1  [-Workers N] [-Concurrency N] [-Iterations N] [-Warmup N] [-PartSize 64MiB]
param(
  [int]$Workers = 0,
  [int]$Concurrency = 0,
  [int]$Iterations = 0,
  [int]$Warmup = -1,
  [string]$PartSize = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
  $stamp = Get-Date -Format "yyyyMMddTHHmmss"
  $out = "results/upload-sweep-$stamp.json"

  if (-not (Test-Path "./bench.exe")) {
    $env:CGO_ENABLED = "0"; go build -trimpath -ldflags "-s -w" -o bench.exe ./cmd/bench; $env:CGO_ENABLED = ""
  }

  $benchArgs = @("-out", $out)
  if ($Workers -gt 0)     { $benchArgs += @("-workers", $Workers) }
  if ($Concurrency -gt 0) { $benchArgs += @("-concurrency", $Concurrency) }
  if ($Iterations -gt 0)  { $benchArgs += @("-iterations", $Iterations) }
  if ($Warmup -ge 0)      { $benchArgs += @("-warmup", $Warmup) }
  if ($PartSize)          { $benchArgs += @("-part-size", $PartSize) }

  Write-Host ">> UPLOAD benchmarking configured sizes -> $out"
  ./bench.exe -mode upload @benchArgs

  Write-Host ">> Done. JSON: $out"
}
finally {
  Pop-Location
}
