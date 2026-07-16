# DOWNLOAD sweep (Windows): seed configured sizes, then benchmark download across
# the size curve. For real numbers run scripts/sweep-download.sh on in-region EC2.
# Settings come from bench.config.json (shared + "download" section); params below
# override per-run.
#
# Usage: .\scripts\sweep-download.ps1  [-Workers N] [-Concurrency N] [-Iterations N] [-Warmup N] [-PartSize 64MiB]
param(
  [int]$Workers = 0,
  [int]$Concurrency = 0,
  [int]$Iterations = 0,
  [int]$Warmup = -1,
  [string]$PartSize = "",
  [ValidateSet("", "discard", "ordered-stream", "file")] [string]$Delivery = "",
  [string]$DeliveryPath = "",
  [string]$MaxBuffered = "",
  [switch]$NoChecksum,
  [switch]$NoTls
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
  $stamp = Get-Date -Format "yyyyMMddTHHmmss"
  $out = "results/download-sweep-$stamp.json"

  if (-not (Test-Path "./bench.exe")) {
    $env:CGO_ENABLED = "0"; go build -trimpath -ldflags "-s -w" -o bench.exe ./cmd/bench; $env:CGO_ENABLED = ""
  }

  $partArg = @()
  if ($PartSize) { $partArg = @("-part-size", $PartSize) }

  $benchArgs = @("-out", $out)
  if ($Workers -gt 0)     { $benchArgs += @("-workers", $Workers) }
  if ($Concurrency -gt 0) { $benchArgs += @("-concurrency", $Concurrency) }
  if ($Iterations -gt 0)  { $benchArgs += @("-iterations", $Iterations) }
  if ($Warmup -ge 0)      { $benchArgs += @("-warmup", $Warmup) }
  if ($Delivery)          { $benchArgs += @("-delivery", $Delivery) }
  if ($DeliveryPath)      { $benchArgs += @("-delivery-path", $DeliveryPath) }
  if ($MaxBuffered)       { $benchArgs += @("-max-buffered", $MaxBuffered) }
  if ($NoChecksum)        { $benchArgs += "-no-checksum" }
  if ($NoTls)             { $benchArgs += "-no-tls" }

  Write-Host ">> Seeding configured sizes (bench.config.json), skipping existing objects"
  ./bench.exe -mode seed @partArg

  Write-Host ">> DOWNLOAD benchmarking configured sizes -> $out"
  ./bench.exe -mode download @benchArgs @partArg

  Write-Host ">> Done. JSON: $out"
}
finally {
  Pop-Location
}
