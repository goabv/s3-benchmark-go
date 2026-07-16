# Pull captured benchmark runs from S3 into the local repo (results/runs/), so you
# can review and commit them alongside the code. Reads bucket/region from
# bench.config.json. Run from the project root on your dev machine.
#
# This covers BOTH runners: the custom runner (cmd/bench) and the Transfer Manager
# runner (cmd/tmbench) both upload their run directories under the same
# results/runs/ prefix, so a single sync pulls everything.
#
# Usage:
#   .\scripts\pull-results.ps1                       # just sync from S3
#   .\scripts\pull-results.ps1 -Commit               # sync, then git add + commit results/runs
#   .\scripts\pull-results.ps1 -Commit -Push         # ... and push
#   .\scripts\pull-results.ps1 -Commit -Message "tm vs custom, aes128"
param(
  [switch]$Commit,
  [switch]$Push,
  [string]$Message = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$cfg = Get-Content (Join-Path $root "bench.config.json") -Raw | ConvertFrom-Json
$bucket = $cfg.bucket
$region = $cfg.region
if (-not $bucket) { throw "No bucket in bench.config.json" }

# Honor results.bucket / results.prefix overrides when present.
$resultsBucket = if ($cfg.results.bucket) { $cfg.results.bucket } else { $bucket }
$prefix = if ($cfg.results.prefix) { $cfg.results.prefix } else { "results/runs/" }
$prefix = $prefix.TrimEnd('/') + '/'

$dest = Join-Path $root "results\runs"
New-Item -ItemType Directory -Force -Path $dest | Out-Null

$regionArgs = @()
if ($region) { $regionArgs = @("--region", $region) }

Write-Host ">> Syncing s3://$resultsBucket/$prefix -> results\runs\"
aws s3 sync "s3://$resultsBucket/$prefix" $dest @regionArgs

if (-not $Commit) {
  Write-Host ">> Done. Review with: git status ; then:"
  Write-Host "     git add results/runs && git commit -m 'benchmarks: <describe>' && git push"
  return
}

Push-Location $root
try {
  if (-not $Message) {
    $Message = "benchmarks: results $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
  }
  Write-Host ">> git add results/runs"
  git add -- results/runs
  # Commit only if there is something staged under results/runs.
  git diff --cached --quiet -- results/runs
  if ($LASTEXITCODE -eq 0) {
    Write-Host ">> No new results to commit."
    return
  }
  Write-Host ">> git commit -m `"$Message`""
  git commit -m $Message
  if ($Push) {
    Write-Host ">> git push"
    git push
  }
  else {
    Write-Host ">> Committed. Push with: git push"
  }
}
finally {
  Pop-Location
}
