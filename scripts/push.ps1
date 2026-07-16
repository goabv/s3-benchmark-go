# Push local Go source -> S3 staging prefix (run from Windows dev box).
# Defaults come from bench.config.json (bucket, region, codePrefix); override
# with params if needed.
#
# Usage: .\scripts\push.ps1  [-Bucket ...] [-Prefix code-go/] [-Region us-west-2]
param(
  [string]$Bucket = "",
  [string]$Prefix = "",
  [string]$Region = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot   # project root
$cfg = Get-Content "$root/bench.config.json" -Raw | ConvertFrom-Json

if (-not $Bucket) { $Bucket = $cfg.bucket }
if (-not $Prefix) { $Prefix = $cfg.codePrefix }
if (-not $Region) { $Region = $cfg.region }

$dest = "s3://$Bucket/$Prefix"
$regionArg = @()
if ($Region) { $regionArg = @("--region", $Region) }

Write-Host "Syncing $root -> $dest"
aws s3 sync $root $dest `
  --delete `
  --exclude ".git/*" `
  --exclude "results/*" `
  --exclude "dist/*" `
  --exclude "bench" `
  --exclude "*.exe" `
  --exclude "*.log" `
  @regionArg

Write-Host "Done. On the EC2 box run: ./scripts/pull.sh   then ./scripts/run.sh both <label>"
