param(
  [string]$Source = 'D:\Downloads\cty\lakehouse\news-api\app.py'
)
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Dest = Join-Path $RepoRoot 'services\ingestion-api\src\main.py'
if (-not (Test-Path $Source)) { throw "Original app.py not found: $Source" }
if (Test-Path $Dest) {
    $body = Get-Content -Raw $Dest
    if ($body -notmatch 'BOOTSTRAP_PLACEHOLDER') {
      throw "Target already edited; refusing overwrite: $Dest"
    }
}
Copy-Item -LiteralPath $Source -Destination $Dest -Force
Write-Host 'Imported original Windows app.py to services/ingestion-api/src/main.py'
Write-Host 'V1 app still contains GET and CRUD routes. query-api remains scaffold until refactor.'
Write-Host 'No .env, credentials or venv copied. Review source before git commit.'
