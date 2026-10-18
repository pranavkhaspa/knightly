$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot ".env"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "Go was not found. Install the Go version required by go.mod, reopen PowerShell, and retry."
}

# The application reads process environment variables, not .env files itself.
# Load non-empty .env values only when the current shell has not already set
# that variable. This preserves values exported by setup-local-workspace.ps1.
if (Test-Path $envFile) {
    foreach ($line in Get-Content $envFile) {
        if ($line -match '^\s*#' -or $line -match '^\s*$') { continue }
        if ($line -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$') { continue }
        $name = $Matches[1]
        $value = $Matches[2]
        if ($value.Length -ge 2) {
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
        }
        $current = [Environment]::GetEnvironmentVariable($name, 'Process')
        if ([string]::IsNullOrWhiteSpace($current) -and -not [string]::IsNullOrWhiteSpace($value)) {
            [Environment]::SetEnvironmentVariable($name, $value, 'Process')
        }
    }
}

Set-Location $repoRoot
Write-Host "Starting NightShift from $repoRoot"
Write-Host "Environment: process variables plus non-empty values from .env (existing process values win)."
go run ./cmd/nightshift
if ($LASTEXITCODE -ne 0) {
    throw "NightShift exited with code $LASTEXITCODE. Check the error above and docs/deployment.md."
}
