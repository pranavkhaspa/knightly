$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$policyPath = Join-Path $repoRoot "deploy\opencode\server-policy.jsonc"

if (-not (Get-Command opencode -ErrorAction SilentlyContinue)) {
    throw "OpenCode CLI was not found. Install it first, then reopen PowerShell. See https://opencode.ai/docs/"
}
if (-not (Test-Path $policyPath)) {
    throw "Trusted OpenCode policy file not found: $policyPath"
}

# The custom config contains hard-deny policies. Project-level opencode.json
# selects the model, but cannot override these hard-deny policies.
$env:OPENCODE_CONFIG = $policyPath
if (-not $env:OPENCODE_SERVER_PASSWORD) {
    Write-Warning "Starting without server authentication, bound to 127.0.0.1 only. Do not change the host to 0.0.0.0 without configuring server authentication and firewall rules."
} else {
    if (-not $env:OPENCODE_SERVER_USERNAME) { $env:OPENCODE_SERVER_USERNAME = "opencode" }
    Write-Host "OpenCode server Basic authentication is enabled for user '$env:OPENCODE_SERVER_USERNAME'."
}

Write-Host "Starting OpenCode on http://127.0.0.1:4096 with the trusted shell/external-directory deny policy."
opencode serve --hostname 127.0.0.1 --port 4096
