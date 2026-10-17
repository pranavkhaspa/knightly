$ErrorActionPreference = "Stop"
$base = "http://127.0.0.1:4096"
$headers = @{}
if ($env:OPENCODE_SERVER_PASSWORD) {
    $user = if ($env:OPENCODE_SERVER_USERNAME) { $env:OPENCODE_SERVER_USERNAME } else { "opencode" }
    $pair = "${user}:$env:OPENCODE_SERVER_PASSWORD"
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($pair)
    $headers.Authorization = "Basic " + [Convert]::ToBase64String($bytes)
}

try {
    $response = Invoke-RestMethod -Method Get -Uri ($base + "/api/health") -Headers $headers -TimeoutSec 5
    if (-not $response.healthy) {
        throw "Health response did not report healthy=true."
    }
    Write-Host "OpenCode v2 health check passed at $base/api/health"
    $response | ConvertTo-Json -Depth 8
} catch {
    throw "OpenCode v2 API health check failed at $base/api/health. NightShift's current adapter requires the v2 session-location API; inspect $base/doc and use a compatible OpenCode v2 build. Details: $($_.Exception.Message)"
}
