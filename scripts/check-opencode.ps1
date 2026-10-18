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
    $response = Invoke-RestMethod -Method Get -Uri ($base + "/global/health") -Headers $headers -TimeoutSec 5
    if (-not $response.healthy) {
        throw "Health response did not report healthy=true."
    }
    Write-Host "OpenCode health check passed at $base/global/health"
    $response | ConvertTo-Json -Depth 8
} catch {
    throw "OpenCode v2 API health check failed at $base/global/health. Check that the server is running (.\scripts\start-opencode.ps1). Details: $($_.Exception.Message)"
}
