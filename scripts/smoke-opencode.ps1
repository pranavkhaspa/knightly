# Direct OpenCode smoke test - NO NightShift involved.
# Proves (or disproves) that OpenCode itself can take a prompt and answer it.
# Run this FIRST. If it fails, the problem is OpenCode (login/model/config), not NightShift.
#
# Usage (OpenCode server must already be running via scripts\start-opencode.ps1):
#   .\scripts\smoke-opencode.ps1
#   .\scripts\smoke-opencode.ps1 -Model "opencode/big-pickle" -TimeoutSeconds 180
param(
    [string]$Base = "http://127.0.0.1:4096",
    [string]$Model = "opencode/big-pickle",
    [int]$TimeoutSeconds = 180
)
$ErrorActionPreference = "Stop"

$headers = @{}
if ($env:OPENCODE_SERVER_PASSWORD) {
    $user = if ($env:OPENCODE_SERVER_USERNAME) { $env:OPENCODE_SERVER_USERNAME } else { "opencode" }
    $pair = [System.Text.Encoding]::UTF8.GetBytes("${user}:$env:OPENCODE_SERVER_PASSWORD")
    $headers.Authorization = "Basic " + [Convert]::ToBase64String($pair)
}
function Step($n, $msg) { Write-Host "`n[$n] $msg" -ForegroundColor Cyan }
function Fail($msg) { Stop-EventCapture; Write-Host "FAIL: $msg" -ForegroundColor Red; exit 1 }
$script:eventProc = $null
$script:eventLog = Join-Path $env:TEMP ("opencode-events-" + [guid]::NewGuid().ToString("N").Substring(0, 8) + ".log")
function Stop-EventCapture {
    if ($script:eventProc -and -not $script:eventProc.HasExited) { Stop-Process -Id $script:eventProc.Id -Force -ErrorAction SilentlyContinue }
}
function Show-EventErrors {
    # OpenCode reports failures that happen before an assistant message exists
    # (bad model, missing credentials, invalid config) ONLY on the /event stream.
    Stop-EventCapture
    if (Test-Path $script:eventLog) {
        $hits = @(Select-String -Path $script:eventLog -Pattern "session.error" -SimpleMatch)
        if ($hits.Count -gt 0) {
            Write-Host "OpenCode reported this on its event stream:" -ForegroundColor Yellow
            $hits | ForEach-Object { Write-Host ("  " + $_.Line) -ForegroundColor Yellow }
        } else {
            Write-Host "No session.error was seen on the /event stream (log: $script:eventLog)." -ForegroundColor Yellow
        }
    }
}

Step 1 "Health check ($Base/global/health)"
try { $h = Invoke-RestMethod -Uri "$Base/global/health" -Headers $headers -TimeoutSec 5 }
catch { Fail "Cannot reach OpenCode: $($_.Exception.Message). Start it with .\scripts\start-opencode.ps1" }
Write-Host ("healthy={0} version={1}" -f $h.healthy, $h.version)

$modelParts = $Model.Split("/", 2)
try {
    $providers = Invoke-RestMethod -Uri "$Base/config/providers" -Headers $headers -TimeoutSec 10
    $provider = @($providers.providers) | Where-Object { $_.id -eq $modelParts[0] } | Select-Object -First 1
    if (-not $provider) {
        Write-Host ("WARN: provider '{0}' is not available on this server. Run 'opencode auth login' (choose OpenCode Zen) and restart the server." -f $modelParts[0]) -ForegroundColor Yellow
    } elseif (-not $provider.models.PSObject.Properties[$modelParts[1]]) {
        $available = ($provider.models.PSObject.Properties | ForEach-Object { $_.Name }) -join ", "
        Write-Host ("WARN: model '{0}' is not offered by provider '{1}'. Available: {2}" -f $modelParts[1], $modelParts[0], $available) -ForegroundColor Yellow
    } else {
        Write-Host ("model {0} is available" -f $Model) -ForegroundColor Green
    }
} catch { Write-Host "Could not list providers (non-fatal): $($_.Exception.Message)" -ForegroundColor Yellow }

Step 2 "Create a throwaway git repo to work in"
$dir = Join-Path $env:TEMP ("opencode-smoke-" + [guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Path $dir | Out-Null
Set-Content -Path (Join-Path $dir "hello.txt") -Value "hello"
git -C $dir init -q
git -C $dir config user.email "smoke@example.com"
git -C $dir config user.name "Smoke"
git -C $dir add . ; git -C $dir commit -qm init
Write-Host $dir

$q = "?directory=" + [uri]::EscapeDataString($dir)

Step 3 "Create session"
$session = Invoke-RestMethod -Method Post -Uri "$Base/session$q" -Headers $headers -ContentType "application/json" -Body (@{ title = "smoke" } | ConvertTo-Json)
Write-Host "session id: $($session.id)  directory: $($session.directory)"

Step 4 "Send prompt (model $Model)"
if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
    $curlArgs = @("-sN")
    if ($env:OPENCODE_SERVER_PASSWORD) { $curlArgs += @("-u", ("{0}:{1}" -f $(if ($env:OPENCODE_SERVER_USERNAME) { $env:OPENCODE_SERVER_USERNAME } else { "opencode" }), $env:OPENCODE_SERVER_PASSWORD)) }
    $curlArgs += "$Base/event$q"
    $script:eventProc = Start-Process -FilePath "curl.exe" -ArgumentList $curlArgs -RedirectStandardOutput $script:eventLog -NoNewWindow -PassThru
    Start-Sleep -Milliseconds 500
}
$parts = $Model.Split("/", 2)
$body = @{
    model = @{ providerID = $parts[0]; modelID = $parts[1] }
    parts = @(@{ type = "text"; text = "Create a file named smoke.txt containing exactly the word PONG. Then reply with one short sentence." })
} | ConvertTo-Json -Depth 6
try { Invoke-RestMethod -Method Post -Uri "$Base/session/$($session.id)/prompt_async$q" -Headers $headers -ContentType "application/json" -Body $body | Out-Null }
catch { Fail "prompt_async rejected: $($_.Exception.Message). If this is a 404 your OpenCode is too old: upgrade it." }
Write-Host "prompt accepted"

Step 5 "Wait for the assistant (up to $TimeoutSeconds s)"
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$assistant = $null
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 2
    # Windows PowerShell 5.1 returns a JSON array from Invoke-RestMethod as ONE object,
    # which made $msgs.Count 1 and turned $assistant.info.error into a false positive.
    # Parse the text ourselves and keep the array intact.
    $rawMessages = (Invoke-WebRequest -UseBasicParsing -Uri "$Base/session/$($session.id)/message$q" -Headers $headers).Content
    $parsedMessages = ConvertFrom-Json $rawMessages
    $msgs = if ($null -eq $parsedMessages) { @() } else { @($parsedMessages) }
    $assistant = $msgs | Where-Object { $_.info.role -eq "assistant" } | Select-Object -Last 1
    $status = Invoke-RestMethod -Uri "$Base/session/status$q" -Headers $headers
    $st = $status.PSObject.Properties[$session.id]
    $stType = if ($st) { $st.Value.type } else { "idle" }
    Write-Host ("  messages={0} assistant={1} status={2}" -f $msgs.Count, [bool]$assistant, $stType)
    if ($assistant -and $assistant.info.error) {
        Show-EventErrors
        Fail ("Assistant returned an error: {0} {1}`n  -> run: opencode auth login   (then: opencode auth list)" -f $assistant.info.error.name, ($assistant.info.error.data | ConvertTo-Json -Compress))
    }
    if ($assistant -and $assistant.info.time.completed -and $stType -eq "idle") { break }
}
if (-not $assistant) { Show-EventErrors; Fail "No assistant message was ever created. The prompt was accepted but the model never ran. Check 'opencode auth list', the model name, and the OpenCode server console output." }
if (-not $assistant.info.time.completed) { Fail "Assistant started but did not finish within $TimeoutSeconds s." }

Stop-EventCapture
Step 6 "Result"
$text = ($assistant.parts | Where-Object { $_.type -eq "text" } | ForEach-Object { $_.text }) -join "`n"
Write-Host "Assistant said: $text"
$file = Join-Path $dir "smoke.txt"
if (Test-Path $file) { Write-Host "smoke.txt contents: $(Get-Content $file -Raw)" -ForegroundColor Green; Write-Host "`nPASS: OpenCode answered and edited a file." -ForegroundColor Green }
else { Write-Host "WARN: assistant replied but smoke.txt was not created (check the edit permission in deploy\opencode\server-policy.jsonc)." -ForegroundColor Yellow; exit 2 }