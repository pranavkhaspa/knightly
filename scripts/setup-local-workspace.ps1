[CmdletBinding()]
param(
    # Use this only when this folder came from a source ZIP without .git.
    # It initializes a local repository, but intentionally does not create a
    # synthetic commit or configure a remote. Commit the baseline yourself.
    [switch]$InitializeGit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$GitArgs)
    & git -C $repoRoot @GitArgs
    if ($LASTEXITCODE -ne 0) {
        throw "git $($GitArgs -join ' ') failed with exit code $LASTEXITCODE."
    }
}

$gitRootOutput = & git -C $repoRoot rev-parse --show-toplevel 2>$null
$hasGitRepository = ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace(($gitRootOutput | Out-String).Trim()))

if (-not $hasGitRepository) {
    if (-not $InitializeGit) {
        throw @"
This source ZIP does not contain Git metadata, and NightShift requires a real Git repository to create worktrees.
If you cloned your GitHub repository, run this script from that checkout instead.
If you intentionally want to use this ZIP as a local-only repository, run:
  .\scripts\setup-local-workspace.ps1 -InitializeGit
Then make a baseline commit with git add -A and git commit, and run this script again.
"@
    }

    Invoke-Git init
    Write-Host 'Initialized a local Git repository. No remote or commit was created.' -ForegroundColor Yellow
    Write-Host 'Next run: git add -A; git commit -m "Baseline for NightShift coding workspaces"' -ForegroundColor Yellow
    Write-Host 'Then run this script again.' -ForegroundColor Yellow
    return
}

$repoRoot = (& git -C $repoRoot rev-parse --show-toplevel).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($repoRoot)) {
    throw 'Could not resolve the Git repository root.'
}

$head = & git -C $repoRoot rev-parse --verify HEAD 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($head | Out-String).Trim())) {
    throw 'The repository has no baseline commit. Commit the project files before configuring coding workspaces.'
}

$status = & git -C $repoRoot status --porcelain
if ($LASTEXITCODE -ne 0) {
    throw 'Could not inspect Git working-tree status.'
}
if (-not [string]::IsNullOrWhiteSpace(($status | Out-String).Trim())) {
    throw @"
The repository has uncommitted changes. A Git worktree starts from a commit and will not include these changes.
Commit or stash the changes, then run this script again.
"@
}

# Keep task worktrees beside the source checkout, never inside it.
$workspaceRoot = Join-Path (Split-Path -Parent $repoRoot) 'nightshift-workspaces'
if (([System.IO.Path]::GetFullPath($workspaceRoot)).StartsWith(([System.IO.Path]::GetFullPath($repoRoot) + [System.IO.Path]::DirectorySeparatorChar), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'Workspace root must be outside the source repository.'
}
New-Item -ItemType Directory -Force -Path $workspaceRoot | Out-Null

$allowedRepositoriesJson = ConvertTo-Json -InputObject @($repoRoot) -Compress
$codingRepositoriesJson = ConvertTo-Json -InputObject @{ project_nightshift = $repoRoot } -Compress
$testCommandsJson = ConvertTo-Json -InputObject @{ project_nightshift = @('go', 'test', './...') } -Compress

$env:NIGHTSHIFT_PROJECT_ID = 'project_nightshift'
$env:NIGHTSHIFT_WORKSPACE_ROOT = $workspaceRoot
$env:NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON = $allowedRepositoriesJson
$env:NIGHTSHIFT_CODING_PROJECT_REPOSITORIES_JSON = $codingRepositoriesJson
$env:NIGHTSHIFT_CODING_TEST_COMMANDS_JSON = $testCommandsJson

Write-Host ''
Write-Host 'NightShift local workspace configuration is ready.' -ForegroundColor Green
Write-Host "Repository:      $repoRoot"
Write-Host "Workspace root:  $workspaceRoot"
Write-Host "Project ID:      $env:NIGHTSHIFT_PROJECT_ID"
Write-Host "Test command:    go test ./..."
Write-Host ''
Write-Host 'These environment variables apply to this PowerShell process only.'
Write-Host 'OpenCode is not configured or enabled by this script.' -ForegroundColor Yellow
Write-Host 'Start NightShift from this same terminal after completing the OpenCode setup.'
