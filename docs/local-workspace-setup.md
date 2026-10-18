# Local coding workspace setup (Windows / PowerShell)

This configures the current NightShift checkout as the trusted coding repository for `project_nightshift`. It does **not** configure or start OpenCode.

## Important: the source ZIP has no Git metadata

NightShift uses `git worktree add` to create each task's isolated workspace. A source ZIP does not include the `.git` directory, so it cannot be used as a worktree source until you either clone the actual repository or initialize a local Git repository and commit a baseline.

Preferred option, if you have repository access:

```powershell
git clone https://github.com/pranavkhaspa/knightly.git
cd knightly
```

If you're intentionally using the combined Dev1 + Dev2 ZIP as a local-only codebase, extract it and run from its root:

```powershell
.\scripts\setup-local-workspace.ps1 -InitializeGit
# Review the files, then create the baseline commit:
git add -A
git commit -m "Baseline for NightShift coding workspaces"
.\scripts\setup-local-workspace.ps1
```

The script intentionally does not create a synthetic commit or add a remote. Configure your Git identity first if Git asks for it. To connect a local-only repository to GitHub later, add the correct remote yourself.

## What the script configures

- `NIGHTSHIFT_PROJECT_ID=project_nightshift`
- `NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON`: only the resolved checkout root
- `NIGHTSHIFT_CODING_PROJECT_REPOSITORIES_JSON`: maps `project_nightshift` to that checkout
- `NIGHTSHIFT_CODING_TEST_COMMANDS_JSON`: runs `go test ./...` as an argv vector, not through a shell
- `NIGHTSHIFT_WORKSPACE_ROOT`: sibling folder named `nightshift-workspaces`, outside the repository

The script refuses to proceed if the repository has no baseline commit or has uncommitted changes. This is deliberate: a Git worktree starts from a commit and will not include uncommitted edits from the source checkout.

## Run NightShift

Run the script in the same PowerShell terminal that you will use to start NightShift, so the configured environment variables are inherited by the process. Start FalkorDB as described in the main README, then start NightShift. The coding worker is only wired when `NIGHTSHIFT_OPENCODE_URL` is configured; this setup script deliberately leaves OpenCode untouched.

On Windows, the configured test command requires Go to be installed and available as `go` on `PATH`. The project `go.mod` currently requires Go 1.27 or newer.
