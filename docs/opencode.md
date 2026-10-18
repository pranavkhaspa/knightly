# Dev2: OpenCode coding tasks

NightShift's coding integration is optional. Leave `NIGHTSHIFT_OPENCODE_URL` empty to keep coding execution disabled.

## OpenCode API used

The adapter uses OpenCode's stable HTTP API, scoped to the task worktree with the `directory` query parameter: `POST /session`, `POST /session/:id/prompt_async`, `GET /session/:id/message`, `GET /session/status`, `POST /session/:id/abort`, and `GET /global/health`. NightShift polls the session until the assistant finishes. It fails the task with a readable error when OpenCode reports a provider error (for example `ProviderAuthError`), never creates an assistant message (`session.no_response`), or stops making progress. The code diff is read from `git` inside the worktree (OpenCode's own diff endpoint is only a fallback). Inspect `http://127.0.0.1:4096/doc` to confirm these endpoints on your installed version.

## 0. Prove OpenCode works on its own (do this first)

With the server running (section 2), run `.\scripts\smoke-opencode.ps1`. It creates a throwaway repo, sends a prompt directly to OpenCode, and prints either the assistant's reply and the edited file (PASS) or the exact failure. If this fails, fix OpenCode (login, model, config) before debugging NightShift.

## 1. Install and authenticate OpenCode on Windows

Install the OpenCode CLI using the official instructions: <https://opencode.ai/docs/>. Open a new PowerShell window and verify:

```powershell
opencode --version
opencode auth login
```

In the interactive login flow, choose OpenCode Zen and complete the browser/login steps. NightShift does not perform this login for you; OpenCode stores and uses the provider credentials in its own user profile.

The configured model is `opencode/big-pickle`. Big Pickle is currently listed as free for a limited time, but availability can change. OpenCode's model page notes that data submitted to Big Pickle during its free period may be used to improve the model; do not submit secrets, production credentials, or sensitive private code until you have reviewed the current terms. See <https://opencode.ai/docs/zen/>.

## 2. Start the local OpenCode server with the restrictive policy

From the extracted project root, run:

```powershell
.\scripts\start-opencode.ps1
```

This sets `OPENCODE_CONFIG` to `deploy/opencode/server-policy.jsonc` and starts OpenCode on `127.0.0.1:4096`. The policy (OpenCode `permission` config) denies bash, web fetch and external-directory access and explicitly allows read/search/edit tools; everything the agent needs is `allow` because a headless server cannot answer an `ask` prompt. NightShift runs configured tests separately, using a trusted argv array instead of a model-generated shell command.

The server is intentionally bound to loopback. Do not expose it on `0.0.0.0` or the public internet. For a local-only server, leave server authentication disabled; loopback binding is the access boundary. If you set `OPENCODE_SERVER_PASSWORD` before starting OpenCode, it uses HTTP Basic auth (default username `opencode`). Set the matching `NIGHTSHIFT_OPENCODE_SERVER_PASSWORD` in NightShift's protected `.env`; optionally set `NIGHTSHIFT_OPENCODE_SERVER_USERNAME` if you changed the username. Do not commit either password.

In another PowerShell window, verify server health:

```powershell
.\scripts\check-opencode.ps1
```

The script checks `/global/health`. You can also inspect the OpenAPI document at `http://127.0.0.1:4096/doc`.

## 3. Configure NightShift

Copy `.env.example` to `.env` and configure these values. Use the actual absolute path of your local NightShift checkout (the directory containing `.git` and `go.mod`):

```dotenv
NIGHTSHIFT_OPENCODE_URL=http://127.0.0.1:4096
NIGHTSHIFT_OPENCODE_SERVER_USERNAME=
NIGHTSHIFT_OPENCODE_SERVER_PASSWORD=
NIGHTSHIFT_OPENCODE_MODEL=opencode/big-pickle
NIGHTSHIFT_OPENCODE_TOKEN=
NIGHTSHIFT_OPENCODE_POLICY_CONFIRMED=true
NIGHTSHIFT_WORKSPACE_ROOT=../nightshift-workspaces
NIGHTSHIFT_ALLOWED_REPOSITORIES_JSON=["D:/Projects/knightly"]
NIGHTSHIFT_CODING_PROJECT_REPOSITORIES_JSON={"project_nightshift":"D:/Projects/knightly"}
NIGHTSHIFT_CODING_TEST_COMMANDS_JSON={"project_nightshift":["go","test","./..."]}
NIGHTSHIFT_CODING_RUN_TIMEOUT_SECONDS=7200
```

Replace `D:/Projects/knightly` with your real checkout path. JSON values must be valid JSON; don't include comments inside them. The repository path must be allowlisted and match the project mapping. The workspace root must be outside the original checkout. The default model is also recorded in the root `opencode.json`; the adapter passes the model explicitly when creating a session.

`NIGHTSHIFT_OPENCODE_SERVER_USERNAME` and `NIGHTSHIFT_OPENCODE_SERVER_PASSWORD` are for OpenCode's native HTTP Basic auth. Leave both blank when using the local server with no password. `NIGHTSHIFT_OPENCODE_TOKEN` is only for a trusted reverse proxy that explicitly expects Bearer auth; the stock OpenCode server does not use Bearer auth.

Only set `NIGHTSHIFT_OPENCODE_POLICY_CONFIRMED=true` after starting OpenCode with the supplied policy file and verifying that the running server is the trusted local instance. The application cannot independently prove the effective policy via its API.

## 4. Run NightShift

Start the configured FalkorDB service as described in `docs/deployment.md`. In a separate PowerShell window, run `scripts/start-nightshift.ps1`; it imports non-empty values from `.env` while preserving values already set by `setup-local-workspace.ps1`. The coding integration stays disabled until `NIGHTSHIFT_OPENCODE_URL` is non-empty and the repository, workspace, test command, and policy acknowledgement validate.

The flow is: chat command → NightShift persists a scoped coding task → queue worker claims it → Git worktree is created → OpenCode edits that worktree → NightShift collects the provider event stream and Git diff → the configured test runner executes tests → task/run/artifact state is persisted and a result event is posted to the originating conversation.

## Security boundary

A Git worktree separates edits from the original checkout; it is not a sandbox. OpenCode's shell and external-directory tools are hard-denied by the supplied v2 policy. The test runner executes only deployment-configured argument vectors and never executes a model-generated command. Review the diff before accepting changes; this integration does not automatically commit, push, open a PR, or deploy.

The server policy file is supplied separately from project code because a task can modify files in its own worktree. Keep the server policy in a trusted path and start OpenCode with that config. Do not treat a policy file inside an untrusted repository as a security boundary.

## Recovery and artifacts

Provider sessions, task metadata, workspace metadata, run records, and bounded diff/test artifacts are stored in FalkorDB. Worktrees are retained for review. Failed tests are recorded as blocked/review-required rather than successful completion. Publishing remains a separate approval-controlled action.

## Troubleshooting: "accepted the prompt but produced no assistant response"

OpenCode reports failures that happen *before* an assistant message exists (unknown model, missing credentials, invalid config) only on its `/event` stream, never in `/session/:id/message`. NightShift now listens on `/event` for the session (started before the prompt is sent) and fails the task with OpenCode's own error text (for example `ProviderAuthError` or `ProviderModelNotFoundError`) instead of waiting for the no-response timeout. `.\scripts\smoke-opencode.ps1` does the same capture and also checks that the provider/model is offered by the server, so run it first.

Typical causes, in order of likelihood:

1. Not logged in: run `opencode auth login`, choose OpenCode Zen, then **restart** `scripts\start-opencode.ps1` (the server reads credentials at start).
2. Model not offered: confirm `opencode/big-pickle` appears in the smoke script's model check; otherwise set `NIGHTSHIFT_OPENCODE_MODEL` to a model your login can use and update `opencode.json`.
3. Invalid server policy: the file is passed through `OPENCODE_CONFIG`; any unknown key is rejected by OpenCode and the session never starts. The supplied `deploy/opencode/server-policy.jsonc` uses only the documented `permission` keys.
4. Stale server: stop any old `opencode` process on port 4096 before starting a new one.
