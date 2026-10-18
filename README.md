# NightShift

NightShift is an always-on developer intern built for the FalkorDB hackathon.
It connects public technical signals to a user's active project, preserves
decisions across sessions, coordinates bounded agents, and explains every
recommendation through graph evidence.

> **FalkorDB is the product store, not a visualization.**

The current foundation is intentionally small: a native Go daemon, the
official FalkorDB Go client, an embedded status page, a per-user graph
configuration, and an idempotent graph bootstrap. Product state is not copied
into SQLite or another relational database. A later replay spool may buffer
uncommitted network payloads, but it will never answer product queries.

## Why FalkorDB is essential

NightShift will not create a recommendation from keyword matching alone. The
intended decision path is:

```text
User → Project → Requirement → Technology ← Item → Source
```

That path is queried in FalkorDB and stored with the recommendation as its
evidence. The same graph will own evolving memories, decisions, agent tasks,
leases, tool calls, and approved actions. Removing FalkorDB removes the
relationships that make those behaviors possible.

## Current foundation

- Go modular monolith; no mandatory Node.js or Python runtime.
- FalkorDB is the only durable application state store.
- One named control graph plus isolated named user graphs.
- Localhost-only HTTP default; non-loopback binds are rejected unless token or
  username/password authentication is configured.
- Scope and approval identity come from trusted deployment configuration; HTTP
  clients cannot select a tenant, user, or approver with request fields.
- External actions disabled by default and gated by trusted explicit approval.
- `/health/live` stays available while FalkorDB is unavailable; `/health/ready`
  returns a diagnostic `503` until graph connectivity is healthy.
- Explicit initialization state starts the scheduler only after graph setup is
  complete; `/api/v1/status` reports initialization, scheduler, and collection
  state.
- A framework-free dashboard shows the active graph path and persisted task
  status when FalkorDB is available.
- Deterministic mode is the baseline; an LLM is optional and not yet wired.

The next vertical slice is public RSS/GitHub fixture ingestion → graph
relationships → multi-hop recommendation → evidence path → feedback and
evolving memory → graph-backed Relevance/Planner handoff.

## Requirements

- Go 1.27
- FalkorDB 1.0+ reachable through its Redis-compatible endpoint
- Docker is optional and only used for local FalkorDB development

The runtime does not require Docker, Node.js, Python, SQLite, Redis, or a local
LLM. FalkorDB itself exposes the Redis-compatible protocol; it is not being
used as a generic Redis application database.

## Local Dev2 workspace setup (Windows)

Before enabling OpenCode, configure the local checkout as a trusted repository and place task worktrees outside it. See [`docs/local-workspace-setup.md`](docs/local-workspace-setup.md) and run `scripts/setup-local-workspace.ps1` from PowerShell. A source ZIP does not contain `.git`; use a real clone or initialize and commit a local baseline first. This step configures workspace paths only and does not start or configure OpenCode.

## Quick start

```bash
cp .env.example .env
set -a; source .env; set +a
docker compose -f deploy/docker/compose.yaml up -d falkordb
go mod download
go run ./cmd/nightshift
```

The checked-in Compose service binds its published ports to host loopback. It
uses a development-only token unless `NIGHTSHIFT_AUTH_TOKEN` is set in `.env`;
replace that value before exposing the service beyond a local workstation.

In another terminal:

```bash
curl -fsS http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
curl -fsS http://127.0.0.1:8080/api/v1/status
curl -fsS http://127.0.0.1:8080/api/v1/dashboard
curl -fsS http://127.0.0.1:8080/api/v1/sources
```

Open <http://127.0.0.1:8080/> to see the embedded status page. The first
successful startup creates the `nightshift_control` graph and the configured
demo user graph with the seed relationships from
`migrations/falkordb/001_core.cypher`.

To run only FalkorDB and use a host-built binary:

```bash
make graph-up
make build
./bin/nightshift
```

## OpenCode coding tasks (Dev2)

NightShift can optionally delegate a scoped task to OpenCode. Configure the OpenCode server URL, a workspace root outside the source repositories, an explicit repository allowlist, a trusted server-policy acknowledgement, a project-to-local-repository mapping, and per-project test commands. See [`docs/opencode.md`](docs/opencode.md) before enabling this feature.

With coding configured, a user can type a request such as `Fix the authentication bug` in a NightShift conversation. NightShift stores the request and bounded project/graph context as a durable FalkorDB task. The serial Dev2 queue worker claims the task, creates an isolated Git worktree, starts or resumes OpenCode, streams normalized progress events, and returns a bounded diff plus test results. NightShift persists the artifacts and final status, posts the outcome to chat, and leaves failed tests blocked for review. `review diff` displays the latest coding task's changed files and test output. The direct `POST /api/v1/tasks/{id}/coding` endpoint remains available for an existing task. Read artifacts with `GET /api/v1/tasks/{id}/artifacts/{artifact_id}`. Commits, pushes, pull requests, and deployment are not performed by this workflow.

## Configuration

Copy `.env.example` to `.env` and export it with your preferred environment
loader. Important settings:

| Variable | Purpose | Safe default |
|---|---|---|
| `NIGHTSHIFT_LISTEN` | HTTP bind address | `127.0.0.1:8080` |
| `NIGHTSHIFT_INSTANCE_ID` | stable owner/status identity; falls back to `HOSTNAME` | `local-dev` on a host |
| `NIGHTSHIFT_TENANT_ID` | trusted single-user installation tenant scope | `tenant_demo` |
| `NIGHTSHIFT_USER_ID` | trusted single-user installation user scope | `user_demo` |
| `NIGHTSHIFT_PROJECT_ID` | preferred project for collection recommendations | `project_nightshift` |
| `FALKORDB_URL` | `redis://`, `rediss://`, `falkor://`, or `falkors://` URL | local FalkorDB |
| `FALKORDB_CONTROL_GRAPH` | deployment/control metadata graph | `nightshift_control` |
| `FALKORDB_GRAPH` | user-owned graph for this installation | `nightshift_u_demo` |
| `FALKORDB_QUERY_TIMEOUT_MS` | bounded Cypher query timeout | `5000` |
| `NIGHTSHIFT_DATA_DIR` | local approved-plan artifact directory | `./data` |
| `NIGHTSHIFT_ACTIONS_ENABLED` | approval-gated local plan execution | `false` |
| `NIGHTSHIFT_AUTH_TOKEN` | bearer credential for a non-loopback bind | unset |
| `NIGHTSHIFT_AUTH_USERNAME` / `NIGHTSHIFT_AUTH_PASSWORD` | Basic credential alternative to the token | unset |
| `NIGHTSHIFT_AUTH_TENANT_ID` / `NIGHTSHIFT_AUTH_USER_ID` | trusted graph scope attached by auth middleware | `tenant_demo` / `user_demo` |
| `NIGHTSHIFT_AUTH_APPROVER_ID` | trusted actor recorded for approvals | `user_demo` |
| `NIGHTSHIFT_OIDC_ISSUER` / `NIGHTSHIFT_OIDC_AUDIENCE` / `NIGHTSHIFT_OIDC_JWKS_URL` | verified OIDC bearer identity (optional) | unset |
| `NIGHTSHIFT_OIDC_TENANT_CLAIM` | token claim mapped to the tenant scope | `tenant` |
| `NIGHTSHIFT_TRUSTED_PROXY_SECRET` | HMAC secret for signature-verified proxy identity (optional) | unset |
| `NIGHTSHIFT_LOG_FORMAT` | structured log encoding (`json` or `text`) | `json` |
| `NIGHTSHIFT_LOG_LEVEL` | `debug`, `info`, `warn`, or `error` | `info` |
| `NIGHTSHIFT_OTLP_ENDPOINT` | optional non-blocking OTLP/HTTP metrics export endpoint | unset |
| `NIGHTSHIFT_METRICS_PUBLIC` | expose `GET /metrics` without credentials (trusted scrape network only) | `false` |

Do not put credentials in graph names, URLs returned by the API, logs, source
fixtures, screenshots, or committed files. The only local artifact written by
the current action executor is an explicitly approved Markdown plan; its
action state and idempotency record remain in FalkorDB.

For a non-loopback bind, set either `NIGHTSHIFT_AUTH_TOKEN` or both Basic
credential variables. Use a high-entropy secret from a secret manager in
production. The daemon wires the auth middleware around the route handler; it
attaches the configured scope and approver to request context and ignores
client-supplied scope or `approved_by` values. See
[`docs/deployment.md`](docs/deployment.md) for the integration contract.

## Demo flow

With FalkorDB running and the daemon started, trigger the deterministic public
fixture flow:

```bash
curl -fsS -X POST http://127.0.0.1:8080/api/v1/runs
curl -fsS http://127.0.0.1:8080/api/v1/recommendations
```

Take the returned recommendation ID and inspect the graph evidence, record
feedback, and create a persisted planner proposal:

```bash
curl -fsS http://127.0.0.1:8080/api/v1/graph/path/<recommendation-id>
curl -fsS -X POST http://127.0.0.1:8080/api/v1/recommendations/<recommendation-id>/feedback \
  -H 'Content-Type: application/json' -d '{"kind":"useful","note":"Connected to the active project."}'
curl -fsS -X POST http://127.0.0.1:8080/api/v1/recommendations/<recommendation-id>/plan \
  -H 'Content-Type: application/json' -d '{"goal":"Prepare the smallest implementation task."}'
```

The plan action is `proposed` until the user explicitly approves it. To enable
the local Markdown executor for a controlled demo, set
`NIGHTSHIFT_ACTIONS_ENABLED=true`, then call the approval and execute routes.
GitHub writes are not exposed by the current MVP.

## Checks

Run these from the repository root after installing Go:

```bash
gofmt -l .
go test ./...
go vet ./...
go build -trimpath -o bin/nightshift ./cmd/nightshift
```

The health checks and repeatable concurrency evidence are separate commands:

```bash
make health-check
make concurrency-evidence
```

`health-check` intentionally fails on `/health/ready` while initialization or
the graph is unavailable; that is the signal a deployment supervisor should use
for dependency-aware readiness. The FalkorDB integration suite is also separate from the unit suite
because it needs a live database, and it is disabled unless explicitly opted
into:

```bash
make graph-up
FALKORDB_INTEGRATION=1 make integration-test
```

The integration suite uses unique temporary graph names, verifies graph-backed
ingestion/evidence, action idempotency, and concurrent lease completion, then
deletes those graphs. Set `FALKORDB_URL` when FalkorDB is remote.

## Evidence status

Measured by automated checks: bounded source failures, failure recovery,
approval-gated action retries, graph evidence persistence, exactly-once lease
behavior, and the auth/configuration boundaries covered by the unit tests.
`make concurrency-evidence` repeats the 32-caller in-memory coordination
harness 100 times with the race detector. The opt-in integration command is the
evidence path for a live FalkorDB rather than a mock.

No performance number is claimed here for memory, CPU, startup time, dashboard
latency, or a particular ARM/old-PC class. Those PRD targets remain unmeasured
until the repeatable workload is run on the target hardware and its output is
recorded. The daemon runs a bounded scheduler around the durable source
collector. `POST /api/v1/runs` queues an explicit manual collection, scheduled
collection also persists graph-backed recommendations and Relevance-to-Planner
handoffs, and `/api/v1/scheduler/{pause,resume,status}` controls and reports the
loop.

## Portable deployment

### Old PC, Raspberry Pi, or UNO Q

Build the same daemon for Linux ARM64 or x86_64 and point it at FalkorDB Cloud
or a private FalkorDB host:

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o dist/nightshift-linux-arm64 ./cmd/nightshift
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o dist/nightshift-linux-amd64 ./cmd/nightshift
```

The current edge process uses bounded HTTP and graph requests and no local
model. FalkorDB can remain
remote when the device does not have enough memory for the graph server.

### VPS

Run the binary and FalkorDB on a private network, or run the binary with a
remote FalkorDB endpoint. The systemd unit in
`deploy/systemd/nightshift.service` creates a `0700` state directory, uses an
unprivileged service account, and expects a protected environment file. Keep
the daemon on loopback and put HTTPS authentication/TLS in a reverse proxy.

### Render

`deploy/render.yaml` deploys the web daemon as a stateless service. Configure
`FALKORDB_URL` and `NIGHTSHIFT_AUTH_TOKEN` as Render secrets pointing to a
reachable FalkorDB endpoint and a high-entropy HTTP credential. Do not rely on
the Render filesystem for durable product state. Multiple instances must share
the same FalkorDB deployment; scheduler leadership and task leases use the
graph-backed coordination contract.

The full install, systemd environment file, multiple-instance guidance, and
portable binary matrix are in `docs/deployment.md`. For local Windows development, use `scripts/setup-local-workspace.ps1`, configure `.env` using `docs/opencode.md`, start OpenCode in one PowerShell window, and run `scripts/start-nightshift.ps1` in another. The launcher imports non-empty `.env` values while preserving workspace values already set in the current process.

## Hackathon positioning

Primary track: **Agent Memory and Coordination**. Secondary fit: **Agents
That Act on Connected Data**. The demo target is:

1. Ingest public or synthetic technical signals.
2. Show the FalkorDB path connecting a signal to a project requirement.
3. Let the user accept, reject, or defer the recommendation.
4. Persist the decision and evolving memory in the user graph.
5. Hand a graph-backed task from Relevance to Planner.
6. Require approval before creating a local plan or GitHub sandbox issue.
7. Restart the service and retrieve the same decision from FalkorDB.

See `docs/hackathon.md`, `docs/architecture.md`, and `docs/graph-schema.md`.

## AI coding disclosure

This repository is being developed with an AI coding assistant. Team members
must review the code, understand the graph model and Cypher, and disclose the
assistant usage in the final submission as required by the hackathon rules.
