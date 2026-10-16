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
- Localhost-only HTTP default.
- External actions disabled by configuration and rejected by validation.
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

- Go 1.24 or newer
- FalkorDB 1.0+ reachable through its Redis-compatible endpoint
- Docker is optional and only used for local FalkorDB development

The runtime does not require Docker, Node.js, Python, SQLite, Redis, or a local
LLM. FalkorDB itself exposes the Redis-compatible protocol; it is not being
used as a generic Redis application database.

## Quick start

```bash
cp .env.example .env
set -a; source .env; set +a
docker compose -f deploy/docker/compose.yaml up -d falkordb
go mod download
go run ./cmd/nightshift
```

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

Do not put credentials in graph names, URLs returned by the API, logs, source
fixtures, screenshots, or committed files. The only local artifact written by
the current action executor is an explicitly approved Markdown plan; its
action state and idempotency record remain in FalkorDB.

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

## Evidence boundary

The automated checks provide pass/fail evidence for bounded source failures,
failure recovery, approval-gated action retries, graph evidence persistence,
and exactly-once lease behavior. `make concurrency-evidence` repeats the
32-caller in-memory coordination harness 100 times with the race detector.
The opt-in integration command is the evidence path for a live FalkorDB rather
than a mock.

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
`deploy/systemd/nightshift.service` is a starting point; add a protected
environment file and an unprivileged service account before production use.

### Render

`deploy/render.yaml` deploys the web daemon as a stateless service. Configure
`FALKORDB_URL` as a Render secret pointing to FalkorDB Cloud or a private
FalkorDB endpoint. Do not rely on the Render filesystem for durable product
state. Multiple instances must share the same FalkorDB deployment; scheduler
leadership and task leases use the graph-backed coordination contract.

The full install, systemd environment file, multiple-instance guidance, and
portable binary matrix are in `docs/deployment.md`.

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
