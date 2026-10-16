# Reliability evidence

This repository keeps reliability evidence next to the narrow interfaces it
protects. The tests exercise the existing adapters, graph repositories, action
service, and coordination contract; they do not replace the production storage
boundary with SQLite or a process-local product database.

## Default checks

The normal unit suite does not make network calls:

```bash
go test ./...
go vet ./...
```

The source tests cover bounded item limits, canceled requests, non-success
responses, malformed feeds, oversized bodies, and recovery of the pipeline on a
later source run. The action tests cover approval enforcement, persisted
provider failure, explicit re-approval, successful retry, and completed-action
replay without a second executor call. The auth/config tests cover loopback
defaults, rejection of unauthenticated non-loopback binds, token and Basic
authentication, trusted scope/approver context, and security headers.

## Concurrency evidence

The in-memory harness models the atomic lease/idempotency contract without
pretending to measure FalkorDB performance:

```bash
make concurrency-evidence
```

That command runs the 32-caller exactly-once test 100 times under `-race`.
`internal/coordination/coordination_test.go` also covers failed work release,
expired lease recovery, stale-owner rejection, and replay from the stored
payload.

## Live FalkorDB integration

Integration tests are gated twice: each test requires
`FALKORDB_INTEGRATION=1`, and the Make target exits without running a test when
the variable is absent. The endpoint must be a live FalkorDB Redis-compatible
URL; it is never silently substituted with an in-memory fake.

```bash
FALKORDB_INTEGRATION=1 \
FALKORDB_URL=redis://127.0.0.1:6379 \
make integration-test
```

The suite creates unique control and user graph names, pings the endpoint,
initializes the graph, verifies ingestion and a five-node evidence path,
persists and replays an approved idempotent action, and runs concurrent graph
lease callers. It deletes its temporary graphs during test cleanup.

## Claims not yet measured

The reliability suite is pass/fail correctness evidence. It does not establish
the PRD/ps.md targets for resident memory, CPU, startup time, dashboard latency,
feed-run wall time, or ARM/old-PC behavior. Those remain unmeasured until the
documented workload is executed on the target device and the output is saved
with the environment and command.
