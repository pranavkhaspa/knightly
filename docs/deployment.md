# Portable deployment

NightShift is a single native Go daemon. It serves the embedded dashboard and
uses FalkorDB for durable project, graph-path, task, lease, and idempotency
state. No SQLite file is created by the native deployment.

## Build portable binaries

Build on a current Linux or CI machine, then copy only the resulting binary
and a protected environment file to the target. `CGO_ENABLED=0` keeps the
binary independent of the target's glibc version.

```bash
mkdir -p dist
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags='-s -w' -o dist/nightshift-linux-amd64 ./cmd/nightshift
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -ldflags='-s -w' -o dist/nightshift-linux-arm64 ./cmd/nightshift
```

The ARM64 binary is suitable for modern Raspberry Pi and UNO Q Linux images;
the AMD64 binary is suitable for old x86 PCs and most VPS hosts. Build with a
new Go toolchain and verify the target's Linux kernel before copying the
binary. The target does not need Go, Node.js, Python, Docker, or SQLite.

For a quick local graph, Docker is still optional:

```bash
make graph-up
./dist/nightshift-linux-amd64
curl -fsS http://127.0.0.1:8080/health/ready
```

The two health endpoints have different meanings:

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/health/live >/dev/null
curl --fail --silent --show-error http://127.0.0.1:8080/health/ready
```

`/health/live` is process liveness and does not contact FalkorDB. `/health/ready`
performs a bounded graph probe and returns `503` with the graph error when the
dependency is unavailable. Use liveness for restart supervision and readiness
for traffic routing or deployment verification.

## Old Linux PC or small ARM device

1. Run the native binary as an unprivileged user.
2. Point `FALKORDB_URL` at a remote FalkorDB Cloud/private endpoint when the
   device does not have enough memory for FalkorDB itself.
3. Use one or two workers in the future scheduler; the current dashboard and
   graph queries already have bounded request and query timeouts.
4. Keep `NIGHTSHIFT_LISTEN=127.0.0.1:8080` unless the device is on a trusted
   network. Use an SSH tunnel or a TLS reverse proxy for remote access.

The binary has no local product database. A restart reconstructs the dashboard
from FalkorDB, including the active project path and persisted tasks.

## VPS with systemd

The checked-in unit is a hardened starting point and expects an optional
environment file at `/etc/nightshift/nightshift.env`:

```bash
sudo useradd --system --home /var/lib/nightshift --shell /usr/sbin/nologin nightshift
sudo install -d -o nightshift -g nightshift /var/lib/nightshift /etc/nightshift
sudo install -m 0755 dist/nightshift-linux-amd64 /usr/local/bin/nightshift
sudo install -m 0640 -o root -g nightshift .env.example /etc/nightshift/nightshift.env
sudoedit /etc/nightshift/nightshift.env
sudo install -m 0644 deploy/systemd/nightshift.service /etc/systemd/system/nightshift.service
sudo systemctl daemon-reload
sudo systemctl enable --now nightshift
systemctl status nightshift
curl -fsS http://127.0.0.1:8080/health/ready
```

Change the environment file to use the real `FALKORDB_URL`, set a stable
unique `NIGHTSHIFT_INSTANCE_ID` such as `vps-worker-01`, and keep credentials
out of unit files and graph names. Put HTTPS authentication and TLS in the
VPS reverse proxy; the daemon itself remains a small HTTP service.

## Render

`deploy/render.yaml` uses the native Docker image, exposes `/health/live`, and
does not use the Render filesystem for state. In the Render dashboard:

1. Create the Blueprint from this repository.
2. Set the secret `FALKORDB_URL` to a reachable `redis://` or `rediss://`
   FalkorDB endpoint.
3. Keep `FALKORDB_CONTROL_GRAPH` and `FALKORDB_GRAPH` stable across deploys.
4. Confirm `/health/ready` from the service logs after the first deploy.

Start with `numInstances: 1`. The live/read-only dashboard can be scaled after
the graph endpoint is shared; each container falls back to its container
hostname for `NIGHTSHIFT_INSTANCE_ID`. If a worker is added, give each worker
a unique explicit instance ID and use the graph-backed lease/idempotency
contract rather than process-local locks.

Do not use an ephemeral Render disk for product state. A new instance must be
able to answer the same dashboard and task-status request from FalkorDB alone.

## Multiple instances and graph coordination

Every instance must use the same:

```text
FALKORDB_URL
FALKORDB_CONTROL_GRAPH
FALKORDB_GRAPH
```

The control graph owns leases and idempotency records. A worker claims a
stable operation key, performs bounded work, and stores its result only while
its lease token is valid. A stale worker cannot publish after another worker
recovers the lease. The contract and concurrency harness live in
`internal/coordination`; the FalkorDB adapter is in
`internal/falkor/coordination.go`.

For Docker Compose, the default service publishes port 8080 for a convenient
single-instance demo. If scaling the daemon on one host, remove the fixed host
port mapping and place an HTTP proxy in front of the replicas, or run each
replica with a different host port. Never run separate replicas against
different graphs and expect shared task state.

## Operations checklist

```bash
curl -fsS http://127.0.0.1:8080/health/live
curl -i http://127.0.0.1:8080/health/ready
curl -fsS http://127.0.0.1:8080/api/v1/status
curl -fsS http://127.0.0.1:8080/api/v1/dashboard
```

`/health/live` does not require FalkorDB. `/health/ready` and the dashboard
report graph connectivity separately, which makes remote-graph outages
visible without causing the process supervisor to restart a healthy daemon.

## Evidence and measurement boundary

The repository's correctness evidence is repeatable:

```bash
make concurrency-evidence
FALKORDB_INTEGRATION=1 FALKORDB_URL=redis://127.0.0.1:6379 make integration-test
```

The first command exercises 32 concurrent callers for the same coordination
key 100 times under the race detector. The second command requires a live
FalkorDB and verifies graph persistence, evidence paths, action idempotency,
and graph-backed concurrent completion. The integration command is intentionally
opt-in and never runs against a mock while making a live-graph claim.

No memory, CPU, startup, dashboard-latency, or target-board support figure is
reported as measured by this repository. The resource values in `PRD.md` and
`ps.md` are targets until a documented workload is run on the claimed hardware
and the results are captured.
