# Portable deployment

NightShift is a single native Go daemon. It serves the embedded dashboard and
uses FalkorDB for durable project, graph-path, task, lease, and idempotency
state. No SQLite file is created by the native deployment.

## Deployment targets

- **systemd on a Linux host is the primary production deployment.** The checked-in
  unit in `deploy/systemd/nightshift.service` runs the native binary as an
  unprivileged user with a hardened sandbox. Use it for VPS, old-PC, and
  small-ARM targets.
- **Docker Compose is for local development only.** `deploy/docker/compose.yaml`
  runs a local FalkorDB and an optional private SearXNG sidecar so a developer
  gets a working graph and no-card web search without a cloud account. It is not
  the production topology.

## Pinned dependency versions

External images and the OpenCode toolchain are pinned to exact versions. Update
a pin only as a reviewed migration, then re-run the related package tests.

| Dependency | Pinned version | Pinned in |
|---|---|---|
| FalkorDB | `falkordb/falkordb:v4.22.1-alpine` | `deploy/docker/compose.yaml` |
| SearXNG | `searxng/searxng:2026.10.11-ad46ebd7d` | `deploy/docker/compose.yaml` |
| OpenCode | `opencode-ai@1.19.3` (`npm install -g opencode-ai@1.19.3`) | Dev2 adapter and operator toolchain |

Do not use floating `latest` tags for FalkorDB or SearXNG, and do not track the
OpenCode `latest` npm dist-tag. The Dev2 OpenCode adapter must target the pinned
server version's HTTP API and permissions model, and must pass its fake-server
conformance tests before it is enabled against a real server.

## Security boundary

The default `NIGHTSHIFT_LISTEN=127.0.0.1:8080` is deliberately local-only.
`internal/config` rejects `:8080`, `0.0.0.0:8080`, `[::]:8080`, and other
non-loopback binds unless `NIGHTSHIFT_AUTH_TOKEN` or a complete
`NIGHTSHIFT_AUTH_USERNAME`/`NIGHTSHIFT_AUTH_PASSWORD` pair is set. Credentials
are compared without logging them. Use a high-entropy secret and transport the
secret through the deployment secret store rather than a checked-in file.

The daemon composes the auth package with the application routes during
startup. The wiring is equivalent to:

```go
policy, err := auth.New(auth.Config{
    Token:                 cfg.AuthToken,
    Username:              cfg.AuthUsername,
    Password:              cfg.AuthPassword,
    Scope:                 identity.Scope{TenantID: cfg.AuthTenantID, UserID: cfg.AuthUserID},
    ApproverID:            cfg.AuthApproverID,
    RequireAuthentication: cfg.AuthRequired,
    PublicPaths:           []string{"/health/live"},
    Resolver:              resolver, // OIDC or trusted-proxy identity, optional
})
if err != nil { /* fail startup */ }
server.Handler = policy.Wrap(app.routes())
```

Only expose `/health/live` without credentials when the platform requires an
unauthenticated liveness probe. Keep `/health/ready` and application routes
protected when the daemon is reachable from a network. In an approval handler,
remove `approved_by` from the decoded client request and use
`auth.ApproverFromRequest(request)`. Use `auth.ScopeFromRequest(request)` for
all user-scoped repository calls. These helpers fail when the middleware was
not installed and never read client headers, query parameters, or JSON as an
authority source.

The middleware adds `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`,
`X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, and a restrictive
permissions policy. TLS termination, rate limiting, and external identity
providers remain the responsibility of the reverse proxy or platform.

## Verified per-request identity (multi-user)

The deployment-wide `NIGHTSHIFT_AUTH_TENANT_ID`/`NIGHTSHIFT_AUTH_USER_ID`
identity is only for loopback-only single-user operation. Before claiming
general multi-user support, set exactly one verified identity source so the
tenant, user, and approver are taken from verified evidence on every request:

- **OIDC** (`NIGHTSHIFT_OIDC_ISSUER`, `NIGHTSHIFT_OIDC_AUDIENCE`,
  `NIGHTSHIFT_OIDC_JWKS_URL`, optional `NIGHTSHIFT_OIDC_TENANT_CLAIM`). The
  daemon verifies the RS256/ES256 bearer token against the provider JWKS,
  checks `iss`, `aud`, `exp`, and `nbf`, and maps the tenant/user claims onto
  the scope. The approver defaults to the token subject.
- **Trusted proxy** (`NIGHTSHIFT_TRUSTED_PROXY_SECRET`). An authenticated
  reverse proxy (for example oauth2-proxy) sets the tenant/user/approver
  headers and an HMAC-SHA256 signature over `tenant\nuser\napprover\ntimestamp`.
  Requests with a missing, stale, or unverifiable signature are rejected. The
  signature is the credential: a client cannot forge scope or approver without
  the shared secret.

When a resolver is configured it is the authentication authority, so no static
`NIGHTSHIFT_AUTH_TOKEN` is required. `auth.ScopeFromRequest` and
`auth.ApproverFromRequest` return the verified identity, handlers route every
user-scoped repository call through it, and client-supplied `tenant_id`,
`user_id`, or `approved_by` fields are ignored. Each verified scope resolves to
its own control-graph-selected user graph, so provider accounts and graph
queries are isolated per verified tenant/user.

One OpenCode server is **not** adversarial multi-tenant isolation. Run one
OpenCode process and one bounded task workspace per trusted user/task, and never
share a workspace across tenants. The OpenCode integration (Dev2) must enforce
this boundary before it is exposed to multiple users.

## Build portable binaries

Build with Go 1.27 on a current Linux or CI machine, then copy only the resulting binary
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

`make verify-arch` (or `make build-linux`) compiles both AMD64 and ARM64
targets to prove the portable build. Run it before tagging a release; a
successful cross-compile is the repository's supported-architecture evidence.

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

This is the primary production deployment: the native binary supervised by
systemd on a Linux host. The checked-in unit is a hardened starting point and
expects an optional environment file at `/etc/nightshift/nightshift.env`:

```bash
sudo useradd --system --home /var/lib/nightshift --shell /usr/sbin/nologin nightshift
sudo install -d -o nightshift -g nightshift -m 0700 /var/lib/nightshift
sudo install -d -o root -g nightshift -m 0750 /etc/nightshift
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
out of unit files and graph names. Keep `NIGHTSHIFT_LISTEN=127.0.0.1:8080` and
put HTTPS authentication and TLS in the VPS reverse proxy; the daemon itself
remains a small HTTP service. The unit applies `UMask=0077`, creates the state
directory with mode `0700`, and allows writes only below that directory.

## Render

`deploy/render.yaml` uses the native Docker image, exposes `/health/live`, and
does not use the Render filesystem for durable state. The Compose FalkorDB
image is pinned to `falkordb/falkordb:v4.22.1-alpine`; update that tag as a
reviewed migration, rather than using `latest`. In the Render dashboard:

1. Create the Blueprint from this repository.
2. Set the secrets `FALKORDB_URL` and `NIGHTSHIFT_AUTH_TOKEN` to reachable
   values. The Render service binds `0.0.0.0:8080`, so startup fails until the
   token is configured.
3. Keep `FALKORDB_CONTROL_GRAPH` and `FALKORDB_GRAPH` stable across deploys.
4. Confirm `/health/ready` with the bearer token after the first deploy.

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

### Local Compose and SearXNG

Docker Compose is a local development aid, not a production topology.
`deploy/docker/compose.yaml` also starts an optional, private SearXNG sidecar
for no-card web search. It is pinned to `searxng/searxng:2026.10.11-ad46ebd7d`;
update the tag deliberately and re-run the `internal/search` tests, following
the same reviewed-migration discipline used for the FalkorDB image.

The sidecar mounts `deploy/docker/searxng/settings.yml`, which enables the
documented `/search` JSON output (`search.formats: [html, json]`) that
NightShift consumes and binds the instance to loopback only. NightShift reaches
it through `NIGHTSHIFT_SEARCH_URL: http://searxng:8080` inside the Compose
network. Plain-HTTP search URLs are accepted only when `NIGHTSHIFT_ENV` is
`development`; any remote SearXNG instance must use HTTPS.

Search result text is untrusted external input. Bounded excerpts are stored as
`Evidence` nodes (`evidence_kind = "web_search"`, `untrusted_origin = "web"`);
raw page bodies are never persisted. Chat citations carry the same `web`
origin marker so downstream consumers can keep result text out of the trusted
instruction channel.

NightShift's runtime search does not depend on OpenCode's built-in `websearch`.
That tool is an ad-hoc aid for a coding session only; it is never a shipped
policy, credential, or search dependency of the daemon.

## Gmail read-only (P0)

Gmail is read-only in phase P0. The only OAuth scope requested is
`https://www.googleapis.com/auth/gmail.readonly`; there is no code path that
sends, deletes, labels, archives, or marks mail read, and the provider adapter
issues only `GET` requests.

Configure `GMAIL_CLIENT_ID`, `GMAIL_CLIENT_SECRET`, and `GMAIL_REDIRECT_URL`.
An operator connects an account through `POST /api/v1/mail/accounts/connect` by
redeeming an authorization code (with a `state` value) or by importing tokens
obtained out of band for local setup.

OAuth refresh tokens are stored only in `NIGHTSHIFT_MAIL_CREDENTIALS_FILE`
(default `./data/mail-credentials.json`): a single JSON file written atomically
with owner-only permissions (`0600`) inside an owner-only directory (`0700`).
Tokens never enter FalkorDB, logs, prompts, or artifacts. The graph stores only
bounded account metadata (`ProviderAccount`) and bounded normalized messages
(`MailMessage` plus `MailThread` links). Message bodies are treated as hostile
external input and are marked with `untrusted_origin = "mail"`.

## Google Calendar read-only (P0)

Calendar is read-only in phase P0. The only OAuth scope requested is
`https://www.googleapis.com/auth/calendar.readonly`; there is no code path that
creates, edits, or deletes events, changes attendees, or responds to
invitations, and the provider adapter issues only `GET` requests.

Calendar reuses the same Google OAuth client and redirect as Gmail
(`GMAIL_CLIENT_ID`, `GMAIL_CLIENT_SECRET`, `GMAIL_REDIRECT_URL`). An operator
starts consent through `GET /api/v1/calendar/accounts/authorize` (passing a
`state`), then completes it with `POST /api/v1/calendar/accounts/connect` using
the returned `code` and the same `state`; tokens can also be imported for local
setup. The same owner-only credential file stores the refresh tokens.

Event start/end times are normalized into `NIGHTSHIFT_TIMEZONE` (default `UTC`),
and `today`/`upcoming` windows are resolved in that location. The graph stores
only bounded account metadata (`ProviderAccount` with `provider = "gcal"`),
bounded normalized events (`CalendarEvent` linked with `HAS_EVENT`, plus a
`CalendarSource` reference via `IN_CALENDAR`), and never stores token material.
Event text is treated as hostile external input and is marked with
`untrusted_origin = "calendar"`.

## Reminders and notifications (P0)

Reminders are graph-backed: `Reminder` nodes store the title, body, due time,
timezone, recurrence, quiet hours, status, lease, and idempotency key, and
delivery writes a deduplicated `InboxItem` (`(Reminder)-[:DELIVERED_AS]->(InboxItem)`).
One-shot and fixed-interval recurring reminders are supported.

The daemon reconciles due reminders on startup and on each scheduled run. A due
reminder is claimed under an atomic graph lease, delivered to the inbox first,
then advanced to its next occurrence. Delivery is keyed by a stable per-occurrence
idempotency key, so a second instance or a post-restart pass never delivers the
same occurrence twice. Quiet hours are evaluated in the reminder's timezone and
postpone delivery to the window end.

The inbox sink always works and has no external side effect. The optional
webhook sink is disabled until `NIGHTSHIFT_REMINDER_WEBHOOK_URL` is set. Any sink
that creates an external side effect is approval-gated: an automatic tick marks
the reminder `pending_approval` and only `POST /api/v1/reminders/{id}/approve`
delivers it.

## GitHub integration (P0)

GitHub is disabled until `GITHUB_TOKEN` is set. Use a fine-grained personal
access token (or a GitHub App installation token) with least privilege:
read-only access for the objects NightShift reports on, plus issue, pull
request, and contents write only if approved writes are wanted. The token is
never stored in FalkorDB, logs, prompts, or artifacts.

`GITHUB_REPOSITORIES` is a comma-separated `owner/name` allowlist. Both reads and
approved writes are rejected before any request if the repository is missing
from the allowlist, so the provider can never be reached for an unlisted
repository. `GITHUB_API_BASE` defaults to `https://api.github.com` and must be
HTTPS unless it points at loopback.

Reads are GET-only and bounded: repository metadata, issues, pull requests,
commit check runs, and releases. `GET /api/v1/github/repositories/{owner}/{name}`,
`/issues`, `/pulls`, `/checks` (requires `head_sha`), `/releases`, and `/objects`
expose them. The graph stores only bounded references (`ExternalObject` linked
with `HAS_OBJECT` and `IN_REPOSITORY`, plus a `Repository` reference): object ids,
links, commit SHAs, and bounded summaries. Object text is treated as hostile
external input and marked with `untrusted_origin = "github"`.

Writes go through the shared action layer, so they require `NIGHTSHIFT_ACTIONS_ENABLED`
and an explicit approval before execution. Only `github_issue`, `github_branch`,
`github_commit`, `github_push`, and `github_pull_request` are supported; force-push
and auto-merge are always refused by both the executor and the writer.

## Observability

Logs are structured (`log/slog`) and JSON by default. Every record carries a
correlation identifier: the outer middleware reuses an inbound `X-Request-ID`
or generates one, sets it on the response, and stores it on the request context
so handlers can attach it to their own records. Every string attribute and the
record message pass through a redactor that removes bearer/basic credentials,
JWTs, cookies, and `token`/`secret`/`password`/`api_key` assignments, so a log
line never contains a credential, token, cookie, prompt, or mail body value.

```text
NIGHTSHIFT_LOG_FORMAT=json      # or "text" for local sessions
NIGHTSHIFT_LOG_LEVEL=info       # debug | info | warn | error
```

`GET /metrics` renders bounded Prometheus counters and histograms. Keep it
behind authentication (the default) and only set `NIGHTSHIFT_METRICS_PUBLIC=true`
on a trusted scrape network. Metric labels are bounded to route patterns,
providers, and fixed status strings; raw paths, query strings, payloads, and
prompts are never used as label values.

| Metric | Meaning |
|---|---|
| `nightshift_http_requests_total` / `nightshift_http_request_duration_seconds` | request count and latency by method, route pattern, status |
| `nightshift_auth_total` | authentication decisions by outcome and reason |
| `nightshift_provider_request_duration_seconds` / `nightshift_provider_errors_total` | outbound provider latency and errors by provider |
| `nightshift_graph_query_duration_seconds` / `nightshift_graph_errors_total` | FalkorDB command latency and errors by operation |
| `nightshift_scheduler_run_duration_seconds` / `nightshift_scheduler_runs_total` | scheduled run latency and outcome |
| `nightshift_tasks_total` | agent tasks by kind and result |
| `nightshift_reminders_total` | reminders reconciled by status |
| `nightshift_briefings_total` | briefing pipeline runs by status |
| `nightshift_approvals_total` | action approvals by result |
| `nightshift_otlp_exports_total` | OTLP export attempts by result |

Setting `NIGHTSHIFT_OTLP_ENDPOINT` (an absolute http(s) URL such as
`http://otel-collector:4318`) enables non-blocking OTLP/HTTP JSON export. Export
runs on a background goroutine and posts `/{endpoint}/v1/metrics`; a full queue
drops the sample instead of blocking a request. Omit the variable to disable it.

```bash
curl -fsS -H "Authorization: Bearer $NIGHTSHIFT_AUTH_TOKEN" http://127.0.0.1:8080/metrics
```

## Migrations and health

The file `migrations/falkordb/001_core.cypher` documents the foundation graph
seed. The current binary runs the idempotent bootstrap through
`graph.Initialize()` at startup; there is no separate migration binary and no
SQLite database. Back up, migrate, roll back, gate readiness, and rotate secrets
using the steps below.

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/health/live >/dev/null
curl --fail --silent --show-error -H "Authorization: Bearer $NIGHTSHIFT_AUTH_TOKEN" \
  http://127.0.0.1:8080/health/ready
```

If `/health/live` is configured as a public path for a platform probe, it only
reports that the process is serving HTTP. It does not prove FalkorDB health.

### Backup and restore

All durable product state lives in FalkorDB. Back up both graphs together so
leases, idempotency records, and user nodes stay consistent.

```bash
# Logical dump of the control and user graphs (run against the FalkorDB host).
redis-cli -h "$FALKOR_HOST" -p "$FALKOR_PORT" \
  GRAPH.COPY nightshift_control nightshift_control_backup_$(date +%Y%m%d)
redis-cli -h "$FALKOR_HOST" -p "$FALKOR_PORT" \
  GRAPH.COPY nightshift_u_demo nightshift_u_demo_backup_$(date +%Y%m%d)

# Restore by copying the backup graph name back over the live name after a stop.
redis-cli -h "$FALKOR_HOST" -p "$FALKOR_PORT" GRAPH.COPY nightshift_u_demo_backup_20261017 nightshift_u_demo
```

Also copy `NIGHTSHIFT_MAIL_CREDENTIALS_FILE` (mode `0600`, outside the repo) if
Gmail/Calendar are connected; FalkorDB never holds token material.

### Migration

The file `migrations/falkordb/001_core.cypher` documents the foundation graph
seed. The binary runs an idempotent bootstrap through `graph.Initialize()` at
startup; there is no separate migration binary and no SQLite database. Before a
schema change: back up both graphs, review the Cypher for idempotency and
bounded clauses, deploy to one instance, verify the recorded schema version and
readiness, then roll out the remaining replicas. Keep graph names and the control
graph stable across upgrades.

### Rollback

1. Stop the daemon (`sudo systemctl stop nightshift`) and scale workers to zero.
2. Restore the pre-upgrade graph backup over the live graph names.
3. Deploy the previous binary and start a single instance.
4. Confirm `/health/ready` is `200` and `/api/v1/status` reports the previous
   schema version before restoring traffic and workers.

A failed graph probe leaves `/health/live` at `200` while `/health/ready` returns
`503`; use liveness for process restart supervision and readiness for traffic
routing, so an unhealthy graph never triggers a restart loop.

### Readiness

```bash
curl --fail --silent --show-error http://127.0.0.1:8080/health/live >/dev/null
curl --fail --silent --show-error -H "Authorization: Bearer $NIGHTSHIFT_AUTH_TOKEN" \
  http://127.0.0.1:8080/health/ready
```

Gate traffic and worker advancement on `/health/ready`. Do not use it as a
process liveness probe: a remote-graph outage should not restart a healthy
daemon.

### Secret rotation

Rotate one credential at a time and keep a window where both the old and new
values are accepted by the peer:

1. `NIGHTSHIFT_AUTH_TOKEN` / Basic credentials: set the new value in the secret
   store, reload the unit (`sudo systemctl reload-or-restart nightshift`), then
   update clients. Prefer a rolling token that the reverse proxy can hold.
2. `NIGHTSHIFT_TRUSTED_PROXY_SECRET`: configure the proxy to sign with the new
   secret, restart the daemon with the new secret, then remove the old secret.
3. `GITHUB_TOKEN`: create the replacement token, restart the daemon, then revoke
   the old token.
4. Google OAuth (`GMAIL_CLIENT_SECRET`): rotate in the Google console, restart,
   and re-connect accounts if refresh tokens are invalidated.
5. `NIGHTSHIFT_OIDC_*` / JWKS: unchanged issuer config picks up rotated JWKS keys
   from the provider; no daemon restart is required.

Credential values are never logged or stored in FalkorDB; rotate the mail
credential file by re-running the Gmail/Calendar connect flow.

## Operations checklist

```bash
curl -fsS http://127.0.0.1:8080/health/live
curl -i -H "Authorization: Bearer $NIGHTSHIFT_AUTH_TOKEN" http://127.0.0.1:8080/health/ready
curl -fsS -H "Authorization: Bearer $NIGHTSHIFT_AUTH_TOKEN" http://127.0.0.1:8080/api/v1/status
curl -fsS -H "Authorization: Bearer $NIGHTSHIFT_AUTH_TOKEN" http://127.0.0.1:8080/api/v1/dashboard
```

`/health/live` does not require FalkorDB. `/health/ready` and the dashboard
report graph connectivity separately, which makes remote-graph outages
visible without causing the process supervisor to restart a healthy daemon.

## Uninstall and data removal

Back up FalkorDB graphs before removing a deployment. Graph state is durable in
FalkorDB and is not removed by deleting the daemon binary or its local data
directory.

```bash
# Docker Compose: stop services; add -v only when deleting local volumes is intended.
docker compose -f deploy/docker/compose.yaml down
docker compose -f deploy/docker/compose.yaml down -v

# systemd: stop the service, then remove its unit, binary, config, and state.
sudo systemctl disable --now nightshift
sudo rm /etc/systemd/system/nightshift.service /usr/local/bin/nightshift
sudo rm -rf /etc/nightshift /var/lib/nightshift
sudo systemctl daemon-reload
sudo userdel nightshift
```

For Render, delete the Blueprint after exporting the environment values and
remove the external FalkorDB deployment separately if its graphs are no longer
needed. Deleting a Render service does not delete remote graph state.

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

Measured by repository checks: configuration/auth rejection and acceptance
boundaries, health endpoint semantics, source failure recovery, approval-gated
action behavior, graph persistence, and the in-memory concurrency contract.
No memory, CPU, startup, dashboard-latency, or target-board support figure is
reported as measured by this repository. The resource values in `PRD.md` and
`ps.md` are targets until a documented workload is run on the claimed hardware
and the results are captured.
