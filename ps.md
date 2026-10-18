# NightShift System Specification

**Product:** NightShift
**Document type:** Technical project/system specification
**Status:** Draft for implementation
**Version:** 0.1
**Audience:** Product team, implementers, reviewers, demo operators, and future maintainers

---

## Hackathon implementation override (2026-10-16)

The implementation in this repository intentionally narrows this draft
specification to a FalkorDB-only durable product state model. FalkorDB owns
profile, project context, source/item relationships, recommendations, memory,
decisions, tasks, handoffs, leases, and action idempotency. SQLite is not part
of the MVP. If a crash-replay spool is added later, it is transport-only and
cannot be queried as application context. `docs/architecture.md` is the
current implementation decision record; the older SQLite sections below are
retained as historical design alternatives until this specification is fully
rewritten.

---

## 1. Purpose of this document

This document converts the product requirements in `PRD.md` into an implementation-oriented specification. It explains the architecture, modules, data boundaries, performance decisions, interfaces, failure behavior, and deployment choices required to run NightShift efficiently on modest Linux hardware.

The system is designed for:

- Raspberry Pi 3, 4, and 5
- Arduino UNO Q, preferably the 4 GB model
- Older x86 PCs with limited RAM
- Small home servers
- ARM64 or x86_64 Linux machines

The design intentionally avoids requiring a cluster, Kubernetes, a browser automation fleet, a vector database, or a local large language model.

---

## 2. Architecture decision record

### ADR-001: Use a modular monolith for the edge runtime

**Decision:** Build one `nightshift` daemon with internal modules instead of multiple always-running services.

**Why:**

- Lowest baseline memory use
- Fewer network hops
- Simpler installation on a Raspberry Pi or old PC
- Easier cross-compilation
- Easier restart and backup behavior
- One place to enforce approval and security policies

The code shall preserve clean module boundaries so individual components can become services later if scale requires it. Splitting into microservices during the hackathon is explicitly discouraged.

### ADR-002: Use Go for the edge runtime

**Decision:** Use Go for the production daemon and API.

**Why Go fits this product:**

- Produces a single portable binary.
- Has low idle memory usage compared with a Python plus web-server plus worker stack.
- Has strong standard-library support for HTTP, TLS, JSON, URL handling, timeouts, templates, and concurrency.
- Cross-compiles easily for ARM64 and x86_64.
- Has predictable goroutine-based concurrency that can be bounded.
- Supports an embedded static frontend through `embed`.
- Works well for long-lived network services.

Python may be used for one-off data preparation or experiments, but it is not required on the target device. If the team chooses Python for speed of prototyping, the production architecture should still preserve the same boundaries and resource limits.

### ADR-003: Use SQLite only for local operational state

**Decision:** Use SQLite for the local queue, cache index, provider cursors, leases, idempotency records, and health history. Use FalkorDB for connected product context.

SQLite is not a replacement for FalkorDB. It exists so the edge runtime can survive network outages and avoid needing Redis or another queue service.

### ADR-004: Use FalkorDB as the context database

**Decision:** Store entities, relationships, memory, decisions, tasks, evidence, and graph-driven reasoning state in FalkorDB.

The implementation shall use an official FalkorDB client for the chosen language and shall validate queries against the supported FalkorDB Cypher subset. It shall not assume every Neo4j-only clause or procedure is available.

### ADR-005: Use a static, server-served web interface

**Decision:** Build the browser UI as static assets and serve them from the Go binary.

The target device shall not need a Node.js process at runtime. The frontend may use plain TypeScript, a small frontend library, or server-rendered HTML with progressive enhancement. The default recommendation is server-rendered HTML plus small JavaScript modules for the lowest runtime cost.

### ADR-006: Use remote model inference by default

**Decision:** Treat an LLM as an optional provider. The default low-resource configuration uses a remote API. Deterministic collection, normalization, graph traversal, ranking, and display must still work when the model is unavailable.

The system shall not assume that an UNO Q, Raspberry Pi, or DDR2-era PC can run a useful large model locally. A future local model adapter is allowed but is not part of the MVP performance guarantee.

### ADR-007: Keep the runtime dependency budget small

**Decision:** Prefer the Go standard library and a small, explicitly reviewed dependency set.

Recommended dependencies:

- Official `falkordb-go` client for FalkorDB access
- `modernc.org/sqlite` for a CGO-free SQLite build when its measured resource use is acceptable
- A small configuration parser only if the team keeps YAML or TOML configuration; JSON plus `encoding/json` is the lowest-dependency option

Do not add an ORM, a general-purpose job queue, a vector database, a browser automation framework, or a large dependency injection framework. Use direct SQL migrations and small interfaces. If the selected SQLite driver has materially higher memory use on the target board, benchmark it against a CGO-enabled SQLite driver and document the trade-off rather than guessing.

The frontend may use a build-time toolchain, but the deployed device shall receive static assets only. The runtime shall not start a JavaScript build server.

---

## 3. System context

```text
┌──────────────────────────────────────────────────────────────┐
│ User browser                                                │
│ Dashboard · briefing · graph path · approvals · settings    │
└───────────────────────────────┬──────────────────────────────┘
                                │ HTTP on local network/Tailscale/etc.
┌───────────────────────────────▼──────────────────────────────┐
│ NightShift edge daemon (single native binary)               │
│                                                              │
│ HTTP/API · scheduler · bounded workers · source adapters     │
│ normalizer · ranking · agent state machine · action policy   │
│ SQLite queue/cache · metrics · structured logs               │
└───────────────┬──────────────────────┬───────────────────────┘
                │                      │
                │ official client     │ HTTPS with strict limits
                ▼                      ▼
       ┌──────────────────┐   ┌──────────────────────────────┐
       │ FalkorDB         │   │ Public sources               │
       │ graph context    │   │ RSS · Atom · JSON · GitHub    │
       │ memory · tasks   │   │ public job feeds · YouTube    │
       └────────┬─────────┘   └──────────────────────────────┘
                │
                ▼
       ┌──────────────────┐
       │ Optional LLM API │
       │ shortlist only   │
       └──────────────────┘

       ┌──────────────────┐
       │ Approved actions │
       │ GitHub sandbox   │
       │ local plan file  │
       └──────────────────┘
```

### 3.1 Trust boundaries

1. **Browser boundary:** The browser is untrusted input. Validate all form fields server-side.
2. **Public-content boundary:** Feed titles, descriptions, issues, and articles are untrusted content. They must never override system policy or become executable instructions automatically.
3. **Provider boundary:** Model providers and GitHub are external services. Apply timeouts, retries, authentication isolation, and response-size limits.
4. **Graph boundary:** FalkorDB is a protected data service. Do not expose it directly to the public internet unless authentication, network restrictions, and TLS are configured.
5. **Action boundary:** Any external write requires a separate approval decision and idempotency key.

---

## 4. Repository layout

The implementation should use a layout similar to the following:

```text
nightshift/
├── cmd/
│   └── nightshift/
│       └── main.go
├── internal/
│   ├── app/                 # dependency wiring and lifecycle
│   ├── config/              # environment and file configuration
│   ├── runtime/             # startup, shutdown, signals, health
│   ├── scheduler/           # timers, manual runs, quiet hours
│   ├── queue/               # SQLite durable jobs and leases
│   ├── cache/               # HTTP cache metadata and model cache
│   ├── sources/              # RSS, Atom, JSON, GitHub adapters
│   ├── normalize/            # canonical items, hashing, cleanup
│   ├── graph/                # FalkorDB connection, Cypher, migrations
│   ├── context/              # profile, project, memory, evidence
│   ├── rank/                 # deterministic relevance scoring
│   ├── agents/               # agent state machine and handoffs
│   ├── llm/                  # optional model provider interface
│   ├── actions/              # approval and external write policy
│   ├── github/               # least-privilege GitHub adapter
│   ├── web/                  # routes, handlers, templates, API
│   ├── metrics/              # counters, durations, resource readings
│   └── logging/              # redaction and structured log setup
├── migrations/
│   ├── sqlite/
│   └── graph/
├── web/
│   ├── templates/
│   └── static/
├── deploy/
│   ├── systemd/
│   ├── docker/
│   └── scripts/
├── testdata/
│   ├── public-fixtures/
│   └── synthetic-user/
├── docs/
├── go.mod
├── go.sum
├── Makefile
├── README.md
├── PRD.md
└── ps.md
```

### Module rules

- `web` may call application services, not FalkorDB directly.
- `agents` may request graph operations through `context` and `graph` interfaces.
- `sources` may fetch and normalize content, but may not decide what a user should do.
- `rank` may read structured context and return scores, but may not execute external actions.
- `actions` is the only module allowed to perform external writes.
- `config` owns configuration validation and redaction.
- No module may import the concrete LLM provider directly; use an interface.

---

## 5. Process lifecycle

### 5.1 Startup sequence

1. Parse command-line flags.
2. Load configuration file and environment variables.
3. Validate required configuration.
4. Initialize redacted structured logging.
5. Open SQLite with bounded connection settings.
6. Apply SQLite migrations.
7. Initialize the FalkorDB client lazily or with a short readiness check.
8. Register source adapters.
9. Load the scheduler state.
10. Start the HTTP server.
11. Start the scheduler loop.
12. Start the bounded worker pool.
13. Mark the service ready only after the local queue and HTTP server are healthy.

The service should start and serve the health endpoint even when FalkorDB or the model provider is temporarily unavailable. Readiness must report the missing dependency clearly.

### 5.2 Shutdown sequence

1. Stop accepting new external action requests.
2. Stop scheduling new work.
3. Allow current fetches to finish until the shutdown deadline.
4. Persist worker leases and incomplete task state.
5. Close HTTP response bodies.
6. Flush structured logs and metrics.
7. Close SQLite.
8. Exit with a useful status code.

### 5.3 Restart behavior

On restart:

- Expired job leases become runnable again.
- Completed jobs remain completed.
- Action records with a completed idempotency key are not repeated.
- Partially written graph operations are retried using upserts.
- The user can see the previous error and retry manually.

---

## 6. Configuration specification

Configuration may come from a file, environment variables, or both. Environment variables override file values. Secrets must be supplied through environment variables or a protected secret file.

### Example configuration

```yaml
server:
  listen: "127.0.0.1:8080"
  read_timeout: "5s"
  write_timeout: "15s"
  idle_timeout: "60s"

runtime:
  data_dir: "/var/lib/nightshift"
  max_workers: 2
  max_queue_depth: 100
  run_deadline: "5m"
  low_resource_mode: true

sqlite:
  path: "/var/lib/nightshift/nightshift.db"
  max_open_connections: 1
  busy_timeout: "5s"
  durability: "balanced"

falkordb:
  address: "redis://127.0.0.1:6379"
  graph_name: "nightshift_demo"
  connect_timeout: "3s"
  query_timeout: "5s"
  max_rows: 100

llm:
  enabled: true
  provider: "remote"
  model: "configured-by-deployment"
  request_timeout: "30s"
  max_input_bytes: 16384
  max_output_tokens: 600
  daily_request_budget: 100

sources:
  default_interval: "30m"
  max_items_per_source: 50
  max_body_bytes: 2097152

actions:
  enabled: false
  require_approval: true
  github_mode: "sandbox-only"
```

### Configuration validation

The service shall fail fast for:

- Invalid listen address
- Negative worker count
- Empty data directory
- Missing graph name
- Invalid URL scheme
- Unbounded source response settings
- External actions enabled without approval enforcement
- GitHub writes enabled without a configured sandbox policy

The service shall not print secret values when validation fails.

---

## 7. Local operational storage

SQLite stores operational data that must survive a network or graph outage. It is not the product's relationship store.

### 7.1 SQLite tables

#### `jobs`

```text
id              TEXT PRIMARY KEY
kind            TEXT NOT NULL
payload_json    BLOB NOT NULL
status          TEXT NOT NULL
priority        INTEGER NOT NULL DEFAULT 0
available_at    INTEGER NOT NULL
lease_until     INTEGER
attempts        INTEGER NOT NULL DEFAULT 0
max_attempts    INTEGER NOT NULL DEFAULT 3
idempotency_key TEXT
last_error      TEXT
created_at      INTEGER NOT NULL
updated_at      INTEGER NOT NULL
```

#### `http_cache`

```text
cache_key       TEXT PRIMARY KEY
etag            TEXT
last_modified   TEXT
content_hash    TEXT
last_status     INTEGER
last_fetched_at INTEGER
expires_at      INTEGER
```

#### `provider_cursors`

```text
provider        TEXT NOT NULL
source_id       TEXT NOT NULL
cursor          TEXT
updated_at      INTEGER NOT NULL
PRIMARY KEY(provider, source_id)
```

#### `action_journal`

```text
idempotency_key TEXT PRIMARY KEY
action_type     TEXT NOT NULL
target          TEXT NOT NULL
approval_id     TEXT NOT NULL
status          TEXT NOT NULL
external_id     TEXT
response_hash   TEXT
created_at      INTEGER NOT NULL
updated_at      INTEGER NOT NULL
```

#### `model_cache`

```text
cache_key       TEXT PRIMARY KEY
prompt_version  TEXT NOT NULL
content_hash    TEXT NOT NULL
response_json   BLOB NOT NULL
expires_at      INTEGER
created_at      INTEGER NOT NULL
```

#### `health_events`

```text
id              INTEGER PRIMARY KEY AUTOINCREMENT
component       TEXT NOT NULL
status          TEXT NOT NULL
duration_ms     INTEGER
error_code      TEXT
created_at      INTEGER NOT NULL
```

Health history shall be aggressively retained, for example the last 500–2,000 records, rather than growing forever.

### 7.2 SQLite settings

Recommended defaults for a single-process edge daemon:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA temp_store = MEMORY;
```

The implementation shall allow a `durable` profile that uses stronger synchronization for critical journal writes. The balanced profile is appropriate for normal operation on an SD card; backups and action idempotency are still required.

Use one writer path and short transactions. Never hold a SQLite transaction open while waiting for the network or an LLM.

---

## 8. FalkorDB graph specification

### 8.1 Graph naming and isolation

The MVP may use one graph for a single demo user, for example `nightshift_demo`. The data model shall include `tenant_id` or `user_id` on every user-owned node so that the implementation can move to one graph per user or tenant later.

For a multi-user deployment, prefer FalkorDB's multigraph topology:

```text
nightshift_user_<stable-id>
nightshift_demo
```

Do not put private graph names, tokens, or raw credentials in URLs or browser-visible responses.

### 8.2 Node properties

Properties should be small, typed, and useful for query predicates. Suggested fields:

```text
User:
  id, display_name, tenant_id, created_at, updated_at

Interest:
  id, name, normalized_name, weight, created_at

Skill:
  id, name, normalized_name, level, created_at

Project:
  id, name, description, status, repository_url, created_at, updated_at

Requirement:
  id, project_id, title, status, priority, normalized_text

Source:
  id, name, kind, url, enabled, trust_score, last_seen_at

Item:
  id, canonical_url, title, summary, published_at, content_hash,
  language, content_kind, fetched_at

Technology:
  id, name, normalized_name, category

Job:
  id, title, company_name, url, location, published_at, source_id

Session:
  id, started_at, ended_at, summary, status

Memory:
  id, kind, text, importance, confidence, created_at, expires_at

Decision:
  id, title, decision, status, created_at, updated_at

Evidence:
  id, url, excerpt, evidence_kind, content_hash, observed_at

Agent:
  id, kind, version, capabilities

Task:
  id, kind, status, priority, attempts, created_at, updated_at

Action:
  id, type, target, status, idempotency_key, approved_at, completed_at
```

Long raw content should normally remain in the local cache or an explicitly configured object store. FalkorDB should hold the normalized summary, content hash, source URL, and the relationships needed for reasoning. If the demo requires raw excerpts, enforce a byte limit.

### 8.3 Relationship names

Use a small, stable vocabulary:

```text
INTERESTED_IN
HAS_SKILL
OWNS
HAS_REQUIREMENT
NEEDS
FROM
ABOUT
MENTIONS
RELATES_TO
REQUIRES
AT
CONTAINS
REMEMBERS
BASED_ON
SUPPORTS
ASSIGNED_TO
HANDED_OFF_TO
DEPENDS_ON
PRODUCES
TARGETS
CREATES
APPROVED
DISMISSED
LIKED
DEFERRED
```

Relationship properties should be used for small metadata such as `weight`, `created_at`, `confidence`, and `source`. Do not serialize large JSON documents into relationship properties.

### 8.4 Idempotent graph writes

All ingestion and memory writes shall be repeatable. The graph repository shall use stable IDs and upsert-like patterns that can safely run twice.

Stable ID examples:

```text
source_id = sha256(canonical_source_url)
item_id = sha256(canonical_url or source_id + content_hash)
topic_id = normalize(name)
task_id = UUID generated before enqueue
action_idempotency_key = sha256(action_type + target + approval_id)
```

Graph writes shall be small and bounded. A single source run must not create an unbounded transaction.

### 8.5 Query rules

- Always use parameters rather than string interpolation.
- Always filter by stable IDs or tenant scope.
- Always use `LIMIT` for user-facing reads.
- Return only properties required by the caller.
- Prefer two small queries over one extremely broad query.
- Record the query name and duration, not the full query with secret parameters.
- Validate every query against the version of FalkorDB used in deployment.
- Do not assume Neo4j procedures, APOC, or vector indexes are available.

### 8.6 Example graph queries

These examples are illustrative and must be checked against the selected FalkorDB client and supported Cypher version.

#### Find recent items related to user interests

```cypher
MATCH (u:User {id: $user_id})
      -[:INTERESTED_IN]->(interest:Interest)
      <-[:ABOUT]-(item:Item)
      -[:FROM]->(source:Source)
WHERE item.published_at >= $since
RETURN item.id AS item_id,
       item.title AS title,
       item.canonical_url AS url,
       interest.name AS matched_interest,
       source.name AS source
ORDER BY item.published_at DESC
LIMIT $limit
```

#### Connect a project requirement to a public item

```cypher
MATCH (p:Project {id: $project_id})
      -[:HAS_REQUIREMENT]->(r:Requirement)
      -[:NEEDS]->(tech:Technology)
      <-[:MENTIONS]-(item:Item)
      -[:FROM]->(source:Source)
RETURN p.name AS project,
       r.title AS requirement,
       tech.name AS technology,
       item.title AS item,
       item.canonical_url AS url,
       source.name AS source
LIMIT $limit
```

#### Find the next ready task and capable agent

```cypher
MATCH (task:Task {status: 'ready'})
      <-[:ASSIGNED_TO]-(agent:Agent)
WHERE task.priority >= $minimum_priority
RETURN task.id AS task_id,
       task.kind AS task_kind,
       task.priority AS priority,
       agent.id AS agent_id,
       agent.kind AS agent_kind
ORDER BY task.priority DESC
LIMIT 1
```

If the implementation cannot express a query safely in the supported subset, move the filtering into a bounded application-side operation rather than introducing a dependency on an unsupported procedure.

---

## 9. Ingestion pipeline

### 9.1 Pipeline stages

```text
Schedule
  → acquire source lease
  → conditional HTTP request
  → bounded response reader
  → parse feed/API
  → canonicalize URL
  → content hash
  → deduplicate
  → cheap relevance filter
  → graph upsert
  → shortlist ranking
  → optional model analysis
  → recommendation and evidence write
  → briefing task
```

Each stage shall produce a bounded result and a structured error. Do not keep a whole source response, all normalized items, and all model prompts in memory at the same time.

### 9.2 Source adapter interface

The concrete language implementation may vary, but the behavior should resemble:

```go
type SourceAdapter interface {
    Kind() string
    Fetch(ctx context.Context, source SourceConfig, cursor Cursor) (FetchResult, error)
}

type FetchResult struct {
    Items       []RawItem
    NextCursor  *Cursor
    ETag        string
    LastModified string
    NotModified bool
}
```

The adapter shall not call the LLM or create external actions.

### 9.3 HTTP limits

Every outbound request shall have:

- Context deadline
- Maximum response bytes
- Redirect limit
- Allowed schemes, normally HTTPS and explicitly allowed HTTP for local development
- User agent identifying NightShift
- Connection reuse
- Retry policy only for retryable status codes and network errors
- Respect for `Retry-After` where available

Recommended low-resource defaults:

```text
connect timeout: 3 seconds
request timeout: 15 seconds
feed response limit: 2 MiB
API response limit: 4 MiB
redirect limit: 3
retries: 2
backoff: 1 second, then 4 seconds
```

### 9.4 Deduplication

The normalizer shall:

1. Remove tracking query parameters where safe.
2. Normalize scheme and host casing.
3. Normalize trailing slash behavior.
4. Preserve meaningful query parameters.
5. Compute a content hash over normalized title, URL, and description.
6. Check both canonical URL and content hash.
7. Avoid creating a new graph item for a minor feed update unless the content meaningfully changed.

### 9.5 Deterministic filtering

Before model calls, calculate a cheap score using:

- Exact interest term match
- Normalized technology match
- Skill match
- Project requirement match
- Source trust score
- Freshness
- Previous dismissal penalty
- Duplicate penalty

The first version may use a weighted score:

```text
deterministic_score =
  5.0 * requirement_match
+ 3.0 * technology_match
+ 2.0 * interest_match
+ 1.0 * skill_match
+ 1.0 * source_quality
+ 0.5 * freshness
- 3.0 * repeated_dismissal
- 2.0 * duplicate_similarity
```

Normalize the final score into a known range. Do not present this as an objective truth; present it as a ranking signal.

### 9.6 LLM shortlist policy

Only send the top candidates to the model. The default policy is:

- Collect up to 50 raw items per source.
- Deterministically score all items.
- Send at most 5–10 items to the model in one batch.
- Send at most 8–16 KB of text per item.
- Cache the response by content hash and prompt version.
- Store structured JSON, not an unbounded transcript.

If the model is unavailable, show deterministic recommendations marked as `analysis_pending` or `deterministic_only`.

---

## 10. Agent runtime and state machine

### 10.1 Agent roles

#### Scout Agent

Input: source run and raw items.
Output: normalized items and candidate relationships.
External writes: none.

#### Graph Curator Agent

Input: normalized item.
Output: topic, skill, technology, project, and evidence relationships.
External writes: FalkorDB only.

#### Relevance Agent

Input: candidate items and user/project graph context.
Output: ranked recommendations with reasons and evidence.
External writes: FalkorDB and local queue.

#### Planner Agent

Input: approved recommendation or PRD requirement.
Output: bounded task and implementation plan.
External writes: graph; no GitHub write without approval.

#### Memory/Reviewer Agent

Input: session events, feedback, decisions, and completed tasks.
Output: compact durable memory and review status.
External writes: graph and local queue.

### 10.2 Task state machine

```text
queued
  → leased
  → running
  → succeeded

queued/leased/running
  → retry_wait
  → queued

running
  → failed

action_proposed
  → awaiting_approval
  → approved
  → executing
  → succeeded | failed

awaiting_approval
  → rejected | expired
```

### 10.3 Lease behavior

Workers shall claim a task using a short SQLite transaction:

1. Select a ready task whose lease is absent or expired.
2. Set `lease_until` to now plus the worker lease duration.
3. Increment `attempts`.
4. Commit.
5. Perform work outside the transaction.
6. Extend the lease only when needed and only within a maximum task duration.
7. Mark succeeded or retry/failed in a new short transaction.

Recommended defaults:

```text
worker lease: 60 seconds
lease heartbeat: 20 seconds for long tasks
maximum task duration: 5 minutes
maximum automatic attempts: 3
```

### 10.4 Backpressure

When the queue reaches its configured maximum:

- Do not accept more scheduled ingestion jobs.
- Coalesce duplicate source-run jobs.
- Drop or delay low-priority recommendation refreshes.
- Preserve approval and recovery tasks.
- Show the queue-full state in the health UI.

### 10.5 Prompt construction

Prompts shall be assembled from structured, bounded context:

```text
system policy
  + user goal summary
  + selected graph facts
  + selected evidence excerpts
  + current task
  + required JSON schema
```

Do not attach the entire graph, entire session history, or entire PRD to every model call.

---

## 11. Memory design

### 11.1 Memory classes

#### Episodic memory

What happened in a session:

- User asked a question
- Agent found an item
- User marked it useful
- Planner created a task

#### Semantic memory

Stable facts:

- User follows edge AI
- User knows Python
- Project uses FalkorDB
- User dislikes duplicate hype articles

#### Procedural memory

Preferred ways of working:

- Ask before creating a PR
- Prefer small pull requests
- Run tests before proposing an action
- Use public sources only

#### Operational state

Current work:

- Task status
- Agent owner
- Dependency
- Retry count
- Approval status

### 11.2 Memory compaction

Do not append every chat message forever. The Memory Agent shall periodically:

1. Select recent events within a bounded window.
2. Remove duplicate or low-value events.
3. Create a compact summary with source event IDs.
4. Store the summary as a memory node.
5. Keep only the most useful raw event references according to retention policy.

The summary must never replace the evidence links required to explain a decision.

### 11.3 Memory retrieval

Retrieve memory in layers:

1. Current task and current session
2. Active project and open requirements
3. Recent decisions and feedback
4. Stable preferences
5. Older memories only when graph relationships make them relevant

Use a hard result limit and a byte budget. A memory retrieval function should return a small context object rather than a giant transcript.

### 11.4 Memory correctness

The UI shall distinguish:

- User-provided fact
- Source-backed fact
- Agent inference
- Proposed decision
- Approved decision
- Unknown or stale information

NightShift shall not present an inferred memory as a confirmed user preference without a confidence label or user confirmation.

---

## 12. Relevance and ranking implementation

### 12.1 Ranking stages

Ranking shall be explainable and inexpensive before it becomes intelligent.

#### Stage A: hard filters

Reject or defer items that:

- Are older than the configured retention window unless explicitly requested
- Have no usable title or canonical URL
- Exceed content-size limits
- Come from a disabled source
- Are exact duplicates
- Match a user blocklist
- Contain a prohibited data classification

#### Stage B: relationship score

Calculate a graph relationship score from bounded graph observations:

```text
requirement match: 0–5
project technology match: 0–4
interest match: 0–3
skill match: 0–2
source trust: 0–2
freshness: 0–2
novelty: 0–2
feedback adjustment: -4–+2
```

The exact weights are configuration, not code constants. Store the score version with each recommendation so future ranking changes do not make historical explanations ambiguous.

#### Stage C: model analysis

For only the shortlist, ask the model to return structured fields:

```json
{
  "relevance": "high|medium|low",
  "summary": "short summary",
  "why_it_matters": "bounded explanation",
  "suggested_action": "bounded action",
  "technology_names": ["..."],
  "confidence": 0.0,
  "evidence_claims": ["..."]
}
```

Reject responses that are not valid structured output. Do not execute an action from free-form model text.

### 12.2 Explainability record

Every recommendation shall store or reference:

- Ranking version
- Candidate item ID
- Project ID, if applicable
- Matched interest IDs
- Matched skill IDs
- Matched requirement IDs
- Source ID
- Graph query name
- Model analysis ID, if used
- Evidence IDs
- Recommended action type
- Timestamp

The UI should convert this into readable language rather than exposing only an opaque numerical score.

### 12.3 Deterministic fallback

When the LLM is disabled or unavailable, NightShift shall still:

- Fetch sources
- Normalize items
- Match known terms
- Query the graph
- Rank candidates
- Show a deterministic explanation
- Allow the user to save or ignore the item

This mode is important for low-resource devices, offline development, and reliable judging.

---

## 13. HTTP API and web interface

The API is internal to the installation by default. It should listen on localhost unless the user explicitly enables LAN access.

### 13.1 API endpoints

```text
GET    /health/live
GET    /health/ready
GET    /api/v1/status

GET    /api/v1/profile
PUT    /api/v1/profile

GET    /api/v1/projects
POST   /api/v1/projects
GET    /api/v1/projects/{id}
PUT    /api/v1/projects/{id}

GET    /api/v1/sources
POST   /api/v1/sources
PUT    /api/v1/sources/{id}
POST   /api/v1/sources/{id}/run

GET    /api/v1/briefings
GET    /api/v1/recommendations/{id}
POST   /api/v1/recommendations/{id}/feedback

GET    /api/v1/sessions
GET    /api/v1/sessions/{id}
POST   /api/v1/sessions

GET    /api/v1/tasks
GET    /api/v1/tasks/{id}
POST   /api/v1/tasks/{id}/retry

POST   /api/v1/actions/{id}/approve
POST   /api/v1/actions/{id}/reject

GET    /api/v1/graph/path/{id}
POST   /api/v1/runs
POST   /api/v1/runtime/pause
POST   /api/v1/runtime/resume
```

### 13.2 API rules

- Return JSON for `/api/v1` endpoints.
- Use typed error codes, not only human-readable strings.
- Include a request ID in every response.
- Reject unknown fields for security-sensitive requests.
- Limit body size.
- Do not return tokens or full graph credentials.
- Use `409 Conflict` for an outdated approval or action state.
- Use `429 Too Many Requests` for queue backpressure.
- Use `503 Service Unavailable` when the graph or provider is unavailable.

### 13.3 Error shape

```json
{
  "error": {
    "code": "GRAPH_UNAVAILABLE",
    "message": "The graph backend is temporarily unavailable.",
    "request_id": "req_...",
    "retryable": true
  }
}
```

### 13.4 Dashboard views

#### Dashboard

Show:

- Current device status
- Last successful run
- Number of new items
- Pending approvals
- Open tasks
- Top recommendations
- Current memory and CPU readings

#### Recommendation detail

Show:

- Title and source
- Summary
- Why it matched
- Graph path
- Evidence links
- Related project requirement
- Suggested action
- Feedback controls

#### Task view

Show:

- Task state
- Assigned agent
- Handoff history
- Dependencies
- Inputs and outputs
- Approval state
- External result

#### Health view

Show:

- Runtime version
- Architecture
- Uptime
- SQLite status
- FalkorDB status
- LLM status
- Source errors
- Queue depth
- Resident memory
- CPU percentage

### 13.5 Frontend efficiency

- Prefer server-rendered pages for first paint.
- Use small, page-specific JavaScript modules.
- Avoid a large client-side state-management framework.
- Avoid polling faster than 10–30 seconds.
- Use server-sent events only for an active run view; otherwise refresh on demand.
- Gzip or Brotli static assets if the server implementation supports it.
- Keep the first-load JavaScript budget below 100 KB compressed where practical.
- Do not include a graph visualization library in the default dashboard unless the demo requires it. A readable path list is cheaper and more accessible.

---

## 14. External action design

### 14.1 Action lifecycle

```text
proposed
  → shown to user
  → approved or rejected
  → idempotency checked
  → executing
  → external result recorded
  → succeeded or failed
```

### 14.2 GitHub sandbox policy

The MVP shall support a configured sandbox repository only. The configuration shall contain:

- Repository owner
- Repository name
- Allowed action types
- Allowed branch prefix
- Maximum files per action
- Maximum diff size
- Require approval: always true for MVP

The GitHub adapter shall not have permission to:

- Delete repositories
- Change organization settings
- Rotate credentials
- Merge pull requests automatically
- Deploy production systems

### 14.3 Idempotency

Before an external write:

1. Create an idempotency key.
2. Insert a `pending` journal record.
3. Check whether the same key already has a completed result.
4. Execute the external call.
5. Store the external ID and response hash.
6. Mark the journal record succeeded.

If the network fails after the external service may have accepted the request, do not blindly retry. Query the external service or require user confirmation.

### 14.4 Local plan fallback

If GitHub is not configured, the approved action shall generate a local Markdown plan in the configured data directory. This keeps the core demo functional without external credentials.

---

## 15. Security specification

### 15.1 Authentication

For a single-user local installation, the default is localhost-only access. If LAN access is enabled, require one of:

- A configured local password
- Reverse-proxy authentication
- A private network such as Tailscale or WireGuard

Do not expose an unauthenticated action-enabled installation to the public internet.

### 15.2 Authorization

The action service shall enforce authorization even if a UI route is bypassed. A client cannot approve an action by changing a browser request unless the server validates:

- The action exists
- The action is awaiting approval
- The user has permission
- The approval has not expired
- The target is permitted
- The idempotency key is valid

### 15.3 Content isolation

Fetched content is data, not policy. The model prompt shall explicitly label source text as untrusted evidence. Never concatenate source text into a system instruction.

### 15.4 Redaction

The logger shall redact:

- Authorization headers
- Cookies
- API keys
- GitHub tokens
- Query parameters that contain credentials
- Full user-provided secrets accidentally present in text

The graph path view shall not expose secrets accidentally stored in a source excerpt. Add a configurable content redaction pass before evidence is persisted.

### 15.5 File permissions

The service account should own its data directory with mode `0700`. Configuration files containing secrets should be `0600`. The systemd unit should use a dedicated unprivileged user.

---

## 16. Resource and performance specification

### 16.1 Memory budget

Target memory for the native daemon:

```text
idle daemon                         30–80 MB
HTTP request handling                5–20 MB temporary
one feed parse                       2–16 MB bounded
one model request                    1–10 MB bounded
SQLite page cache                    4–16 MB
active worker overhead               5–20 MB
recommended total daemon ceiling     160 MB
```

These are engineering targets rather than guarantees. Measure on the selected board using the real workload.

### 16.2 CPU budget

- One worker by default on devices with two or fewer cores.
- Two workers by default on four-core devices.
- Never use unbounded goroutines per feed item.
- Use a semaphore for network calls and a separate semaphore for model calls.
- Pause low-priority work when CPU is saturated.
- Do not run model analysis on every item.

### 16.3 Storage budget

Default retention:

```text
raw fetched bodies:       24 hours or disabled
normalized items:         30 days
recommendations:          90 days
decisions:                indefinite until deleted
tasks/actions:            indefinite until archived
health events:            last 2,000
model cache:              7 days or size-bounded
```

Add a maximum total data directory size. When storage is close to the threshold:

1. Delete expired raw bodies.
2. Compact model cache.
3. Archive old health events.
4. Remove old duplicate items.
5. Never delete unarchived decisions or pending actions automatically.

### 16.4 Network budget

Use per-source and global limits:

```text
maximum requests per source per hour: configurable
maximum model requests per run:       10
maximum model input per item:         16 KB
maximum concurrent outbound calls:    2
```

### 16.5 Efficient Go implementation choices

- Use `context.Context` for every network and graph operation.
- Reuse one `http.Client` with a tuned `Transport`.
- Set `MaxIdleConnsPerHost` to a small value such as 2–4.
- Use streaming JSON decoding when response shape allows it.
- Use `io.LimitReader` before parsing external bodies.
- Reuse buffers carefully; do not introduce a complex pool prematurely.
- Use `json.Decoder` for large responses.
- Use `sync.Pool` only after profiling shows allocation pressure.
- Avoid reflection-heavy generic serialization in hot paths.
- Keep log messages short and structured.
- Use a single graph connection pool with a small maximum.
- Cache normalized topic IDs and source metadata.
- Avoid repeatedly loading the full user profile; keep a small in-process snapshot with invalidation.

### 16.6 Efficient data choices

- Store timestamps as Unix integers or a consistently sortable representation.
- Use stable IDs instead of large URLs as primary keys.
- Store content hashes rather than duplicate bodies.
- Use compressed or abbreviated summaries in the graph.
- Use SQLite blobs only for bounded payloads.
- Do not store every log line in FalkorDB.

### 16.7 Performance modes

#### `full`

- Two workers
- Model shortlist enabled
- More frequent source polling
- Larger recommendation limit

#### `balanced`

- One or two workers
- Model shortlist capped
- 30-minute default polling
- Three recommendations

#### `low`

- One worker
- Model calls disabled unless manually triggered
- 60-minute polling
- One or two recommendations
- Smaller content and graph result limits
- No live graph visualization

The user shall be able to switch modes without editing code.

---

## 17. Deployment specification

### 17.1 Native Linux deployment

The preferred production path is a static binary plus systemd.

Files:

```text
/usr/local/bin/nightshift
/etc/nightshift/config.yaml
/var/lib/nightshift/nightshift.db
/var/lib/nightshift/cache/
/var/log/journal/ or journald
```

The systemd unit should:

- Run as an unprivileged user
- Use a private data directory
- Restart on failure with a delay
- Set a file-descriptor limit
- Deny unnecessary privileges
- Use a memory limit only after measuring realistic peaks
- Allow graceful shutdown

Example unit shape:

```ini
[Unit]
Description=NightShift always-on agent
After=network-online.target
Wants=network-online.target

[Service]
User=nightshift
Group=nightshift
ExecStart=/usr/local/bin/nightshift --config /etc/nightshift/config.yaml
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/nightshift

[Install]
WantedBy=multi-user.target
```

Adjust systemd hardening only after verifying that SQLite, certificate discovery, and optional tools still work.

### 17.2 Cross-compilation

The build should produce at least:

```text
linux/arm64
linux/amd64
```

The build should be reproducible from a clean checkout. Record the compiler version and dependency lockfile in the repository.

### 17.3 Docker development deployment

Docker Compose is useful for development and judging on a modern machine:

```text
nightshift edge daemon
falkordb server/browser
```

Do not require Docker on the low-resource target. Provide the native binary path as the primary installation.

### 17.4 FalkorDB placement

Supported deployment choices:

1. FalkorDB Cloud
2. FalkorDB on another LAN machine
3. FalkorDB on the same old PC
4. FalkorDB container on the target device after architecture testing

The edge daemon shall work when FalkorDB is remote. It shall expose clear latency and connectivity errors rather than silently falling back to a different graph database.

### 17.5 UNO Q deployment

The UNO Q should run:

- The native ARM64 NightShift binary
- A systemd service
- The local web server
- The scheduler and collectors

The first hardware deployment test shall measure:

- Binary startup time
- Idle resident memory
- One source-run memory peak
- CPU time for deterministic ranking
- Network recovery behavior
- SQLite write performance
- Whether the selected FalkorDB image/client architecture works

If FalkorDB cannot run reliably on the UNO Q, use remote FalkorDB without changing the product contract.

---

## 18. Testing strategy

### 18.1 Unit tests

Test:

- URL canonicalization
- Content hashing
- Feed normalization
- Deduplication
- Time parsing
- Ranking weights
- Blocklist behavior
- Prompt size limits
- Configuration validation
- Redaction
- Idempotency key generation
- Action state transitions

### 18.2 Integration tests

Use a disposable FalkorDB instance to test:

- Graph migrations
- User/profile/project writes
- Relationship creation
- Multi-hop queries
- Evidence retrieval
- Task handoffs
- Cross-session memory

Use a fake HTTP server for feed providers. Do not use live internet calls in the default test suite.

### 18.3 Recovery tests

Test:

- Process termination during a fetch
- Process termination after a GitHub request but before journal completion
- FalkorDB unavailable
- Model provider unavailable
- Source timeout
- Malformed feed
- Full queue
- Expired lease
- Duplicate scheduler trigger
- Read-only data directory

### 18.4 Resource tests

Run a repeatable fixture workload:

```text
10 sources
500 normalized items
1 user
1 project
20 requirements
100 graph relationships per source run
5 model shortlist candidates
```

Record:

- Peak RSS
- CPU time
- Wall-clock run time
- SQLite database size
- Graph query latency
- Number of HTTP requests
- Number of model requests
- Number of goroutines

Run on the target board and on the old PC class that the team claims to support. Do not claim a benchmark for hardware that was not tested.

### 18.5 Acceptance smoke test

The release smoke test shall:

1. Start FalkorDB.
2. Start NightShift with synthetic fixtures.
3. Create a profile and project.
4. Run the fixture source collector.
5. Verify graph relationships exist.
6. Verify a recommendation appears.
7. Submit feedback.
8. Restart NightShift.
9. Verify the feedback and recommendation are still available.
10. Approve a local plan action.
11. Verify the action journal prevents a duplicate.

---

## 19. Observability and diagnostics

### 19.1 Structured logs

Every log event should include:

```text
timestamp
level
component
request_id or task_id
event
duration_ms, when relevant
error_code, when relevant
```

Do not log complete prompts, source bodies, tokens, or full user messages by default.

### 19.2 Metrics

Track bounded counters and histograms:

```text
source_fetch_total{source, status}
source_fetch_duration_ms{source}
items_normalized_total
items_deduplicated_total
graph_query_total{query, status}
graph_query_duration_ms{query}
model_request_total{provider, status}
task_total{kind, status}
action_total{type, status}
queue_depth
process_resident_bytes
process_cpu_percent
```

Avoid high-cardinality labels such as URLs, user text, or arbitrary item IDs.

### 19.3 Diagnostic bundle

Provide a command that exports a sanitized diagnostic bundle containing:

- Version
- OS and architecture
- Configuration keys with values redacted
- Recent error codes
- Queue counts
- Dependency status
- Resource readings

It must not export secrets or raw private data.

---

## 20. Implementation priorities

### Must have

- Go daemon
- SQLite durable queue
- FalkorDB connection and graph migration
- Two public source adapters
- User profile and project context
- Deterministic graph ranking
- Evidence-backed recommendation
- Persistent memory
- One agent handoff
- Approval workflow
- Local plan or GitHub sandbox issue
- Static web UI
- Restart recovery
- README and demo fixtures

### Should have

- Public GitHub release adapter
- Public job-feed adapter
- Graph path visualization
- Model shortlist and cache
- Systemd unit
- Low-resource mode
- Resource status panel
- Basic test fixture benchmark

### Could have

- YouTube feed adapter
- Email or local notification
- Physical UNO Q LED/button integration
- Branch creation and pull request generation
- Tailscale-friendly deployment notes
- User export and deletion UI

### Will not have in the hackathon MVP

- Private LinkedIn ingestion
- Full browser scraping
- Autonomous production deployment
- Automatic pull-request merging
- A local large model requirement
- Multiple independent backend services
- A full enterprise/company knowledge product

---

## 21. Definition of done

NightShift is ready for the hackathon demo when:

- The public repository builds from a clean checkout.
- A documented command starts the service.
- FalkorDB is the primary graph context layer.
- The graph model and queries are documented.
- Public or synthetic data powers the demo.
- A recommendation depends on a graph relationship.
- The recommendation includes evidence and an explanation path.
- Feedback and a decision survive a restart.
- At least two agents share a persisted task or handoff.
- An external action is disabled until explicitly approved.
- Duplicate action retries are safe.
- The system runs in low-resource mode.
- At least one ARM64 or old Linux machine has been tested.
- Actual memory and CPU measurements are recorded.
- No secrets are present in the repository or demo video.
- AI coding assistant usage is disclosed.
- The team can explain every important architectural decision.

---

## 22. Recommended first implementation checklist

1. Create a Go module and a single `cmd/nightshift` binary.
2. Add configuration loading and redacted startup logs.
3. Add SQLite migrations for jobs, cache, provider cursors, and action journal.
4. Add a FalkorDB client wrapper with parameterized queries and timeouts.
5. Create the first graph migration for `User`, `Interest`, `Project`, `Requirement`, `Source`, and `Item`.
6. Implement one RSS adapter with conditional requests and a 2 MiB response limit.
7. Implement URL canonicalization and content hashing.
8. Write normalized items and relationships to FalkorDB.
9. Add deterministic graph-based ranking.
10. Serve the first briefing page.
11. Add feedback and restart recovery.
12. Add persistent task handoff between Relevance and Planner agents.
13. Add local Markdown plan generation.
14. Add GitHub sandbox writes only after approval is working.
15. Add the second source adapter.
16. Measure memory and CPU on the target board.
17. Add low-resource mode and adjust defaults from measurements.
18. Record the demo and finish the README.

The guiding implementation rule is simple:

> Keep the edge process small, keep the graph meaningful, keep model calls selective, and keep every external action bounded and explainable.
