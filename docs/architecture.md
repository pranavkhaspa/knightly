# FalkorDB-first architecture

## Decision

NightShift uses one Go modular monolith and FalkorDB as the authoritative
store for all durable product state. This deliberately supersedes the draft
specification's SQLite operational-store proposal for the hackathon MVP.

SQLite is not used. If a later implementation needs a crash-replay spool, it
must be an at-least-once transport buffer only: it is deleted after graph
acknowledgement and cannot be queried for user context, tasks, memory,
recommendations, decisions, or action state.

## Graph topology

```text
nightshift_control
  ├── user → graph mapping
  ├── graph schema version
  └── deployment metadata

nightshift_u_<stable-id>
  ├── profile and project context
  ├── sources and normalized items
  ├── recommendations and evidence
  ├── sessions, memories, and decisions
  ├── agents, tasks, and handoffs
  ├── tool calls and run state
  └── action proposals, approvals, and idempotency state
```

Graph names must be derived from a stable, non-display identifier. Never use a
raw email address, display name, token, or user-provided string in a graph
name. Every user-owned node also carries `tenant_id` or `user_id` for
defense-in-depth query filtering.

## Critical path invariant

No recommendation is valid without a qualifying FalkorDB path. The first
recommendation query will traverse:

```text
Project -[:HAS_REQUIREMENT]-> Requirement
         -[:NEEDS]-> Technology
Technology <-[:MENTIONS]- Item -[:FROM]-> Source
```

The recommendation records the query name, ranking version, matched IDs,
source, evidence, and a readable path. The UI must render the path returned by
the graph instead of asking a model to invent an explanation after the fact.

## MCP boundary

MCP is a narrow, typed tool surface over application services. It is not an
arbitrary Cypher endpoint. Planned tools:

- `context.search_relevant_items`
- `context.explain_recommendation`
- `memory.retrieve`
- `planner.propose_plan`
- `feedback.record`

Every invocation becomes a `ToolCall` node linked to a task and session. The
model cannot choose a graph name, execute arbitrary Cypher, run shell commands,
fetch arbitrary URLs, access credentials, or perform a direct external write.

## Evolving memory

Memory is append-oriented and typed: `episodic`, `semantic`, `procedural`, or
`operational`. New memories supersede old memories through an explicit
`SUPERSEDES` relationship. Historical decisions are not overwritten, and every
memory that influences a recommendation keeps its evidence links.

The retrieval order is current task/session, active project, recent decisions
and feedback, stable preferences, then older graph-connected memories. Each
retrieval is bounded by rows and bytes.

## Multi-instance coordination

Multiple daemons can share a user graph. Scheduler and task leases are
graph-backed, keyed by source or task ID, with an instance heartbeat and
lease token. Stable IDs make replay idempotent. A stale worker must not be
able to complete a task after another instance has recovered it.

The concurrency harness is still a planned acceptance check; no concurrency
claim is made until it records actual measurements.

## Portability rule

The edge daemon is a single static Go binary for Linux ARM64 and x86_64. It
serves the static UI itself and uses bounded workers. FalkorDB may run on the
same old PC/VPS or remotely. Render instances are treated as stateless web
workers and must share a durable FalkorDB endpoint.
