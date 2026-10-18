# NightShift production implementation checklist

Status: implementation in progress; completed items are checked below

## Locked product decisions

- [ ] Keep NightShift as a Go modular monolith with an embedded static frontend.
- [ ] Keep FalkorDB as the only authoritative durable product and coordination store.
- [ ] Do not add SQLite as a product database. A future transport-only replay spool
      must never answer product queries.
- [ ] Make the browser experience chat-first rather than dashboard-first.
- [ ] Make OpenCode the first-class coding-agent integration.
- [ ] Remove Freebuff/FreeBuf as a separate integration target.
- [ ] Use SearXNG as the initial no-card web-search provider. Support a private
      self-hosted instance first; allow a configured remote instance later.
- [ ] Use Gmail read-only and Google Calendar read-only as the initial private
      integrations, through Google OAuth.
- [ ] Require explicit approval for every external write: Gmail changes or sends,
      calendar changes, GitHub writes, commits, pushes, pull requests, and deploys.
- [ ] Allow OpenCode to edit only a bounded isolated task workspace after the user
      requests a coding task. Publishing or changing anything outside that workspace
      remains approval-gated.
- [ ] Keep deterministic behavior usable when providers or an optional model are
      unavailable.

## Current baseline

- [x] FalkorDB-backed profile, project, requirements, sources, recommendations,
      evidence paths, memory, decisions, agent tasks, leases, and actions exist.
- [x] RSS/Atom and public GitHub release ingestion exist with bounded HTTP behavior.
- [x] Scheduler pause/resume, source leases, idempotency, and stale recovery exist.
- [x] Typed MCP-style tools exist for context, memory, planning, and feedback.
- [x] Approval-gated local Markdown plan actions exist.
- [x] Authenticated non-loopback deployment behavior exists.
- [x] Docker Compose, systemd, Render, AMD64, and ARM64 assets exist.
- [ ] A reachable FalkorDB service is available for live verification.
- [x] A chat conversation/message/event model exists.
- [x] A replayable SSE chat event endpoint exists.
- [x] The UI is chat-first.
- [ ] SearXNG, Gmail, Google Calendar, and OpenCode integrations are complete.
- [ ] A complete morning report/inbox flow exists.

## Phase 0 — contracts and graph schema

### Durable FalkorDB records

- [x] Add `Conversation` records with tenant, user, project, title, status, and
      timestamps.
- [x] Add ordered `Message` records with role, bounded content, citations, and
      tool/task references.
- [x] Add `Turn` records with idempotency key, status, and terminal result.
- [x] Add ordered `Event` records with a per-stream sequence number.
- [x] Add `Run`/`AgentRun` records with provider and provider-session metadata.
- [x] Extend tasks with phase, progress, retry, lease, conversation, turn, provider,
      and provider-session fields.
- [ ] Add bounded `Artifact` records for diffs, test results, reports, and links (coding diff/test-result artifacts are implemented; report/link artifact types remain).
- [ ] Add `Briefing` and `InboxItem` records with read/archive state.
- [ ] Add `ProviderAccount` metadata without storing OAuth tokens in FalkorDB.
- [ ] Add `Workspace` records with repository, base SHA, branch, and cleanup state (workspace records are implemented; target-branch selection/publishing remains).
- [ ] Add approval/action records for every external write.

Required relationships:

```text
Conversation -[:CONTAINS]-> Message
Conversation -[:HAS_TURN]-> Turn
Turn -[:EMITS]-> Event
Turn -[:CREATES]-> Task
Turn -[:USES]-> ToolCall
Turn -[:PRODUCES]-> Artifact
Turn -[:CITES]-> Evidence
Task -[:RUNS_AS]-> AgentRun
AgentRun -[:USES]-> Workspace
AgentRun -[:PRODUCES]-> Artifact
Briefing -[:INCLUDES]-> Recommendation
Briefing -[:CREATES]-> InboxItem
Reminder -[:CREATES]-> InboxItem
ProviderAccount -[:BELONGS_TO]-> User
ExternalObject -[:FROM_PROVIDER]-> ProviderAccount
```

### Event contract

All providers normalize into bounded events such as:

```text
conversation.created
message.created
turn.started
turn.progress
assistant.delta
assistant.completed
tool.started
tool.progress
tool.completed
agent.started
agent.output
agent.approval_required
workspace.created
workspace.diff_available
workspace.tests_started
workspace.tests_completed
task.created
task.updated
task.completed
task.failed
task.cancelled
report.started
report.ready
report.failed
approval.required
approval.approved
approval.rejected
action.started
action.completed
action.failed
run.completed
run.failed
```

Every event includes `event_id`, `sequence`, `tenant_id`, `user_id`,
`conversation_id`, `turn_id`, `task_id`, `run_id`, `created_at`, `event_type`,
and bounded data.

- [x] Persist events in FalkorDB before publishing them to a client.
- [x] Make event sequences monotonic per conversation/run stream.
- [x] Implement replay from `Last-Event-ID`.
- [x] Make terminal events idempotent.
- [ ] Never persist tokens, cookies, full private payloads, or unbounded raw text.
- [ ] Mark all web, mail, calendar, and GitHub content as untrusted external input.

## Dev1 — chat product and user experience

### Conversation APIs

- [x] `GET /api/v1/conversations`
- [x] `POST /api/v1/conversations`
- [x] `GET /api/v1/conversations/{id}`
- [x] `PATCH /api/v1/conversations/{id}`
- [x] `GET /api/v1/conversations/{id}/messages`
- [x] `POST /api/v1/conversations/{id}/messages`
- [x] `POST /api/v1/conversations/{id}/cancel`
- [x] `GET /api/v1/conversations/{id}/events`
- [x] Use trusted request scope rather than client-supplied tenant/user IDs.
- [x] Enforce bounded message size and pagination.
- [x] Persist user, assistant, system, tool, and progress messages distinctly.

### Chat execution

- [x] Add an `internal/chat` service with deterministic intent routing.
- [x] Retrieve bounded conversation, project, memory, decision, and graph context.
- [x] Invoke only typed application tools.
- [x] Persist citations, tool calls, task references, and assistant output.
- [x] Support idempotent retry of a turn.
- [x] Support cancellation without losing terminal state.

Initial intents:

```text
what_changed
search_web
search_sources
read_mail
calendar_today
create_reminder
morning_report
why_does_this_matter
explain_recommendation
show_task_status
build_demo
review_diff
propose_github_action
```

### Streaming and task state

- [x] Add SSE with heartbeats and bounded write/idle behavior.
- [x] Support reconnect with `Last-Event-ID`.
- [x] Prevent browser refresh from duplicating agent work.
- [x] Add task list/detail/progress/retry/cancel endpoints.
- [x] Add task lease heartbeat and stale recovery.
- [x] Emit an event for every task transition.
- [x] Keep transitions explicit: pending, running, blocked, completed, failed,
      and cancelled.

### Briefing and inbox

- [x] Create an idempotent daily briefing per user/project/date.
- [x] Include graph recommendations, source evidence, web results, mail, calendar,
      GitHub, reminders, unresolved tasks, and pending approvals.
- [x] Show partial-provider failures instead of hiding them.
- [x] Add `GET /api/v1/briefings`, detail, manual run, and read endpoints.
- [x] Add inbox list/detail/read/archive endpoints.
- [x] Deliver reports to chat and inbox.
- [x] Respect timezone and quiet hours.

### Frontend

- [x] Replace the dashboard landing page with a chat-first layout.
- [x] Add conversation sidebar and new-conversation flow.
- [x] Add streamed assistant output and progress cards.
- [x] Add citations, source detail, and FalkorDB graph-path drawer.
- [x] Add task progress, diff, test result, and approval cards.
- [x] Add morning report, inbox, search, calendar, reminder, and health panels.
- [x] Add reconnect/error/empty states and mobile layout.
- [x] Keep the deployed frontend static and embedded; do not require Node.js at
      runtime.

## Approval policy

- [ ] Represent every external write as a durable idempotent action.
- [ ] Show target, proposed effect, provider, scope, evidence, diff, and tests before
      approval.
- [ ] Derive approver identity from trusted auth middleware.
- [ ] Never accept `approved_by` from JSON, query parameters, or headers.
- [ ] Require approval before Gmail send/modify, calendar changes, GitHub writes,
      commits, pushes, pull requests, and deployment actions.
- [ ] Never auto-merge, force-push, or deploy production.
- [ ] On uncertain provider responses, query the provider before retrying.

## Dev2 — OpenCode orchestration

### Provider-neutral adapter

Define an adapter boundary that can normalize a coding run without coupling the
rest of NightShift to OpenCode internals:

```go
type AgentAdapter interface {
    Name() string
    Capabilities(context.Context) (Capabilities, error)
    Start(context.Context, RunRequest) (RunHandle, error)
    Events(context.Context, RunHandle) (<-chan AgentEvent, error)
    Cancel(context.Context, RunHandle) error
    Resume(context.Context, ProviderSessionID, RunRequest) (RunHandle, error)
    Diff(context.Context, RunHandle) (DiffResult, error)
    Tests(context.Context, RunHandle) (TestResult, error)
}
```

- [x] Normalize provider output into the shared event contract (bounded event fields; raw provider payloads are not forwarded).
- [x] Add run deadlines, bounded provider events/output, cancellation, and shutdown interruption (external process supervision is not applicable to the HTTP provider).
- [x] Persist provider session/run IDs in FalkorDB.
- [ ] Make start, resume, cancel, diff, and tests idempotent where possible.

### Workspace safety

- [x] Create a dedicated Git worktree or temporary workspace per coding task.
- [x] Record repository, base commit SHA, provider, and workspace in FalkorDB.
- [ ] Add target-branch selection and approval-gated publishing.
- [ ] Prevent concurrent mutation of a shared checkout.
- [ ] Restrict reads/edits/shell commands to the configured workspace.
- [x] Capture bounded diffs and test output as graph-backed artifacts before cleanup.
- [ ] Clean up only after durable completion/failure state is written.

### OpenCode adapter

Use the current OpenCode HTTP API/server mode rather than scraping terminal output.
The verified API surface includes sessions, prompts, events, interruption, diffs,
worktrees, VCS status/diff, permissions, and web search.

- [x] Discover/configure the OpenCode server endpoint through deployment configuration; live endpoint smoke test remains.
- [ ] Create a session with the bounded task workspace and explicit permissions.
- [x] Send the task prompt with bounded external context JSON-encoded and marked as untrusted data.
- [x] Subscribe to the OpenCode durable session event log and normalize bounded progress events.
- [x] Support interruption and session resume; automatic startup recovery orchestration remains.
- [x] Retrieve diffs and run configured, allowlisted test commands in the workspace.
- [x] Capture graph-backed diff/test artifacts and provider/run metadata.
- [ ] Configure OpenCode permissions so read/edit/shell access is limited to the
      worktree, shell is allowlisted or approval-gated, and pushes are denied by
      default.
- [ ] Do not allow a provider prompt or external file to change NightShift policy.
- [x] Add conformance tests against a fake OpenCode HTTP server.
- [ ] Add an opt-in test against a real local OpenCode server when available.

### Coding workflow

```text
user requests build
→ NightShift creates task and isolated workspace
→ OpenCode edits workspace
→ tests run
→ diff and artifacts are persisted
→ user reviews result
→ approval action is created
→ approved commit/branch/PR action executes exactly once
→ provider and GitHub result are persisted
```

- [x] Verify the configured repository allowlist and project-to-repository mapping.
- [ ] Add target-branch selection and approval-gated publishing.
- [ ] Verify base SHA before starting and before publishing.
- [x] Require test results; failed tests leave the task blocked for review.
- [ ] Use least-privilege GitHub credentials.
- [ ] Verify commit/PR results after an uncertain response.

## Dev3 — search, Gmail, Calendar, reminders, and operations

### SearXNG search

SearXNG is the P0 search provider because it does not require a Brave API key or
credit card. Its documented `/search` endpoint supports JSON when enabled in the
instance settings.

- [ ] Add a `SearchProvider` interface and normalized result type.
- [x] Add SearXNG base URL configuration.
- [ ] Require HTTPS for remote instances unless explicitly in development.
- [ ] Configure a private/local SearXNG instance with JSON output enabled.
- [ ] Pin the optional SearXNG container/image when adding it to local Compose.
- [ ] Bound query, page, result count, title, snippet, and URL lengths.
- [ ] Add timeout, retry classification, rate limiting, and circuit breaking.
- [ ] Store citations/evidence in FalkorDB without storing unbounded page bodies.
- [ ] Treat all result text as untrusted input.
- [x] Add a deterministic HTTP fake-provider test suite.
- [ ] Optionally use OpenCode's built-in websearch only as a coding-session fallback;
      it must not become a hidden NightShift policy or credential dependency.

### Gmail read-only

- [ ] Add a `MailReader` interface.
- [ ] Use Gmail API read-only OAuth scope for P0.
- [ ] Add account connect/disconnect/status routes.
- [ ] Store OAuth refresh tokens only in a protected secret store or local protected
      credential file; never in FalkorDB, logs, prompts, or artifacts.
- [ ] Store only provider account metadata and bounded normalized mail records in
      FalkorDB.
- [ ] Add message/thread cursor and deduplication.
- [ ] Bound headers, body excerpts, and attachment metadata.
- [ ] Do not send, delete, label, archive, or mark mail read in the read-only phase.
- [ ] Treat email bodies and attachments as hostile external instructions.
- [ ] Add fake OAuth/provider fixtures and scope-isolation tests.

### Google Calendar read-only

- [ ] Add a `CalendarReader` interface.
- [ ] Use Google Calendar read-only OAuth scope for P0.
- [ ] Normalize event start/end times using the user's configured timezone.
- [ ] Support today and upcoming-event queries.
- [ ] Persist bounded event metadata and source references.
- [ ] Keep event creation, edits, attendee changes, and invitations approval-gated.
- [ ] Add fake API fixtures, pagination, timezone, and scope-isolation tests.

### Reminders and notifications

- [ ] Add graph-backed one-shot and recurring reminders.
- [ ] Store timezone, quiet hours, status, lease, and idempotency key.
- [ ] Reconcile missed reminders after restart without duplicate delivery.
- [ ] Deliver reminders to the inbox first.
- [ ] Keep optional webhook/local notification sinks disabled until configured.
- [ ] Require approval for any reminder delivery that creates an external side effect.

### GitHub

- [ ] Add read-only issues, pull requests, checks, releases, and repository metadata.
- [ ] Add repository allowlists and provider-account ownership.
- [ ] Add approved issue/branch/commit/push/PR operations through the action layer.
- [ ] Use a GitHub App or fine-grained token with least privilege.
- [ ] Never auto-merge or force-push.
- [ ] Store external object IDs, links, commit SHAs, and bounded result summaries.

### Auth and multi-user scope

- [ ] Replace deployment-wide identity with verified OIDC or trusted-proxy identity
      before claiming general multi-user support.
- [ ] Scope provider accounts and graph queries by verified tenant/user.
- [ ] Prevent client-supplied scope or approver overrides.
- [ ] Keep one OpenCode process/workspace boundary per trusted user/task.
- [ ] Document that one OpenCode server is not adversarial multi-tenant isolation.

### Observability and deployment

- [ ] Add structured logs and correlation IDs.
- [ ] Add provider latency/error counters, graph latency, scheduler, task, reminder,
      briefing, approval, and auth metrics.
- [ ] Add optional non-blocking OpenTelemetry/OTLP export.
- [ ] Add redaction tests for credentials, tokens, cookies, prompts, and mail bodies.
- [ ] Keep systemd as the primary production deployment.
- [ ] Keep Docker Compose for local FalkorDB and optional SearXNG development.
- [ ] Pin OpenCode, SearXNG, and FalkorDB versions in deployment documentation.
- [ ] Add backup/restore, migration, rollback, readiness, and secret-rotation steps.
- [ ] Test AMD64 and ARM64 builds.

## Required end-to-end demos

### Chat and search

- [ ] Ask a current-information question in chat.
- [ ] Show streamed search progress and cited SearXNG results.
- [ ] Show graph-backed relevance and evidence path.
- [ ] Demonstrate that result text cannot trigger commands or policy changes.

### Gmail and Calendar

- [ ] Connect a Google account through OAuth.
- [ ] Ask for important recent mail and today's calendar.
- [ ] Show bounded summaries, source links/IDs, and timezone-correct events.
- [ ] Demonstrate that no mail/calendar write occurs without approval.

### Morning report

- [ ] Generate a scheduled or manual report.
- [ ] Include graph recommendations, search, mail, calendar, GitHub, reminders,
      unresolved tasks, and approvals.
- [ ] Surface partial provider failures.
- [ ] Confirm report persistence and inbox delivery after restart.

### Coding task

- [x] Route a chat coding request into a durable coding task and serial worker queue.
- [x] Show OpenCode progress and final status in chat events/messages.
- [x] Persist isolated workspace metadata, diff, and test output.
- [ ] Verify the full chat-to-FalkorDB-to-OpenCode flow against live FalkorDB and a real OpenCode server.
- [ ] Require approval before commit/push/PR.
- [ ] Verify the resulting GitHub object and persist its link/SHA.
- [ ] Confirm retries do not duplicate the external action.

### Recovery

- [ ] Stop NightShift during a running task.
- [ ] Restart with the same FalkorDB graphs.
- [ ] Recover conversation, task, provider session, approval, and report state.
- [ ] Ensure no duplicate briefing, reminder, commit, push, or PR is produced.

## Release gates

- [ ] `checklist.md` matches the implemented provider and approval scope.
- [ ] Chat flow works end to end against live FalkorDB.
- [ ] OpenCode adapter passes fake-server conformance tests.
- [ ] SearXNG adapter passes fake-provider and live-instance smoke tests.
- [ ] Gmail and Google Calendar read-only flows pass fixture tests and a permitted
      manual OAuth smoke test.
- [ ] All external writes are approval-gated and idempotent.
- [ ] Two-user scope isolation tests pass before enabling multi-user deployment.
- [ ] Two-instance lease and stale-recovery tests pass.
- [ ] Restart recovery tests pass.
- [ ] Redaction tests find no secrets in logs, graph data, prompts, or artifacts.
- [ ] `go test ./...` passes.
- [ ] `go test -race ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `gofmt -l .` returns no source files.
- [ ] AMD64 and ARM64 builds pass.
- [ ] Live FalkorDB integration tests pass.
- [ ] Deployment health/readiness checks pass.
- [ ] Any unverified provider capability is documented as unavailable rather than
      claimed as complete.

## Implementation references

- Existing product requirements: `PRD.md`
- Existing system specification: `ps.md`
- Current FalkorDB architecture: `docs/architecture.md`
- Current deployment guidance: `docs/deployment.md`
- OpenCode tools: <https://opencode.ai/docs/tools/>
- OpenCode v2 HTTP API: <https://opencode.ai/v2/docs/api/>
- OpenCode v2 permissions: <https://opencode.ai/v2/docs/permissions/>
- SearXNG search API: <https://docs.searxng.org/dev/search_api.html>
- SearXNG container installation: <https://docs.searxng.org/admin/installation-docker.html>
- Gmail API: <https://developers.google.com/workspace/gmail/api/guides>
- Google Calendar API: <https://developers.google.com/workspace/calendar/api/guides/overview>

## Immediate next implementation slice

1. Commit this plan separately.
2. Add FalkorDB conversation/message/event repositories and tests.
3. Add chat routes and SSE replay before integrating external providers.
4. Add OpenCode adapter against a fake HTTP server.
5. Add SearXNG, Gmail, and Calendar provider interfaces with fakes.
6. Wire the first chat intent (`search_web`) through persistence, events, and UI.
7. Add the remaining integrations only after that vertical slice is green.
