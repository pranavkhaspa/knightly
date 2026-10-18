# Graph schema and first Cypher path

## Nodes

| Label | Role |
|---|---|
| `User` | isolated user identity and preferences |
| `Interest` | a topic the user follows |
| `Skill` | a capability the user has |
| `Project` | active work context |
| `Requirement` | a structured project need |
| `Technology` | a named tool, framework, model, or platform |
| `Source` | an approved public feed or API |
| `SourceCursor` | conditional HTTP cursor and last collection timestamps |
| `SourceRun` | bounded source attempt, outcome, counts, and error |
| `Item` | a normalized public signal |
| `Recommendation` | a graph-supported candidate suggestion |
| `Evidence` | source or observation supporting a conclusion |
| `Session` | a user/agent interaction session |
| `Memory` | typed evolving durable context |
| `Decision` | proposed or approved user decision |
| `Agent` | bounded specialist worker |
| `Task` | persisted work and handoff state |
| `Action` | bounded proposed or completed write |
| `ToolCall` | selected MCP/application tool invocation |
| `Instance` | daemon heartbeat and ownership evidence |
| `ProviderAccount` | bounded connected external account (for example Gmail) metadata |
| `MailThread` | normalized email thread reference |
| `MailMessage` | bounded normalized email record with hostile-origin marker |
| `CalendarSource` | normalized calendar reference |
| `CalendarEvent` | bounded normalized event with hostile-origin marker |
| `Reminder` | bounded reminder with timezone, recurrence, lease, and status |
| `InboxItem` | bounded, inert notification surfaced to the user |
| `Repository` | allowlisted GitHub repository reference (`owner/name`) |
| `ExternalObject` | bounded GitHub issue/PR/check/release reference with hostile-origin marker |

`Evidence` nodes are bounded and scope-tagged (`tenant_id`, `user_id`). The
`evidence_kind` property distinguishes provenance; web-search citations use
`evidence_kind = "web_search"` with `url`, `title`, `excerpt`, `content_hash`,
and `observed_at`, and mark their text as untrusted external input with
`untrusted_origin = "web"`. Raw page bodies are never stored.

`ProviderAccount` nodes store only account metadata (`provider`, scope, status,
and timestamps) for a connected external service. OAuth refresh tokens are
never stored here. `MailMessage` nodes keep bounded headers, snippets, an
excerpt, and attachment metadata (`attachments_json`) only; message bodies are
never stored. They are linked by `(ProviderAccount)-[:HAS_MESSAGE]->(MailMessage)`
and `(MailMessage)-[:IN_THREAD]->(MailThread)`, and mark their text with
`untrusted_origin = "mail"`.

`CalendarEvent` nodes keep bounded summaries, a description excerpt, location,
start/end, an `all_day` flag, status, attendee metadata (`attendees_json`), and
source links only; token material is never stored. They are linked by
`(ProviderAccount)-[:HAS_EVENT]->(CalendarEvent)` and
`(CalendarEvent)-[:IN_CALENDAR]->(CalendarSource)`, and mark their text with
`untrusted_origin = "calendar"`.

`Reminder` nodes store a bounded title and body, `due_at`, `timezone`,
`recurrence_json`, `quiet_hours_json`, `status`, `lease_until`, and
`idempotency_key`. Delivery creates a deduplicated `InboxItem` node linked by
`(Reminder)-[:DELIVERED_AS]->(InboxItem)`; the item id is the per-occurrence key,
so replays after a restart never duplicate a delivery.

`ExternalObject` nodes store only bounded GitHub references: `provider`,
`object_type` (`repository`, `issue`, `pull_request`, `check`, `release`),
`object_id`, `repository`, `title`, an excerpt `summary`, `link`, `sha`, `state`,
and `observed_at`. Their text is marked with `untrusted_origin = "github"`. They
are linked by `(ProviderAccount)-[:HAS_OBJECT]->(ExternalObject)` and
`(ExternalObject)-[:IN_REPOSITORY]->(Repository)`. The `ProviderAccount`
(`provider = "github"`) records ownership per allowlisted repository; commit
SHAs and links are persisted, but no GitHub credential is ever stored here.

## Relationships

```text
User -[:INTERESTED_IN]-> Interest
User -[:HAS_SKILL]-> Skill
User -[:PREFERS]-> Technology
User -[:OWNS]-> Project
Project -[:HAS_REQUIREMENT]-> Requirement
Requirement -[:NEEDS]-> Technology
Item -[:FROM]-> Source
Source -[:HAS_CURSOR]-> SourceCursor
Source -[:HAS_RUN]-> SourceRun
Item -[:MENTIONS]-> Technology
Item -[:ABOUT]-> Interest
Item -[:RELATES_TO]-> Project
Recommendation -[:FOR]-> User
Recommendation -[:ABOUT]-> Item
Recommendation -[:MATCHES]-> Requirement
Recommendation -[:SUPPORTED_BY]-> Evidence
Recommendation -[:SUGGESTS]-> Action
Action -[:BASED_ON]-> Recommendation
Session -[:CONTAINS]-> Memory
Memory -[:SUPERSEDES]-> Memory
Memory -[:DERIVED_FROM]-> Evidence
Task -[:ASSIGNED_TO]-> Agent
Task -[:DEPENDS_ON]-> Task
Task -[:HANDED_OFF_TO]-> Task
Task -[:PRODUCES]-> Evidence
ToolCall -[:EXECUTED_FOR]-> Task
Action -[:TARGETS]-> Project
User -[:APPROVED]-> Action
ProviderAccount -[:HAS_MESSAGE]-> MailMessage
MailMessage -[:IN_THREAD]-> MailThread
ProviderAccount -[:HAS_EVENT]-> CalendarEvent
CalendarEvent -[:IN_CALENDAR]-> CalendarSource
Reminder -[:DELIVERED_AS]-> InboxItem
ProviderAccount -[:HAS_OBJECT]-> ExternalObject
ExternalObject -[:IN_REPOSITORY]-> Repository
```

## Foundation query

The first vertical slice will use this bounded, parameterized query. It is
kept in the graph repository rather than in an HTTP handler or agent prompt.

```cypher
MATCH path = (p:Project {id: $project_id})
  -[:HAS_REQUIREMENT]->(r:Requirement)
  -[:NEEDS]->(tech:Technology)
  <-[:MENTIONS]-(item:Item)
  -[:FROM]->(source:Source)
WHERE item.id = $item_id
RETURN p.id AS project_id,
       r.id AS requirement_id,
       tech.id AS technology_id,
       item.id AS item_id,
       source.id AS source_id,
       nodes(path) AS path_nodes
LIMIT 10
```

If it returns no path, the item is not a recommendation. It may be displayed
as unconnected ingestion data, but it must not receive a graph-backed
recommendation score.
