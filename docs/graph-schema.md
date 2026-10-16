# Graph schema and first Cypher path

## Nodes

| Label | Role |
|---|---|
| `User` | isolated user identity and preferences |
| `Interest` | a topic the user follows |
| `Project` | active work context |
| `Requirement` | a structured project need |
| `Technology` | a named tool, framework, model, or platform |
| `Source` | an approved public feed or API |
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

## Relationships

```text
User -[:INTERESTED_IN]-> Interest
User -[:HAS_SKILL]-> Skill
User -[:OWNS]-> Project
Project -[:HAS_REQUIREMENT]-> Requirement
Requirement -[:NEEDS]-> Technology
Item -[:FROM]-> Source
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
