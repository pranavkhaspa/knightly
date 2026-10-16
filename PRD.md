# NightShift Product Requirements Document

**Product:** NightShift
**Subtitle:** An always-on, graph-powered developer intern
**Document type:** Product Requirements Document
**Status:** Draft for implementation
**Version:** 0.1
**Last updated:** 2026-10-15

---

## Hackathon implementation focus (2026-10-16)

For the FalkorDB hackathon implementation in this repository, FalkorDB is the
authoritative store for all durable product and coordination state: profile,
project context, recommendations, evidence, memory, decisions, agent tasks,
leases, and action state. The MVP must not duplicate those records in SQLite
or another relational database. A temporary replay buffer may exist only for
uncommitted transport payloads and must never answer product queries. See
`docs/architecture.md` for the implementation decisions that narrow this
draft PRD.

---

## 1. Executive summary

NightShift is a small, always-on personal agent that runs on inexpensive hardware such as a Raspberry Pi, Arduino UNO Q, or an older PC. It watches approved public information sources, connects new information to a user's interests and active projects, remembers previous conversations and decisions, and prepares useful actions for the user.

The product is deliberately narrower than a general autonomous AI assistant. Its first job is to answer four questions reliably:

1. **What changed while I was away?**
2. **Why is it relevant to my interests, skills, or project?**
3. **What evidence and relationships led to that conclusion?**
4. **What is the safest useful next action?**

FalkorDB is the product's context and coordination layer. It stores the relationships between users, interests, skills, projects, requirements, sources, articles, jobs, decisions, memories, agents, and tasks. NightShift must use those relationships to rank information, coordinate agents, resume work, and explain recommendations. FalkorDB must not be an ornamental graph displayed beside a conventional chatbot.

The Arduino UNO Q or similar low-power computer is the always-on edge runtime. It schedules work, collects public feeds, serves the web interface, stores a small local queue, and calls the graph and model services. It is not expected to run a large language model locally by default.

The initial product will focus on a developer's information and project workflow:

- Public AI and software-development signals
- Public technical videos and articles
- Public job postings from permitted sources
- A user's active project, PRD, skills, and interests
- Approved project actions in a sandbox GitHub repository

The MVP will not promise to build arbitrary software autonomously. It will create a traceable brief, implementation plan, task, issue, or one bounded code change after user approval.

---

## 2. Product decision summary

### 2.1 Recommended hackathon tracks

NightShift should enter:

- **Track 02: Agent Memory and Coordination** — primary track
- **Track 01: Agents That Act on Connected Data** — secondary track

Track 03, Company Brain, is not part of the initial scope. It may be considered later if NightShift becomes a team workspace that connects people, documents, projects, tickets, repositories, and decisions.

### 2.2 Core product sentence

> NightShift is an always-on developer intern that uses FalkorDB to connect public technical signals to a user's interests and projects, remember decisions across sessions, coordinate specialist agents, and prepare evidence-backed next actions.

### 2.3 Core demo sentence

> While the user is asleep, NightShift collects public signals, finds the ones connected to the user's current project, explains the graph path behind each recommendation, and prepares an approved GitHub task that is still available in the next session.

### 2.4 What must be true for the product to qualify

If FalkorDB is removed, the product must lose essential behavior:

- It cannot perform the same multi-hop relevance reasoning.
- It cannot recover durable cross-session decisions.
- It cannot coordinate agents through shared graph state.
- It cannot show the evidence path behind an answer.
- It cannot preserve relationships between a project, its requirements, related technologies, and proposed actions.

If those behaviors still work unchanged without FalkorDB, the implementation is not using the graph centrally enough.

---

## 3. Problem statement

Developers and independent builders are exposed to more information than they can evaluate. A single day may include:

- New AI libraries and model releases
- Open-source project updates
- Research papers
- Technical videos
- Community discussions
- Job opportunities
- Project ideas
- Unfinished implementation decisions
- GitHub tasks and code review work

The important answer is rarely present in one record. It depends on relationships:

- An article is relevant because it mentions a technology required by the user's current project.
- A job is relevant because its required skills overlap with the user's profile and it is at a company working on a topic the user follows.
- A GitHub issue matters because it blocks a project requirement that was discussed in a previous session.
- A recommendation is trustworthy because it can show the source, the matching interests, the related requirement, and the decision that caused the action.

Typical chat assistants lose this context between sessions or hide it inside unstructured transcripts. Basic RSS readers and keyword searches do not understand project relationships. Fully autonomous coding systems can act too broadly without retaining why a decision was made.

NightShift addresses the gap by maintaining an explicit, queryable context graph and by limiting autonomous actions to bounded, reviewable steps.

---

## 4. Vision

NightShift should feel like a dependable technical intern who is available at any hour:

- It knows what the user is working on.
- It remembers what the user accepted, rejected, and deferred.
- It watches only approved sources.
- It does not flood the user with unfiltered links.
- It explains why something matters.
- It coordinates specialized agents without losing state.
- It asks for approval before actions that can change external systems.
- It continues operating on modest hardware with low power and low maintenance requirements.

The long-term vision is a personal or small-team context layer that turns fragmented information into a durable sequence of well-explained decisions and tasks.

---

## 5. Goals

### 5.1 MVP goals

1. Provide an always-on runtime that can run unattended on a Raspberry Pi, UNO Q, or older PC.
2. Let a user configure interests, skills, an active project, and source preferences.
3. Ingest public feeds without requiring a browser or a heavy headless automation stack.
4. Extract a small set of useful entities and relationships into FalkorDB.
5. Use multi-hop graph queries to rank relevance to the user's project.
6. Generate a concise, evidence-backed morning briefing.
7. Persist user feedback, decisions, memories, and task state across sessions.
8. Coordinate a small group of specialist agents through graph-backed tasks and handoffs.
9. Allow a user-approved action in a public GitHub sandbox, such as creating an issue or implementation plan.
10. Show the graph path and source evidence behind every recommendation.
11. Recover cleanly from network loss, process restarts, duplicate feeds, and temporary provider failures.
12. Keep memory, CPU, storage, network, and API usage small enough for low-resource devices.

### 5.2 Hackathon goals

1. Make FalkorDB visibly central to the product.
2. Demonstrate both persistent memory and graph-driven action.
3. Deliver a public repository with a clear graph schema and Cypher queries.
4. Provide a short, repeatable demo that works from a clean installation.
5. Use public or synthetic data only in the judging demo.
6. Disclose AI coding assistant usage and ensure the team can explain the submitted system.

---

## 6. Non-goals for the MVP

The following are explicitly out of scope for the first release:

- Replacing every general-purpose coding agent
- Building arbitrary software to 90–95% completion without supervision
- Running a large language model locally on every supported device
- Scraping private or login-protected LinkedIn content
- Accessing private repositories or private personal data in the hackathon demo
- Crawling the whole internet
- Supporting dozens of sources before the core flow works
- Running a browser automation fleet or headless Chromium by default
- Autonomous financial, legal, employment, or production infrastructure decisions
- Automatically merging code or deploying to production
- Storing API keys, OAuth tokens, or credentials inside FalkorDB
- Building a company-wide knowledge platform in the first iteration
- Making a graph visualization that is not connected to actual decisions

---

## 7. Target users and personas

### 7.1 Primary persona: independent developer

**Name:** Arjun, independent builder
**Environment:** Raspberry Pi, old laptop, or small home server
**Needs:** Stay current without spending hours reading feeds; maintain project context; turn ideas into tasks.
**Pain:** Saves links but forgets why they mattered; starts projects repeatedly; loses decisions between chat sessions.

### 7.2 Secondary persona: technical job seeker

**Needs:** Find relevant public opportunities instead of receiving generic job spam.
**Pain:** Job boards do not understand their actual project history, preferred stack, or interests.
**Required safeguard:** Use only permitted public job sources and never submit applications automatically.

### 7.3 Secondary persona: small software team

**Needs:** Share project context, tasks, decisions, and agent handoffs.
**Pain:** Decisions live in chat, issues, documents, and individual memory.
**Future opportunity:** This persona can become the basis for a separate Company Brain version.

### 7.4 System operator

The operator may be the user or a teammate who installs NightShift on a small device. They need:

- A simple installation path
- A health page
- Predictable memory usage
- Logs that do not contain secrets
- Safe restart behavior
- Clear configuration files
- A way to pause all external actions

---

## 8. Primary user journeys

### 8.1 First-time setup

1. User opens the local web interface.
2. User creates a local profile.
3. User enters interests, skills, goals, and preferred sources.
4. User adds an active project and a short PRD.
5. User chooses a graph backend: local, remote, or FalkorDB Cloud.
6. User configures an LLM provider or enables deterministic mode.
7. User connects a public GitHub sandbox repository, if desired.
8. NightShift validates the configuration without making external writes.
9. NightShift creates the initial graph and displays the resulting relationships.

### 8.2 Overnight collection

1. The scheduler wakes at the configured interval.
2. Source adapters send conditional requests using ETags or Last-Modified headers.
3. Unchanged feeds are skipped.
4. New items are normalized and deduplicated.
5. Cheap deterministic filtering removes clearly irrelevant items.
6. Relevant items are connected to topics, skills, projects, and requirements.
7. Only the shortlist is sent to an LLM for deeper analysis.
8. The Relevance Agent writes recommendations and evidence links to FalkorDB.
9. The system creates a briefing task for the user.
10. The system returns to idle and records health metrics.

### 8.3 Morning briefing

1. User opens the dashboard.
2. User sees a small number of high-value recommendations.
3. Each recommendation includes:
   - A short summary
   - Why it matters
   - Matching interests, skills, or project requirements
   - Source URL and publication time
   - Graph path or relationship explanation
   - Suggested next action
4. User chooses **Save**, **Ignore**, **Remind me later**, or **Act**.
5. The feedback updates the user's graph and affects future ranking.

### 8.4 Approved project action

1. User selects a recommendation.
2. Planner Agent creates a proposed task and implementation plan.
3. User reviews the proposed action.
4. User explicitly approves the action.
5. GitHub adapter creates an issue or branch in the configured sandbox.
6. NightShift stores the action, approval, target, and result as graph relationships.
7. A later session can retrieve the decision and continue the work.

### 8.5 Cross-session memory

1. User starts a new browser session or restarts the device.
2. User asks what happened previously.
3. NightShift retrieves relevant sessions, decisions, evidence, tasks, and feedback from FalkorDB.
4. The answer distinguishes facts, recommendations, and unresolved work.
5. The user can open the original source and task.

---

## 9. Functional requirements

### FR-001: User profile

The system shall allow the user to create and edit:

- Display name
- Interests
- Skills
- Preferred technologies
- Career or learning goals
- Location preferences, if relevant to public job data
- Content preferences
- Quiet hours
- Notification preferences

The system shall store user profile data in a user-scoped graph. The MVP shall support one active user per installation and shall keep the data model compatible with multiple isolated users later.

### FR-002: Project context

The system shall allow the user to create an active project with:

- Project name
- Description
- Optional PRD text
- Requirements
- Technologies
- Constraints
- Current status
- Public repository URL, if available
- Desired outcomes

The system shall represent important requirements as graph nodes or structured properties instead of storing the complete PRD as an opaque blob only.

### FR-003: Source configuration

The system shall support source subscriptions with:

- Source name
- Source type
- URL or API endpoint
- Polling interval
- Enabled/disabled state
- Maximum items per run
- Retention policy
- Public-data classification

The MVP shall support RSS or Atom feeds and at least one JSON API. Browser automation is not required.

### FR-004: Ingestion

The system shall:

- Fetch sources on a schedule
- Use connection and body-size timeouts
- Reuse HTTP connections
- Send conditional requests
- Validate response content types
- Reject oversized or unsupported responses
- Normalize timestamps
- Generate stable item IDs
- Deduplicate by canonical URL and content hash
- Retry temporary errors with bounded exponential backoff
- Persist unfinished work before shutdown

### FR-005: Graph context

The system shall write and query relationships among:

- Users
- Interests
- Skills
- Projects
- Requirements
- Sources
- Items
- Technologies
- Jobs
- Companies
- Sessions
- Memories
- Decisions
- Agents
- Tasks
- Actions
- Evidence

The graph shall be used in at least one decision-making query on every recommendation run.

### FR-006: Relevance ranking

The system shall rank items using a staged process:

1. Cheap local filters
2. Graph relationship score
3. Recency and source quality
4. Duplicate and novelty penalties
5. Optional LLM analysis for only the top candidates

The ranking explanation shall identify the major contributing relationships.

### FR-007: Morning briefing

The system shall display a configurable number of recommendations, with a default of three to five.

Each recommendation shall contain:

- Title
- Short summary
- Source
- URL
- Published time
- Relevance score or confidence band
- Why it matched
- Graph path or explanation
- Suggested action
- Current state

### FR-008: Persistent memory

The system shall remember:

- User preferences
- Accepted items
- Rejected items
- Deferrals
- Decisions
- Session summaries
- Agent handoffs
- Open tasks
- Completed actions
- Evidence used for decisions

Memory shall survive process restarts and shall be retrievable in a later session.

### FR-009: Agent coordination

The system shall support a bounded set of agents:

- Scout Agent
- Graph Curator Agent
- Relevance Agent
- Planner Agent
- Memory/Reviewer Agent

Each agent task shall have:

- Task ID
- Agent type
- Status
- Priority
- Created time
- Updated time
- Input references
- Output references
- Retry count
- Error state
- Optional dependency

Agents shall communicate through persisted task and handoff records rather than relying only on in-memory prompts.

### FR-010: Human approval

The system shall require explicit approval before:

- Writing to GitHub
- Creating branches or pull requests
- Sending messages externally
- Applying code changes outside a configured workspace
- Running commands that can modify the host system

The user shall be able to disable all external write actions globally.

### FR-011: GitHub sandbox action

The MVP shall support at least one safe GitHub action:

- Create an issue in a configured public sandbox repository, or
- Create a local implementation plan file

Optional actions include branch creation or a small pull request. These are not required for the first end-to-end demo.

### FR-012: Feedback loop

The user shall be able to mark a recommendation as:

- Useful
- Not useful
- Already known
- Do not show similar items
- Save for later
- Act now

Feedback shall be stored as graph data and shall influence future ranking or source preferences.

### FR-013: Scheduling

The system shall support:

- Manual run
- Scheduled run
- Pause/resume
- Quiet hours
- Per-source interval
- Maximum run duration
- Maximum items per run

The scheduler shall not create unbounded work when the device is offline or a provider is failing.

### FR-014: Web interface

The MVP web interface shall include:

- Setup screen
- Dashboard
- Briefing view
- Recommendation detail view
- Graph path view
- Tasks and agent status view
- Settings
- Health/status view

The production device shall serve a prebuilt static frontend without requiring a Node.js runtime.

### FR-015: Health and recovery

The system shall expose:

- Liveness status
- Readiness status
- Last successful source fetch
- Last successful graph query
- Last model request
- Queue depth
- Error count
- Current version

The process shall resume pending work after a crash without duplicating external actions.

### FR-016: Installation

The project shall provide:

- A low-resource native installation using a single compiled binary and systemd
- An optional Docker Compose development setup
- Configuration examples with placeholders only
- Database initialization and migration commands
- A health-check command
- A clean-uninstall procedure

---

## 10. High-level graph model

### Core nodes

| Node | Purpose |
|---|---|
| `User` | User identity and preferences |
| `Interest` | Topic the user follows |
| `Skill` | User capability or job requirement |
| `Project` | Active or historical project |
| `Requirement` | Structured project need |
| `Source` | Public RSS, API, repository, or job board |
| `Item` | Normalized content item |
| `Technology` | Named tool, framework, model, or platform |
| `Job` | Public job posting |
| `Company` | Company associated with a job or item |
| `Session` | Conversation or work session |
| `Memory` | Durable fact or summary |
| `Decision` | User or agent decision |
| `Evidence` | URL, source excerpt, or graph observation |
| `Agent` | Specialist worker |
| `Task` | Work item and handoff state |
| `Action` | Proposed or executed external action |
| `Repository` | Public code repository |
| `Issue` | Repository issue or local task |

### Core relationships

```text
User -[:INTERESTED_IN]-> Interest
User -[:HAS_SKILL]-> Skill
User -[:OWNS]-> Project
Project -[:HAS_REQUIREMENT]-> Requirement
Requirement -[:NEEDS]-> Technology

Item -[:FROM]-> Source
Item -[:ABOUT]-> Interest
Item -[:MENTIONS]-> Skill
Item -[:MENTIONS]-> Technology
Item -[:RELATES_TO]-> Project
Job -[:AT]-> Company
Job -[:REQUIRES]-> Skill

Session -[:CONTAINS]-> Memory
Decision -[:BASED_ON]-> Evidence
Evidence -[:SUPPORTS]-> Item
Task -[:ASSIGNED_TO]-> Agent
Task -[:DEPENDS_ON]-> Task
Task -[:PRODUCES]-> Evidence
Action -[:TARGETS]-> Project
Action -[:CREATES]-> Issue
User -[:APPROVED]-> Action
```

### Graph design rule

Do not place the entire user profile, PRD, article, and session history into one large text property and call it graph memory. Store important entities and relationships explicitly. Keep raw text small, optional, compressed, and subject to retention rules.

---

## 11. Low-resource product requirements

NightShift shall be useful on modest hardware, not only on a modern development laptop.

### Supported hardware tiers

| Tier | Example | Minimum target |
|---|---|---|
| A | UNO Q 4 GB, Raspberry Pi 4/5, old x86 PC with 4 GB RAM | Full local edge runtime; remote or local graph |
| B | Raspberry Pi 3, older dual-core PC with 2 GB RAM | Full edge runtime with lower concurrency and remote model |
| C | Pi Zero 2 W or similar 512 MB–1 GB device | Collector, scheduler, and UI gateway; remote graph/model recommended |

### Resource targets

These are product targets, not assumptions that every device will achieve the same performance:

- Idle resident memory for the edge process: **under 80 MB** in the native build
- Normal active memory during collection: **under 160 MB**, excluding the graph server and browser
- Default worker concurrency on low-resource devices: **1 or 2**
- Maximum in-memory queue: **100 tasks**, with overflow persisted to SQLite
- Maximum feed response retained in memory: **2 MB** by default
- Maximum item text sent to the model: **8–16 KB per item**, configurable
- Default API response time for local dashboard reads: **under 300 ms** when the graph is healthy
- Default device CPU while idle: **below 2% average**, excluding scheduled OS work
- No polling interval shorter than five minutes by default
- No headless browser required for the MVP
- No local embedding model required for the MVP
- No local large language model required for the MVP

### Efficiency principles

1. Prefer a single native daemon over many always-running services.
2. Prefer SQLite for the local queue and cache over Redis, Kafka, or another broker.
3. Use FalkorDB for relationships and durable context, not for every transient log line.
4. Fetch incrementally with ETags, Last-Modified, and content hashes.
5. Filter deterministically before calling an LLM.
6. Batch model requests when possible.
7. Cache summaries by content hash and prompt version.
8. Keep concurrency bounded.
9. Avoid loading complete feeds or documents into memory.
10. Use a static web frontend served by the backend.
11. Make optional features lazy and disabled by default.
12. Use deadlines, circuit breakers, retries, and backpressure.

---

## 12. Privacy, security, and safety requirements

### 12.1 Data boundaries

The hackathon demo shall use public, synthetic, or clearly permitted data. Private or login-protected data shall not be loaded into the judging graph.

### 12.2 Secret handling

- Secrets shall be supplied through environment variables, an OS secret store, or a protected local configuration file.
- Secrets shall never be written to FalkorDB.
- Secrets shall never be included in graph paths, logs, prompts, screenshots, or demo recordings.
- The repository shall contain `.env.example`, never real credentials.

### 12.3 External action safety

- All write actions shall be disabled by default until configured.
- GitHub credentials shall use the minimum required scope.
- The user shall approve each external write in the MVP.
- Actions shall have an idempotency key.
- Duplicate retries shall not create duplicate issues or pull requests.
- Production deployment and destructive shell commands are out of scope.

### 12.4 Prompt and content safety

Fetched content shall be treated as untrusted input. The system shall not allow an article, issue, or web page to override system policies or execute commands merely because it contains instructions.

### 12.5 User control

The user shall be able to:

- Pause collection
- Disable a source
- Delete a source
- Delete graph data
- Clear memories
- Disable external actions
- Export a local project summary

---

## 13. Non-functional requirements

### Reliability

- A single failed source shall not stop the complete run.
- A model provider outage shall not lose collected items.
- A process restart shall preserve queued tasks.
- A network outage shall degrade to local queueing and health reporting.

### Performance

- The device shall avoid unnecessary wakeups.
- Long network operations shall never block dashboard requests.
- Graph queries shall return only required properties.
- The system shall use pagination or bounded result counts.
- Expensive operations shall be visible in status metrics.

### Maintainability

- Modules shall have narrow interfaces.
- Source adapters shall not know about UI details.
- The graph repository shall be the only layer that builds Cypher.
- External providers shall be behind interfaces.
- Every agent task shall be testable with a fake provider.

### Portability

- The edge runtime shall support Linux ARM64 and Linux x86_64.
- ARM32 support is optional but desirable if it does not complicate the build.
- The production device shall not require Node.js, Python, Java, or Docker to be installed when using the native package.

### Observability

- Logs shall be structured and bounded.
- Logs shall not include secrets or full private content.
- Metrics shall count operations rather than record sensitive payloads.
- Every agent task shall have a correlation ID.

---

## 14. Success metrics

### Hackathon acceptance metrics

1. A clean installation completes on at least one ARM device or an older Linux PC.
2. The demo ingests public content and creates graph relationships.
3. A recommendation is selected using a graph query.
4. The response displays source evidence and a relationship path.
5. A user decision is available after a new session or process restart.
6. Two agents share a task through FalkorDB.
7. A user-approved GitHub issue or local plan is created.
8. The demo works without private or login-protected data.
9. The repository contains the graph model, Cypher, setup instructions, and video.

### Product success metrics

- Percentage of recommendations marked useful
- Percentage of recommendations with complete evidence paths
- Cross-session memory recall rate
- Duplicate recommendation rate
- Successful task handoff rate
- Average device memory and CPU usage
- Average number of model calls per source run
- Time from source ingestion to user-visible recommendation
- Number of failed external actions

Do not invent metric values for the demo. Measure a small test set and report actual results.

---

## 15. MVP acceptance criteria

The MVP is complete when all of the following are true:

### Setup

- A user can start the service on a supported Linux machine.
- The setup page can create a profile and project.
- The system can connect to FalkorDB or clearly report a configuration error.

### Collection

- At least two public sources can be configured.
- A manual run fetches new items.
- Duplicate items are not repeatedly inserted.
- Failed sources are recorded and skipped without crashing the process.

### Graph reasoning

- Items are related to at least one user interest, skill, technology, or project requirement.
- A recommendation is produced from a graph query.
- The UI shows the reason and evidence path.

### Memory and coordination

- User feedback is saved.
- A decision survives a process restart.
- A Planner Agent can receive a task from the Relevance Agent.
- The task status is visible.

### Action

- User approval is required.
- A public GitHub sandbox issue or a local implementation plan is created.
- Duplicate retries are safe.

### Efficiency

- The edge service uses bounded workers.
- The service has no mandatory browser automation.
- The service can run with a remote model and remote graph.
- The installation does not require a heavyweight local model.

---

## 16. Demonstration plan

The demo should last approximately three to five minutes.

1. Show the UNO Q or low-resource Linux machine running NightShift.
2. Open the local dashboard.
3. Show a user profile, an active project, and two public sources.
4. Trigger an overnight run.
5. Show the graph expanding.
6. Ask which new item matters to the active project.
7. Show the graph path and public source evidence.
8. Approve the proposed action.
9. Show the created issue or plan.
10. Start a new session or restart the service.
11. Ask what was decided previously.
12. Show that FalkorDB retrieved the decision, evidence, and task.
13. Briefly show the CPU/memory status page.

The board is the memorable physical anchor. The graph-backed reasoning, memory, coordination, and explanation are the judging substance.

---

## 17. Suggested 72-hour implementation order

### Phase 1: Foundation

- Create the native service skeleton.
- Add configuration loading.
- Add SQLite migrations.
- Connect to FalkorDB.
- Create graph initialization and a health check.
- Serve a basic static web page.

### Phase 2: One complete source-to-graph path

- Implement RSS/Atom collection.
- Normalize and deduplicate items.
- Create topic and technology relationships.
- Add the first graph query.
- Display the result in the UI.

### Phase 3: User context and ranking

- Add user interests, skills, project, and requirements.
- Implement deterministic graph relevance scoring.
- Add a small model shortlist call.
- Store recommendation evidence.

### Phase 4: Memory and coordination

- Add sessions, decisions, memories, and tasks.
- Add Scout, Relevance, Planner, and Memory agents.
- Persist handoffs.
- Test restart recovery.

### Phase 5: Approved action

- Add GitHub sandbox issue creation or local plan generation.
- Add approval and idempotency.
- Record the action result in FalkorDB.

### Phase 6: Hardware, performance, and presentation

- Deploy the same binary to the UNO Q, Raspberry Pi, or old PC.
- Add heartbeat status.
- Record memory and CPU usage.
- Test network failure and restart behavior.
- Prepare README, graph diagram, Cypher examples, and demo video.

Do not start with job scraping, full coding automation, voice control, or ten different integrations. Finish the graph-backed vertical slice first.

---

## 18. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---:|---|
| Scope becomes a general AI platform | High | Keep one core flow: public signal → graph relevance → evidence → approved task |
| FalkorDB becomes decorative | High | Make ranking, memory, handoffs, and explanations depend on graph queries |
| LinkedIn data violates rules | High | Use public job boards and synthetic data |
| Device cannot run all services | High | Run a lightweight edge daemon; use remote FalkorDB and model |
| LLM calls are slow or expensive | Medium | Deterministic prefiltering, batching, caching, and strict token budgets |
| Agent loop gets stuck | Medium | Persist state, bound retries, add deadlines and circuit breakers |
| Duplicate GitHub actions | High | Idempotency keys and explicit action state |
| Graph grows without limit | Medium | Retention policies, compact summaries, deduplication, and periodic cleanup |
| Browser automation consumes resources | High | Use RSS, APIs, and public endpoints for MVP |
| Team cannot explain generated code | High | Keep architecture small, review generated code, and document decisions |

---

## 19. Open decisions

These decisions should be made before implementation begins:

1. Which two public sources are guaranteed for the demo?
2. Will the demo use FalkorDB Cloud, a remote server, or a tested local container?
3. Which model provider is used, and what is the maximum daily request budget?
4. Is the first external action a GitHub issue or a local plan file?
5. Which hardware will be the primary demo device?
6. Is the public repository a real project repository or a dedicated sandbox?
7. What is the default data retention period?
8. Which team member owns the graph layer, the edge runtime, the UI, and the demo?

Recommended defaults:

- RSS/Atom plus public GitHub releases
- FalkorDB Cloud or a remote Linux server
- Remote model provider
- GitHub issue creation in a public sandbox
- Arduino UNO Q or Raspberry Pi 4 as the physical demo
- Synthetic demo user and public project data
- Thirty-day item retention and indefinite retention for decisions/tasks

---

## 20. Glossary

**Edge runtime:** The lightweight NightShift process running on the UNO Q, Raspberry Pi, or old PC.

**Graph context:** The connected FalkorDB data used to make decisions, not just a visualization.

**Memory:** A durable fact, preference, decision, summary, or state that can be retrieved later.

**Handoff:** A persisted transfer of work from one specialist agent to another.

**Evidence:** A source URL, source record, or graph observation supporting a recommendation.

**Bounded action:** A limited operation with a known target, explicit approval, timeout, and safe retry behavior.

**Deterministic mode:** A mode that can collect, normalize, query, rank, and display data without requiring an LLM.

**Low-resource device:** A computer where CPU, memory, storage, power, or network bandwidth is limited compared with a modern cloud server.
