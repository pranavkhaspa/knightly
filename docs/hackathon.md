# FalkorDB hackathon research

Research checked on 16 October 2026:

- [Hackathon overview](https://www.wemakedevs.org/hackathons/falkordb)
- [Rules](https://www.wemakedevs.org/hackathons/falkordb/rules)
- [Schedule](https://www.wemakedevs.org/hackathons/falkordb/schedule)
- [How to build an agent that reasons over a graph](https://www.wemakedevs.org/blogs/how-to-build-an-agent-that-reasons-over-a-graph)

## What judges need to see

- FalkorDB must perform essential work, not merely power a visualization.
- The public repository needs setup instructions, the graph model, and the
  Cypher/algorithms the product depends on.
- The submission needs a working demo video and a live deployment or complete
  local setup instructions.
- Demo data must be public, synthetic, or team-owned.
- AI coding assistant usage is allowed but must be disclosed, and the team
  must understand the submitted implementation.

## Track fit

NightShift's primary fit is **Agent Memory and Coordination**: decisions,
evolving memories, task handoffs, and tool calls persist in a user graph.
It also fits **Agents That Act on Connected Data** because the recommendation
and approved plan depend on a multi-hop path from a project requirement to a
public item.

The public pages contain a track-entry ambiguity: the overview/rules allow
entering multiple tracks, while the blog describes one judged track. Verify
the submission form before selecting the final track list.

## Demo advice taken from the article

1. Ask a shallow graph question and then repeat it with deeper traversal.
2. Display the actual returned path, sources, timestamps, and selected tool.
3. Ask about an unknown entity and show an explicit unknown result instead of
   hallucinating an answer.
4. Persist a finding, start a new session, and retrieve it from FalkorDB.
5. Use fixed, human-written, parameterized graph tools instead of arbitrary
   model-generated Cypher.

## Submission facts to re-check

The event schedule retrieved during setup states an event window from 15
October 2026 through 18 October 2026, with submissions closing at 11:59 PM
IST on 18 October. Confirm the live portal, timezone, track behavior, prize
details, and deployment fields immediately before submission because the rules
state that these may be updated.
