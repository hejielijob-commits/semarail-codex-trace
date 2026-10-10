# SemaRail Codex Trace

This optional Codex desktop plugin sends only turn, tool and subagent metadata from explicitly enabled projects to [SemaRail](https://github.com/hejielijob-commits/SemaRail). It never sends prompts, tool inputs, tool outputs, final answers, transcript files or bearer tokens as Trace fields. Collection is best effort and does not change Codex execution.

Version **0.2.2** requires Python **3.11+**. In the SemaRail Console, open
**Quality → Agent traces** (**质量 → Agent 追踪**) to inspect the Session,
turn, Agent/subagent, tool and Core hierarchy, parallel activity, observed
durations, and linked issues. SemaRail provides authenticated Trace v1 APIs;
the adapter is kept separate from Core.

SemaRail Trace v2 adds collector health, rules-based diagnosis, an independent
Trace comparison page and manually reviewed query regression runs. See the
[SemaRail workflow and boundaries](https://github.com/hejielijob-commits/SemaRail/blob/main/docs/trace-v2.md).
The adapter supplies safe lifecycle and verified Core correlation metadata;
regression questions, SQL and assertions are entered explicitly in the Console.

## Install

Clone this repository into a local plugin folder, register that folder in your
personal or organization Codex plugin marketplace, and install
`semarail-codex-trace` from that marketplace. Review and trust its current Hooks
in Codex before starting a new desktop chat. See the official
[plugin Hook workflow](https://developers.openai.com/plugins/build/plugins)
and [Hooks reference](https://learn.chatgpt.com/docs/hooks).

## Enable a project

Create `~/.semarail/trace-projects.json` with an entry for each enabled project:

```json
{
  "projects": [
    {
      "root": "D:\\projects\\my-project",
      "enabled": true,
      "endpoint": "http://127.0.0.1:48763",
      "authFile": "C:\\Users\\me\\.semarail\\session.json"
    }
  ]
}
```

`authFile` is the existing SemaRail employee session saved by `semarail auth login`. As an alternative, set `tokenEnv` to the name of an environment variable containing a SemaRail service-account key or employee session. Never put a token value in this config. The configured endpoint must match the employee session endpoint. Non-loopback endpoints must use HTTPS. Only paths under enabled roots send events.

The credential must have a policy granting `trace:write` for the SemaRail project served by the endpoint. Use the same authenticated subject for this plugin and the SemaRail MCP bridge so Core diagnostic ownership can be verified. Trace reads require `console:admin` for that project.

Install the plugin from the personal marketplace and approve its hooks in Codex. A new Codex chat picks up the hooks. Each hook projects allowlisted metadata and commits it to the local SQLite outbox before it starts a detached background worker. The worker sends batches of at most 100 events to `/api/v1/traces/events` and removes them only after a successful response. Retries cover transport timeouts, HTTP 408, HTTP 429 and server errors with exponential backoff. HTTP 401 and 403 pause delivery; other client errors quarantine the affected batch. Hook processing never waits for network delivery.

The outbox keeps events for up to seven days and limits each project to 10,000 events or 20 MiB of serialized event metadata. When a limit is reached, it evicts the oldest events and records the drop count. The local database stores only projected metadata, a one-way credential hash, and the last verified SemaRail subject, organization, and project identity. It never stores bearer tokens. If a credential changes, queued events replay only after `/api/v1/auth/capabilities` verifies the same identity. Events queued with another unverified credential remain paused and are never mixed into a batch authorized by the current credential.

Use the installed plugin's `scripts/trace_hook.py` for local queue controls:

```text
python "<plugin-root>/scripts/trace_hook.py" status
python "<plugin-root>/scripts/trace_hook.py" flush
python "<plugin-root>/scripts/trace_hook.py" clean
python "<plugin-root>/scripts/trace_hook.py" clean --all
```

`status` is local and read-only. It shows configured and orphaned queues, retry and quarantine counts, worker lease state, and whether the last collector status is stale. `flush` tries delivery immediately. `clean` removes expired local data; `clean --all` clears queued events and routing metadata from this device. The worker reports aggregate state to `/api/v1/traces/collector-status`; that payload contains no project path, endpoint, credential, or server error text. The Console marks collector reports stale when a device stops reporting.

A graceful worker exit releases its lease. A crash or forced termination can
leave an unrenewed lease for up to 120 seconds; during that window `flush`
reports `worker_active`. After the lease expires, the next Hook or explicit
flush can resume delivery. Queued metadata remains durable during this wait.

All collection hooks run in the background and return no control decisions. The event timestamp is the collector's observation time; displayed Agent/tool intervals measure Hook observation elapsed time and can be missing when a background hook is cancelled. Core timings are measured at Core's execution boundaries. A child tool hook's `agent_id` is used as its owner when present. Codex desktop can omit `UserPromptSubmit` for an app-created initial turn; in that case the first agent-less main tool/turn hook still records its supplied `turn_id` as the root Trace turn.

Subagent events share the root Trace when the agent's start is tied to the allowlisted `agent_id` returned by the `collaborationspawn_agent` tool. Hooks can arrive out of order, so the adapter stores only projected event metadata in `~/.semarail/codex-trace-routing-v1.sqlite3`; it does not store raw Hook JSON, tool arguments, tool responses, prompts, or transcript data. Routing rows are scoped by enabled project, endpoint, a one-way hash of the configured credential, session and Agent ID. Ready events move transactionally into the durable outbox. The first child turn is bound by its `SubagentStart` event. A later turn from a reused Agent ID is left unresolved unless it has exact spawn/turn evidence, so timing alone never assigns it to an older main turn. Nested Agents inherit a root only through their recorded parent Agent and parent turn.

SubagentStop supplies completion, but no reliable success/failure status. No LLM call duration or Token count is inferred.

Codex desktop's collaboration tool can return a canonical `task_name` instead of
an Agent ID. For this envelope, the adapter reads only the child thread ID,
canonical Agent path and parent thread ID from Codex's local thread metadata,
then matches the path and parent exactly to the observed spawn. It never reads
rollouts, transcripts, message previews, titles or prompts. The supported default
metadata database is `~/.codex/state_5.sqlite`; an enabled project's
`codexMetadataFile` may explicitly override its location. This enrichment depends
on the observed desktop metadata schema. Missing or changed schemas, unavailable
rows, or conflicting spawn paths leave the relation unresolved.

## MCP failure completion

Some Codex desktop versions do not emit `PostToolUse` for an MCP result with
`isError=true`. For a stdio SemaRail MCP server, wrap its existing command:

```text
python /path/to/semarail-codex-trace/scripts/trace_mcp.py -- <original-command> <original-arguments>
```

The wrapper forwards the original protocol bytes, tool results, stderr and exit
code. Its background observer extracts only the exact Codex call, session, turn
and Agent identifiers plus the response `_meta.traceId` and `isError`. An event
is sent only after matching an opted-in project's `PreToolUse` registration;
unmatched or ambiguous identifiers do not create associations. Network delivery
does not block the protocol stream. Completion timestamps are response observation
times, and no model timing or Token count is inferred.

Validate with `python scripts/test_trace_hook.py`,
`python scripts/test_trace_mcp.py`, and `python scripts/test_trace_outbox.py`.
The outbox checks use a loopback HTTP collector. Plugin manifest validation can
also use the plugin creator's `validate_plugin.py` when that tool is available.
