"""Focused checks for the metadata-only Codex hook projection."""

from __future__ import annotations

import importlib.util
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


_MODULE_PATH = Path(__file__).with_name("trace_hook.py")
_SPEC = importlib.util.spec_from_file_location("trace_hook", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
trace_hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(trace_hook)


class TraceHookTests(unittest.TestCase):
    def test_tool_event_discards_arguments_and_result_bodies(self) -> None:
        hook = {
            "hook_event_name": "PostToolUse", "session_id": "thr-1", "turn_id": "turn-1",
            "tool_use_id": "call-1", "tool_name": "mcp__semarail__semarail_get_context",
            "tool_input": {"question": "private question", "apiKey": "secret"},
            "tool_response": {
                "_meta": {"traceId": "trace-1"},
                "content": [{"type": "text", "text": "private answer"}],
            },
        }
        events = trace_hook._events(hook)
        self.assertEqual(events[0]["coreTraceId"], "trace-1")
        serialized = json.dumps(events)
        for secret in ("private question", "private answer", "apiKey", "secret"):
            self.assertNotIn(secret, serialized)

    def test_explicit_project_enablement(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            config = Path(temp) / "trace-projects.json"
            config.write_text(json.dumps({"projects": [{"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}]}), encoding="utf-8")
            with patch.dict("os.environ", {"SEMARAIL_TRACE_CONFIG": str(config)}):
                self.assertIsNotNone(trace_hook._project_config(str(root)))
                self.assertIsNone(trace_hook._project_config(str(Path(temp) / "other")))

    def test_outcome_and_subagent_identity(self) -> None:
        hook = {"hook_event_name": "SubagentStart", "agent_id": "agent-1", "model": "gpt-6-luna"}
        self.assertEqual(trace_hook._events(hook)[0]["agentId"], "agent-1")
        self.assertEqual(trace_hook._events({"hook_event_name": "Stop"})[-1]["status"], "success")
        self.assertEqual(trace_hook._events({"hook_event_name": "Interrupt"})[0]["status"], "cancelled")

    def test_spawn_id_is_read_only_from_the_allowlisted_tool_result(self) -> None:
        response = "  \n{\"agent_id\": \"agent-1\", \"prompt\": \"PRIVATE_SPAWN_BODY\"}\n"
        self.assertEqual(trace_hook._spawned_agent_id("collaborationspawn_agent", response), "agent-1")
        self.assertEqual(
            trace_hook._spawned_agent_id("collaborationspawn_agent", {"structuredContent": {"agent_id": "agent-2"}}),
            "agent-2",
        )
        self.assertIsNone(trace_hook._spawned_agent_id("some_other_tool", response))
        self.assertIsNone(trace_hook._spawned_agent_id("collaborationspawn_agent", {"agentId": "agent-3"}))

    def test_safe_task_path_accepts_only_canonical_segments(self) -> None:
        self.assertEqual(trace_hook._safe_task_path("/root/trace_shape_child"), "/root/trace_shape_child")
        self.assertEqual(trace_hook._safe_task_path("/root/work.v2:child"), "/root/work.v2:child")
        self.assertIsNone(trace_hook._safe_task_path("/root/../private/body"))
        self.assertIsNone(trace_hook._safe_task_path("root/trace_shape_child"))
        self.assertIsNone(trace_hook._safe_task_path("/root/private prompt"))

    def test_spawn_task_path_is_read_only_from_the_collaboration_result(self) -> None:
        self.assertEqual(
            trace_hook._spawned_task_path("collaborationspawn_agent", "{\"task_name\":\"/root/worker_1\",\"prompt\":\"PRIVATE\"}"),
            "/root/worker_1",
        )
        self.assertIsNone(trace_hook._spawned_task_path("shell", {"task_name": "/root/worker_1"}))
        self.assertIsNone(trace_hook._spawned_task_path("collaborationspawn_agent", {"task_name": "/root/../worker"}))

    def test_codex_state_reader_selects_only_task_path_and_parent_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state_db = Path(temp) / "state_5.sqlite"
            connection = sqlite3.connect(state_db)
            try:
                connection.execute("CREATE TABLE threads(id TEXT, agent_path TEXT, source TEXT, transcript TEXT)")
                connection.execute(
                    "INSERT INTO threads VALUES(?,?,?,?)",
                    ("child-1", "/root/worker_1", json.dumps({
                        "subagent": {"thread_spawn": {"parent_thread_id": "session-1"}},
                        "prompt": "PRIVATE_PROMPT", "rollout_path": "PRIVATE_ROLLOUT",
                    }), "PRIVATE_TRANSCRIPT"),
                )
                connection.commit()
            finally:
                connection.close()

            actual_connect = sqlite3.connect
            queries: list[str] = []

            class ConnectionProxy:
                def __init__(self, connection):
                    self.connection = connection

                def execute(self, query, params=()):
                    queries.append(query)
                    return self.connection.execute(query, params)

                def close(self):
                    self.connection.close()

            def connect(*args, **kwargs):
                return ConnectionProxy(actual_connect(*args, **kwargs))

            config = {"root": temp, "codexMetadataFile": str(state_db)}
            with patch.object(trace_hook.sqlite3, "connect", side_effect=connect):
                self.assertEqual(
                    trace_hook._read_codex_thread_metadata(config, "child-1"),
                    ("/root/worker_1", "session-1"),
                )
            self.assertEqual(len(queries), 1)
            self.assertEqual(
                queries[0],
                "SELECT id,agent_path,json_extract(source,'$.subagent.thread_spawn.parent_thread_id') FROM threads WHERE id=?",
            )
            for forbidden in ("transcript", "rollout", "prompt", "SELECT *"):
                self.assertNotIn(forbidden, queries[0].lower())

    def test_codex_task_path_and_parent_uuid_route_one_child_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state_db = Path(temp) / "state_5.sqlite"
            connection = sqlite3.connect(state_db)
            try:
                connection.execute("CREATE TABLE threads(id TEXT, agent_path TEXT, source TEXT)")
                connection.execute(
                    "INSERT INTO threads VALUES(?,?,?)",
                    ("child-1", "/root/worker_1", json.dumps({"subagent": {"thread_spawn": {"parent_thread_id": "session-1"}}})),
                )
                connection.commit()
            finally:
                connection.close()
            route_db = Path(temp) / "routing.sqlite3"
            config = {
                "root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763",
                "codexMetadataFile": str(state_db),
            }
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", route_db), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "child-turn", {
                    "hook_event_name": "SubagentStart", "agent_id": "child-1",
                }, config)
                self.assertEqual(sent, [])
                trace_hook._dispatch_hook("session-1", "root-turn", {
                    "hook_event_name": "PostToolUse", "tool_use_id": "spawn-call",
                    "tool_name": "collaborationspawn_agent",
                    "tool_response": {"task_name": "/root/worker_1", "prompt": "PRIVATE_SPAWN_PROMPT"},
                }, config)

            child_events = [event for payload in sent for event in payload["events"] if event.get("agentId") == "child-1"]
            self.assertEqual(len(child_events), 1)
            self.assertEqual(child_events[0]["type"], "subagent_started")
            child_payloads = [payload for payload in sent if any(event.get("agentId") == "child-1" for event in payload["events"])]
            self.assertTrue(child_payloads)
            self.assertTrue(all(payload["sourceTurnId"] == "root-turn" for payload in child_payloads))
            stored = b"".join(path.read_bytes() for path in Path(temp).glob("routing.sqlite3*"))
            self.assertNotIn(b"PRIVATE_SPAWN_PROMPT", stored)

    def test_path_parent_mismatch_and_duplicate_rounds_stay_unresolved(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state_db = Path(temp) / "state_5.sqlite"
            connection = sqlite3.connect(state_db)
            try:
                connection.execute("CREATE TABLE threads(id TEXT, agent_path TEXT, source TEXT)")
                for child, parent in (("wrong-parent-child", "other-session"), ("later-child", "session-1")):
                    connection.execute(
                        "INSERT INTO threads VALUES(?,?,?)",
                        (child, "/root/reused", json.dumps({"subagent": {"thread_spawn": {"parent_thread_id": parent}}})),
                    )
                connection.commit()
            finally:
                connection.close()
            route_db = Path(temp) / "routing.sqlite3"
            config = {
                "root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763",
                "codexMetadataFile": str(state_db),
            }
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", route_db), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "root-turn-a", {
                    "hook_event_name": "PostToolUse", "tool_use_id": "spawn-a",
                    "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/reused"},
                }, config)
                before = len(sent)
                trace_hook._dispatch_hook("session-1", "wrong-child-turn", {
                    "hook_event_name": "SubagentStart", "agent_id": "wrong-parent-child",
                }, config)
                self.assertEqual(len(sent), before)

                # A second spawn with the same path and parent on another turn is ambiguous.
                trace_hook._dispatch_hook("session-1", "root-turn-b", {
                    "hook_event_name": "PostToolUse", "tool_use_id": "spawn-b",
                    "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/reused"},
                }, config)
                before_later_child = len(sent)
                trace_hook._dispatch_hook("session-1", "later-child-turn", {
                    "hook_event_name": "SubagentStart", "agent_id": "later-child",
                }, config)
                self.assertEqual(len(sent), before_later_child)

    def test_nested_task_path_routes_through_parent_agent_to_root_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state_db = Path(temp) / "state_5.sqlite"
            connection = sqlite3.connect(state_db)
            try:
                connection.execute("CREATE TABLE threads(id TEXT, agent_path TEXT, source TEXT)")
                connection.executemany(
                    "INSERT INTO threads VALUES(?,?,?)",
                    [
                        ("child-1", "/root/worker_1", json.dumps({"subagent": {"thread_spawn": {"parent_thread_id": "session-1"}}})),
                        ("nested-1", "/root/worker_1/nested", json.dumps({"subagent": {"thread_spawn": {"parent_thread_id": "child-1"}}})),
                    ],
                )
                connection.commit()
            finally:
                connection.close()
            route_db = Path(temp) / "routing.sqlite3"
            config = {
                "root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763",
                "codexMetadataFile": str(state_db),
            }
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", route_db), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "root-turn", {
                    "hook_event_name": "PostToolUse", "tool_use_id": "spawn-1",
                    "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/worker_1"},
                }, config)
                trace_hook._dispatch_hook("session-1", "child-turn", {
                    "hook_event_name": "SubagentStart", "agent_id": "child-1",
                }, config)
                trace_hook._dispatch_hook("session-1", "child-turn", {
                    "hook_event_name": "PostToolUse", "agent_id": "child-1", "tool_use_id": "spawn-2",
                    "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/worker_1/nested"},
                }, config)
                trace_hook._dispatch_hook("session-1", "nested-turn", {
                    "hook_event_name": "SubagentStart", "agent_id": "nested-1",
                }, config)

            nested_events = [event for payload in sent for event in payload["events"] if event.get("agentId") == "nested-1"]
            self.assertEqual([event["type"] for event in nested_events], ["subagent_started"])
            self.assertTrue(all(payload["sourceTurnId"] == "root-turn" for payload in sent))

    def test_subagent_stop_without_initial_start_cannot_bind_a_followup_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state_db = Path(temp) / "state_5.sqlite"
            connection = sqlite3.connect(state_db)
            try:
                connection.execute("CREATE TABLE threads(id TEXT, agent_path TEXT, source TEXT)")
                connection.execute(
                    "INSERT INTO threads VALUES(?,?,?)",
                    ("child-1", "/root/reused", json.dumps({"subagent": {"thread_spawn": {"parent_thread_id": "session-1"}}})),
                )
                connection.commit()
            finally:
                connection.close()
            route_db = Path(temp) / "routing.sqlite3"
            config = {
                "root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763",
                "codexMetadataFile": str(state_db),
            }
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", route_db), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "root-turn", {
                    "hook_event_name": "PostToolUse", "tool_use_id": "spawn-call",
                    "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/reused"},
                }, config)
                before_child_events = len(sent)
                trace_hook._dispatch_hook("session-1", "followup-turn", {
                    "hook_event_name": "SubagentStop", "agent_id": "child-1",
                }, config)
                trace_hook._dispatch_hook("session-1", "followup-turn", {
                    "hook_event_name": "PreToolUse", "agent_id": "child-1",
                    "tool_use_id": "followup-call", "tool_name": "shell",
                }, config)
                self.assertEqual(len(sent), before_child_events)

            connection = sqlite3.connect(route_db)
            try:
                route = connection.execute(
                    "SELECT root_turn_id,bound_turn_id FROM trace_agent_routes WHERE agent_id='child-1'"
                ).fetchone()
            finally:
                connection.close()
            self.assertIsNone(route)

    def test_late_stop_metadata_resolves_only_the_pending_start_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state_db = Path(temp) / "state_5.sqlite"
            connection = sqlite3.connect(state_db)
            try:
                connection.execute("CREATE TABLE threads(id TEXT, agent_path TEXT, source TEXT)")
                connection.commit()
            finally:
                connection.close()
            route_db = Path(temp) / "routing.sqlite3"
            config = {
                "root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763",
                "codexMetadataFile": str(state_db),
            }
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", route_db), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "root-turn", {
                    "hook_event_name": "PostToolUse", "tool_use_id": "spawn-call",
                    "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/late"},
                }, config)
                trace_hook._dispatch_hook("session-1", "initial-child-turn", {
                    "hook_event_name": "SubagentStart", "agent_id": "child-late",
                }, config)
                self.assertFalse(any(event.get("agentId") == "child-late" for payload in sent for event in payload["events"]))

                connection = sqlite3.connect(state_db)
                try:
                    connection.execute(
                        "INSERT INTO threads VALUES(?,?,?)",
                        ("child-late", "/root/late", json.dumps({"subagent": {"thread_spawn": {"parent_thread_id": "session-1"}}})),
                    )
                    connection.commit()
                finally:
                    connection.close()

                trace_hook._dispatch_hook("session-1", "initial-child-turn", {
                    "hook_event_name": "SubagentStop", "agent_id": "child-late",
                }, config)
                routed = [payload for payload in sent if any(event.get("agentId") == "child-late" for event in payload["events"])]
                self.assertTrue(routed)
                self.assertTrue(all(payload["sourceTurnId"] == "root-turn" for payload in routed))
                self.assertTrue(any(event.get("type") == "subagent_completed" for payload in routed for event in payload["events"]))

    def test_tool_status_uses_explicit_result_evidence(self) -> None:
        self.assertEqual(trace_hook._tool_status({"structuredContent": {"status": "error"}}), "failure")
        self.assertIsNone(trace_hook._tool_status(None))
        self.assertIsNone(trace_hook._tool_status("not a structured result"))
        event = trace_hook._events({
            "hook_event_name": "PostToolUse", "tool_use_id": "call-1", "tool_name": "query.run",
        })[0]
        self.assertNotIn("status", event)

    def test_project_and_credential_changes_do_not_reuse_routes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            project_a = Path(temp) / "project-a"
            project_b = Path(temp) / "project-b"
            project_a.mkdir()
            project_b.mkdir()
            state = Path(temp) / "routing.sqlite3"
            config_a = {"root": str(project_a), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            config_b = {"root": str(project_b), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", state), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "root-turn-a", {
                    "hook_event_name": "PostToolUse", "session_id": "session-1", "turn_id": "root-turn-a",
                    "tool_use_id": "spawn-call", "tool_name": "collaborationspawn_agent",
                    "tool_response": {"agent_id": "agent-1"},
                }, config_a)
                before_other_project = len(sent)
                trace_hook._dispatch_hook("session-1", "child-turn-b", {
                    "hook_event_name": "SubagentStart", "session_id": "session-1", "turn_id": "child-turn-b",
                    "agent_id": "agent-1",
                }, config_b)
                self.assertEqual(len(sent), before_other_project)

            with patch.dict("os.environ", {"TRACE_EMPLOYEE_TOKEN": "sr_session_employee_a"}):
                credential_config = {**config_a, "tokenEnv": "TRACE_EMPLOYEE_TOKEN"}
                scope_a = trace_hook._project_scope(credential_config)
            with patch.dict("os.environ", {"TRACE_EMPLOYEE_TOKEN": "sr_session_employee_b"}):
                scope_b = trace_hook._project_scope(credential_config)
            self.assertNotEqual(scope_a, scope_b)

    def test_child_events_wait_for_exact_spawn_and_initial_child_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state = Path(temp) / "routing.sqlite3"
            config = {"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with patch.object(trace_hook, "_STATE_PATH", state), patch.object(trace_hook, "_send", side_effect=send):
                trace_hook._dispatch_hook("session-1", "child-turn-b", {
                    "hook_event_name": "SubagentStart", "agent_id": "agent-1", "session_id": "session-1",
                    "turn_id": "child-turn-b", "model": "gpt-6-luna",
                }, config)
                trace_hook._dispatch_hook("session-1", "child-turn-b", {
                    "hook_event_name": "PreToolUse", "agent_id": "agent-1", "session_id": "session-1",
                    "turn_id": "child-turn-b", "tool_use_id": "child-call-1", "tool_name": "shell",
                    "tool_input": {"command": "PRIVATE_CHILD_INPUT"},
                }, config)
                trace_hook._dispatch_hook("session-1", "child-turn-b", {
                    "hook_event_name": "PostToolUse", "agent_id": "agent-1", "session_id": "session-1",
                    "turn_id": "child-turn-b", "tool_use_id": "child-call-1", "tool_name": "shell",
                    "tool_response": {"content": [{"type": "text", "text": "PRIVATE_CHILD_OUTPUT"}]},
                }, config)
                self.assertEqual(sent, [])

                # App-created initial main turns can begin at an agent-less tool hook.
                trace_hook._dispatch_hook("session-1", "root-turn-a", {
                    "hook_event_name": "PreToolUse", "session_id": "session-1", "turn_id": "root-turn-a",
                    "tool_use_id": "spawn-call", "tool_name": "collaborationspawn_agent",
                }, config)
                self.assertEqual(sent[-1]["sourceTurnId"], "root-turn-a")

                # The exact spawn response binds the buffered child lifecycle to the parent turn.
                trace_hook._dispatch_hook("session-1", "root-turn-a", {
                    "hook_event_name": "PostToolUse", "session_id": "session-1", "turn_id": "root-turn-a",
                    "tool_use_id": "spawn-call", "tool_name": "collaborationspawn_agent",
                    "tool_response": {"content": [{"type": "text", "text": "\n{\"agent_id\":\"agent-1\",\"task\":\"PRIVATE_SPAWN_BODY\"}\n"}]},
                }, config)
                trace_hook._dispatch_hook("session-1", "child-turn-b", {
                    "hook_event_name": "SubagentStop", "agent_id": "agent-1", "session_id": "session-1",
                    "turn_id": "child-turn-b",
                }, config)

                routed = [item for item in sent if any(event.get("agentId") == "agent-1" or event.get("parentAgentId") == "agent-1" for event in item["events"])]
                self.assertTrue(routed)
                self.assertTrue(all(item["sourceTurnId"] == "root-turn-a" for item in routed))
                lifecycle = [event for item in routed for event in item["events"] if event["type"].startswith("subagent_")]
                self.assertEqual({event["type"] for event in lifecycle}, {"subagent_started", "subagent_completed"})

                sent_count = len(sent)
                # A later turn from the same Agent ID has no matching initial SubagentStart binding.
                trace_hook._dispatch_hook("session-1", "child-followup-c", {
                    "hook_event_name": "PreToolUse", "agent_id": "agent-1", "session_id": "session-1",
                    "turn_id": "child-followup-c", "tool_use_id": "followup-call", "tool_name": "shell",
                }, config)
                trace_hook._dispatch_hook("session-1", "child-followup-c", {
                    "hook_event_name": "SubagentStart", "agent_id": "agent-1", "session_id": "session-1",
                    "turn_id": "child-followup-c",
                }, config)
                self.assertEqual(len(sent), sent_count)

            connection = sqlite3.connect(state)
            try:
                pending_turns = {row[0] for row in connection.execute(
                    "SELECT DISTINCT source_turn_id FROM trace_pending_events"
                )}
            finally:
                connection.close()
            self.assertEqual(pending_turns, {"child-followup-c"})
            stored = b"".join(path.read_bytes() for path in Path(temp).glob("routing.sqlite3*"))
            for private in (b"PRIVATE_CHILD_INPUT", b"PRIVATE_CHILD_OUTPUT", b"PRIVATE_SPAWN_BODY"):
                self.assertNotIn(private, stored)

    def test_nested_spawn_requires_the_parent_agents_bound_turn(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state = Path(temp) / "routing.sqlite3"
            config = {"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            def hook(session, turn, name, **fields):
                trace_hook._dispatch_hook(session, turn, {
                    "hook_event_name": name, "session_id": session, "turn_id": turn, **fields,
                }, config)

            with patch.object(trace_hook, "_STATE_PATH", state), patch.object(trace_hook, "_send", side_effect=send):
                hook("session-1", "root-turn-a", "PostToolUse", tool_use_id="root-spawn", tool_name="collaborationspawn_agent", tool_response={"agent_id": "agent-1"})
                hook("session-1", "child-turn-b", "SubagentStart", agent_id="agent-1")
                hook("session-1", "child-turn-b", "PostToolUse", agent_id="agent-1", tool_use_id="nested-spawn", tool_name="collaborationspawn_agent", tool_response="{\"agent_id\": \"agent-2\"}")
                hook("session-1", "nested-turn-c", "SubagentStart", agent_id="agent-2")
                nested_start = [event for item in sent for event in item["events"] if event.get("type") == "subagent_started" and event.get("agentId") == "agent-2"]
                self.assertEqual(len(nested_start), 1)
                self.assertEqual(nested_start[0]["parentAgentId"], "agent-1")
                self.assertTrue(any(item["sourceTurnId"] == "root-turn-a" and nested_start[0] in item["events"] for item in sent))

                before_unbound_spawn = len(sent)
                hook("session-1", "later-turn-d", "PostToolUse", agent_id="agent-1", tool_use_id="unbound-spawn", tool_name="collaborationspawn_agent", tool_response={"agent_id": "agent-3"})
                hook("session-1", "nested-turn-e", "SubagentStart", agent_id="agent-3")
                self.assertEqual(len(sent), before_unbound_spawn)

    def test_mcp_completion_requires_exact_registered_pretooluse(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state = Path(temp) / "routing.sqlite3"
            config = {"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            metadata = {"callId": "call-1", "sessionId": "session-1", "turnId": "turn-1"}
            with (patch.object(trace_hook, "_STATE_PATH", state),
                  patch.object(trace_hook, "_project_config", return_value=config),
                  patch.object(trace_hook, "_token", return_value="sr_session_test"),
                  patch.object(trace_hook, "_send", side_effect=send)):
                trace_hook._observe_mcp_result(metadata, {"traceId": "trace-1", "isError": False}, str(root), "2026-09-30T10:00:00Z")
                self.assertEqual(sent, [])

                trace_hook._dispatch_hook("session-1", "turn-1", {
                    "hook_event_name": "PreToolUse", "tool_use_id": "call-1",
                    "tool_name": "mcp__semarail__query_run",
                }, config)
                self.assertEqual(len(sent), 1)
                self.assertEqual(sent[0]["sourceTurnId"], "turn-1")
                self.assertEqual([event["type"] for event in sent[0]["events"]], ["tool_started", "tool_completed"])
                completed = sent[0]["events"][1]
                self.assertEqual(completed["eventId"], "mcp-result:call-1")
                self.assertEqual(completed["toolUseId"], "call-1")
                self.assertEqual(completed["toolName"], "mcp__semarail__query_run")
                self.assertEqual(completed["status"], "success")
                self.assertEqual(completed["coreTraceId"], "trace-1")

                # A completion from another turn or agent cannot reuse this registration.
                trace_hook._observe_mcp_result(
                    {**metadata, "turnId": "turn-2"}, {"traceId": "trace-wrong-turn", "isError": True},
                    str(root), "2026-09-30T10:00:01Z",
                )
                trace_hook._observe_mcp_result(
                    {**metadata, "agentId": "agent-other"}, {"traceId": "trace-wrong-agent", "isError": True},
                    str(root), "2026-09-30T10:00:02Z",
                )
                trace_hook._observe_mcp_result(metadata, {"traceId": "trace-duplicate", "isError": False}, str(root), "2026-09-30T10:00:03Z")
                self.assertEqual(len(sent), 1)

            stored = b"".join(path.read_bytes() for path in Path(temp).glob("routing.sqlite3*"))
            for private in (b"PRIVATE_INPUT", b"PRIVATE_OUTPUT"):
                self.assertNotIn(private, stored)

    def test_mcp_result_metadata_is_validated_and_opt_in_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "routing.sqlite3"
            root = Path(temp) / "enabled"
            root.mkdir()
            config = {"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            with patch.object(trace_hook, "_STATE_PATH", state), patch.object(trace_hook, "_token", return_value="sr_session_test"):
                with patch.object(trace_hook, "_project_config", return_value=None):
                    trace_hook._observe_mcp_result(
                        {"callId": "call-1", "sessionId": "session-1", "turnId": "turn-1"},
                        {"traceId": "trace-1", "isError": False}, str(root), "2026-09-30T10:00:00Z",
                    )
                with patch.object(trace_hook, "_project_config", return_value=config):
                    trace_hook._observe_mcp_result(
                        {"callId": "call-1", "sessionId": "session-1", "turnId": "turn-1"},
                        {"traceId": "trace-1", "isError": 1}, str(root), "2026-09-30T10:00:00Z",
                    )
                self.assertFalse(state.exists())

    def test_mcp_result_race_is_buffered_until_matching_pretooluse(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            state = Path(temp) / "routing.sqlite3"
            config = {"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            sent: list[dict] = []

            def send(_config, payload):
                sent.append(payload)
                return True

            with (patch.object(trace_hook, "_STATE_PATH", state),
                  patch.object(trace_hook, "_project_config", return_value=config),
                  patch.object(trace_hook, "_token", return_value="sr_session_test"),
                  patch.object(trace_hook, "_send", side_effect=send)):
                trace_hook._observe_mcp_result(
                    {"callId": "call-2", "sessionId": "session-1", "turnId": "turn-2"},
                    {"traceId": "trace-2", "isError": True}, str(root), "2026-09-30T10:01:00Z",
                )
                self.assertEqual(sent, [])
                trace_hook._dispatch_hook("session-1", "turn-2", {
                    "hook_event_name": "PreToolUse", "tool_use_id": "call-2",
                    "tool_name": "mcp__semarail__query_run",
                }, config)
                self.assertEqual(len(sent), 1)
                self.assertEqual(sent[0]["sourceTurnId"], "turn-2")
                self.assertEqual(sent[0]["events"][-1]["eventId"], "mcp-result:call-2")
                self.assertEqual(sent[0]["events"][-1]["status"], "failure")
                self.assertEqual(sent[0]["events"][-1]["coreTraceId"], "trace-2")

    def test_main_emits_only_allowlisted_metadata_for_enabled_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "enabled"
            root.mkdir()
            config = Path(temp) / "trace-projects.json"
            config.write_text(json.dumps({"projects": [{"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}]}), encoding="utf-8")
            hook = {
                "hook_event_name": "PostToolUse", "session_id": "session-1", "turn_id": "turn-1",
                "cwd": str(root), "tool_use_id": "call-1", "tool_name": "mcp__semarail__query",
                "tool_input": {"question": "private prompt"},
                "tool_response": {"_meta": {"traceId": "trace-owned"}, "content": [{"type": "text", "text": "private result"}]},
                "transcript_path": "private transcript path",
            }
            stdin = io.TextIOWrapper(io.BytesIO(json.dumps(hook).encode()), encoding="utf-8")
            with patch.dict("os.environ", {"SEMARAIL_TRACE_CONFIG": str(config)}), patch.object(trace_hook, "_STATE_PATH", Path(temp) / "routing.sqlite3"), patch.object(trace_hook.sys, "stdin", stdin), patch.object(trace_hook, "_send") as send:
                trace_hook.main()
            sent = send.call_args.args[1]
            self.assertEqual(sent["events"][0]["coreTraceId"], "trace-owned")
            encoded = json.dumps(sent)
            for secret in ("private prompt", "private result", "private transcript path"):
                self.assertNotIn(secret, encoded)

    def test_stop_returns_no_effect_json_even_when_collection_fails(self) -> None:
        hook = {"hook_event_name": "Stop", "session_id": "session-1", "turn_id": "turn-1", "cwd": "C:/disabled"}
        stdin = io.TextIOWrapper(io.BytesIO(json.dumps(hook).encode()), encoding="utf-8")
        stdout = io.StringIO()
        with (patch.object(trace_hook.sys, "stdin", stdin), patch.object(trace_hook.sys, "stdout", stdout),
              patch.object(trace_hook, "_project_config", return_value={"enabled": True}),
              patch.object(trace_hook, "_dispatch_hook", side_effect=RuntimeError("collector unavailable"))):
            trace_hook.main()
        self.assertEqual(stdout.getvalue(), "{}\n")


if __name__ == "__main__":
    unittest.main()
