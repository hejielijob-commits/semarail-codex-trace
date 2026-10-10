"""Integration checks for durable Trace delivery and credential binding."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sqlite3
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch


_MODULE_PATH = Path(__file__).with_name("trace_hook.py")
_SPEC = importlib.util.spec_from_file_location("trace_hook_outbox", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
trace_hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(trace_hook)


class _Collector:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests: list[tuple[str, dict, str]] = []
        self.event_statuses: list[int] = []
        self.drop_event_ack_count = 0
        self.event_gate: tuple[threading.Event, threading.Event] | None = None
        self.identities = {
            "sr_session_token_a": ("subject-1", "org-1", "project-1"),
            "sr_session_token_b": ("subject-1", "org-1", "project-1"),
            "sr_session_token_other": ("subject-2", "org-2", "project-2"),
        }
        self.accepted_ids: set[str] = set()
        self.attempted_ids: list[list[str]] = []

        collector = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _body(self) -> dict:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                return json.loads(raw) if raw else {}

            def _reply(self, status: int, body: dict) -> None:
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:
                token = self.headers.get("Authorization", "").removeprefix("Bearer ")
                with collector.lock:
                    collector.requests.append((self.path, {}, token))
                    identity = collector.identities.get(token)
                if identity is None:
                    self._reply(401, {"error": "private server error body"})
                    return
                subject_id, organization_id, project_id = identity
                self._reply(200, {"subject": {"id": subject_id, "organizationId": organization_id}, "projectId": project_id})

            def do_POST(self) -> None:
                token = self.headers.get("Authorization", "").removeprefix("Bearer ")
                body = self._body()
                with collector.lock:
                    collector.requests.append((self.path, body, token))
                    if self.path.endswith("/events"):
                        events = body.get("events", [])
                        ids = [event.get("eventId") for event in events]
                        collector.attempted_ids.append(ids)
                        collector.accepted_ids.update(ids)
                        if collector.event_gate is not None:
                            started, release = collector.event_gate
                            started.set()
                            release.wait(3)
                        if collector.drop_event_ack_count:
                            collector.drop_event_ack_count -= 1
                            self.close_connection = True
                            try:
                                self.connection.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass
                            self.connection.close()
                            return
                        status = collector.event_statuses.pop(0) if collector.event_statuses else 200
                    else:
                        status = 201
                self._reply(status, {"schemaVersion": 1, "accepted": True})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class TraceOutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.root = self.base / "enabled-project"
        self.root.mkdir()
        self.collector = _Collector()
        self.config = {
            "root": str(self.root), "enabled": True, "endpoint": self.collector.endpoint,
            "tokenEnv": "SEMARAIL_TEST_TOKEN",
        }
        self.db = self.base / "trace.sqlite3"
        self.env_patch = patch.dict(os.environ, {"SEMARAIL_TEST_TOKEN": "sr_session_token_a"})
        self.env_patch.start()
        self.db_patch = patch.object(trace_hook, "_STATE_PATH", self.db)
        self.db_patch.start()
        self.config_patch = patch.object(trace_hook, "_all_project_configs", return_value=[self.config])
        self.config_patch.start()
        self.addCleanup(self._cleanup)

    def _cleanup(self) -> None:
        self.config_patch.stop()
        self.db_patch.stop()
        self.env_patch.stop()
        self.collector.close()
        self.temp.cleanup()

    def _event(self, event_id: str = "tool-start:call-1", *, tool_id: str = "call-1", at: str = "2026-10-09T10:00:00Z") -> dict:
        return {
            "eventId": event_id, "occurredAt": at, "type": "tool_started",
            "toolUseId": tool_id, "toolName": "shell", "status": "running",
        }

    def _enqueue(self, events: list[dict] | None = None, *, session: str = "session-1", turn: str = "turn-1", now: float | None = None) -> None:
        connection = trace_hook._routing_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            trace_hook._enqueue_payload(connection, self.config, session, turn, events or [self._event()], now or time.time())
            connection.commit()
        finally:
            connection.close()

    def _rows(self) -> list[tuple]:
        connection = trace_hook._routing_connection()
        try:
            return connection.execute(
                "SELECT event_id,event_json,state,credential_hash FROM trace_outbox_events ORDER BY id"
            ).fetchall()
        finally:
            connection.close()

    def test_network_failure_retries_then_acknowledges_durable_event(self) -> None:
        self.collector.event_statuses = [503, 201]
        self._enqueue()
        self.assertEqual(len(self._rows()), 1)
        self.assertEqual(trace_hook._run_worker(trace_hook._project_key(self.config), max_attempts=2), 0)
        self.assertEqual(len(self._rows()), 0)
        self.assertEqual(len(self.collector.attempted_ids), 2)
        self.assertEqual(self.collector.attempted_ids[0], self.collector.attempted_ids[1])
        status_body = [body for path, body, _token in self.collector.requests if path.endswith("collector-status")][-1]
        self.assertEqual(status_body["retryCount"], 1)
        self.assertEqual(status_body["pendingCount"], 0)

    def test_http_429_uses_the_retry_path(self) -> None:
        self.collector.event_statuses = [429, 201]
        self._enqueue()
        trace_hook._run_worker(trace_hook._project_key(self.config), max_attempts=2)
        self.assertEqual(len(self.collector.attempted_ids), 2)
        self.assertEqual(self._rows(), [])

    def test_http_408_uses_the_retry_path(self) -> None:
        self.collector.event_statuses = [408, 201]
        self._enqueue()
        trace_hook._run_worker(trace_hook._project_key(self.config), max_attempts=2)
        self.assertEqual(len(self.collector.attempted_ids), 2)
        self.assertEqual(self.collector.attempted_ids[0], self.collector.attempted_ids[1])
        self.assertEqual(self._rows(), [])
        status_body = [body for path, body, _token in self.collector.requests if path.endswith("collector-status")][-1]
        self.assertEqual(status_body["retryCount"], 1)

    def test_lost_ack_replays_same_id_and_server_deduplicates(self) -> None:
        self.collector.drop_event_ack_count = 1
        self._enqueue()
        trace_hook._run_worker(trace_hook._project_key(self.config), max_attempts=2)
        self.assertEqual(len(self.collector.attempted_ids), 2)
        self.assertEqual(self.collector.attempted_ids[0], self.collector.attempted_ids[1])
        self.assertEqual(len(self.collector.accepted_ids), 1)
        self.assertEqual(self._rows(), [])

    def test_concurrent_workers_share_one_project_lease(self) -> None:
        self._enqueue()
        started, release = threading.Event(), threading.Event()
        self.collector.event_gate = (started, release)
        result: list[int] = []
        first = threading.Thread(target=lambda: result.append(trace_hook._run_worker(trace_hook._project_key(self.config), 1)))
        first.start()
        self.assertTrue(started.wait(2))
        second_result = trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        release.set()
        first.join(timeout=5)
        self.assertFalse(first.is_alive())
        self.assertEqual(second_result, 0)
        self.assertEqual(len(self.collector.attempted_ids), 1)

    def test_rotated_credential_replays_only_after_same_identity_is_verified(self) -> None:
        project_key = trace_hook._project_key(self.config)
        self.assertTrue(trace_hook._validate_identity(self.config, project_key)[0])
        self._enqueue()
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        self.assertEqual(trace_hook._run_worker(project_key, 1), 0)
        self.assertEqual(len(self.collector.attempted_ids), 1)
        self.assertEqual(self.collector.requests[-2][2], "sr_session_token_b")

    def test_rotation_rechecks_hashes_after_capabilities_request(self) -> None:
        project_key = trace_hook._project_key(self.config)
        self.assertTrue(trace_hook._validate_identity(self.config, project_key)[0])
        self._enqueue([self._event("verified-before-rotation", tool_id="call-a")])
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        original_request = trace_hook._api_request
        injected = False

        def enqueue_during_verification(config: dict, path: str, body=None, token_override=None):
            nonlocal injected
            response = original_request(config, path, body, token_override)
            if path.endswith("/auth/capabilities") and token_override == "sr_session_token_b" and not injected:
                injected = True
                os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_other"
                self._enqueue([self._event("unverified-during-rotation", tool_id="call-other")])
                os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
            return response

        with patch.object(trace_hook, "_api_request", side_effect=enqueue_during_verification):
            result = trace_hook._run_worker(project_key, 1)

        hashes = {row[3] for row in self._rows()}
        self.assertTrue(injected)
        self.assertEqual(result, 3)
        self.assertEqual(self.collector.attempted_ids, [])
        self.assertEqual(hashes, {
            trace_hook.hashlib.sha256(b"sr_session_token_a").hexdigest(),
            trace_hook.hashlib.sha256(b"sr_session_token_other").hexdigest(),
        })
        self.assertEqual(trace_hook._local_status()["projects"][0]["pauseReason"], "unbound_credential")

    def test_batch_excludes_unknown_credential_enqueued_after_validation(self) -> None:
        project_key = trace_hook._project_key(self.config)
        self.assertTrue(trace_hook._validate_identity(self.config, project_key)[0])
        self._enqueue([self._event("verified-before-batch", tool_id="call-a")])
        verified_event_id = self._rows()[0][0]
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        injected = False

        def enqueue_before_batch(_config: dict, _project_key: str) -> int:
            nonlocal injected
            if not injected:
                injected = True
                os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_other"
                self._enqueue([self._event("unverified-before-batch", tool_id="call-other")])
                os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
            return 0

        with patch.object(trace_hook, "_drain_ready_routed_events", side_effect=enqueue_before_batch):
            result = trace_hook._run_worker(project_key, 1)

        rows = self._rows()
        self.assertTrue(injected)
        self.assertEqual(result, 3)
        self.assertEqual(self.collector.attempted_ids, [[verified_event_id]])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], "pending")
        self.assertEqual(rows[0][3], trace_hook.hashlib.sha256(b"sr_session_token_other").hexdigest())

    def test_routed_events_keep_their_enqueue_credential_hash_when_drained(self) -> None:
        project_key = trace_hook._project_key(self.config)
        now = time.time()
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_other"
        project_scope = trace_hook._project_scope(self.config)
        connection = trace_hook._routing_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            trace_hook._record_agent_route(
                connection, project_scope, "session-routed", "agent-routed", "root-turn", None, "root-turn", now,
            )
            trace_hook._bind_agent_turn(connection, project_scope, "session-routed", "agent-routed", "child-turn", now)
            trace_hook._store_pending_events(
                connection, project_scope, "session-routed", "agent-routed", "child-turn",
                [self._event("routed-credential", tool_id="call-routed")], now, self.config,
            )
            connection.commit()
        finally:
            connection.close()

        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_a"
        trace_hook._drain_ready_routed_events(self.config, project_key)
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][3], trace_hook.hashlib.sha256(b"sr_session_token_other").hexdigest())

    def test_rotated_credential_with_changed_identity_pauses_old_queue(self) -> None:
        project_key = trace_hook._project_key(self.config)
        self.assertTrue(trace_hook._validate_identity(self.config, project_key)[0])
        self._enqueue()
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_other"
        result = trace_hook._run_worker(project_key, 1)
        self.assertEqual(result, 3)
        self.assertEqual(self.collector.attempted_ids, [])
        status = trace_hook._local_status()["projects"][0]
        self.assertEqual(status["pauseReason"], "identity_changed")
        self.assertEqual(status["pendingCount"], 1)

    def test_unbound_queue_pauses_if_credential_changes_before_identity_binding(self) -> None:
        self._enqueue()
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        result = trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        self.assertEqual(result, 3)
        self.assertFalse(any(path.endswith("auth/capabilities") for path, _body, _token in self.collector.requests))
        self.assertEqual(self._rows()[0][2], "pending")

    def test_old_worker_cannot_rebind_rows_enqueued_under_a_new_credential(self) -> None:
        project_key = trace_hook._project_key(self.config)
        self.assertTrue(trace_hook._validate_identity(self.config, project_key)[0])
        self._enqueue([self._event("old-event", tool_id="old-call")])
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        self._enqueue([self._event("new-event", tool_id="new-call")])
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_a"  # The detached worker inherited A.
        ready, reason, _token = trace_hook._validate_identity(self.config, project_key)
        self.assertFalse(ready)
        self.assertEqual(reason, "unbound_credential")
        self.assertEqual({row[3] for row in self._rows()}, {
            trace_hook.hashlib.sha256(b"sr_session_token_a").hexdigest(),
            trace_hook.hashlib.sha256(b"sr_session_token_b").hexdigest(),
        })
        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        self.assertEqual(trace_hook._run_worker(project_key, 1), 0)
        self.assertEqual(len(self.collector.attempted_ids), 1)

    def test_limits_evict_oldest_and_expire_after_seven_days(self) -> None:
        with patch.object(trace_hook, "_MAX_OUTBOX_EVENTS", 2):
            self._enqueue([self._event(f"start:{i}", tool_id=f"call-{i}") for i in range(3)])
        self.assertEqual(len(self._rows()), 2)
        conn = trace_hook._routing_connection()
        try:
            state = conn.execute("SELECT dropped_capacity FROM trace_project_state").fetchone()[0]
            oldest = conn.execute("SELECT event_json FROM trace_outbox_events ORDER BY id LIMIT 1").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(state, 1)
        self.assertIn("call-1", oldest)

        old_time = time.time() - trace_hook._OUTBOX_TTL_SECONDS - 1
        self._enqueue([self._event("old-start", tool_id="call-old")], now=old_time)
        conn = trace_hook._routing_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            trace_hook._prune_outbox(conn, trace_hook._project_key(self.config), time.time())
            conn.commit()
            state = conn.execute("SELECT dropped_expired FROM trace_project_state").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(state, 1)

    def test_byte_limit_and_batch_size_are_enforced(self) -> None:
        long_events = [self._event(f"start:{i}", tool_id=f"call-{i}") for i in range(3)]
        long_events[0]["toolName"] = "x" * 100
        with patch.object(trace_hook, "_MAX_OUTBOX_BYTES", 250):
            self._enqueue(long_events)
            conn = trace_hook._routing_connection()
            try:
                size = conn.execute("SELECT COALESCE(SUM(event_bytes),0) FROM trace_outbox_events").fetchone()[0]
                dropped = conn.execute("SELECT dropped_capacity FROM trace_project_state").fetchone()[0]
            finally:
                conn.close()
        self.assertLessEqual(size, 250)
        self.assertGreater(dropped, 0)

        conn = trace_hook._routing_connection()
        try:
            conn.execute("DELETE FROM trace_outbox_events")
            conn.commit()
        finally:
            conn.close()
        self._enqueue([self._event(f"tool-start:call-{i}", tool_id=f"call-{i}") for i in range(101)])
        trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        batches = [ids for ids in self.collector.attempted_ids]
        self.assertEqual(sorted(map(len, batches)), [1, 100])

    def test_worker_drains_routed_events_after_the_hook_stops(self) -> None:
        project_scope = trace_hook._project_scope(self.config)
        project_key = trace_hook._project_key(self.config)
        events = [self._event(f"tool-start:child-{i}", tool_id=f"child-{i}") for i in range(101)]
        connection = trace_hook._routing_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            trace_hook._record_agent_route(connection, project_scope, "session-1", "agent-1", "root-turn", None, "root-turn", time.time())
            trace_hook._bind_agent_turn(connection, project_scope, "session-1", "agent-1", "child-turn", time.time())
            trace_hook._store_pending_events(connection, project_scope, "session-1", "agent-1", "child-turn", events, time.time(), self.config)
            connection.commit()
        finally:
            connection.close()
        trace_hook._run_worker(project_key, 1)
        self.assertEqual(sorted(map(len, self.collector.attempted_ids)), [1, 100])
        event_requests = [body for path, body, _token in self.collector.requests if path.endswith("/events")]
        self.assertTrue(all(body["sourceTurnId"] == "root-turn" for body in event_requests))

    def test_http_auth_errors_pause_and_other_client_errors_quarantine(self) -> None:
        self.collector.event_statuses = [401]
        self._enqueue()
        trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        self.assertEqual(len(self._rows()), 1)
        self.assertEqual(trace_hook._local_status()["projects"][0]["pauseReason"], "authentication")
        before = len(self.collector.attempted_ids)
        trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        self.assertEqual(len(self.collector.attempted_ids), before)

        os.environ["SEMARAIL_TEST_TOKEN"] = "sr_session_token_b"
        self.collector.event_statuses = [422]
        trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        self.assertEqual(self._rows()[0][2], "quarantined")

    def test_conflicting_event_id_is_counted_and_http_409_quarantines(self) -> None:
        self._enqueue([self._event("stable-event", at="2026-10-09T10:00:00Z")])
        self._enqueue([self._event("stable-event", at="2026-10-09T10:00:01Z")])
        connection = trace_hook._routing_connection()
        try:
            self.assertEqual(connection.execute("SELECT dropped_conflict FROM trace_project_state").fetchone()[0], 1)
        finally:
            connection.close()
        self.collector.event_statuses = [409]
        trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        self.assertEqual(self._rows()[0][2], "quarantined")
        connection = trace_hook._routing_connection()
        try:
            self.assertEqual(connection.execute("SELECT dropped_conflict FROM trace_project_state").fetchone()[0], 2)
        finally:
            connection.close()

    def test_projected_hook_is_durable_before_detached_worker_and_contains_no_secrets(self) -> None:
        hook = {
            "hook_event_name": "PostToolUse", "session_id": "session-1", "turn_id": "turn-1",
            "cwd": str(self.root), "tool_use_id": "call-1", "tool_name": "mcp__example__query",
            "tool_input": {"prompt": "PRIVATE_PROMPT", "apiKey": "PRIVATE_SECRET"},
            "tool_response": {"content": [{"type": "text", "text": "PRIVATE_RESULT"}]},
            "transcript_path": "PRIVATE_TRANSCRIPT_PATH",
        }
        worker_saw_pending = []

        def worker(_config: dict) -> None:
            worker_saw_pending.append(len(self._rows()))

        with patch.object(trace_hook, "_launch_worker", side_effect=worker):
            trace_hook._dispatch_hook("session-1", "turn-1", hook, self.config)
        self.assertEqual(worker_saw_pending, [1])
        stored = b"".join(path.read_bytes() for path in self.base.glob("trace.sqlite3*"))
        for private in (b"PRIVATE_PROMPT", b"PRIVATE_SECRET", b"PRIVATE_RESULT", b"PRIVATE_TRANSCRIPT_PATH", b"sr_session_token_a"):
            self.assertNotIn(private, stored)

    def test_secret_shaped_metadata_is_rejected_before_it_can_reach_the_server(self) -> None:
        event = self._event()
        event["toolName"] = "api_key=PRIVATE_SECRET"
        self.assertIsNone(trace_hook._queue_event(event))
        event = self._event("sr_session_PRIVATE_SECRET")
        self.assertIsNone(trace_hook._queue_event(event))
        invalid = self._event("invalid-event")
        invalid["toolName"] = "mcp__api_key=PRIVATE_SECRET"
        self._enqueue([invalid])
        connection = trace_hook._routing_connection()
        try:
            self.assertEqual(connection.execute("SELECT dropped_invalid FROM trace_project_state").fetchone()[0], 1)
        finally:
            connection.close()

    def test_secret_shaped_identity_and_routing_fields_never_enter_storage(self) -> None:
        marker = "synthetic-secret"
        with patch.object(trace_hook, "_api_request", return_value=(200, {
            "subject": {"id": "token:" + marker, "organizationId": "org-1"}, "projectId": "project-1",
        })):
            self.assertEqual(trace_hook._validate_identity(self.config, trace_hook._project_key(self.config))[:2],
                             (False, "identity_unavailable"))
        hooks = [
            {"hook_event_name": "SubagentStart", "agent_id": "api-key:" + marker},
            {"hook_event_name": "PreToolUse", "tool_use_id": "api-key:" + marker,
             "tool_name": "mcp__server__list_models"},
            {"hook_event_name": "PreToolUse", "tool_use_id": "call-1",
             "tool_name": "mcp__server__token:" + marker},
            {"hook_event_name": "PostToolUse", "tool_use_id": "password:" + marker,
             "tool_name": "collaborationspawn_agent", "tool_response": {"task_name": "/root/worker"}},
        ]
        with patch.object(trace_hook, "_launch_worker"):
            for hook in hooks:
                trace_hook._dispatch_hook("session-1", "turn-1", hook, self.config)
        stored = b"".join(path.read_bytes() for path in self.base.glob("trace.sqlite3*"))
        self.assertNotIn(marker.encode(), stored)

    def test_routed_duplicate_ids_are_idempotent_and_conflicts_are_counted(self) -> None:
        connection = trace_hook._routing_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            event = self._event("same-event")
            trace_hook._store_pending_events(
                connection, trace_hook._project_scope(self.config), "session-1", "agent-1", "child-turn",
                [event], time.time(), self.config,
            )
            trace_hook._store_pending_events(
                connection, trace_hook._project_scope(self.config), "session-1", "agent-1", "child-turn",
                [event], time.time(), self.config,
            )
            changed = {**event, "toolName": "different-tool"}
            trace_hook._store_pending_events(
                connection, trace_hook._project_scope(self.config), "session-1", "agent-1", "child-turn",
                [changed], time.time(), self.config,
            )
            connection.execute("UPDATE trace_pending_events SET event_bytes=0")
            connection.commit()
        finally:
            connection.close()

        connection = trace_hook._routing_connection()
        try:
            row = connection.execute("SELECT event_json,event_bytes FROM trace_pending_events").fetchone()
            dropped_conflict = connection.execute("SELECT dropped_conflict FROM trace_project_state").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(json.loads(row[0])["toolName"], "shell")
        self.assertEqual(row[1], len(row[0].encode("utf-8")))
        self.assertEqual(dropped_conflict, 1)

    def test_cli_flush_reports_an_active_worker_instead_of_claiming_the_queue_is_empty(self) -> None:
        self._enqueue()
        connection = trace_hook._routing_connection()
        try:
            connection.execute(
                "INSERT INTO trace_worker_lease(project_key,owner_id,lease_until) VALUES(?,?,?)",
                (trace_hook._project_key(self.config), "active-worker", time.time() + 60),
            )
            connection.commit()
        finally:
            connection.close()
        output = io.StringIO()
        with patch.object(trace_hook.sys, "stdout", output):
            result = trace_hook._cli(["flush"])
        self.assertEqual(result, 0)
        self.assertIn('"result": "worker_active"', output.getvalue())
        self.assertNotIn('"result": "flushed"', output.getvalue())

    def test_crashed_worker_lease_blocks_flush_until_expiry_then_recovers(self) -> None:
        self._enqueue()
        project_key = trace_hook._project_key(self.config)
        crash_script = "\n".join(
            (
                "import importlib.util, os, sys",
                "spec = importlib.util.spec_from_file_location('trace_hook_crash_test', sys.argv[1])",
                "module = importlib.util.module_from_spec(spec)",
                "spec.loader.exec_module(module)",
                "owner = module._worker_lease(sys.argv[2])",
                "if owner is None: raise SystemExit(3)",
                "os._exit(0)",
            )
        )
        crash_home = self.base / "crash-home"
        crash_home.mkdir()
        crash_environment = {
            "HOME": str(crash_home),
            "USERPROFILE": str(crash_home),
            "SEMARAIL_TRACE_STATE": str(self.db),
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
            "TEMP": os.environ.get("TEMP", ""),
            "TMP": os.environ.get("TMP", ""),
        }
        crashed = subprocess.run(
            [sys.executable, "-c", crash_script, str(_MODULE_PATH), project_key],
            capture_output=True,
            text=True,
            env=crash_environment,
            timeout=10,
            check=False,
        )
        self.assertEqual(crashed.returncode, 0, "crash simulation failed to leave a lease")

        connection = trace_hook._routing_connection()
        try:
            lease = connection.execute(
                "SELECT owner_id,lease_until FROM trace_worker_lease WHERE project_key=?",
                (project_key,),
            ).fetchone()
        finally:
            connection.close()
        self.assertIsNotNone(lease)
        crashed_owner, crashed_lease_until = lease
        self.assertGreater(crashed_lease_until, time.time())
        self.assertEqual(len(self._rows()), 1)
        self.assertEqual(self._rows()[0][2], "pending")

        output = io.StringIO()
        with patch.object(trace_hook.sys, "stdout", output):
            result = trace_hook._cli(["flush"])
        self.assertEqual(result, 0)
        self.assertIn('"result": "worker_active"', output.getvalue())
        self.assertNotIn('"result": "flushed"', output.getvalue())
        self.assertEqual(self.collector.attempted_ids, [])
        self.assertEqual(len(self._rows()), 1)
        connection = trace_hook._routing_connection()
        try:
            lease = connection.execute(
                "SELECT owner_id,lease_until FROM trace_worker_lease WHERE project_key=?",
                (project_key,),
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(lease[0], crashed_owner)
        self.assertGreater(lease[1], time.time())

        connection = trace_hook._routing_connection()
        try:
            connection.execute(
                "UPDATE trace_worker_lease SET lease_until=? WHERE project_key=?",
                (0, project_key),
            )
            connection.commit()
        finally:
            connection.close()

        claimed_owners: list[str | None] = []
        claim_lease = trace_hook._worker_lease

        def claim_new_owner(key: str) -> str | None:
            owner = claim_lease(key)
            claimed_owners.append(owner)
            return owner

        output = io.StringIO()
        with patch.object(trace_hook, "_worker_lease", side_effect=claim_new_owner), patch.object(
            trace_hook.sys, "stdout", output
        ):
            result = trace_hook._cli(["flush"])
        self.assertEqual(result, 0)
        self.assertIn('"result": "flushed"', output.getvalue())
        self.assertEqual(len(claimed_owners), 1)
        self.assertIsNotNone(claimed_owners[0])
        self.assertNotEqual(claimed_owners[0], crashed_owner)
        self.assertEqual(len(self.collector.attempted_ids), 1)
        self.assertEqual(len(self._rows()), 0)
        connection = trace_hook._routing_connection()
        try:
            lease_count = connection.execute(
                "SELECT COUNT(*) FROM trace_worker_lease WHERE project_key=?",
                (project_key,),
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(lease_count, 0)

    def test_real_detached_worker_survives_hook_exit_retries_idempotently_and_quiesces(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            root = base / "project-root"
            root.mkdir()
            state_path = base / "trace-state.sqlite3"
            config_path = base / "trace-projects.json"
            isolated_home = base / "worker-home"
            isolated_home.mkdir()
            event_started = threading.Event()
            release_first_event = threading.Event()
            status_started = threading.Event()
            release_first_status = threading.Event()
            guard = threading.Lock()
            event_attempts: list[list[str]] = []
            event_turns: list[str] = []
            accepted_ids: set[str] = set()
            status_reports: list[dict] = []
            received_requests: list[tuple[str, dict, str]] = []
            active_event_posts = 0
            max_active_event_posts = 0

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, _format: str, *_args: object) -> None:
                    return

                def _read_body(self) -> dict:
                    length = int(self.headers.get("Content-Length", "0"))
                    raw = self.rfile.read(length) if length else b""
                    return json.loads(raw) if raw else {}

                def _reply(self, status: int, body: dict) -> None:
                    raw = json.dumps(body).encode("utf-8")
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

                def do_GET(self) -> None:
                    token = self.headers.get("Authorization", "").removeprefix("Bearer ")
                    with guard:
                        received_requests.append((self.path, {}, token))
                    if token != "sr_session_test_detached_worker":
                        self._reply(401, {"error": "PRIVATE_SERVER_DIAGNOSTIC"})
                        return
                    self._reply(200, {
                        "subject": {"id": "subject-process-test", "organizationId": "org-process-test"},
                        "projectId": "project-process-test",
                    })

                def do_POST(self) -> None:
                    nonlocal active_event_posts, max_active_event_posts
                    token = self.headers.get("Authorization", "").removeprefix("Bearer ")
                    body = self._read_body()
                    with guard:
                        received_requests.append((self.path, body, token))
                    if self.path.endswith("/events"):
                        ids = [event.get("eventId") for event in body.get("events", [])]
                        turn_id = body.get("sourceTurnId", "")
                        with guard:
                            index = len(event_attempts)
                            event_attempts.append(ids)
                            event_turns.append(turn_id)
                            active_event_posts += 1
                            max_active_event_posts = max(max_active_event_posts, active_event_posts)
                        try:
                            if index == 0:
                                event_started.set()
                                release_first_event.wait(10)
                                self._reply(503, {"error": "PRIVATE_SERVER_DIAGNOSTIC"})
                                return
                            with guard:
                                accepted_ids.update(ids)
                            if index == 1:
                                # The server accepted the stable IDs, but the
                                # response is lost. The next request must replay
                                # the same IDs without creating duplicate spans.
                                self.close_connection = True
                                try:
                                    self.connection.shutdown(socket.SHUT_RDWR)
                                except OSError:
                                    pass
                                self.connection.close()
                                return
                            self._reply(201, {"schemaVersion": 1, "accepted": True})
                        finally:
                            with guard:
                                active_event_posts -= 1
                    else:
                        with guard:
                            status_reports.append(body)
                        if body.get("pendingCount") == 0 and not status_started.is_set():
                            status_started.set()
                            release_first_status.wait(10)
                        self._reply(201, {"schemaVersion": 1, "collectorId": body.get("collectorId")})

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()
            endpoint = f"http://127.0.0.1:{server.server_port}"
            config = {
                "root": str(root), "enabled": True, "endpoint": endpoint,
                "tokenEnv": "SEMARAIL_DETACHED_TEST_TOKEN",
            }
            config_path.write_text(json.dumps({"projects": [config]}), encoding="utf-8")
            project_key = trace_hook._project_key(config)
            env = {
                key: os.environ[key]
                for key in ("PATH", "SystemRoot", "WINDIR", "TEMP", "TMP")
                if key in os.environ
            }
            env.update({
                "SEMARAIL_TRACE_CONFIG": str(config_path),
                "SEMARAIL_TRACE_STATE": str(state_path),
                "SEMARAIL_DETACHED_TEST_TOKEN": "sr_session_test_detached_worker",
                "HOME": str(isolated_home),
                "USERPROFILE": str(isolated_home),
            })

            def run_actual_hook(turn_id: str, tool_id: str) -> tuple[bytes, bytes]:
                hook = {
                    "hook_event_name": "PostToolUse", "session_id": "session-process-test",
                    "turn_id": turn_id, "cwd": str(root), "tool_use_id": tool_id,
                    "tool_name": "shell",
                    "tool_input": {"query": "PRIVATE_PROMPT", "apiKey": "PRIVATE_SECRET"},
                    "tool_response": {"content": [{"type": "text", "text": "PRIVATE_RESULT"}]},
                    "transcript_path": str(base / "PRIVATE_TRANSCRIPT_PATH"),
                }
                process = subprocess.Popen(
                    [sys.executable, str(_MODULE_PATH)], cwd=root, env=env,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                stdout, stderr = process.communicate(json.dumps(hook).encode("utf-8"), timeout=10)
                if process.returncode != 0:
                    raise AssertionError(f"hook exited {process.returncode}: {stderr.decode(errors='replace')}")
                return stdout, stderr

            def lease_state() -> tuple[int, int, int, int]:
                connection = sqlite3.connect(state_path, timeout=1)
                try:
                    lease = connection.execute(
                        "SELECT COUNT(*),COALESCE(MAX(lease_until),0),COALESCE(MAX(wake_requested),0) "
                        "FROM trace_worker_lease WHERE project_key=?",
                        (project_key,),
                    ).fetchone()
                    pending = connection.execute(
                        "SELECT COUNT(*) FROM trace_outbox_events WHERE project_key=? AND state='pending'",
                        (project_key,),
                    ).fetchone()[0]
                    return lease[0], int(lease[1] > time.time()), pending, lease[2]
                finally:
                    connection.close()

            def wait_until(predicate, timeout: float = 20) -> bool:
                deadline = time.time() + timeout
                while time.time() < deadline:
                    if predicate():
                        return True
                    time.sleep(0.05)
                return bool(predicate())

            try:
                first_stdout, first_stderr = run_actual_hook("turn-process-a", "call-process-a")
                self.assertEqual(first_stdout, b"")
                self.assertEqual(first_stderr, b"")
                self.assertTrue(event_started.wait(5), "detached worker did not reach the loopback collector")
                # The hook process has already exited while its hidden child is
                # blocked on the collector, proving delivery outlives the hook.
                self.assertEqual(lease_state(), (1, 1, 1, 0))
                release_first_event.set()
                self.assertTrue(status_started.wait(10), "worker did not retry and reach collector-status")

                second_stdout, second_stderr = run_actual_hook("turn-process-b", "call-process-b")
                self.assertEqual(second_stdout, b"")
                self.assertEqual(second_stderr, b"")
                self.assertEqual(lease_state(), (1, 1, 1, 1))
                release_first_status.set()

                self.assertTrue(wait_until(lambda: lease_state()[0] == 0), "detached worker did not release its lease")
                self.assertEqual(lease_state(), (0, 0, 0, 0))
                time.sleep(0.25)
                with guard:
                    attempts_snapshot = [list(ids) for ids in event_attempts]
                    turns_snapshot = list(event_turns)
                    accepted_snapshot = set(accepted_ids)
                    reports_snapshot = list(status_reports)
                    requests_snapshot = list(received_requests)
                    max_active = max_active_event_posts
                self.assertEqual(len(attempts_snapshot), 4)
                self.assertEqual(attempts_snapshot[0], attempts_snapshot[1])
                self.assertEqual(attempts_snapshot[1], attempts_snapshot[2])
                self.assertNotEqual(attempts_snapshot[2], attempts_snapshot[3])
                self.assertEqual(turns_snapshot, ["turn-process-a"] * 3 + ["turn-process-b"])
                self.assertEqual(len(accepted_snapshot), 2)
                self.assertEqual(max_active, 1)
                self.assertGreaterEqual(len(reports_snapshot), 4)
                self.assertEqual((reports_snapshot[0]["pendingCount"], reports_snapshot[0]["retryCount"]), (1, 1))
                self.assertEqual((reports_snapshot[1]["pendingCount"], reports_snapshot[1]["retryCount"]), (1, 2))
                report = reports_snapshot[-1]
                self.assertEqual(report["pendingCount"], 0)
                self.assertEqual(report["retryCount"], 2)
                self.assertEqual(report["droppedCount"], 0)
                self.assertEqual(report["pausedCount"], 0)
                self.assertEqual(report["quarantinedCount"], 0)
                self.assertIsNotNone(report["lastSuccessAt"])
                self.assertEqual(report["dropReasons"], {"expired": 0, "capacity": 0, "invalid": 0, "conflict": 0})
                self.assertEqual(set(report), {
                    "schemaVersion", "collectorId", "observedAt", "pendingCount", "retryCount", "droppedCount",
                    "pausedCount", "quarantinedCount", "lastSuccessAt", "dropReasons",
                })
                self.assertEqual(str(trace_hook.uuid.UUID(report["collectorId"])), report["collectorId"])
                self.assertTrue(report["observedAt"].endswith("Z"))
                self.assertEqual({r["collectorId"] for r in reports_snapshot}, {report["collectorId"]})
                payloads_snapshot = [(path, body) for path, body, _token in requests_snapshot]
                serialized = json.dumps([payloads_snapshot, reports_snapshot])
                for secret in (
                    str(root), endpoint, "sr_session_test_detached_worker", "PRIVATE_PROMPT", "PRIVATE_SECRET",
                    "PRIVATE_RESULT", "PRIVATE_TRANSCRIPT_PATH", "PRIVATE_SERVER_DIAGNOSTIC",
                ):
                    self.assertNotIn(secret, serialized)
                db_bytes = b"".join(path.read_bytes() for path in base.glob("trace-state.sqlite3*"))
                for secret in (
                    str(root).encode(), endpoint.encode(), b"sr_session_test_detached_worker", b"PRIVATE_PROMPT",
                    b"PRIVATE_SECRET", b"PRIVATE_RESULT", b"PRIVATE_TRANSCRIPT_PATH", b"PRIVATE_SERVER_DIAGNOSTIC",
                ):
                    self.assertNotIn(secret, db_bytes)
                with guard:
                    attempts_after_quiescence = len(event_attempts)
                self.assertEqual(attempts_after_quiescence, 4)
                self.assertEqual(len([r for r in requests_snapshot if r[0].endswith("/api/v1/traces/collector-status")]), len(reports_snapshot))
            finally:
                release_first_event.set()
                release_first_status.set()
                # All server responses now succeed; leave any owned detached
                # worker to finish and confirm its lease is gone before server
                # teardown, without terminating unrelated processes.
                wait_until(lambda: not state_path.exists() or lease_state()[0] == 0, timeout=20)
                server.shutdown()
                server.server_close()
                server_thread.join(timeout=2)

    def test_routed_subagent_event_is_in_outbox_before_worker_launch(self) -> None:
        launch_snapshots: list[list[dict]] = []

        def inspect_queue(_config: dict) -> None:
            batch = trace_hook._outbox_batch(
                trace_hook._project_key(self.config), time.time(), trace_hook._credential_hash(self.config),
            )
            launch_snapshots.append(batch[1]["events"] if batch else [])

        with patch.object(trace_hook, "_launch_worker", side_effect=inspect_queue):
            trace_hook._dispatch_hook("session-1", "root-turn", {
                "hook_event_name": "PostToolUse", "tool_use_id": "spawn-call", "tool_name": "collaborationspawn_agent",
                "tool_response": {"agent_id": "agent-1"},
            }, self.config)
            trace_hook._dispatch_hook("session-1", "child-turn", {
                "hook_event_name": "SubagentStart", "agent_id": "agent-1", "model": "gpt-6-luna",
            }, self.config)
        self.assertEqual(len(launch_snapshots), 2)
        connection = trace_hook._routing_connection()
        try:
            routed = connection.execute(
                "SELECT source_turn_id,event_json FROM trace_outbox_events WHERE event_json LIKE '%subagent_started%'"
            ).fetchone()
        finally:
            connection.close()
        self.assertIsNotNone(routed)
        self.assertEqual(routed[0], "root-turn")
        self.assertEqual(json.loads(routed[1])["type"], "subagent_started")

    def test_collector_status_is_exact_and_local_status_marks_absence_stale(self) -> None:
        self._enqueue()
        trace_hook._run_worker(trace_hook._project_key(self.config), 1)
        status_body = next(body for path, body, _token in self.collector.requests if path.endswith("collector-status"))
        self.assertEqual(set(status_body), {
            "schemaVersion", "collectorId", "observedAt", "pendingCount", "retryCount", "droppedCount",
            "pausedCount", "quarantinedCount", "lastSuccessAt", "dropReasons",
        })
        self.assertEqual(set(status_body["dropReasons"]), {"expired", "capacity", "invalid", "conflict"})
        serialized = json.dumps(status_body)
        for private in (str(self.root), self.collector.endpoint, "sr_session_token_a"):
            self.assertNotIn(private, serialized)

        empty_root = self.base / "empty-project"
        empty_root.mkdir()
        empty_key = trace_hook._project_key({**self.config, "root": str(empty_root)})[:12]
        with patch.object(trace_hook, "_all_project_configs", return_value=[self.config, {**self.config, "root": str(empty_root)}]):
            items = trace_hook._local_status()["projects"]
        empty = next(item for item in items if item["projectKey"] == empty_key)
        self.assertTrue(empty["statusStale"])
        self.assertEqual(empty["pendingCount"], 0)

    def test_installed_script_cli_flushes_actual_hook_queue_and_reports_status(self) -> None:
        config_path = self.base / "trace-projects.json"
        config_path.write_text(json.dumps({"projects": [self.config]}), encoding="utf-8")
        hook = {
            "hook_event_name": "PostToolUse", "session_id": "session-cli", "turn_id": "turn-cli",
            "cwd": str(self.root), "tool_use_id": "call-cli", "tool_name": "shell",
            "tool_input": {"command": "PRIVATE_COMMAND"}, "tool_response": {"content": [{"text": "PRIVATE_OUTPUT"}]},
        }
        with patch.object(trace_hook, "_launch_worker"):
            trace_hook._dispatch_hook("session-cli", "turn-cli", hook, self.config)
        environment = os.environ.copy()
        environment["SEMARAIL_TRACE_CONFIG"] = str(config_path)
        environment["SEMARAIL_TRACE_STATE"] = str(self.db)
        flush = subprocess.run(
            [sys.executable, str(_MODULE_PATH), "flush"], capture_output=True, text=True,
            env=environment, timeout=15, check=False,
        )
        self.assertEqual(flush.returncode, 0, flush.stdout + flush.stderr)
        self.assertEqual(len(self.collector.attempted_ids), 1)
        status = subprocess.run(
            [sys.executable, str(_MODULE_PATH), "status"], capture_output=True, text=True,
            env=environment, timeout=10, check=False,
        )
        self.assertEqual(status.returncode, 0, status.stderr)
        local = json.loads(status.stdout)
        self.assertEqual(local["projects"][0]["pendingCount"], 0)
        for secret in (str(self.root), self.collector.endpoint, "sr_session_token_a", "PRIVATE_COMMAND", "PRIVATE_OUTPUT"):
            self.assertNotIn(secret, flush.stdout + status.stdout)
        clean = subprocess.run(
            [sys.executable, str(_MODULE_PATH), "clean", "--all"], capture_output=True, text=True,
            env=environment, timeout=10, check=False,
        )
        self.assertEqual(clean.returncode, 0, clean.stderr)

    def test_hook_launches_detached_worker_without_credentials_or_project_paths(self) -> None:
        with patch.object(trace_hook.subprocess, "Popen") as popen:
            trace_hook._launch_worker(self.config)
        args, kwargs = popen.call_args
        serialized = json.dumps(args)
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn("sr_session_token_a", serialized)
        self.assertEqual(args[0][-2:], ["--worker", trace_hook._project_key(self.config)])
        self.assertEqual(kwargs["stdin"], trace_hook.subprocess.DEVNULL)
        self.assertEqual(kwargs["stdout"], trace_hook.subprocess.DEVNULL)
        self.assertEqual(kwargs["stderr"], trace_hook.subprocess.DEVNULL)
        if os.name == "nt":
            self.assertTrue(kwargs["creationflags"] & trace_hook.subprocess.CREATE_NO_WINDOW)
            self.assertTrue(kwargs["creationflags"] & trace_hook.subprocess.DETACHED_PROCESS)
        else:
            self.assertTrue(kwargs["start_new_session"])


if __name__ == "__main__":
    unittest.main()
