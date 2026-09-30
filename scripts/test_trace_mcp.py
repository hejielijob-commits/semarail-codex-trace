"""Focused tests for the byte-preserving MCP stdio observer."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import threading
from types import ModuleType
import unittest
from pathlib import Path
from unittest.mock import patch


_MODULE_PATH = Path(__file__).with_name("trace_mcp.py")
_SPEC = importlib.util.spec_from_file_location("trace_mcp", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
trace_mcp = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(trace_mcp)


def _request(
    rpc_id: str,
    *,
    session_id: str = "session-1",
    turn_id: str = "turn-1",
    thread_id: str | None = "session-1",
    call_id: str = "call-1",
) -> bytes:
    turn_metadata = {"session_id": session_id, "turn_id": turn_id}
    if thread_id is not None:
        turn_metadata["thread_id"] = thread_id
    value = {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "method": "tools/call",
        "params": {
            "name": "mcp__semarail__query",
            "arguments": {"question": "PRIVATE_TOOL_ARGUMENT"},
            "_meta": {"callId": call_id, "x-codex-turn-metadata": turn_metadata},
        },
    }
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


def _response(rpc_id: str, *, is_error: bool | None = None) -> bytes:
    result = {
        "_meta": {"traceId": "trace-1", "unrelated": "PRIVATE_META"},
        "content": [{"type": "text", "text": "PRIVATE_TOOL_RESULT"}],
    }
    if is_error is not None:
        result["isError"] = is_error
    value = {"jsonrpc": "2.0", "id": rpc_id, "result": result}
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


class TraceMCPTests(unittest.TestCase):
    def test_callback_load_requires_enabled_project(self) -> None:
        adapter = ModuleType("trace_hook")
        callback = lambda *_args: None
        adapter._observe_mcp_result = callback
        adapter._project_config = lambda cwd: None
        with patch.dict(sys.modules, {"trace_hook": adapter}), patch.object(trace_mcp.os, "getcwd", return_value="C:/disabled"):
            self.assertIsNone(trace_mcp._load_callback())

        adapter._project_config = lambda cwd: {"root": cwd, "enabled": True}
        with patch.dict(sys.modules, {"trace_hook": adapter}), patch.object(trace_mcp.os, "getcwd", return_value="C:/enabled"):
            self.assertIs(trace_mcp._load_callback(), callback)

    def test_observer_integrates_with_registered_pretooluse_callback(self) -> None:
        hook_path = Path(__file__).with_name("trace_hook.py")
        hook_spec = importlib.util.spec_from_file_location("trace_hook", hook_path)
        assert hook_spec is not None and hook_spec.loader is not None
        adapter = importlib.util.module_from_spec(hook_spec)
        hook_spec.loader.exec_module(adapter)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "enabled"
            root.mkdir()
            config = {"root": str(root), "enabled": True, "endpoint": "http://127.0.0.1:48763"}
            state_path = Path(temporary) / "routing.sqlite3"
            sent: list[dict] = []
            completion_sent = threading.Event()

            def send(_config: dict, payload: dict) -> bool:
                sent.append(payload)
                if any(event.get("type") == "tool_completed" for event in payload.get("events", [])):
                    completion_sent.set()
                return True

            with (
                patch.dict(sys.modules, {"trace_hook": adapter}),
                patch.object(trace_mcp.os, "getcwd", return_value=str(root)),
                patch.object(adapter, "_STATE_PATH", state_path),
                patch.object(adapter, "_project_config", return_value=config),
                patch.object(adapter, "_token", return_value="sr_session_test"),
                patch.object(adapter, "_send", side_effect=send),
            ):
                callback = trace_mcp._load_callback()
                self.assertIs(callback, adapter._observe_mcp_result)
                adapter._dispatch_hook("session-1", "turn-1", {
                    "hook_event_name": "PreToolUse",
                    "tool_use_id": "call-integrated",
                    "tool_name": "mcp__semarail__query_run",
                }, config)

                observer = trace_mcp._MCPObserver(callback, str(root))
                observer.feed_requests(_request("rpc-integrated", call_id="call-integrated"))
                observer.feed_responses(_response("rpc-integrated", is_error=True))
                self.assertTrue(completion_sent.wait(1))
                observer.close()

            completed = [event for batch in sent for event in batch.get("events", []) if event.get("type") == "tool_completed"]
            self.assertEqual(len(completed), 1)
            self.assertEqual(completed[0]["toolUseId"], "call-integrated")
            self.assertEqual(completed[0]["coreTraceId"], "trace-1")
            self.assertEqual(completed[0]["status"], "failure")
            serialized = json.dumps(sent)
            for private in ("PRIVATE_TOOL_ARGUMENT", "PRIVATE_TOOL_RESULT", "PRIVATE_META"):
                self.assertNotIn(private, serialized)

    def test_wrapper_preserves_full_streams_and_child_exit_code(self) -> None:
        payload = (
            b'{"jsonrpc":"2.0","id":7,"method":"tools/call"}\r\n'
            b"opaque-bytes:\x00\xff\nlast-fragment"
        )
        child_stdout_prefix = b"child-start\r\n"
        child_stdout_suffix = b"\nchild-end"
        child = (
            "import sys; data=sys.stdin.buffer.read(); "
            f"sys.stdout.buffer.write({child_stdout_prefix!r}+data+{child_stdout_suffix!r}); "
            "sys.stdout.buffer.flush(); sys.stderr.buffer.write(b'child-error\\n'); "
            "sys.exit(23)"
        )
        completed = subprocess.run(
            [sys.executable, str(_MODULE_PATH), "--", sys.executable, "-c", child],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=10,
        )
        self.assertEqual(completed.returncode, 23)
        self.assertEqual(completed.stdout, child_stdout_prefix + payload + child_stdout_suffix)
        self.assertEqual(completed.stderr, b"child-error\n")

    def test_only_allowlisted_metadata_reaches_async_callback(self) -> None:
        calls: list[tuple[dict, dict, str, str]] = []
        delivered = threading.Event()

        def callback(metadata: dict, result: dict, cwd: str, occurred_at: str) -> None:
            calls.append((metadata, result, cwd, occurred_at))
            delivered.set()

        observer = trace_mcp._MCPObserver(callback, "C:/project")
        observer.feed_requests(_request("rpc-child", thread_id="agent-2"))
        observer.feed_responses(_response("rpc-child", is_error=True))
        self.assertTrue(delivered.wait(1))
        observer.close()

        self.assertEqual(len(calls), 1)
        metadata, result, cwd, occurred_at = calls[0]
        self.assertEqual(
            metadata,
            {"callId": "call-1", "sessionId": "session-1", "turnId": "turn-1", "agentId": "agent-2"},
        )
        self.assertEqual(result, {"traceId": "trace-1", "isError": True})
        self.assertEqual(cwd, "C:/project")
        self.assertTrue(occurred_at.endswith("Z"))
        encoded = json.dumps({"metadata": metadata, "result": result})
        for private in ("PRIVATE_TOOL_ARGUMENT", "PRIVATE_TOOL_RESULT", "PRIVATE_META"):
            self.assertNotIn(private, encoded)

    def test_root_thread_omits_agent_and_absent_is_error_defaults_false(self) -> None:
        calls: list[tuple[dict, dict, str, str]] = []
        delivered = threading.Event()

        def callback(metadata: dict, result: dict, cwd: str, occurred_at: str) -> None:
            calls.append((metadata, result, cwd, occurred_at))
            delivered.set()

        observer = trace_mcp._MCPObserver(callback, "C:/project")
        observer.feed_requests(_request("rpc-root", thread_id="session-1"))
        observer.feed_responses(_response("rpc-root"))
        self.assertTrue(delivered.wait(1))
        observer.close()
        self.assertEqual(calls[0][0], {"callId": "call-1", "sessionId": "session-1", "turnId": "turn-1"})
        self.assertEqual(calls[0][1], {"traceId": "trace-1", "isError": False})

    def test_invalid_or_unmatched_calls_are_not_observed(self) -> None:
        calls: list[tuple[dict, dict, str, str]] = []
        observer = trace_mcp._MCPObserver(lambda *args: calls.append(args), "C:/project")

        # Missing callId fails strict metadata validation, even if Core returned a trace id.
        observer.feed_requests(_request("rpc-no-call", call_id="INVALID ID"))
        observer.feed_responses(_response("rpc-no-call"))

        # Reusing an outstanding RPC ID with unusable metadata drops old attribution.
        observer.feed_requests(_request("rpc-reused", call_id="call-3"))
        observer.feed_requests(_request("rpc-reused", call_id="INVALID ID"))
        observer.feed_responses(_response("rpc-reused"))

        # Two valid in-flight calls with the same JSON-RPC id are ambiguous too.
        observer.feed_requests(_request("rpc-duplicate", call_id="call-5"))
        observer.feed_requests(_request("rpc-duplicate", call_id="call-6"))
        observer.feed_responses(_response("rpc-duplicate"))

        self.assertIsNone(trace_mcp._rpc_id_key("x" * 129))
        self.assertIsNone(trace_mcp._rpc_id_key(float("nan")))

        # Missing thread identity cannot be guessed as a main-thread call.
        observer.feed_requests(_request("rpc-no-thread", thread_id=None, call_id="call-4"))
        observer.feed_responses(_response("rpc-no-thread"))

        # A JSON-RPC error and an invalid isError value are not actual MCP results.
        observer.feed_requests(_request("rpc-error"))
        observer.feed_responses(b'{"jsonrpc":"2.0","id":"rpc-error","error":{"code":-1}}\n')
        observer.feed_requests(_request("rpc-bad-flag", call_id="call-2"))
        observer.feed_responses(
            b'{"jsonrpc":"2.0","id":"rpc-bad-flag","result":{"_meta":{"traceId":"trace-2"},"isError":"false"}}\n'
        )

        # A valid result for an unknown JSON-RPC id has no matching request.
        observer.feed_responses(_response("rpc-unmatched"))
        observer.close()
        self.assertEqual(calls, [])

    def test_request_and_response_parsing_handles_fragmented_lines(self) -> None:
        calls: list[tuple[dict, dict, str, str]] = []
        delivered = threading.Event()

        def callback(*args: tuple) -> None:
            calls.append(args)
            delivered.set()

        observer = trace_mcp._MCPObserver(callback, "C:/project")
        request = _request("rpc-fragmented")
        response = _response("rpc-fragmented")
        for index in range(0, len(request), 7):
            observer.feed_requests(request[index : index + 7])
        for index in range(0, len(response), 9):
            observer.feed_responses(response[index : index + 9])
        self.assertTrue(delivered.wait(1))
        observer.close()
        self.assertEqual(calls[0][1], {"traceId": "trace-1", "isError": False})

    def test_callback_fault_and_full_queue_never_block_stream_observation(self) -> None:
        callback_started = threading.Event()
        release_callback = threading.Event()
        second_callback_started = threading.Event()
        callback_count = 0
        callback_lock = threading.Lock()

        def broken_callback(*_args: tuple) -> None:
            nonlocal callback_count
            with callback_lock:
                callback_count += 1
                current = callback_count
            if current == 1:
                callback_started.set()
                release_callback.wait(2)
            elif current == 2:
                second_callback_started.set()
            raise RuntimeError("collector unavailable")

        observer = trace_mcp._MCPObserver(broken_callback, "C:/project", callback_queue_size=1)
        observer.feed_requests(_request("rpc-1", call_id="call-1"))
        observer.feed_responses(_response("rpc-1"))
        self.assertTrue(callback_started.wait(1))

        # One response occupies the bounded queue while the worker is blocked.
        observer.feed_requests(_request("rpc-2", call_id="call-2"))
        observer.feed_responses(_response("rpc-2"))
        self.assertEqual(observer._queue.qsize(), 1)

        # A third event is dropped immediately instead of waiting on the callback.
        feed_finished = threading.Event()

        def feed_third_response() -> None:
            observer.feed_requests(_request("rpc-3", call_id="call-3"))
            observer.feed_responses(_response("rpc-3"))
            feed_finished.set()

        feed_thread = threading.Thread(target=feed_third_response, daemon=True)
        feed_thread.start()
        returned_without_wait = feed_finished.wait(0.5)
        release_callback.set()
        self.assertTrue(returned_without_wait)
        self.assertTrue(second_callback_started.wait(1))
        self.assertEqual(callback_count, 2)
        observer.close()


if __name__ == "__main__":
    unittest.main()
