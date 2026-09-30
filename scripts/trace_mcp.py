"""Pass-through stdio wrapper that correlates metadata-only MCP tool results.

Run as ``python trace_mcp.py -- <stdio-server-command> [args...]``. All protocol
bytes are forwarded unchanged. Observation is best-effort and never exposes
tool parameters or result bodies to the trace adapter.
"""

from __future__ import annotations

import json
import math
import os
import queue
import re
import subprocess
import sys
import threading
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any, BinaryIO, Callable


_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_MAX_FRAME_BYTES = 1_048_576
_MAX_OUTSTANDING = 512
_MAX_CALLBACK_QUEUE = 128
_COPY_BYTES = 65_536
_STOP = object()

_Callback = Callable[[dict[str, str], dict[str, Any], str, str], Any]


def _identifier(value: Any) -> str | None:
    return value if isinstance(value, str) and _IDENTIFIER.fullmatch(value) else None


def _rpc_id_key(value: Any) -> tuple[str, Any] | None:
    """Return a stable key for JSON-RPC string or numeric request IDs."""
    if isinstance(value, str) and len(value) <= 128:
        return ("string", value)
    if isinstance(value, int) and not isinstance(value, bool):
        return ("number", value)
    if isinstance(value, float) and math.isfinite(value):
        return ("number", value)
    return None


class _JsonLineDecoder:
    """Decode bounded newline-delimited JSON frames without changing the stream."""

    def __init__(self, max_frame_bytes: int = _MAX_FRAME_BYTES) -> None:
        self._max_frame_bytes = max_frame_bytes
        self._partial = bytearray()
        self._dropping_oversize = False

    def feed(self, data: bytes, *, final: bool = False) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        offset = 0
        while offset < len(data):
            newline = data.find(b"\n", offset)
            if newline < 0:
                remainder = data[offset:]
                if not self._dropping_oversize:
                    if len(self._partial) + len(remainder) > self._max_frame_bytes:
                        self._partial.clear()
                        self._dropping_oversize = True
                    else:
                        self._partial.extend(remainder)
                break

            frame_part = data[offset:newline]
            if self._dropping_oversize:
                self._dropping_oversize = False
                self._partial.clear()
            elif len(self._partial) + len(frame_part) <= self._max_frame_bytes:
                self._partial.extend(frame_part)
                messages.extend(self._decode(bytes(self._partial)))
                self._partial.clear()
            else:
                self._partial.clear()
            offset = newline + 1

        if final and not self._dropping_oversize and self._partial:
            messages.extend(self._decode(bytes(self._partial)))
            self._partial.clear()
        return messages

    @staticmethod
    def _decode(frame: bytes) -> list[dict[str, Any]]:
        if not frame.strip():
            return []
        try:
            value = json.loads(frame)
        except (UnicodeDecodeError, ValueError):
            return []
        if isinstance(value, dict):
            return [value]
        if isinstance(value, list):
            # JSON-RPC batches are uncommon for MCP, but preserving them on the
            # wire while examining each object makes correlation unambiguous.
            return [item for item in value if isinstance(item, dict)]
        return []


class _MCPObserver:
    """Correlate calls and send only allowlisted metadata to a background callback."""

    def __init__(
        self,
        callback: _Callback | None,
        cwd: str,
        *,
        max_outstanding: int = _MAX_OUTSTANDING,
        callback_queue_size: int = _MAX_CALLBACK_QUEUE,
        max_frame_bytes: int = _MAX_FRAME_BYTES,
    ) -> None:
        self._callback = callback
        self._cwd = cwd
        self._max_outstanding = max(1, max_outstanding)
        self._requests = _JsonLineDecoder(max_frame_bytes)
        self._responses = _JsonLineDecoder(max_frame_bytes)
        self._pending: OrderedDict[tuple[str, Any], dict[str, str] | None] = OrderedDict()
        self._pending_lock = threading.Lock()
        self._queue: queue.Queue[Any] | None = None
        self._worker: threading.Thread | None = None
        if callback is not None:
            self._queue = queue.Queue(maxsize=max(1, callback_queue_size))
            self._worker = threading.Thread(target=self._deliver, name="trace-mcp-observer", daemon=True)
            self._worker.start()

    def feed_requests(self, data: bytes) -> None:
        if self._callback is None:
            return
        for message in self._requests.feed(data):
            self._remember_request(message)

    def finish_requests(self) -> None:
        if self._callback is None:
            return
        for message in self._requests.feed(b"", final=True):
            self._remember_request(message)

    def feed_responses(self, data: bytes) -> None:
        if self._callback is None:
            return
        for message in self._responses.feed(data):
            self._observe_response(message)

    def finish_responses(self) -> None:
        if self._callback is None:
            return
        for message in self._responses.feed(b"", final=True):
            self._observe_response(message)

    def close(self) -> None:
        if self._queue is not None:
            try:
                self._queue.put_nowait(_STOP)
            except queue.Full:
                # The worker is best-effort and daemonized. Do not wait on it
                # while the stdio transport is closing.
                pass

    def _remember_request(self, message: dict[str, Any]) -> None:
        if message.get("jsonrpc") != "2.0":
            return
        rpc_id = _rpc_id_key(message.get("id"))
        if rpc_id is None:
            return
        with self._pending_lock:
            if rpc_id in self._pending:
                # Reusing an outstanding JSON-RPC id is ambiguous. Retain a
                # tombstone until a response arrives so neither response can
                # be attributed to either tool call.
                self._pending[rpc_id] = None
                self._pending.move_to_end(rpc_id)
                return
        if message.get("method") != "tools/call":
            return
        params = message.get("params")
        if not isinstance(params, dict):
            return
        request_meta = params.get("_meta")
        if not isinstance(request_meta, dict):
            return
        call_id = _identifier(request_meta.get("callId"))
        turn_meta = request_meta.get("x-codex-turn-metadata")
        if not isinstance(turn_meta, dict):
            return
        session_id = _identifier(turn_meta.get("session_id"))
        turn_id = _identifier(turn_meta.get("turn_id"))
        thread_id = _identifier(turn_meta.get("thread_id"))
        if call_id is None or session_id is None or turn_id is None or thread_id is None:
            return

        metadata = {"callId": call_id, "sessionId": session_id, "turnId": turn_id}
        if thread_id != session_id:
            metadata["agentId"] = thread_id

        with self._pending_lock:
            # The output pump shares this map; protect check-plus-insert so a
            # response can never race with a reused request ID.
            if rpc_id in self._pending:
                self._pending[rpc_id] = None
                self._pending.move_to_end(rpc_id)
                return
            if len(self._pending) >= self._max_outstanding:
                # Drop new calls on capacity pressure. Evicting an in-flight
                # ID could let a delayed response match a later reused ID.
                return
            self._pending[rpc_id] = metadata

    def _observe_response(self, message: dict[str, Any]) -> None:
        if message.get("jsonrpc") != "2.0":
            return
        rpc_id = _rpc_id_key(message.get("id"))
        if rpc_id is None:
            return
        with self._pending_lock:
            metadata = self._pending.pop(rpc_id, None)
        if metadata is None or "result" not in message:
            return
        result = message.get("result")
        if not isinstance(result, dict):
            return
        result_meta = result.get("_meta")
        if not isinstance(result_meta, dict):
            return
        trace_id = _identifier(result_meta.get("traceId"))
        if trace_id is None:
            return
        is_error = result.get("isError", False)
        if not isinstance(is_error, bool):
            return

        # The event time is captured before queueing so callback or local
        # collector latency does not become part of the observed tool interval.
        occurred_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        safe_result = {"traceId": trace_id, "isError": is_error}
        assert self._queue is not None
        try:
            self._queue.put_nowait((metadata, safe_result, self._cwd, occurred_at))
        except queue.Full:
            # Never back-pressure Codex for a trace event.
            pass

    def _deliver(self) -> None:
        assert self._queue is not None and self._callback is not None
        while True:
            item = self._queue.get()
            if item is _STOP:
                return
            metadata, result, cwd, occurred_at = item
            try:
                self._callback(metadata, result, cwd, occurred_at)
            except Exception:
                # Observation must never alter a child process or protocol stream.
                continue


def _load_callback() -> _Callback | None:
    """Load the callback only for an explicitly enabled project."""
    try:
        from trace_hook import _observe_mcp_result, _project_config
        cwd = os.getcwd()
        if _project_config(cwd) is None:
            return None
    except Exception:
        return None
    return _observe_mcp_result


def _read_chunks(stream: BinaryIO):
    reader = getattr(stream, "read1", stream.read)
    while True:
        chunk = reader(_COPY_BYTES)
        if not chunk:
            return
        yield chunk


def _write_all(stream: BinaryIO, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = stream.write(view)
        if written is None or written <= 0:
            raise BrokenPipeError("stdio stream closed")
        view = view[written:]


def _run_child(command: list[str], *, callback: _Callback | None = None) -> int:
    try:
        child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None, bufsize=0)
    except OSError as error:
        print(f"trace_mcp: unable to start child process: {error}", file=sys.stderr)
        return 127

    assert child.stdin is not None and child.stdout is not None
    observer = _MCPObserver(callback, os.getcwd())

    def copy_input() -> None:
        try:
            for chunk in _read_chunks(sys.stdin.buffer):
                # Record metadata before forwarding so an immediate child reply
                # cannot beat request registration in the output pump.
                try:
                    observer.feed_requests(chunk)
                except Exception:
                    # Parser or observer failures must not interrupt raw bytes.
                    pass
                _write_all(child.stdin, chunk)
                child.stdin.flush()
            try:
                observer.finish_requests()
            except Exception:
                pass
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                child.stdin.close()
            except (OSError, ValueError):
                pass

    def copy_output() -> None:
        try:
            for chunk in _read_chunks(child.stdout):
                # Timestamp and enqueue before the downstream write can block.
                try:
                    observer.feed_responses(chunk)
                except Exception:
                    # Parser or observer failures must not interrupt raw bytes.
                    pass
                _write_all(sys.stdout.buffer, chunk)
                sys.stdout.buffer.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            try:
                observer.finish_responses()
            except Exception:
                pass
            try:
                child.stdout.close()
            except (OSError, ValueError):
                pass

    input_thread = threading.Thread(target=copy_input, name="trace-mcp-stdin", daemon=True)
    output_thread = threading.Thread(target=copy_output, name="trace-mcp-stdout", daemon=True)
    input_thread.start()
    output_thread.start()
    return_code = child.wait()
    output_thread.join()
    observer.close()
    return return_code


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    try:
        separator = arguments.index("--")
    except ValueError:
        print("usage: trace_mcp.py -- <stdio-server-command> [args...]", file=sys.stderr)
        return 2
    command = arguments[separator + 1 :]
    if not command:
        print("usage: trace_mcp.py -- <stdio-server-command> [args...]", file=sys.stderr)
        return 2
    return _run_child(command, callback=_load_callback())


if __name__ == "__main__":
    raise SystemExit(main())
