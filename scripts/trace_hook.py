"""Best-effort Codex hook adapter for SemaRail's metadata-only Trace API."""

from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_MAX_HOOK_BYTES = 2_097_152
_CONFIG_PATH = Path.home() / ".semarail" / "trace-projects.json"
_STATE_PATH = Path.home() / ".semarail" / "codex-trace-routing-v1.sqlite3"
_ROUTE_TTL_SECONDS = 7 * 24 * 60 * 60
_MAX_PENDING_EVENTS = 4096
_MAX_FLUSH_EVENTS = 100
_MCP_RESULT_TTL_SECONDS = 5 * 60
_MAX_PENDING_MCP_RESULTS = 512
_MAX_MCP_REGISTRATIONS = 2048
_TASK_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request: Request, fp: Any, code: int, message: str, headers: Any, url: str) -> None:
        return None


def _identifier(value: Any) -> str | None:
    return value if isinstance(value, str) and _ID.fullmatch(value) else None


def _safe_task_path(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 256 or not value.startswith("/"):
        return None
    parts = value.split("/")[1:]
    if not parts or any(part in {".", ".."} or not _TASK_PATH_SEGMENT.fullmatch(part) for part in parts):
        return None
    return value


def _codex_metadata_path(config: dict[str, Any]) -> Path:
    value = config.get("codexMetadataFile")
    if isinstance(value, str) and value:
        path = Path(value).expanduser()
        if not path.is_absolute() and isinstance(config.get("root"), str):
            path = Path(config["root"]) / path
        return path
    return Path.home() / ".codex" / "state_5.sqlite"


def _read_codex_thread_metadata(config: dict[str, Any], agent_id: str) -> tuple[str, str] | None:
    """Read only the exact child path and thread-spawn parent fields from Codex state."""
    if _identifier(agent_id) is None:
        return None
    try:
        uri = _codex_metadata_path(config).resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=0.15)
        try:
            row = connection.execute(
                "SELECT id,agent_path,json_extract(source,'$.subagent.thread_spawn.parent_thread_id') "
                "FROM threads WHERE id=?",
                (agent_id,),
            ).fetchone()
        finally:
            connection.close()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return None
    if not row or row[0] != agent_id:
        return None
    task_path = _safe_task_path(row[1])
    parent_id = _identifier(row[2])
    if task_path is None or parent_id is None:
        return None
    return task_path, parent_id


def _project_config(cwd: Any) -> dict[str, Any] | None:
    if not isinstance(cwd, str):
        return None
    raw_path = os.environ.get("SEMARAIL_TRACE_CONFIG")
    path = Path(raw_path) if raw_path else _CONFIG_PATH
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
        current = Path(cwd).resolve()
        projects = config.get("projects")
        if not isinstance(projects, list):
            return None
        for candidate in projects:
            if not isinstance(candidate, dict) or candidate.get("enabled") is not True:
                continue
            root = candidate.get("root")
            if not isinstance(root, str) or not current.is_relative_to(Path(root).resolve()):
                continue
            return candidate
    except (OSError, ValueError, TypeError):
        return None
    return None


def _endpoint(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        return None
    if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        return None
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return None
    try:
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            return None
    except ValueError:
        return None
    return value.rstrip("/")


def _token(config: dict[str, Any], endpoint: str) -> str | None:
    env_name = config.get("tokenEnv")
    if isinstance(env_name, str) and _ENV_NAME.fullmatch(env_name):
        value = os.environ.get(env_name)
        return value if isinstance(value, str) and value.startswith(("sr_live_", "sr_session_")) else None
    auth_file = config.get("authFile")
    if not isinstance(auth_file, str):
        return None
    try:
        session = json.loads(Path(auth_file).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(session, dict) or session.get("endpoint", "").rstrip("/") != endpoint:
        return None
    value = session.get("accessToken")
    return value if isinstance(value, str) and value.startswith("sr_session_") else None


def _core_trace_id(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None
    meta = response.get("_meta", response.get("meta"))
    if isinstance(meta, dict):
        value = _identifier(meta.get("traceId"))
        if value:
            return value
    error = response.get("error")
    if isinstance(error, dict):
        value = _identifier(error.get("traceId"))
        if value:
            return value
    content = response.get("content")
    if isinstance(content, list):
        for block in content[:4]:
            if not isinstance(block, dict) or block.get("type") != "text":
                continue
            value = block.get("text")
            if not isinstance(value, str) or len(value) > 4096:
                continue
            try:
                decoded = json.loads(value)
            except ValueError:
                continue
            if isinstance(decoded, dict):
                trace = _identifier(decoded.get("traceId"))
                if trace:
                    return trace
    return None


def _tool_status(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None
    structured = response.get("structuredContent")
    if response.get("isError") is True or response.get("error") is not None:
        return "failure"
    if isinstance(structured, dict) and str(structured.get("status", "")).lower() in {"error", "failed", "failure"}:
        return "failure"
    exit_code = response.get("exit_code", response.get("exitCode"))
    if isinstance(exit_code, int) and not isinstance(exit_code, bool):
        return "success" if exit_code == 0 else "failure"
    if isinstance(structured, dict) or isinstance(response.get("content"), list):
        return "success"
    return None


def _response_json(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        decoded = json.loads(value)
    except ValueError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _spawned_agent_id(tool_name: Any, response: Any) -> str | None:
    """Read only the collaboration spawn result's allowlisted agent_id."""
    if tool_name != "collaborationspawn_agent":
        return None

    candidates: list[Any] = []
    if isinstance(response, dict):
        candidates.append(response)
        structured = response.get("structuredContent")
        if isinstance(structured, dict):
            candidates.append(structured)
        meta = response.get("_meta")
        if isinstance(meta, dict):
            candidates.append(meta)
        content = response.get("content")
        if isinstance(content, list):
            for block in content[:4]:
                if isinstance(block, dict) and block.get("type") == "text":
                    decoded = _response_json(block.get("text"))
                    if decoded is not None:
                        candidates.append(decoded)
    elif isinstance(response, str):
        decoded = _response_json(response)
        if decoded is not None:
            candidates.append(decoded)

    for candidate in candidates:
        agent_id = _identifier(candidate.get("agent_id"))
        if agent_id:
            return agent_id
    return None


def _spawned_task_path(tool_name: Any, response: Any) -> str | None:
    """Read only the collaboration result's canonical task_name path."""
    if tool_name != "collaborationspawn_agent":
        return None
    candidates: list[Any] = []
    if isinstance(response, dict):
        candidates.append(response)
        for key in ("structuredContent", "structured_content", "result", "data", "output", "response"):
            nested = response.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
            elif isinstance(nested, str):
                decoded = _response_json(nested)
                if decoded is not None:
                    candidates.append(decoded)
        content = response.get("content")
        if isinstance(content, list):
            for block in content[:4]:
                if isinstance(block, dict) and block.get("type") == "text":
                    decoded = _response_json(block.get("text"))
                    if decoded is not None:
                        candidates.append(decoded)
    elif isinstance(response, str):
        decoded = _response_json(response)
        if decoded is not None:
            candidates.append(decoded)
    for candidate in candidates:
        task_path = _safe_task_path(candidate.get("task_name"))
        if task_path:
            return task_path
    return None


def _project_scope(config: dict[str, Any]) -> str:
    root = config.get("root")
    endpoint = _endpoint(config.get("endpoint")) or ""
    if not isinstance(root, str):
        return ""
    try:
        normalized_root = os.path.normcase(str(Path(root).resolve())).casefold()
    except (OSError, ValueError):
        normalized_root = os.path.normcase(root).casefold()
    credential = _token(config, endpoint) if endpoint else None
    credential_scope = hashlib.sha256(credential.encode("utf-8")).hexdigest() if credential else "missing"
    return hashlib.sha256(f"{normalized_root}\n{endpoint}\n{credential_scope}".encode("utf-8")).hexdigest()


def _routing_connection() -> sqlite3.Connection:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(_STATE_PATH, timeout=1.5)
    connection.execute("PRAGMA busy_timeout=1500")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_agent_routes ("
        "project_scope TEXT NOT NULL, session_id TEXT NOT NULL, agent_id TEXT NOT NULL, root_turn_id TEXT, "
        "parent_agent_id TEXT, parent_turn_id TEXT, bound_turn_id TEXT, ambiguous INTEGER NOT NULL DEFAULT 0, "
        "turn_ambiguous INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL, "
        "PRIMARY KEY(project_scope,session_id,agent_id))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_pending_events ("
        "project_scope TEXT NOT NULL, session_id TEXT NOT NULL, agent_id TEXT NOT NULL, "
        "source_turn_id TEXT NOT NULL, event_id TEXT NOT NULL, event_type TEXT NOT NULL, "
        "event_json TEXT NOT NULL, observed_at REAL NOT NULL, "
        "PRIMARY KEY(project_scope,session_id,agent_id,source_turn_id,event_id))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_mcp_registrations ("
        "project_scope TEXT NOT NULL, session_id TEXT NOT NULL, source_turn_id TEXT NOT NULL, "
        "call_id TEXT NOT NULL, agent_id TEXT NOT NULL, tool_name TEXT NOT NULL, "
        "registered_at REAL NOT NULL, completed_at REAL, ambiguous INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY(project_scope,session_id,source_turn_id,call_id,agent_id))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_pending_mcp_results ("
        "project_scope TEXT NOT NULL, session_id TEXT NOT NULL, source_turn_id TEXT NOT NULL, "
        "call_id TEXT NOT NULL, agent_id TEXT NOT NULL, trace_id TEXT NOT NULL, "
        "is_error INTEGER NOT NULL, occurred_at TEXT NOT NULL, observed_at REAL NOT NULL, ambiguous INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY(project_scope,session_id,source_turn_id,call_id,agent_id))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_spawn_paths ("
        "project_scope TEXT NOT NULL, session_id TEXT NOT NULL, task_path TEXT NOT NULL, parent_thread_id TEXT NOT NULL, "
        "parent_agent_id TEXT, parent_turn_id TEXT NOT NULL, root_turn_id TEXT, spawn_call_id TEXT NOT NULL, "
        "ambiguous INTEGER NOT NULL DEFAULT 0, "
        "updated_at REAL NOT NULL, PRIMARY KEY(project_scope,session_id,task_path,parent_thread_id))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_agent_metadata ("
        "project_scope TEXT NOT NULL, session_id TEXT NOT NULL, agent_id TEXT NOT NULL, task_path TEXT NOT NULL, "
        "parent_thread_id TEXT NOT NULL, source_turn_id TEXT NOT NULL, ambiguous INTEGER NOT NULL DEFAULT 0, "
        "updated_at REAL NOT NULL, PRIMARY KEY(project_scope,session_id,agent_id))"
    )
    return connection


def _prune_routing_state(connection: sqlite3.Connection, now: float) -> None:
    cutoff = now - _ROUTE_TTL_SECONDS
    connection.execute("DELETE FROM trace_pending_events WHERE observed_at < ?", (cutoff,))
    connection.execute("DELETE FROM trace_agent_routes WHERE updated_at < ?", (cutoff,))
    connection.execute("DELETE FROM trace_mcp_registrations WHERE registered_at < ?", (now - _MCP_RESULT_TTL_SECONDS,))
    connection.execute("DELETE FROM trace_pending_mcp_results WHERE observed_at < ?", (now - _MCP_RESULT_TTL_SECONDS,))
    connection.execute("DELETE FROM trace_spawn_paths WHERE updated_at < ?", (cutoff,))
    connection.execute("DELETE FROM trace_agent_metadata WHERE updated_at < ?", (cutoff,))
    connection.execute(
        "DELETE FROM trace_pending_events WHERE rowid IN ("
        "SELECT rowid FROM trace_pending_events ORDER BY observed_at DESC LIMIT -1 OFFSET ?)",
        (_MAX_PENDING_EVENTS,),
    )
    connection.execute(
        "DELETE FROM trace_pending_mcp_results WHERE rowid IN ("
        "SELECT rowid FROM trace_pending_mcp_results ORDER BY observed_at DESC LIMIT -1 OFFSET ?)",
        (_MAX_PENDING_MCP_RESULTS,),
    )
    connection.execute(
        "DELETE FROM trace_mcp_registrations WHERE rowid IN ("
        "SELECT rowid FROM trace_mcp_registrations ORDER BY registered_at DESC LIMIT -1 OFFSET ?)",
        (_MAX_MCP_REGISTRATIONS,),
    )


def _route_for_agent(
    connection: sqlite3.Connection, project_scope: str, session_id: str, agent_id: str,
    ) -> tuple[str | None, str | None, str | None, str | None, bool, bool] | None:
    row = connection.execute(
        "SELECT root_turn_id,parent_agent_id,parent_turn_id,bound_turn_id,ambiguous,turn_ambiguous "
        "FROM trace_agent_routes WHERE project_scope=? AND session_id=? AND agent_id=?",
        (project_scope, session_id, agent_id),
    ).fetchone()
    if row is None:
        return None
    return row[0], row[1], row[2], row[3], bool(row[4]), bool(row[5])


def _resolve_descendant_routes(
    connection: sqlite3.Connection, project_scope: str, session_id: str, now: float,
) -> None:
    # A nested spawn can finish before its caller's route arrives. Resolve only
    # through the recorded parent agent ID, never by event timing.
    while True:
        rows = connection.execute(
            "SELECT child.agent_id,parent.root_turn_id FROM trace_agent_routes AS child "
            "JOIN trace_agent_routes AS parent ON parent.project_scope=child.project_scope "
            "AND parent.session_id=child.session_id "
            "AND parent.agent_id=child.parent_agent_id AND parent.bound_turn_id=child.parent_turn_id "
            "WHERE child.project_scope=? AND child.session_id=? AND child.root_turn_id IS NULL "
            "AND child.ambiguous=0 AND parent.root_turn_id IS NOT NULL AND parent.ambiguous=0",
            (project_scope, session_id),
        ).fetchall()
        if not rows:
            return
        connection.executemany(
            "UPDATE trace_agent_routes SET root_turn_id=?,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND agent_id=? "
            "AND root_turn_id IS NULL AND ambiguous=0",
            ((root_turn, now, project_scope, session_id, agent_id) for agent_id, root_turn in rows),
        )


def _record_agent_route(
    connection: sqlite3.Connection,
    project_scope: str,
    session_id: str,
    agent_id: str,
    root_turn_id: str | None,
    parent_agent_id: str | None,
    parent_turn_id: str | None,
    now: float,
) -> None:
    existing = _route_for_agent(connection, project_scope, session_id, agent_id)
    if existing is None:
        connection.execute(
            "INSERT INTO trace_agent_routes(project_scope,session_id,agent_id,root_turn_id,parent_agent_id,parent_turn_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (project_scope, session_id, agent_id, root_turn_id, parent_agent_id, parent_turn_id, now),
        )
    else:
        old_root, old_parent, old_parent_turn, _bound_turn, ambiguous, turn_ambiguous = existing
        if ambiguous:
            return
        if old_root is not None and root_turn_id is not None and old_root != root_turn_id:
            connection.execute(
                "UPDATE trace_agent_routes SET root_turn_id=NULL,parent_agent_id=NULL,parent_turn_id=NULL,ambiguous=1,updated_at=? "
                "WHERE project_scope=? AND session_id=? AND agent_id=?",
                (now, project_scope, session_id, agent_id),
            )
            return
        resolved_root = old_root or root_turn_id
        # Keep a parent edge only when the recorded spawn relationship agrees.
        resolved_parent = old_parent if old_parent == parent_agent_id else (old_parent or parent_agent_id)
        if old_parent and parent_agent_id and old_parent != parent_agent_id:
            resolved_parent = None
        resolved_parent_turn = old_parent_turn or parent_turn_id
        if old_parent_turn and parent_turn_id and old_parent_turn != parent_turn_id:
            resolved_parent_turn = None
            turn_ambiguous = True
        connection.execute(
            "UPDATE trace_agent_routes SET root_turn_id=?,parent_agent_id=?,parent_turn_id=?,turn_ambiguous=?,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND agent_id=?",
            (resolved_root, resolved_parent, resolved_parent_turn, int(turn_ambiguous), now,
             project_scope, session_id, agent_id),
        )
    _resolve_descendant_routes(connection, project_scope, session_id, now)


def _bind_agent_turn(
    connection: sqlite3.Connection, project_scope: str, session_id: str, agent_id: str,
    turn_id: str, now: float,
) -> None:
    route = _route_for_agent(connection, project_scope, session_id, agent_id)
    if route is None:
        return
    _root, _parent, _parent_turn, bound_turn, ambiguous, turn_ambiguous = route
    if ambiguous or turn_ambiguous or (bound_turn is not None and bound_turn != turn_id):
        return
    if bound_turn is None:
        connection.execute(
            "UPDATE trace_agent_routes SET bound_turn_id=?,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND agent_id=? AND bound_turn_id IS NULL "
            "AND ambiguous=0 AND turn_ambiguous=0",
            (turn_id, now, project_scope, session_id, agent_id),
        )
        _resolve_descendant_routes(connection, project_scope, session_id, now)


def _set_event_parent(event: dict[str, Any], parent_agent_id: str | None) -> dict[str, Any]:
    if event.get("type") in {"subagent_started", "subagent_completed"}:
        if parent_agent_id is None:
            event.pop("parentAgentId", None)
        else:
            event["parentAgentId"] = parent_agent_id
    return event


def _store_pending_events(
    connection: sqlite3.Connection, project_scope: str, session_id: str, agent_id: str,
    turn_id: str, events: list[dict[str, Any]], now: float,
) -> None:
    connection.executemany(
        "INSERT OR IGNORE INTO trace_pending_events(project_scope,session_id,agent_id,source_turn_id,event_id,event_type,event_json,observed_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        ((project_scope, session_id, agent_id, turn_id, event["eventId"], event["type"],
          json.dumps(event, separators=(",", ":")), now) for event in events),
    )


def _bind_unambiguous_pending_start(
    connection: sqlite3.Connection, project_scope: str, session_id: str, agent_id: str, now: float,
) -> None:
    rows = connection.execute(
        "SELECT DISTINCT source_turn_id FROM trace_pending_events "
        "WHERE project_scope=? AND session_id=? AND agent_id=? AND event_type='subagent_started'",
        (project_scope, session_id, agent_id),
    ).fetchall()
    if len(rows) == 1:
        _bind_agent_turn(connection, project_scope, session_id, agent_id, rows[0][0], now)
    elif len(rows) > 1:
        connection.execute(
            "UPDATE trace_agent_routes SET turn_ambiguous=1,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND agent_id=? AND bound_turn_id IS NULL",
            (now, project_scope, session_id, agent_id),
        )


def _flush_routed_events(
    connection: sqlite3.Connection, project_scope: str, session_id: str,
) -> list[tuple[str, list[tuple[str, str, dict[str, Any]]]]]:
    rows = connection.execute(
        "SELECT pending.agent_id,pending.source_turn_id,pending.event_id,pending.event_json,route.root_turn_id,route.parent_agent_id "
        "FROM trace_pending_events AS pending JOIN trace_agent_routes AS route "
        "ON route.project_scope=pending.project_scope AND route.session_id=pending.session_id "
        "AND route.agent_id=pending.agent_id AND route.bound_turn_id=pending.source_turn_id "
        "WHERE pending.project_scope=? AND pending.session_id=? AND route.root_turn_id IS NOT NULL "
        "AND route.ambiguous=0 AND route.turn_ambiguous=0 "
        "ORDER BY pending.observed_at,pending.event_id LIMIT ?",
        (project_scope, session_id, _MAX_FLUSH_EVENTS),
    ).fetchall()
    batches: dict[str, list[tuple[str, str, dict[str, Any]]]] = {}
    for agent_id, source_turn_id, event_id, event_json, root_turn_id, parent_agent_id in rows:
        try:
            event = json.loads(event_json)
        except ValueError:
            connection.execute(
                "DELETE FROM trace_pending_events WHERE project_scope=? AND session_id=? AND agent_id=? "
                "AND source_turn_id=? AND event_id=?",
                (project_scope, session_id, agent_id, source_turn_id, event_id),
            )
            continue
        if isinstance(event, dict):
            _set_event_parent(event, parent_agent_id)
            batches.setdefault(root_turn_id, []).append((agent_id, source_turn_id, event))
    return [(root_turn, events) for root_turn, events in batches.items()]


def _mark_path_routes_ambiguous(
    connection: sqlite3.Connection, project_scope: str, session_id: str,
    task_path: str, parent_thread_id: str, now: float,
) -> None:
    connection.execute(
        "UPDATE trace_agent_routes SET root_turn_id=NULL,parent_agent_id=NULL,parent_turn_id=NULL,ambiguous=1,updated_at=? "
        "WHERE project_scope=? AND session_id=? AND agent_id IN ("
        "SELECT agent_id FROM trace_agent_metadata WHERE project_scope=? AND session_id=? "
        "AND task_path=? AND parent_thread_id=?)",
        (now, project_scope, session_id, project_scope, session_id, task_path, parent_thread_id),
    )


def _resolve_path_spawn_roots(
    connection: sqlite3.Connection, project_scope: str, session_id: str, now: float,
) -> None:
    while True:
        rows = connection.execute(
            "SELECT spawn.task_path,spawn.parent_thread_id,parent.root_turn_id "
            "FROM trace_spawn_paths AS spawn JOIN trace_agent_routes AS parent "
            "ON parent.project_scope=spawn.project_scope AND parent.session_id=spawn.session_id "
            "AND parent.agent_id=spawn.parent_agent_id AND parent.bound_turn_id=spawn.parent_turn_id "
            "WHERE spawn.project_scope=? AND spawn.session_id=? AND spawn.root_turn_id IS NULL "
            "AND spawn.ambiguous=0 AND parent.root_turn_id IS NOT NULL AND parent.ambiguous=0 AND parent.turn_ambiguous=0",
            (project_scope, session_id),
        ).fetchall()
        if not rows:
            return
        connection.executemany(
            "UPDATE trace_spawn_paths SET root_turn_id=?,updated_at=? WHERE project_scope=? AND session_id=? "
            "AND task_path=? AND parent_thread_id=? AND root_turn_id IS NULL AND ambiguous=0",
            ((root_turn, now, project_scope, session_id, task_path, parent_thread)
             for task_path, parent_thread, root_turn in rows),
        )


def _resolve_path_agent_routes(
    connection: sqlite3.Connection, project_scope: str, session_id: str, now: float,
) -> bool:
    rows = connection.execute(
        "SELECT agent.agent_id,agent.task_path,agent.parent_thread_id,agent.source_turn_id, "
        "spawn.parent_agent_id,spawn.parent_turn_id,spawn.root_turn_id "
        "FROM trace_agent_metadata AS agent JOIN trace_spawn_paths AS spawn "
        "ON spawn.project_scope=agent.project_scope AND spawn.session_id=agent.session_id "
        "AND spawn.task_path=agent.task_path AND spawn.parent_thread_id=agent.parent_thread_id "
        "WHERE agent.project_scope=? AND agent.session_id=? AND agent.ambiguous=0 "
        "AND agent.source_turn_id<>'' AND spawn.ambiguous=0",
        (project_scope, session_id),
    ).fetchall()
    by_path: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
    changed = False
    for row in rows:
        by_path.setdefault((row[1], row[2]), []).append(row)
    for (task_path, parent_thread_id), matches in by_path.items():
        if len(matches) != 1:
            connection.execute(
                "UPDATE trace_spawn_paths SET root_turn_id=NULL,ambiguous=1,updated_at=? "
                "WHERE project_scope=? AND session_id=? AND task_path=? AND parent_thread_id=?",
                (now, project_scope, session_id, task_path, parent_thread_id),
            )
            _mark_path_routes_ambiguous(connection, project_scope, session_id, task_path, parent_thread_id, now)
            continue
        agent_id, _path, _parent_thread, child_turn, parent_agent_id, parent_turn, root_turn = matches[0]
        existing = _route_for_agent(connection, project_scope, session_id, agent_id)
        if existing and (existing[4] or existing[5]):
            continue
        if (existing and existing[0] == root_turn and existing[1] == parent_agent_id
                and existing[2] == parent_turn and existing[3] == child_turn):
            continue
        _record_agent_route(
            connection, project_scope, session_id, agent_id, root_turn,
            parent_agent_id, parent_turn, now,
        )
        _bind_agent_turn(connection, project_scope, session_id, agent_id, child_turn, now)
        updated = _route_for_agent(connection, project_scope, session_id, agent_id)
        if updated and not updated[4] and not updated[5] and updated[3] == child_turn:
            changed = True
    return changed


def _resolve_path_routes(
    connection: sqlite3.Connection, project_scope: str, session_id: str, now: float,
) -> None:
    # Nested exact path links may become resolvable as each parent route arrives.
    for _ in range(32):
        _resolve_path_spawn_roots(connection, project_scope, session_id, now)
        if not _resolve_path_agent_routes(connection, project_scope, session_id, now):
            return


def _record_agent_metadata(
    connection: sqlite3.Connection, project_scope: str, session_id: str, agent_id: str,
    task_path: str, parent_thread_id: str, source_turn_id: str, now: float,
) -> None:
    row = connection.execute(
        "SELECT task_path,parent_thread_id,source_turn_id,ambiguous FROM trace_agent_metadata "
        "WHERE project_scope=? AND session_id=? AND agent_id=?",
        (project_scope, session_id, agent_id),
    ).fetchone()
    if row is None:
        connection.execute(
            "INSERT INTO trace_agent_metadata(project_scope,session_id,agent_id,task_path,parent_thread_id,source_turn_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (project_scope, session_id, agent_id, task_path, parent_thread_id, source_turn_id, now),
        )
    elif row[3]:
        return
    elif row[0] != task_path or row[1] != parent_thread_id or (row[2] and source_turn_id and row[2] != source_turn_id):
        connection.execute(
            "UPDATE trace_agent_metadata SET ambiguous=1,updated_at=? WHERE project_scope=? AND session_id=? AND agent_id=?",
            (now, project_scope, session_id, agent_id),
        )
        connection.execute(
            "UPDATE trace_agent_routes SET root_turn_id=NULL,ambiguous=1,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND agent_id=?",
            (now, project_scope, session_id, agent_id),
        )
        return
    elif not row[2] and source_turn_id:
        connection.execute(
            "UPDATE trace_agent_metadata SET source_turn_id=?,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND agent_id=? AND source_turn_id=''",
            (source_turn_id, now, project_scope, session_id, agent_id),
        )


def _initial_child_turn(
    connection: sqlite3.Connection, project_scope: str, session_id: str,
    agent_id: str, observed_start_turn: str | None = None,
) -> str | None:
    if observed_start_turn is not None:
        return observed_start_turn
    route = _route_for_agent(connection, project_scope, session_id, agent_id)
    if route and route[3] and not route[4] and not route[5]:
        return route[3]
    rows = connection.execute(
        "SELECT DISTINCT source_turn_id FROM trace_pending_events WHERE project_scope=? "
        "AND session_id=? AND agent_id=? AND event_type='subagent_started'",
        (project_scope, session_id, agent_id),
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else None


def _record_spawn_path(
    connection: sqlite3.Connection, project_scope: str, session_id: str, task_path: str,
    parent_thread_id: str, parent_agent_id: str | None, parent_turn_id: str,
    root_turn_id: str | None, spawn_call_id: str, now: float,
) -> None:
    key = (project_scope, session_id, task_path, parent_thread_id)
    row = connection.execute(
        "SELECT parent_agent_id,parent_turn_id,root_turn_id,spawn_call_id,ambiguous FROM trace_spawn_paths "
        "WHERE project_scope=? AND session_id=? AND task_path=? AND parent_thread_id=?",
        key,
    ).fetchone()
    if row is None:
        connection.execute(
            "INSERT INTO trace_spawn_paths(project_scope,session_id,task_path,parent_thread_id,parent_agent_id,parent_turn_id,root_turn_id,spawn_call_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (*key, parent_agent_id, parent_turn_id, root_turn_id, spawn_call_id, now),
        )
    elif row[4]:
        return
    elif row[0] == parent_agent_id and row[1] == parent_turn_id and row[3] == spawn_call_id:
        if row[2] is None and root_turn_id is not None:
            connection.execute(
                "UPDATE trace_spawn_paths SET root_turn_id=?,updated_at=? WHERE project_scope=? "
                "AND session_id=? AND task_path=? AND parent_thread_id=? AND ambiguous=0",
                (root_turn_id, now, *key),
            )
        elif row[2] is not None and root_turn_id is not None and row[2] != root_turn_id:
            connection.execute(
                "UPDATE trace_spawn_paths SET root_turn_id=NULL,ambiguous=1,updated_at=? "
                "WHERE project_scope=? AND session_id=? AND task_path=? AND parent_thread_id=?",
                (now, *key),
            )
            _mark_path_routes_ambiguous(connection, project_scope, session_id, task_path, parent_thread_id, now)
            return
    else:
        connection.execute(
            "UPDATE trace_spawn_paths SET root_turn_id=NULL,ambiguous=1,updated_at=? "
            "WHERE project_scope=? AND session_id=? AND task_path=? AND parent_thread_id=?",
            (now, *key),
        )
        _mark_path_routes_ambiguous(connection, project_scope, session_id, task_path, parent_thread_id, now)
        return


def _dispatch_hook(session_id: str, turn_id: str, hook: dict[str, Any], config: dict[str, Any]) -> None:
    events = _events(hook)
    name = hook.get("hook_event_name")
    agent_id = _identifier(hook.get("agent_id"))
    project_scope = _project_scope(config)
    tool_name = hook.get("tool_name")
    spawned_agent_id = _spawned_agent_id(tool_name, hook.get("tool_response")) if name == "PostToolUse" else None
    spawned_task_path = (
        _spawned_task_path(tool_name, hook.get("tool_response"))
        if name == "PostToolUse" and not spawned_agent_id else None
    )
    now = datetime.now(UTC).timestamp()
    pending_batches: list[tuple[str, list[tuple[str, str, dict[str, Any]]]]] = []
    direct_payload: dict[str, Any] | None = None
    buffered_mcp_completion: dict[str, Any] | None = None

    if events and agent_id is None:
        # App-created initial turns may omit UserPromptSubmit. Their main hook
        # events still carry the exact root turn ID and create its Trace.
        direct_payload = {
            "schemaVersion": 1, "source": "codex", "sourceSessionId": session_id,
            "sourceTurnId": turn_id, "events": events[:_MAX_FLUSH_EVENTS],
        }

    connection: sqlite3.Connection | None = None
    try:
        connection = _routing_connection()
        connection.execute("BEGIN IMMEDIATE")
        _prune_routing_state(connection, now)
        if name == "PreToolUse":
            call_id = _identifier(hook.get("tool_use_id"))
            registered_tool_name = _identifier(hook.get("tool_name"))
            if call_id and registered_tool_name:
                buffered_mcp_completion = _register_mcp_call(
                    connection, project_scope, session_id, turn_id, call_id, agent_id,
                    registered_tool_name, now,
                )
        if spawned_agent_id:
            if agent_id is None:
                # An agent-less collaboration call is an exact root-turn anchor.
                child_root = turn_id
                parent_agent_id = None
                parent_turn_id = turn_id
            else:
                caller_route = _route_for_agent(connection, project_scope, session_id, agent_id)
                if (caller_route and caller_route[0] and caller_route[3] == turn_id
                        and not caller_route[4] and not caller_route[5]):
                    child_root = caller_route[0]
                else:
                    child_root = None
                parent_agent_id = agent_id
                parent_turn_id = turn_id
            _record_agent_route(
                connection, project_scope, session_id, spawned_agent_id, child_root,
                parent_agent_id, parent_turn_id, now,
            )
            _bind_unambiguous_pending_start(connection, project_scope, session_id, spawned_agent_id, now)

        if spawned_task_path:
            spawn_call_id = _identifier(hook.get("tool_use_id"))
            if spawn_call_id:
                caller_thread_id = agent_id or session_id
                if agent_id is None:
                    child_root = turn_id
                else:
                    caller_route = _route_for_agent(connection, project_scope, session_id, agent_id)
                    child_root = (
                        caller_route[0]
                        if caller_route and caller_route[0] and caller_route[3] == turn_id
                        and not caller_route[4] and not caller_route[5]
                        else None
                    )
                _record_spawn_path(
                    connection, project_scope, session_id, spawned_task_path, caller_thread_id,
                    agent_id, turn_id, child_root, spawn_call_id, now,
                )

        if name in {"SubagentStart", "SubagentStop"} and agent_id:
            child_metadata = _read_codex_thread_metadata(config, agent_id)
            if child_metadata is not None:
                initial_child_turn = _initial_child_turn(
                    connection, project_scope, session_id, agent_id,
                    turn_id if name == "SubagentStart" else None,
                )
                _record_agent_metadata(
                    connection, project_scope, session_id, agent_id,
                    child_metadata[0], child_metadata[1], initial_child_turn or "", now,
                )

        _resolve_path_routes(connection, project_scope, session_id, now)

        if events and agent_id is not None:
            route = _route_for_agent(connection, project_scope, session_id, agent_id)
            if route:
                for event in events:
                    _set_event_parent(event, route[1])
            # Even a known Agent ID is insufficient for a reused Agent's new
            # turn. The initial child turn is bound only by its SubagentStart.
            routed_events = [*events]
            if buffered_mcp_completion is not None:
                routed_events.append(buffered_mcp_completion)
            _store_pending_events(connection, project_scope, session_id, agent_id, turn_id, routed_events, now)
            _bind_unambiguous_pending_start(connection, project_scope, session_id, agent_id, now)
        elif buffered_mcp_completion is not None:
            if direct_payload is None:
                direct_payload = {
                    "schemaVersion": 1, "source": "codex", "sourceSessionId": session_id,
                    "sourceTurnId": turn_id, "events": [],
                }
            direct_payload["events"].append(buffered_mcp_completion)
        pending_batches = _flush_routed_events(connection, project_scope, session_id)
        connection.commit()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
        # Collection stays best effort if local routing state is unavailable.
        if direct_payload is not None:
            _send(config, direct_payload)
        return
    finally:
        if connection is not None:
            connection.close()

    if direct_payload is not None:
        _send(config, direct_payload)
    for root_turn_id, routed_batch in pending_batches:
        batch = [event for _agent_id, _source_turn_id, event in routed_batch]
        if _send(config, {
            "schemaVersion": 1, "source": "codex", "sourceSessionId": session_id,
            "sourceTurnId": root_turn_id, "events": batch,
        }):
            _ack_pending_events(project_scope, session_id, routed_batch)


def _ack_pending_events(project_scope: str, session_id: str, events: list[tuple[str, str, dict[str, Any]]]) -> None:
    connection: sqlite3.Connection | None = None
    try:
        connection = _routing_connection()
        connection.executemany(
            "DELETE FROM trace_pending_events WHERE project_scope=? AND session_id=? AND agent_id=? "
            "AND source_turn_id=? AND event_id=?",
            ((project_scope, session_id, agent_id, source_turn_id, event["eventId"])
             for agent_id, source_turn_id, event in events),
        )
        connection.commit()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        pass
    finally:
        if connection is not None:
            connection.close()


def _utc_timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mcp_completion_event(
    call_id: str, tool_name: str, agent_id: str | None, trace_id: str, is_error: bool, occurred_at: str,
) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "eventId": f"mcp-result:{call_id}",
        "occurredAt": occurred_at,
        "type": "tool_completed",
        "toolUseId": call_id,
        "toolName": tool_name,
        "status": "failure" if is_error else "success",
        "coreTraceId": trace_id,
    }
    if agent_id is not None:
        fields["parentAgentId"] = agent_id
    return fields


def _register_mcp_call(
    connection: sqlite3.Connection, project_scope: str, session_id: str, turn_id: str,
    call_id: str, agent_id: str | None, tool_name: str, now: float,
) -> dict[str, Any] | None:
    if not tool_name.startswith("mcp__"):
        return None
    stored_agent_id = agent_id or ""
    connection.execute(
        "INSERT OR IGNORE INTO trace_mcp_registrations(project_scope,session_id,source_turn_id,call_id,agent_id,tool_name,registered_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (project_scope, session_id, turn_id, call_id, stored_agent_id, tool_name, now),
    )
    registration = connection.execute(
        "SELECT tool_name,completed_at,ambiguous FROM trace_mcp_registrations "
        "WHERE project_scope=? AND session_id=? AND source_turn_id=? AND call_id=? AND agent_id=?",
        (project_scope, session_id, turn_id, call_id, stored_agent_id),
    ).fetchone()
    if registration is None:
        return None
    registered_name, completed_at, ambiguous = registration
    if registered_name != tool_name:
        connection.execute(
            "UPDATE trace_mcp_registrations SET ambiguous=1 WHERE project_scope=? AND session_id=? "
            "AND source_turn_id=? AND call_id=? AND agent_id=?",
            (project_scope, session_id, turn_id, call_id, stored_agent_id),
        )
        return None
    if completed_at is not None or ambiguous:
        return None
    pending = connection.execute(
        "SELECT trace_id,is_error,occurred_at,ambiguous FROM trace_pending_mcp_results "
        "WHERE project_scope=? AND session_id=? AND source_turn_id=? AND call_id=? AND agent_id=?",
        (project_scope, session_id, turn_id, call_id, stored_agent_id),
    ).fetchone()
    if pending is None:
        return None
    trace_id, is_error, occurred_at, pending_ambiguous = pending
    if pending_ambiguous:
        connection.execute(
            "UPDATE trace_mcp_registrations SET ambiguous=1 WHERE project_scope=? AND session_id=? "
            "AND source_turn_id=? AND call_id=? AND agent_id=?",
            (project_scope, session_id, turn_id, call_id, stored_agent_id),
        )
        return None
    connection.execute(
        "UPDATE trace_mcp_registrations SET completed_at=? WHERE project_scope=? AND session_id=? "
        "AND source_turn_id=? AND call_id=? AND agent_id=? AND completed_at IS NULL AND ambiguous=0",
        (now, project_scope, session_id, turn_id, call_id, stored_agent_id),
    )
    connection.execute(
        "DELETE FROM trace_pending_mcp_results WHERE project_scope=? AND session_id=? "
        "AND source_turn_id=? AND call_id=? AND agent_id=?",
        (project_scope, session_id, turn_id, call_id, stored_agent_id),
    )
    return _mcp_completion_event(call_id, tool_name, agent_id, trace_id, bool(is_error), occurred_at)


def _publish_registered_mcp_event(
    config: dict[str, Any], project_scope: str, session_id: str, turn_id: str,
    agent_id: str | None, event: dict[str, Any],
) -> None:
    now = datetime.now(UTC).timestamp()
    connection: sqlite3.Connection | None = None
    direct_payload: dict[str, Any] | None = None
    pending_batches: list[tuple[str, list[tuple[str, str, dict[str, Any]]]]] = []
    try:
        connection = _routing_connection()
        connection.execute("BEGIN IMMEDIATE")
        _prune_routing_state(connection, now)
        if agent_id is None:
            direct_payload = {
                "schemaVersion": 1, "source": "codex", "sourceSessionId": session_id,
                "sourceTurnId": turn_id, "events": [event],
            }
        else:
            _store_pending_events(connection, project_scope, session_id, agent_id, turn_id, [event], now)
            pending_batches = _flush_routed_events(connection, project_scope, session_id)
        connection.commit()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
        return
    finally:
        if connection is not None:
            connection.close()
    if direct_payload is not None:
        _send(config, direct_payload)
    for root_turn_id, routed_batch in pending_batches:
        batch = [pending_event for _agent, _source_turn, pending_event in routed_batch]
        if _send(config, {
            "schemaVersion": 1, "source": "codex", "sourceSessionId": session_id,
            "sourceTurnId": root_turn_id, "events": batch,
        }):
            _ack_pending_events(project_scope, session_id, routed_batch)


def _observe_mcp_result(
    metadata: dict[str, str], result: dict[str, Any], cwd: str, occurred_at: str,
) -> None:
    """Route an MCP completion only after a matching, opted-in PreToolUse."""
    config = _project_config(cwd)
    if not isinstance(config, dict) or config.get("enabled") is not True:
        return
    endpoint = _endpoint(config.get("endpoint"))
    if endpoint is None or _token(config, endpoint) is None:
        return
    if not isinstance(metadata, dict) or not isinstance(result, dict):
        return
    call_id = _identifier(metadata.get("callId"))
    session_id = _identifier(metadata.get("sessionId"))
    turn_id = _identifier(metadata.get("turnId"))
    raw_agent_id = metadata.get("agentId")
    agent_id = _identifier(raw_agent_id) if raw_agent_id is not None else None
    trace_id = _identifier(result.get("traceId"))
    is_error = result.get("isError")
    timestamp = _utc_timestamp(occurred_at)
    if (call_id is None or session_id is None or turn_id is None or trace_id is None
            or (raw_agent_id is not None and agent_id is None)
            or not isinstance(is_error, bool) or timestamp is None):
        return

    project_scope = _project_scope(config)
    stored_agent_id = agent_id or ""
    now = datetime.now(UTC).timestamp()
    connection: sqlite3.Connection | None = None
    completion: dict[str, Any] | None = None
    try:
        connection = _routing_connection()
        connection.execute("BEGIN IMMEDIATE")
        _prune_routing_state(connection, now)
        registrations = connection.execute(
            "SELECT agent_id,tool_name,completed_at,ambiguous FROM trace_mcp_registrations "
            "WHERE project_scope=? AND session_id=? AND source_turn_id=? AND call_id=?",
            (project_scope, session_id, turn_id, call_id),
        ).fetchall()
        exact = [row for row in registrations if row[0] == stored_agent_id]
        if registrations and (len(registrations) != 1 or len(exact) != 1):
            # A call ID already registered under a different owner is a mismatch,
            # not an out-of-order completion eligible for buffering.
            connection.commit()
            return
        if exact:
            _registered_agent_id, tool_name, completed_at, ambiguous = exact[0]
            if completed_at is not None or ambiguous or not isinstance(tool_name, str) or not tool_name.startswith("mcp__"):
                connection.commit()
                return
            connection.execute(
                "UPDATE trace_mcp_registrations SET completed_at=? WHERE project_scope=? AND session_id=? "
                "AND source_turn_id=? AND call_id=? AND agent_id=? AND completed_at IS NULL AND ambiguous=0",
                (now, project_scope, session_id, turn_id, call_id, stored_agent_id),
            )
            completion = _mcp_completion_event(call_id, tool_name, agent_id, trace_id, is_error, timestamp)
        else:
            connection.execute(
                "INSERT OR IGNORE INTO trace_pending_mcp_results(project_scope,session_id,source_turn_id,call_id,agent_id,trace_id,is_error,occurred_at,observed_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (project_scope, session_id, turn_id, call_id, stored_agent_id, trace_id, int(is_error), timestamp, now),
            )
            pending = connection.execute(
                "SELECT trace_id,is_error,occurred_at FROM trace_pending_mcp_results WHERE project_scope=? "
                "AND session_id=? AND source_turn_id=? AND call_id=? AND agent_id=?",
                (project_scope, session_id, turn_id, call_id, stored_agent_id),
            ).fetchone()
            if pending is not None and pending != (trace_id, int(is_error), timestamp):
                connection.execute(
                    "UPDATE trace_pending_mcp_results SET ambiguous=1 WHERE project_scope=? AND session_id=? "
                    "AND source_turn_id=? AND call_id=? AND agent_id=?",
                    (project_scope, session_id, turn_id, call_id, stored_agent_id),
                )
        connection.commit()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
        return
    finally:
        if connection is not None:
            connection.close()
    if completion is not None:
        _publish_registered_mcp_event(config, project_scope, session_id, turn_id, agent_id, completion)


def _events(hook: dict[str, Any]) -> list[dict[str, Any]]:
    name = hook.get("hook_event_name")
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    model = _identifier(hook.get("model"))
    tool_id = _identifier(hook.get("tool_use_id"))
    tool_name = _identifier(hook.get("tool_name"))
    agent_id = _identifier(hook.get("agent_id"))
    parent_agent_id = _identifier(hook.get("parent_agent_id"))

    def event(event_id: str, kind: str, **fields: Any) -> dict[str, Any]:
        return {"eventId": event_id, "occurredAt": now, "type": kind, **{key: value for key, value in fields.items() if value is not None}}

    if name == "UserPromptSubmit":
        return [event("turn-start", "turn_started", model=model, status="running")]
    if name == "PreToolUse" and tool_id and tool_name:
        return [event(f"tool-start:{tool_id}", "tool_started", toolUseId=tool_id, toolName=tool_name, parentAgentId=agent_id, status="running")]
    if name == "PostToolUse" and tool_id and tool_name:
        response = hook.get("tool_response")
        core_trace = _core_trace_id(response) if "semarail" in tool_name.lower() else None
        return [event(f"tool-end:{tool_id}", "tool_completed", toolUseId=tool_id, toolName=tool_name, parentAgentId=agent_id, status=_tool_status(response), coreTraceId=core_trace)]
    if name == "SubagentStart" and agent_id:
        return [event(f"subagent-start:{agent_id}", "subagent_started", agentId=agent_id, parentAgentId=parent_agent_id, model=model, status="running")]
    if name == "SubagentStop" and agent_id:
        return [event(f"subagent-end:{agent_id}", "subagent_completed", agentId=agent_id, parentAgentId=parent_agent_id)]
    if name == "Stop":
        return [event("output", "output"), event("turn-end", "turn_completed", status="success")]
    if name == "Interrupt":
        return [event("turn-interrupt", "turn_interrupted", status="cancelled")]
    return []


def _send(config: dict[str, Any], payload: dict[str, Any]) -> bool:
    endpoint = _endpoint(config.get("endpoint"))
    if endpoint is None:
        return False
    token = _token(config, endpoint)
    if token is None:
        return False
    request = Request(
        endpoint + "/api/v1/traces/events",
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with build_opener(_NoRedirect()).open(request, timeout=1.5) as response:
            response.read(1024)
        return True
    except (OSError, ValueError):
        # Observability must never change Codex's tool or turn outcome.
        return False


def main() -> None:
    requires_json_output = False
    try:
        raw = sys.stdin.buffer.read(_MAX_HOOK_BYTES + 1)
        if len(raw) > _MAX_HOOK_BYTES:
            return
        hook = json.loads(raw)
        if not isinstance(hook, dict):
            return
        requires_json_output = hook.get("hook_event_name") in {"Stop", "SubagentStop"}
        session_id = _identifier(hook.get("session_id"))
        turn_id = _identifier(hook.get("turn_id"))
        if session_id is None or turn_id is None:
            return
        config = _project_config(hook.get("cwd"))
        if config is None:
            return
        _dispatch_hook(session_id, turn_id, hook, config)
    except Exception:
        # No hook output: prompt, tool arguments, result bodies and secrets remain local.
        return
    finally:
        # Codex requires JSON output for these two lifecycle hooks. An empty
        # object has no control effect and keeps collection best effort.
        if requires_json_output:
            sys.stdout.write("{}\n")


if __name__ == "__main__":
    main()
