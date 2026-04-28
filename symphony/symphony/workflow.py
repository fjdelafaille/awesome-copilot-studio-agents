"""Parse WORKFLOW.md files: YAML front matter + prompt body.

The spec requires dynamic reload: callers should call ``load`` on each poll
tick and compare the returned config to detect changes.
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Optional

import yaml

from .config import (
    AgentConfig,
    CodexConfig,
    HooksConfig,
    PollingConfig,
    ServerConfig,
    TrackerConfig,
    WorkflowConfig,
    WorkspaceConfig,
    _resolve_env,
)

_FRONT_MATTER_RE = re.compile(r"^---[ \t]*\r?\n(.*?)\r?\n---[ \t]*\r?\n(.*)", re.DOTALL)


def _parse_front_matter(content: str) -> tuple[dict, str]:
    """Split ``---`` YAML front matter from the markdown body."""
    m = _FRONT_MATTER_RE.match(content)
    if not m:
        return {}, content
    raw_yaml, body = m.group(1), m.group(2)
    data = yaml.safe_load(raw_yaml) or {}
    return data, body


def _default_workspace_root() -> str:
    return os.path.join(tempfile.gettempdir(), "symphony_workspaces")


def load(path: str) -> WorkflowConfig:
    """Read *path* and return a :class:`WorkflowConfig`.

    Raises :class:`ValueError` if the file cannot be parsed.
    """
    p = Path(path).resolve()
    try:
        content = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Cannot read workflow file {path!r}: {exc}") from exc

    data, body = _parse_front_matter(content)
    cfg = WorkflowConfig()
    cfg.prompt_template = body.strip()

    # ---- tracker --------------------------------------------------------
    t = data.get("tracker") or {}
    api_key_raw = t.get("api_key", "$LINEAR_API_KEY")
    cfg.tracker = TrackerConfig(
        kind=t.get("kind", ""),
        endpoint=t.get("endpoint", "https://api.linear.app/graphql"),
        api_key=_resolve_env(str(api_key_raw)) or "",
        project_slug=t.get("project_slug", ""),
        active_states=t.get("active_states", ["Todo", "In Progress"]),
        terminal_states=t.get(
            "terminal_states",
            ["Closed", "Cancelled", "Canceled", "Duplicate", "Done"],
        ),
    )

    # ---- polling --------------------------------------------------------
    p_cfg = data.get("polling") or {}
    cfg.polling = PollingConfig(
        interval_ms=int(p_cfg.get("interval_ms", 30_000)),
    )

    # ---- workspace ------------------------------------------------------
    w = data.get("workspace") or {}
    root: str = w.get("root", "") or ""
    root = _resolve_env(root) or root
    if root.startswith("~"):
        root = os.path.expanduser(root)
    elif root and not os.path.isabs(root):
        # Relative paths resolve relative to the directory holding WORKFLOW.md
        root = str(p.parent / root)
    cfg.workspace = WorkspaceConfig(root=root or _default_workspace_root())

    # ---- hooks ----------------------------------------------------------
    h = data.get("hooks") or {}
    hook_timeout = int(h.get("timeout_ms", 60_000))
    if hook_timeout <= 0:
        raise ValueError("hooks.timeout_ms must be a positive integer")
    cfg.hooks = HooksConfig(
        after_create=h.get("after_create"),
        before_run=h.get("before_run"),
        after_run=h.get("after_run"),
        before_remove=h.get("before_remove"),
        timeout_ms=hook_timeout,
    )

    # ---- agent ----------------------------------------------------------
    a = data.get("agent") or {}
    max_turns = int(a.get("max_turns", 20))
    if max_turns < 1:
        raise ValueError("agent.max_turns must be > 0")
    raw_by_state: dict = a.get("max_concurrent_agents_by_state") or {}
    by_state: dict[str, int] = {}
    for k, v in raw_by_state.items():
        try:
            iv = int(v)
            if iv > 0:
                by_state[str(k).lower()] = iv
        except (TypeError, ValueError):
            pass  # spec says invalid entries are ignored
    cfg.agent = AgentConfig(
        max_concurrent_agents=int(a.get("max_concurrent_agents", 10)),
        max_turns=max_turns,
        max_retry_backoff_ms=int(a.get("max_retry_backoff_ms", 300_000)),
        max_concurrent_agents_by_state=by_state,
    )

    # ---- codex ----------------------------------------------------------
    c = data.get("codex") or {}
    cfg.codex = CodexConfig(
        command=c.get("command", "codex app-server"),
        approval_policy=c.get("approval_policy"),
        thread_sandbox=c.get("thread_sandbox"),
        turn_sandbox_policy=c.get("turn_sandbox_policy"),
        turn_timeout_ms=int(c.get("turn_timeout_ms", 3_600_000)),
        read_timeout_ms=int(c.get("read_timeout_ms", 5_000)),
        stall_timeout_ms=int(c.get("stall_timeout_ms", 300_000)),
    )

    # ---- server (optional) ----------------------------------------------
    s = data.get("server") or {}
    port: Optional[int] = s.get("port")
    if port is not None:
        port = int(port)
    cfg.server = ServerConfig(port=port)

    return cfg


def mtime(path: str) -> Optional[float]:
    """Return the modification time of *path*, or ``None`` if inaccessible."""
    try:
        return os.path.getmtime(path)
    except OSError:
        return None
