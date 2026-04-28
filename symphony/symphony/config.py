"""Typed configuration dataclasses with defaults matching the Symphony spec."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


def _resolve_env(value: Optional[str]) -> Optional[str]:
    """Expand ``$VAR_NAME`` references to environment variable values."""
    if value and value.startswith("$"):
        return os.environ.get(value[1:], "")
    return value


@dataclass
class TrackerConfig:
    kind: str = ""
    endpoint: str = "https://api.linear.app/graphql"
    api_key: str = ""
    project_slug: str = ""
    active_states: list[str] = field(
        default_factory=lambda: ["Todo", "In Progress"]
    )
    terminal_states: list[str] = field(
        default_factory=lambda: ["Closed", "Cancelled", "Canceled", "Duplicate", "Done"]
    )


@dataclass
class PollingConfig:
    interval_ms: int = 30_000


@dataclass
class WorkspaceConfig:
    root: str = ""


@dataclass
class HooksConfig:
    after_create: Optional[str] = None
    before_run: Optional[str] = None
    after_run: Optional[str] = None
    before_remove: Optional[str] = None
    timeout_ms: int = 60_000


@dataclass
class AgentConfig:
    max_concurrent_agents: int = 10
    max_turns: int = 20
    max_retry_backoff_ms: int = 300_000
    max_concurrent_agents_by_state: dict[str, int] = field(default_factory=dict)


@dataclass
class CodexConfig:
    command: str = "codex app-server"
    approval_policy: Optional[str] = None
    thread_sandbox: Optional[str] = None
    turn_sandbox_policy: Optional[str] = None
    turn_timeout_ms: int = 3_600_000
    read_timeout_ms: int = 5_000
    stall_timeout_ms: int = 300_000


@dataclass
class ServerConfig:
    port: Optional[int] = None


@dataclass
class WorkflowConfig:
    tracker: TrackerConfig = field(default_factory=TrackerConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    hooks: HooksConfig = field(default_factory=HooksConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    codex: CodexConfig = field(default_factory=CodexConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    prompt_template: str = ""

    def validate(self) -> list[str]:
        """Return a list of validation error messages (empty = valid)."""
        errors: list[str] = []
        if not self.tracker.kind:
            errors.append("tracker.kind is required")
        elif self.tracker.kind != "linear":
            errors.append(f"Unsupported tracker.kind: {self.tracker.kind!r}")
        if not self.tracker.api_key:
            errors.append("tracker.api_key is required (or set LINEAR_API_KEY)")
        if not self.tracker.project_slug:
            errors.append("tracker.project_slug is required")
        if not self.prompt_template:
            errors.append("Workflow prompt body (after front matter) is empty")
        if self.agent.max_turns < 1:
            errors.append("agent.max_turns must be > 0")
        if self.hooks.timeout_ms <= 0:
            errors.append("hooks.timeout_ms must be a positive integer")
        return errors
