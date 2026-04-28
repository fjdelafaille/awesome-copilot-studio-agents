"""Domain models for Symphony."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Issue tracker domain
# ---------------------------------------------------------------------------


@dataclass
class BlockerRef:
    """A lightweight reference to a blocking issue."""

    id: str
    identifier: str
    state: str


@dataclass
class Issue:
    """Normalised representation of a tracker issue."""

    id: str
    identifier: str
    title: str
    state: str
    description: Optional[str] = None
    priority: Optional[int] = None
    branch_name: Optional[str] = None
    url: Optional[str] = None
    labels: list[str] = field(default_factory=list)
    blocked_by: list[BlockerRef] = field(default_factory=list)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "identifier": self.identifier,
            "title": self.title,
            "state": self.state,
            "description": self.description,
            "priority": self.priority,
            "branch_name": self.branch_name,
            "url": self.url,
            "labels": list(self.labels),
            "blocked_by": [
                {"id": b.id, "identifier": b.identifier, "state": b.state}
                for b in self.blocked_by
            ],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


# ---------------------------------------------------------------------------
# Orchestrator state machine
# ---------------------------------------------------------------------------


class OrchestrationState(str, Enum):
    UNCLAIMED = "unclaimed"
    CLAIMED = "claimed"
    RUNNING = "running"
    RETRY_QUEUED = "retry_queued"
    RELEASED = "released"


class RunPhase(str, Enum):
    PREPARING_WORKSPACE = "preparing_workspace"
    BUILDING_PROMPT = "building_prompt"
    LAUNCHING_AGENT_PROCESS = "launching_agent_process"
    INITIALIZING_SESSION = "initializing_session"
    STREAMING_TURN = "streaming_turn"
    FINISHING = "finishing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    STALLED = "stalled"
    CANCELED_BY_RECONCILIATION = "canceled_by_reconciliation"

    def is_terminal(self) -> bool:
        return self in (
            RunPhase.SUCCEEDED,
            RunPhase.FAILED,
            RunPhase.TIMED_OUT,
            RunPhase.STALLED,
            RunPhase.CANCELED_BY_RECONCILIATION,
        )


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------


@dataclass
class TokenTotals:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    def add(self, other: TokenTotals) -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.total_tokens += other.total_tokens


@dataclass
class RunState:
    """Live state for a single running issue worker."""

    issue_id: str
    issue_identifier: str
    issue_snapshot: Issue
    phase: RunPhase
    session_id: Optional[str]
    turn_count: int
    last_event: Optional[str]
    started_at: datetime
    last_event_at: Optional[datetime]
    tokens: TokenTotals
    task: asyncio.Task  # type: ignore[type-arg]

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "issue_identifier": self.issue_identifier,
            "state": self.issue_snapshot.state,
            "phase": self.phase.value,
            "session_id": self.session_id,
            "turn_count": self.turn_count,
            "last_event": self.last_event,
            "started_at": self.started_at.isoformat(),
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
            "tokens": {
                "input_tokens": self.tokens.input_tokens,
                "output_tokens": self.tokens.output_tokens,
                "total_tokens": self.tokens.total_tokens,
            },
        }


@dataclass
class RetryEntry:
    """A queued retry for an issue."""

    issue_id: str
    issue_identifier: str
    attempt: int
    due_at: datetime
    error: Optional[str]
    timer_handle: Optional[asyncio.TimerHandle] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "issue_identifier": self.issue_identifier,
            "attempt": self.attempt,
            "due_at": self.due_at.isoformat(),
            "error": self.error,
        }


@dataclass
class OrchestratorSnapshot:
    """Point-in-time view of orchestrator state for status API."""

    generated_at: datetime
    running: list[dict[str, Any]]
    retrying: list[dict[str, Any]]
    codex_totals: TokenTotals
    seconds_running: float
