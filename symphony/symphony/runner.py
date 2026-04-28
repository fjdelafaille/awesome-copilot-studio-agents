"""Codex app-server subprocess runner.

Responsibilities
----------------
* Launch ``bash -lc <codex.command>`` in the per-issue workspace directory.
* Drive a JSON-line protocol over stdin/stdout:
    - Write a ``ThreadStart`` request to start the thread.
    - Write ``TurnStart`` requests to run turns.
    - Read JSON events from stdout; dispatch to the orchestrator callback.
* Enforce turn, read, and stall timeouts.
* Extract ``thread_id`` and ``turn_id`` to compose
  ``session_id = "<thread_id>-<turn_id>"``.
* Handle approval requests per implementation policy.
* Reject unsupported tool calls without stalling.
* Accumulate token counts.

Protocol note
-------------
The Codex app-server protocol is the authoritative source of truth.  This
implementation follows the message shapes described in the Symphony spec and
what can be inferred from public Codex documentation.  The key message types
are:

  Outbound (stdin):
    {"type": "thread_start", ...}
    {"type": "turn_start",   ...}
    {"type": "approval_response", "approved": bool, "turn_id": "..."}
    {"type": "shutdown"}

  Inbound (stdout, one JSON object per line):
    {"type": "thread_created",  "id": "...", ...}
    {"type": "turn_started",    "id": "...", "thread_id": "...", ...}
    {"type": "message",         "role": "assistant", "content": "...", ...}
    {"type": "approval_request","turn_id": "...", "command": [...], ...}
    {"type": "turn_complete",   "id": "...", "exit_code": 0, ...}
    {"type": "turn_error",      "id": "...", "error": "...", ...}
    {"type": "usage",           "input_tokens": N, "output_tokens": N, ...}
    {"type": "error",           "error": "...", ...}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

from .config import CodexConfig
from .models import RunPhase, TokenTotals

log = logging.getLogger(__name__)

# Type alias for the event callback the orchestrator installs.
EventCallback = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


@dataclass
class RunResult:
    """Summary of a completed agent run."""

    phase: RunPhase
    session_id: Optional[str]
    turn_count: int
    error: Optional[str]
    tokens: TokenTotals = field(default_factory=TokenTotals)


class AgentRunner:
    """Manages one Codex app-server subprocess for one issue attempt."""

    def __init__(
        self,
        *,
        config: CodexConfig,
        workspace_path: str,
        prompt: str,
        max_turns: int,
        on_event: Optional[EventCallback] = None,
    ) -> None:
        self._config = config
        self._workspace_path = workspace_path
        self._prompt = prompt
        self._max_turns = max_turns
        self._on_event = on_event

        self._proc: Optional[asyncio.subprocess.Process] = None
        self._thread_id: Optional[str] = None
        self._current_turn_id: Optional[str] = None
        self._session_id: Optional[str] = None
        self._turn_count = 0
        self._tokens = TokenTotals()
        self._last_event_time = time.monotonic()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self) -> RunResult:
        """Execute the agent session and return a summary."""
        try:
            return await self._run_session()
        except asyncio.CancelledError:
            await self._kill_proc()
            return RunResult(
                phase=RunPhase.CANCELED_BY_RECONCILIATION,
                session_id=self._session_id,
                turn_count=self._turn_count,
                error="Cancelled by reconciliation",
                tokens=self._tokens,
            )
        except Exception as exc:
            await self._kill_proc()
            return RunResult(
                phase=RunPhase.FAILED,
                session_id=self._session_id,
                turn_count=self._turn_count,
                error=str(exc),
                tokens=self._tokens,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _run_session(self) -> RunResult:
        # Phase: LaunchingAgentProcess
        await self._notify_phase(RunPhase.LAUNCHING_AGENT_PROCESS)
        self._proc = await asyncio.create_subprocess_shell(
            f"bash -lc {_shell_quote(self._config.command)}",
            cwd=self._workspace_path,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Phase: InitializingSession – start thread
        await self._notify_phase(RunPhase.INITIALIZING_SESSION)
        thread_params: dict[str, Any] = {
            "type": "thread_start",
            "working_directory": self._workspace_path,
        }
        if self._config.approval_policy is not None:
            thread_params["approval_policy"] = self._config.approval_policy
        if self._config.thread_sandbox is not None:
            thread_params["sandbox"] = self._config.thread_sandbox
        await self._write(thread_params)

        # Wait for thread_created with read timeout
        thread_event = await self._read_event(
            timeout_s=self._config.read_timeout_ms / 1_000.0,
        )
        if thread_event.get("type") != "thread_created":
            await self._kill_proc()
            return RunResult(
                phase=RunPhase.FAILED,
                session_id=None,
                turn_count=0,
                error=f"Expected thread_created, got: {thread_event.get('type')!r}",
                tokens=self._tokens,
            )
        self._thread_id = thread_event.get("id", "")

        # Run turns
        first_turn = True
        prompt = self._prompt
        while self._turn_count < self._max_turns:
            await self._notify_phase(RunPhase.STREAMING_TURN)
            result = await self._run_turn(prompt, first_turn=first_turn)
            first_turn = False

            if result.phase != RunPhase.SUCCEEDED:
                return result

            self._turn_count += 1
            # A clean turn exit means the agent finished its work for this
            # session – return success.
            break

        await self._notify_phase(RunPhase.FINISHING)
        await self._shutdown()
        return RunResult(
            phase=RunPhase.SUCCEEDED,
            session_id=self._session_id,
            turn_count=self._turn_count,
            error=None,
            tokens=self._tokens,
        )

    async def _run_turn(self, prompt: str, *, first_turn: bool) -> RunResult:
        turn_params: dict[str, Any] = {
            "type": "turn_start",
            "input": prompt,
        }
        if self._config.turn_sandbox_policy is not None:
            turn_params["sandbox_policy"] = self._config.turn_sandbox_policy
        await self._write(turn_params)

        # Read events until turn_complete or an error terminal
        turn_timeout_s = self._config.turn_timeout_ms / 1_000.0
        stall_timeout_s = (
            self._config.stall_timeout_ms / 1_000.0
            if self._config.stall_timeout_ms > 0
            else None
        )
        deadline = time.monotonic() + turn_timeout_s

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                await self._kill_proc()
                return RunResult(
                    phase=RunPhase.TIMED_OUT,
                    session_id=self._session_id,
                    turn_count=self._turn_count,
                    error="Turn timeout exceeded",
                    tokens=self._tokens,
                )

            read_timeout = min(remaining, stall_timeout_s or remaining)
            try:
                event = await self._read_event(timeout_s=read_timeout)
            except asyncio.TimeoutError:
                if stall_timeout_s and (time.monotonic() - self._last_event_time) >= stall_timeout_s:
                    await self._kill_proc()
                    return RunResult(
                        phase=RunPhase.STALLED,
                        session_id=self._session_id,
                        turn_count=self._turn_count,
                        error="Stall timeout: no activity",
                        tokens=self._tokens,
                    )
                continue

            self._last_event_time = time.monotonic()
            etype = event.get("type", "")

            if etype == "turn_started":
                self._current_turn_id = event.get("id", "")
                if self._thread_id and self._current_turn_id:
                    self._session_id = f"{self._thread_id}-{self._current_turn_id}"

            elif etype == "usage":
                self._accumulate_tokens(event)

            elif etype == "approval_request":
                await self._handle_approval(event)

            elif etype == "turn_complete":
                exit_code = event.get("exit_code", 0)
                if exit_code != 0:
                    return RunResult(
                        phase=RunPhase.FAILED,
                        session_id=self._session_id,
                        turn_count=self._turn_count,
                        error=f"Turn exited with code {exit_code}",
                        tokens=self._tokens,
                    )
                return RunResult(
                    phase=RunPhase.SUCCEEDED,
                    session_id=self._session_id,
                    turn_count=self._turn_count,
                    error=None,
                    tokens=self._tokens,
                )

            elif etype in ("turn_error", "error"):
                err = event.get("error", "unknown error")
                return RunResult(
                    phase=RunPhase.FAILED,
                    session_id=self._session_id,
                    turn_count=self._turn_count,
                    error=err,
                    tokens=self._tokens,
                )

            elif etype == "unsupported_tool":
                # Reject unsupported tool calls without stalling.
                log.warning("Unsupported tool call in turn %s: %s", self._current_turn_id, event)
                await self._write(
                    {
                        "type": "tool_response",
                        "error": "Tool not supported by this Symphony implementation",
                        "turn_id": self._current_turn_id,
                    }
                )

            if self._on_event:
                await self._on_event(etype, event)

        # Unreachable, but keeps type checker happy.
        raise RuntimeError("Unreachable")

    async def _handle_approval(self, event: dict[str, Any]) -> None:
        """Respond to an approval_request event.

        The spec intentionally does not mandate a single approval policy.
        This implementation auto-approves by default (permissive posture).
        Production deployments should override this to add operator gating.
        """
        turn_id = event.get("turn_id", self._current_turn_id)
        log.info("Approval requested for turn %s – auto-approving", turn_id)
        await self._write(
            {
                "type": "approval_response",
                "approved": True,
                "turn_id": turn_id,
            }
        )

    def _accumulate_tokens(self, event: dict[str, Any]) -> None:
        """Extract token counts from a usage event (delta semantics)."""
        self._tokens.input_tokens += int(event.get("input_tokens", 0))
        self._tokens.output_tokens += int(event.get("output_tokens", 0))
        total = event.get("total_tokens")
        if total is not None:
            # Use explicit total if provided; otherwise sum.
            delta = int(total) - self._tokens.total_tokens
            self._tokens.total_tokens += max(delta, 0)
        else:
            self._tokens.total_tokens = (
                self._tokens.input_tokens + self._tokens.output_tokens
            )

    async def _write(self, obj: dict[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            return
        line = json.dumps(obj) + "\n"
        try:
            self._proc.stdin.write(line.encode())
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    async def _read_event(self, timeout_s: float) -> dict[str, Any]:
        if self._proc is None or self._proc.stdout is None:
            raise RuntimeError("Process not started")
        line = await asyncio.wait_for(
            self._proc.stdout.readline(),
            timeout=timeout_s,
        )
        if not line:
            raise EOFError("Subprocess stdout closed")
        try:
            return json.loads(line.decode())
        except json.JSONDecodeError as exc:
            log.debug("Ignoring non-JSON line from app-server: %r", line[:200])
            return {"type": "_unparseable", "_raw": line.decode()[:200]}

    async def _shutdown(self) -> None:
        await self._write({"type": "shutdown"})
        if self._proc:
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                await self._kill_proc()

    async def _kill_proc(self) -> None:
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.kill()
                await asyncio.wait_for(self._proc.wait(), timeout=3.0)
            except (ProcessLookupError, asyncio.TimeoutError):
                pass

    async def _notify_phase(self, phase: RunPhase) -> None:
        if self._on_event:
            await self._on_event("_phase", {"phase": phase.value})


def _shell_quote(s: str) -> str:
    """Minimal shell-safe quoting for a single argument."""
    return "'" + s.replace("'", "'\\''") + "'"
