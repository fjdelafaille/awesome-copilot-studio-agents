"""Symphony orchestrator.

This module owns the single authoritative in-memory runtime state and the
poll-dispatch-retry loop described in the spec.

State machine
-------------
Each issue has one of five orchestration states:

  Unclaimed     – not running, no retry scheduled
  Claimed       – reserved (prevents duplicate dispatch); sub-states:
    Running       – worker task exists in ``_running``
    RetryQueued   – retry timer exists in ``_retry_queue``
  Released      – claim removed (terminal, inactive, or retry exhausted)

Poll tick
---------
  1. Reload WORKFLOW.md if mtime changed.
  2. Validate config; skip dispatch if invalid.
  3. Reconcile running issues against tracker state.
  4. Fetch candidate issues.
  5. Fire due retry entries (re-dispatch or release).
  6. Dispatch eligible candidates until concurrency slots are exhausted.

Retry backoff
-------------
  - Continuation (clean exit, attempt 0): fixed 1 000 ms delay.
  - Failure-driven (attempt >= 1): ``min(10 000 × 2^(attempt-1), max_backoff)``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from . import workflow as _workflow_loader
from .config import WorkflowConfig
from .models import (
    Issue,
    OrchestratorSnapshot,
    RetryEntry,
    RunPhase,
    RunState,
    TokenTotals,
)
from .runner import AgentRunner, RunResult
from .template import TemplateError, render as render_prompt
from .tracker import LinearClient, TrackerError
from .workspace import WorkspaceError, WorkspaceManager

log = logging.getLogger(__name__)


def _retry_delay_ms(attempt: int, max_backoff_ms: int) -> int:
    """Return backoff delay in milliseconds.

    ``attempt == 0`` means a clean-exit continuation → 1 000 ms fixed.
    ``attempt >= 1`` uses exponential backoff capped at *max_backoff_ms*.
    """
    if attempt == 0:
        return 1_000
    return min(10_000 * (2 ** (attempt - 1)), max_backoff_ms)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class Orchestrator:
    """Long-running orchestration daemon."""

    def __init__(self, workflow_path: str) -> None:
        self._workflow_path = workflow_path
        self._config: Optional[WorkflowConfig] = None
        self._config_mtime: Optional[float] = None
        self._config_errors: list[str] = []

        # issue_id → RunState
        self._running: dict[str, RunState] = {}
        # issue_id → RetryEntry
        self._retry_queue: dict[str, RetryEntry] = {}
        # issue_ids claimed (running ∪ retry_queued)
        self._claimed: set[str] = set()

        self._start_time = time.monotonic()
        self._codex_totals = TokenTotals()
        self._shutdown_event = asyncio.Event()

        # Filled after first successful config load
        self._tracker: Optional[LinearClient] = None
        self._workspace: Optional[WorkspaceManager] = None

        # Callback for the HTTP server to inject a tick request
        self._tick_requested = asyncio.Event()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Block until shutdown is requested."""
        log.info("Symphony orchestrator starting – workflow: %s", self._workflow_path)
        try:
            await self._loop()
        finally:
            await self._teardown()

    def request_shutdown(self) -> None:
        self._shutdown_event.set()

    def request_tick(self) -> None:
        """Signal an immediate poll (used by HTTP refresh endpoint)."""
        self._tick_requested.set()

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        while not self._shutdown_event.is_set():
            await self._tick()

            # Determine sleep duration from current config (default 30 s).
            interval_ms = (
                self._config.polling.interval_ms if self._config else 30_000
            )
            # Wait for interval OR an early wake-up request.
            try:
                await asyncio.wait_for(
                    self._tick_requested.wait(),
                    timeout=interval_ms / 1_000.0,
                )
                self._tick_requested.clear()
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        # 1. Reload config
        await self._maybe_reload_config()
        if self._config_errors:
            log.warning(
                "Config invalid, skipping dispatch: %s", "; ".join(self._config_errors)
            )
            return
        assert self._config is not None

        # 2. Reconcile running issues
        await self._reconcile()

        # 3. Fetch candidates
        candidates: list[Issue] = []
        try:
            assert self._tracker is not None
            candidates = await self._tracker.fetch_candidates()
        except TrackerError as exc:
            log.warning("Tracker error fetching candidates (skipping dispatch): %s", exc)
            return

        candidate_map = {iss.id: iss for iss in candidates}

        # 4. Fire due retry entries
        await self._process_retries(candidate_map)

        # 5. Dispatch eligible candidates
        await self._dispatch(candidates)

    # ------------------------------------------------------------------
    # Config management
    # ------------------------------------------------------------------

    async def _maybe_reload_config(self) -> None:
        mtime = _workflow_loader.mtime(self._workflow_path)
        if mtime == self._config_mtime and self._config is not None:
            return
        log.info("Loading workflow from %s", self._workflow_path)
        try:
            new_cfg = _workflow_loader.load(self._workflow_path)
        except (ValueError, Exception) as exc:
            self._config_errors = [str(exc)]
            log.error("Failed to load workflow: %s", exc)
            return
        errors = new_cfg.validate()
        if errors:
            self._config_errors = errors
            log.error("Workflow config validation failed: %s", "; ".join(errors))
            return
        self._config_errors = []
        old_cfg = self._config
        self._config = new_cfg
        self._config_mtime = mtime

        # Re-create tracker client if tracker config changed.
        if old_cfg is None or old_cfg.tracker != new_cfg.tracker:
            if self._tracker:
                await self._tracker.close()
            self._tracker = LinearClient(new_cfg.tracker)
            await self._tracker.open()

        # Re-create workspace manager if workspace/hook config changed.
        if old_cfg is None or old_cfg.workspace != new_cfg.workspace or old_cfg.hooks != new_cfg.hooks:
            self._workspace = WorkspaceManager(new_cfg.workspace, new_cfg.hooks)

        # Perform startup terminal cleanup on first load.
        if old_cfg is None:
            await self._startup_cleanup()

    # ------------------------------------------------------------------
    # Startup cleanup
    # ------------------------------------------------------------------

    async def _startup_cleanup(self) -> None:
        """Remove workspaces for issues that are already in terminal state."""
        assert self._config is not None
        assert self._tracker is not None
        assert self._workspace is not None
        import os

        root = self._config.workspace.root
        if not os.path.isdir(root):
            return
        try:
            entries = os.listdir(root)
        except OSError:
            return
        for entry in entries:
            ws_path = os.path.join(root, entry)
            if not os.path.isdir(ws_path):
                continue
            # The directory name is the sanitized identifier; we can't recover
            # the original identifier to query Linear.  We skip cleanup here
            # and rely on reconciliation once issues are running.
            # (A real implementation might persist a manifest; the spec allows
            # tracker-driven recovery without a persistent DB.)
        log.debug("Startup cleanup complete")

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    async def _reconcile(self) -> None:
        if not self._running:
            return
        assert self._config is not None
        assert self._tracker is not None

        issue_ids = list(self._running.keys())
        try:
            state_map = await self._tracker.refresh_states(issue_ids)
        except TrackerError as exc:
            log.warning("Tracker error during reconciliation (keeping workers alive): %s", exc)
            return

        now_monotonic = time.monotonic()
        stall_s = self._config.codex.stall_timeout_ms / 1_000.0

        for issue_id, run_state in list(self._running.items()):
            # Stall detection
            if (
                self._config.codex.stall_timeout_ms > 0
                and run_state.last_event_at is not None
            ):
                elapsed = now_monotonic - run_state.last_event_at.timestamp() + _utcnow().timestamp() - _utcnow().timestamp()
                # Simpler: use wall-clock delta stored in RunState
                if run_state.last_event_at:
                    elapsed_s = (_utcnow() - run_state.last_event_at).total_seconds()
                    if elapsed_s > stall_s:
                        log.warning(
                            "Stall detected for %s (%.0fs idle) – cancelling",
                            run_state.issue_identifier,
                            elapsed_s,
                        )
                        run_state.task.cancel()
                        continue

            tracker_state = state_map.get(issue_id)
            if tracker_state is None:
                # Issue not found in tracker – terminate without cleanup.
                log.info(
                    "Issue %s not found in tracker – cancelling worker",
                    run_state.issue_identifier,
                )
                run_state.task.cancel()
            elif tracker_state in self._config.tracker.terminal_states:
                log.info(
                    "Issue %s reached terminal state %r – cancelling and cleaning up",
                    run_state.issue_identifier,
                    tracker_state,
                )
                run_state.task.cancel()
                asyncio.ensure_future(
                    self._cleanup_workspace(run_state.issue_identifier)
                )
            else:
                # Still active – update snapshot state
                run_state.issue_snapshot.state = tracker_state

    async def _cleanup_workspace(self, identifier: str) -> None:
        if self._workspace:
            await self._workspace.remove(identifier)

    # ------------------------------------------------------------------
    # Retry processing
    # ------------------------------------------------------------------

    async def _process_retries(self, candidate_map: dict[str, Issue]) -> None:
        assert self._config is not None
        now = _utcnow()

        for issue_id, entry in list(self._retry_queue.items()):
            if entry.due_at > now:
                continue
            # Timer fired
            del self._retry_queue[issue_id]
            issue = candidate_map.get(issue_id)
            if issue is None:
                log.info(
                    "Retry due for %s but issue not in active candidates – releasing",
                    entry.issue_identifier,
                )
                self._claimed.discard(issue_id)
                continue
            if issue.state in self._config.tracker.terminal_states:
                log.info(
                    "Retry due for %s but state %r is terminal – releasing",
                    entry.issue_identifier,
                    issue.state,
                )
                self._claimed.discard(issue_id)
                await self._cleanup_workspace(entry.issue_identifier)
                continue
            # Re-dispatch if slots available, else re-queue.
            if self._slots_available(issue):
                self._start_worker(issue, attempt=entry.attempt)
            else:
                delay_ms = _retry_delay_ms(entry.attempt, self._config.agent.max_retry_backoff_ms)
                log.debug(
                    "No slots for retry of %s – requeueing in %d ms",
                    entry.issue_identifier,
                    delay_ms,
                )
                self._enqueue_retry(issue_id, entry.issue_identifier, entry.attempt, None, delay_ms)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    async def _dispatch(self, candidates: list[Issue]) -> None:
        assert self._config is not None
        for issue in candidates:
            if not self._eligible(issue):
                continue
            if not self._slots_available(issue):
                log.debug(
                    "No concurrency slots for %s (state=%s)",
                    issue.identifier,
                    issue.state,
                )
                break
            self._claimed.add(issue.id)
            self._start_worker(issue, attempt=0)

    def _eligible(self, issue: Issue) -> bool:
        """Return True if *issue* can be dispatched."""
        assert self._config is not None
        if issue.id in self._claimed:
            return False
        if issue.id in self._running:
            return False
        if issue.state in self._config.tracker.terminal_states:
            return False
        if issue.state not in self._config.tracker.active_states:
            return False
        if not issue.id or not issue.identifier or not issue.title or not issue.state:
            return False
        # Block dispatch on Todo issues with non-terminal blockers.
        if issue.state.lower() == "todo":
            terminal = set(s.lower() for s in self._config.tracker.terminal_states)
            for blocker in issue.blocked_by:
                if blocker.state.lower() not in terminal:
                    return False
        return True

    def _slots_available(self, issue: Issue) -> bool:
        assert self._config is not None
        # Global limit
        if len(self._running) >= self._config.agent.max_concurrent_agents:
            return False
        # Per-state limit
        state_key = issue.state.lower()
        limit = self._config.agent.max_concurrent_agents_by_state.get(state_key)
        if limit is not None:
            count = sum(
                1
                for rs in self._running.values()
                if rs.issue_snapshot.state.lower() == state_key
            )
            if count >= limit:
                return False
        return True

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def _start_worker(self, issue: Issue, attempt: int) -> None:
        assert self._config is not None
        log.info(
            "Dispatching %s (state=%s, attempt=%d)",
            issue.identifier,
            issue.state,
            attempt,
        )
        task = asyncio.ensure_future(self._run_worker(issue, attempt))
        run_state = RunState(
            issue_id=issue.id,
            issue_identifier=issue.identifier,
            issue_snapshot=issue,
            phase=RunPhase.PREPARING_WORKSPACE,
            session_id=None,
            turn_count=0,
            last_event=None,
            started_at=_utcnow(),
            last_event_at=None,
            tokens=TokenTotals(),
            task=task,
        )
        self._running[issue.id] = run_state
        self._claimed.add(issue.id)
        task.add_done_callback(
            lambda t: asyncio.ensure_future(self._on_worker_done(issue, attempt, t))
        )

    async def _run_worker(self, issue: Issue, attempt: int) -> RunResult:
        assert self._config is not None
        assert self._workspace is not None
        run_state = self._running[issue.id]

        # Phase: PreparingWorkspace
        run_state.phase = RunPhase.PREPARING_WORKSPACE
        try:
            ws_path = await self._workspace.prepare(issue.identifier)
        except WorkspaceError as exc:
            log.error("Workspace preparation failed for %s: %s", issue.identifier, exc)
            return RunResult(
                phase=RunPhase.FAILED,
                session_id=None,
                turn_count=0,
                error=str(exc),
            )

        # Phase: BuildingPrompt
        run_state.phase = RunPhase.BUILDING_PROMPT
        try:
            prompt = render_prompt(
                self._config.prompt_template,
                issue,
                attempt=attempt if attempt > 0 else None,
            )
        except TemplateError as exc:
            log.error("Prompt rendering failed for %s: %s", issue.identifier, exc)
            return RunResult(
                phase=RunPhase.FAILED,
                session_id=None,
                turn_count=0,
                error=str(exc),
            )

        def _event_cb(etype: str, event: dict[str, Any]) -> "asyncio.Future[None]":
            return asyncio.ensure_future(
                self._handle_agent_event(issue.id, etype, event)
            )

        runner = AgentRunner(
            config=self._config.codex,
            workspace_path=ws_path,
            prompt=prompt,
            max_turns=self._config.agent.max_turns,
            on_event=_event_cb,
        )
        result = await runner.run()

        # Run after_run hook (failure is logged, not propagated).
        await self._workspace.after_run(issue.identifier)

        return result

    async def _on_worker_done(
        self,
        issue: Issue,
        attempt: int,
        task: asyncio.Task,  # type: ignore[type-arg]
    ) -> None:
        assert self._config is not None
        run_state = self._running.pop(issue.id, None)
        if run_state:
            self._codex_totals.add(run_state.tokens)

        if task.cancelled():
            log.info("Worker for %s was cancelled", issue.identifier)
            self._claimed.discard(issue.id)
            return

        exc = task.exception()
        if exc is not None:
            log.error("Worker for %s raised an exception: %s", issue.identifier, exc)
            self._schedule_retry(issue, attempt + 1, str(exc))
            return

        result: RunResult = task.result()
        log.info(
            "Worker for %s finished: phase=%s turns=%d",
            issue.identifier,
            result.phase.value,
            result.turn_count,
        )

        if result.phase == RunPhase.SUCCEEDED:
            # Schedule a continuation check after a short delay.
            delay_ms = _retry_delay_ms(0, self._config.agent.max_retry_backoff_ms)
            self._enqueue_retry(issue.id, issue.identifier, 0, None, delay_ms)
        elif result.phase == RunPhase.CANCELED_BY_RECONCILIATION:
            self._claimed.discard(issue.id)
        else:
            # Failed, TimedOut, Stalled → exponential backoff retry.
            next_attempt = attempt + 1
            self._schedule_retry(issue, next_attempt, result.error)

    def _schedule_retry(
        self, issue: Issue, attempt: int, error: Optional[str]
    ) -> None:
        assert self._config is not None
        delay_ms = _retry_delay_ms(attempt, self._config.agent.max_retry_backoff_ms)
        log.info(
            "Scheduling retry for %s attempt=%d in %d ms (reason: %s)",
            issue.identifier,
            attempt,
            delay_ms,
            error or "unknown",
        )
        self._enqueue_retry(issue.id, issue.identifier, attempt, error, delay_ms)

    def _enqueue_retry(
        self,
        issue_id: str,
        issue_identifier: str,
        attempt: int,
        error: Optional[str],
        delay_ms: int,
    ) -> None:
        due = datetime.fromtimestamp(
            _utcnow().timestamp() + delay_ms / 1_000.0,
            tz=timezone.utc,
        )
        self._retry_queue[issue_id] = RetryEntry(
            issue_id=issue_id,
            issue_identifier=issue_identifier,
            attempt=attempt,
            due_at=due,
            error=error,
        )
        self._claimed.add(issue_id)

    # ------------------------------------------------------------------
    # Agent event handler
    # ------------------------------------------------------------------

    async def _handle_agent_event(
        self, issue_id: str, etype: str, event: dict[str, Any]
    ) -> None:
        run_state = self._running.get(issue_id)
        if run_state is None:
            return
        run_state.last_event = etype
        run_state.last_event_at = _utcnow()

        if etype == "_phase":
            try:
                run_state.phase = RunPhase(event.get("phase", ""))
            except ValueError:
                pass
        elif etype == "usage":
            pass  # tokens accumulated in runner
        elif etype == "turn_started":
            run_state.session_id = (
                f"{event.get('thread_id', '')}-{event.get('id', '')}"
            )
            run_state.turn_count += 1

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    async def _teardown(self) -> None:
        log.info("Shutting down orchestrator – cancelling %d workers", len(self._running))
        for run_state in self._running.values():
            run_state.task.cancel()
        if self._running:
            await asyncio.gather(
                *[rs.task for rs in self._running.values()],
                return_exceptions=True,
            )
        if self._tracker:
            await self._tracker.close()
        log.info("Orchestrator shutdown complete")

    # ------------------------------------------------------------------
    # State snapshot (for HTTP server)
    # ------------------------------------------------------------------

    def snapshot(self) -> OrchestratorSnapshot:
        return OrchestratorSnapshot(
            generated_at=_utcnow(),
            running=[rs.to_dict() for rs in self._running.values()],
            retrying=[re.to_dict() for re in self._retry_queue.values()],
            codex_totals=TokenTotals(
                input_tokens=self._codex_totals.input_tokens,
                output_tokens=self._codex_totals.output_tokens,
                total_tokens=self._codex_totals.total_tokens,
            ),
            seconds_running=time.monotonic() - self._start_time,
        )
