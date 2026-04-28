"""Per-issue workspace management.

Each issue gets a deterministic directory derived from its identifier:
  ``<workspace.root>/<sanitized_identifier>``

Safety invariants (from spec):
* Workspace path MUST stay inside workspace root (prefix check).
* Workspace key is sanitized: characters outside ``[A-Za-z0-9._-]`` are
  replaced with ``_``.

Lifecycle hooks are run as shell scripts with a configurable timeout.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Optional

from .config import HooksConfig, WorkspaceConfig

log = logging.getLogger(__name__)

_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def sanitize_key(identifier: str) -> str:
    """Replace unsafe characters with ``_``."""
    return _SAFE_CHARS.sub("_", identifier)


def workspace_path(config: WorkspaceConfig, identifier: str) -> str:
    """Return the absolute workspace path for *identifier*.

    Raises :class:`WorkspaceError` if the derived path escapes the root.
    """
    root = os.path.realpath(config.root)
    key = sanitize_key(identifier)
    path = os.path.realpath(os.path.join(root, key))
    # Safety invariant: path must be a strict subdirectory of root.
    if not (path.startswith(root + os.sep) or path == root):
        raise WorkspaceError(
            f"Workspace path {path!r} escapes workspace root {root!r}"
        )
    # Reject root itself as a workspace (key must be non-empty after sanitize).
    if path == root:
        raise WorkspaceError(
            f"Sanitized key for {identifier!r} produced an empty path"
        )
    return path


# ---------------------------------------------------------------------------
# Hook runner
# ---------------------------------------------------------------------------


async def _run_hook(
    script: str,
    cwd: str,
    timeout_ms: int,
    extra_env: Optional[dict[str, str]] = None,
) -> None:
    """Execute *script* as a shell command inside *cwd*.

    Raises :class:`WorkspaceError` on non-zero exit or timeout.
    """
    env = {**os.environ, **(extra_env or {})}
    proc = await asyncio.create_subprocess_shell(
        script,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    timeout_s = timeout_ms / 1_000.0
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        raise WorkspaceError(
            f"Hook timed out after {timeout_ms} ms"
        )
    if proc.returncode != 0:
        err_text = (stderr or b"").decode(errors="replace")[:500]
        raise WorkspaceError(
            f"Hook exited with code {proc.returncode}: {err_text}"
        )


# ---------------------------------------------------------------------------
# Workspace manager
# ---------------------------------------------------------------------------


class WorkspaceManager:
    def __init__(self, config: WorkspaceConfig, hooks: HooksConfig) -> None:
        self._config = config
        self._hooks = hooks

    def path_for(self, identifier: str) -> str:
        return workspace_path(self._config, identifier)

    async def prepare(self, identifier: str) -> str:
        """Ensure the workspace directory exists.

        Creates it (and runs ``after_create`` hook) if new.
        Always runs ``before_run`` hook.

        Returns the absolute workspace path.
        """
        root = self._config.root
        Path(root).mkdir(parents=True, exist_ok=True)

        ws_path = self.path_for(identifier)
        newly_created = not os.path.exists(ws_path)

        if newly_created:
            try:
                os.makedirs(ws_path, exist_ok=True)
            except OSError as exc:
                raise WorkspaceError(
                    f"Failed to create workspace {ws_path!r}: {exc}"
                ) from exc
            if self._hooks.after_create:
                log.debug("Running after_create hook for %s", identifier)
                try:
                    await _run_hook(
                        self._hooks.after_create,
                        cwd=ws_path,
                        timeout_ms=self._hooks.timeout_ms,
                    )
                except WorkspaceError:
                    # Failure aborts workspace creation: clean up.
                    shutil.rmtree(ws_path, ignore_errors=True)
                    raise

        if self._hooks.before_run:
            log.debug("Running before_run hook for %s", identifier)
            await _run_hook(
                self._hooks.before_run,
                cwd=ws_path,
                timeout_ms=self._hooks.timeout_ms,
            )

        return ws_path

    async def after_run(self, identifier: str) -> None:
        """Run the ``after_run`` hook; failure is logged but ignored."""
        if not self._hooks.after_run:
            return
        ws_path = self.path_for(identifier)
        if not os.path.exists(ws_path):
            return
        try:
            await _run_hook(
                self._hooks.after_run,
                cwd=ws_path,
                timeout_ms=self._hooks.timeout_ms,
            )
        except WorkspaceError as exc:
            log.warning("after_run hook failed for %s (ignored): %s", identifier, exc)

    async def remove(self, identifier: str) -> None:
        """Run ``before_remove`` hook then delete the workspace directory."""
        ws_path = self.path_for(identifier)
        if not os.path.exists(ws_path):
            return
        if self._hooks.before_remove:
            log.debug("Running before_remove hook for %s", identifier)
            try:
                await _run_hook(
                    self._hooks.before_remove,
                    cwd=ws_path,
                    timeout_ms=self._hooks.timeout_ms,
                )
            except WorkspaceError as exc:
                log.warning(
                    "before_remove hook failed for %s (ignored): %s", identifier, exc
                )
        try:
            shutil.rmtree(ws_path)
            log.info("Removed workspace for %s at %s", identifier, ws_path)
        except OSError as exc:
            log.warning("Failed to remove workspace %s: %s", ws_path, exc)


class WorkspaceError(Exception):
    """Raised for workspace creation, path safety, or hook failures."""
