"""CLI entry point.

Usage
-----
    python -m symphony WORKFLOW.md [--port PORT]

    # or, after ``pip install .``:
    symphony WORKFLOW.md [--port PORT]

``--port`` overrides ``server.port`` from the workflow file.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from .orchestrator import Orchestrator
from .server import StatusServer
from . import workflow as _wf


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s – %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    # Silence noisy third-party loggers at INFO level.
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


async def _async_main(workflow_path: str, port_override: int | None) -> None:
    orch = Orchestrator(workflow_path)

    # Pre-load config once to check for immediate errors and read server port.
    try:
        cfg = _wf.load(workflow_path)
    except ValueError as exc:
        logging.critical("Cannot load workflow: %s", exc)
        sys.exit(1)
    errors = cfg.validate()
    if errors:
        logging.critical("Workflow config errors:\n  %s", "\n  ".join(errors))
        sys.exit(1)

    port = port_override if port_override is not None else cfg.server.port
    server: StatusServer | None = None
    if port is not None:
        server = StatusServer(orch, port)

    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        logging.info("Shutdown signal received")
        orch.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    tasks: list[asyncio.Task] = []  # type: ignore[type-arg]
    if server:
        await server.start()

    orch_task = asyncio.create_task(orch.run())
    tasks.append(orch_task)

    try:
        await orch_task
    finally:
        if server:
            await server.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="symphony",
        description="Symphony – coding-agent orchestration daemon",
    )
    parser.add_argument("workflow", metavar="WORKFLOW.md", help="Path to WORKFLOW.md")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP status server port (overrides server.port in workflow)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()
    _configure_logging(args.verbose)
    asyncio.run(_async_main(args.workflow, args.port))


if __name__ == "__main__":
    main()
