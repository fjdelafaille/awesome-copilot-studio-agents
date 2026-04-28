"""Optional HTTP status server.

Enabled when ``server.port`` is set in WORKFLOW.md.

Endpoints
---------
GET  /                       Human-readable operator dashboard (HTML).
GET  /api/v1/state           System state snapshot (JSON).
GET  /api/v1/<identifier>    Per-issue debug details (JSON).
POST /api/v1/refresh         Trigger an immediate poll cycle → 202 Accepted.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from .orchestrator import Orchestrator

log = logging.getLogger(__name__)


def _json_response(data: object, status: int = 200) -> web.Response:
    return web.Response(
        status=status,
        content_type="application/json",
        text=json.dumps(data, default=str),
    )


def _build_app(orchestrator: "Orchestrator") -> web.Application:
    app = web.Application()

    # ------------------------------------------------------------------
    # GET /
    # ------------------------------------------------------------------
    async def index(_req: web.Request) -> web.Response:
        snap = orchestrator.snapshot()
        running_rows = "".join(
            f"<tr><td>{r['issue_identifier']}</td>"
            f"<td>{r['state']}</td>"
            f"<td>{r['phase']}</td>"
            f"<td>{r['turn_count']}</td>"
            f"<td>{r['session_id'] or '—'}</td></tr>"
            for r in snap.running
        )
        retrying_rows = "".join(
            f"<tr><td>{r['issue_identifier']}</td>"
            f"<td>{r['attempt']}</td>"
            f"<td>{r['due_at']}</td>"
            f"<td>{r['error'] or '—'}</td></tr>"
            for r in snap.retrying
        )
        html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Symphony</title>
<style>
body{{font-family:sans-serif;margin:2rem}}
table{{border-collapse:collapse;width:100%;margin-bottom:1rem}}
th,td{{border:1px solid #ccc;padding:.4rem .8rem;text-align:left}}
th{{background:#f4f4f4}}
</style>
</head>
<body>
<h1>Symphony</h1>
<p>Generated at: {snap.generated_at.isoformat()} &bull;
   Uptime: {snap.seconds_running:.0f}s</p>
<h2>Running ({len(snap.running)})</h2>
<table>
<tr><th>Issue</th><th>State</th><th>Phase</th><th>Turns</th><th>Session</th></tr>
{running_rows or '<tr><td colspan="5">— none —</td></tr>'}
</table>
<h2>Retrying ({len(snap.retrying)})</h2>
<table>
<tr><th>Issue</th><th>Attempt</th><th>Due</th><th>Last error</th></tr>
{retrying_rows or '<tr><td colspan="4">— none —</td></tr>'}
</table>
<h2>Token totals</h2>
<table>
<tr><th>Input</th><th>Output</th><th>Total</th></tr>
<tr><td>{snap.codex_totals.input_tokens}</td>
    <td>{snap.codex_totals.output_tokens}</td>
    <td>{snap.codex_totals.total_tokens}</td></tr>
</table>
</body></html>"""
        return web.Response(content_type="text/html", text=html)

    # ------------------------------------------------------------------
    # GET /api/v1/state
    # ------------------------------------------------------------------
    async def api_state(_req: web.Request) -> web.Response:
        snap = orchestrator.snapshot()
        return _json_response(
            {
                "generated_at": snap.generated_at.isoformat(),
                "counts": {
                    "running": len(snap.running),
                    "retrying": len(snap.retrying),
                },
                "running": snap.running,
                "retrying": snap.retrying,
                "codex_totals": {
                    "input_tokens": snap.codex_totals.input_tokens,
                    "output_tokens": snap.codex_totals.output_tokens,
                    "total_tokens": snap.codex_totals.total_tokens,
                    "seconds_running": snap.seconds_running,
                },
                "rate_limits": None,
            }
        )

    # ------------------------------------------------------------------
    # GET /api/v1/{identifier}
    # ------------------------------------------------------------------
    async def api_issue(req: web.Request) -> web.Response:
        identifier = req.match_info["identifier"]
        snap = orchestrator.snapshot()

        running = next(
            (r for r in snap.running if r["issue_identifier"] == identifier), None
        )
        retrying = next(
            (r for r in snap.retrying if r["issue_identifier"] == identifier), None
        )
        if running is None and retrying is None:
            return _json_response({"error": "Not found"}, status=404)

        return _json_response(
            {
                "issue_identifier": identifier,
                "running": running,
                "retrying": retrying,
            }
        )

    # ------------------------------------------------------------------
    # POST /api/v1/refresh
    # ------------------------------------------------------------------
    async def api_refresh(_req: web.Request) -> web.Response:
        orchestrator.request_tick()
        return _json_response({"queued": True}, status=202)

    app.router.add_get("/", index)
    app.router.add_get("/api/v1/state", api_state)
    app.router.add_get("/api/v1/{identifier}", api_issue)
    app.router.add_post("/api/v1/refresh", api_refresh)
    return app


class StatusServer:
    def __init__(self, orchestrator: "Orchestrator", port: int) -> None:
        self._orchestrator = orchestrator
        self._port = port
        self._runner: web.AppRunner | None = None

    async def start(self) -> int:
        """Start the server and return the actual port (useful when port=0)."""
        app = _build_app(self._orchestrator)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        actual_port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        log.info("Status server listening on http://0.0.0.0:%d", actual_port)
        return actual_port

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
