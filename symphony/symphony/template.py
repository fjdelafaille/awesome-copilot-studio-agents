"""Strict Jinja2-based prompt template renderer.

The spec requires:
* Unknown variables MUST fail rendering (no silent empty-string fallback).
* Unknown filters MUST fail rendering.
* Available variables: ``issue`` (Issue dict) and ``attempt`` (int | None).
"""

from __future__ import annotations

from typing import Any, Optional

from jinja2 import Environment, StrictUndefined, TemplateSyntaxError, UndefinedError

from .models import Issue


class TemplateError(Exception):
    """Raised when template rendering fails."""


def render(template_source: str, issue: Issue, attempt: Optional[int]) -> str:
    """Render *template_source* with ``issue`` and ``attempt`` context.

    Parameters
    ----------
    template_source:
        The raw Jinja2 template string (the prompt body from WORKFLOW.md).
    issue:
        The normalised issue that will be injected as the ``issue`` variable.
    attempt:
        ``None`` on the first run; a positive integer on retries/continuations.

    Raises
    ------
    TemplateError
        On syntax errors, unknown variables, or unknown filters.
    """
    env = Environment(undefined=StrictUndefined, autoescape=False)
    try:
        tmpl = env.from_string(template_source)
    except TemplateSyntaxError as exc:
        raise TemplateError(f"Template syntax error: {exc}") from exc

    context: dict[str, Any] = {
        "issue": issue.to_dict(),
        "attempt": attempt,
    }
    try:
        return tmpl.render(**context)
    except UndefinedError as exc:
        raise TemplateError(f"Template variable error: {exc}") from exc
    except Exception as exc:
        raise TemplateError(f"Template rendering failed: {exc}") from exc
