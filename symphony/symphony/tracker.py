"""Linear GraphQL issue tracker client.

Responsibilities
----------------
- Fetch candidate issues (paginated, project-filtered).
- Refresh state for a batch of issue IDs.
- Normalise raw Linear payloads to :class:`~symphony.models.Issue`.

Design notes
------------
* Queries are isolated here so they can be updated independently when the
  Linear schema drifts.
* Auth is carried via ``Authorization: <api_key>`` header.
* Pagination uses Linear's cursor-based ``pageInfo.endCursor``.
* Blocker relations are read from the inverse ``blocks`` relationship type:
  issues that *block* the current issue appear in ``relations`` filtered to
  ``type == "blocks"`` on the *related* side.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import aiohttp

from .config import TrackerConfig
from .models import BlockerRef, Issue

log = logging.getLogger(__name__)

_PAGE_SIZE = 50

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

_CANDIDATES_QUERY = """
query SymphonyCandidates(
  $projectSlug: String!
  $states: [String!]!
  $after: String
  $first: Int!
) {
  issues(
    filter: {
      project: { slugId: { eq: $projectSlug } }
      state: { name: { in: $states } }
    }
    first: $first
    after: $after
    orderBy: priority
  ) {
    nodes {
      id
      identifier
      title
      description
      priority
      branchName
      url
      createdAt
      updatedAt
      state { name }
      labels { nodes { name } }
      relations(filter: { type: { eq: "blocks" } }) {
        nodes {
          relatedIssue {
            id
            identifier
            state { name }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

_REFRESH_QUERY = """
query SymphonyRefresh($ids: [ID!]!) {
  issues(filter: { id: { in: $ids } }) {
    nodes {
      id
      identifier
      state { name }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def _normalise_issue(node: dict[str, Any]) -> Issue:
    state_name: str = (node.get("state") or {}).get("name", "")
    labels: list[str] = [
        n["name"].lower()
        for n in ((node.get("labels") or {}).get("nodes") or [])
        if n.get("name")
    ]
    blocked_by: list[BlockerRef] = []
    for rel in ((node.get("relations") or {}).get("nodes") or []):
        ri = rel.get("relatedIssue") or {}
        if ri.get("id"):
            blocked_by.append(
                BlockerRef(
                    id=ri["id"],
                    identifier=ri.get("identifier", ""),
                    state=(ri.get("state") or {}).get("name", ""),
                )
            )
    return Issue(
        id=node["id"],
        identifier=node.get("identifier", ""),
        title=node.get("title", ""),
        description=node.get("description"),
        priority=node.get("priority"),
        state=state_name,
        branch_name=node.get("branchName"),
        url=node.get("url"),
        labels=labels,
        blocked_by=blocked_by,
        created_at=node.get("createdAt"),
        updated_at=node.get("updatedAt"),
    )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class LinearClient:
    """Async HTTP client for the Linear GraphQL API."""

    def __init__(self, config: TrackerConfig) -> None:
        self._config = config
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        headers = {
            "Content-Type": "application/json",
            "Authorization": self._config.api_key,
        }
        self._session = aiohttp.ClientSession(headers=headers)

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> "LinearClient":
        await self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _gql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if self._session is None:
            raise RuntimeError("LinearClient is not open")
        payload = {"query": query, "variables": variables}
        async with self._session.post(
            self._config.endpoint,
            json=payload,
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise TrackerError(
                    f"Linear API returned HTTP {resp.status}: {text[:200]}"
                )
            data = await resp.json()
        errors = data.get("errors")
        if errors:
            msgs = "; ".join(e.get("message", str(e)) for e in errors)
            raise TrackerError(f"Linear GraphQL errors: {msgs}")
        if "data" not in data:
            raise TrackerError(f"Malformed Linear response: {data!r}")
        return data["data"]  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_candidates(self) -> list[Issue]:
        """Return all issues in *active_states* for the configured project."""
        cfg = self._config
        issues: list[Issue] = []
        after: Optional[str] = None

        while True:
            variables: dict[str, Any] = {
                "projectSlug": cfg.project_slug,
                "states": cfg.active_states,
                "first": _PAGE_SIZE,
                "after": after,
            }
            try:
                data = await self._gql(_CANDIDATES_QUERY, variables)
            except TrackerError:
                raise
            except Exception as exc:
                raise TrackerError(f"Transport error fetching candidates: {exc}") from exc

            result = data.get("issues") or {}
            nodes = result.get("nodes") or []
            for node in nodes:
                try:
                    issues.append(_normalise_issue(node))
                except (KeyError, TypeError) as exc:
                    log.warning("Skipping malformed issue node: %s – %s", node, exc)

            page_info = result.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
            if not after:
                break

        log.debug("Fetched %d candidate issues from Linear", len(issues))
        return issues

    async def refresh_states(self, issue_ids: list[str]) -> dict[str, str]:
        """Return a mapping of issue_id -> current state name for *issue_ids*.

        Issues not found in the response are omitted from the result.
        """
        if not issue_ids:
            return {}
        try:
            data = await self._gql(_REFRESH_QUERY, {"ids": issue_ids})
        except TrackerError:
            raise
        except Exception as exc:
            raise TrackerError(f"Transport error refreshing issue states: {exc}") from exc

        result: dict[str, str] = {}
        for node in (data.get("issues") or {}).get("nodes") or []:
            iid: str = node.get("id", "")
            state_name: str = (node.get("state") or {}).get("name", "")
            if iid:
                result[iid] = state_name
        return result


class TrackerError(Exception):
    """Raised for Linear API transport, HTTP, or GraphQL errors."""
