"""Tests for dashboard rebuild client models."""

from __future__ import annotations

from docverse.client.models import (
    OrgDashboardRebuildEntry,
    OrgDashboardRebuildResponse,
)


def test_org_dashboard_rebuild_response_wraps_entries() -> None:
    """The org-wide rebuild response is an object envelope, not an array."""
    entry = OrgDashboardRebuildEntry(
        project_slug="alpha",
        job_id="abc123",
        job_url="https://example.com/orgs/o/jobs/abc123",
    )
    response = OrgDashboardRebuildResponse(entries=[entry])
    dumped = response.model_dump()
    assert dumped == {
        "entries": [
            {
                "project_slug": "alpha",
                "job_id": "abc123",
                "job_url": "https://example.com/orgs/o/jobs/abc123",
            }
        ]
    }


def test_org_dashboard_rebuild_response_defaults_to_empty_list() -> None:
    """An org with no eligible projects yields an empty ``entries`` list."""
    assert OrgDashboardRebuildResponse().model_dump() == {"entries": []}
