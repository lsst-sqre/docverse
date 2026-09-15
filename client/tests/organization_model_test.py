"""Tests for organization client models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.models import (
    LifecycleRuleSet,
    Organization,
    OrganizationCreate,
    OrganizationSummary,
    OrganizationUpdate,
    OrgRole,
)
from docverse.models._examples import EXAMPLE_ORG_ID
from docverse.models.organizations import normalize_base_domain


def test_organization_summary_round_trip() -> None:
    """OrganizationSummary parses the listing entry shape."""
    summary = OrganizationSummary.model_validate(
        {
            "self_url": "https://example.com/docverse/orgs/lsst",
            "id": EXAMPLE_ORG_ID,
            "slug": "lsst",
            "title": "Rubin Observatory",
            "role": "reader",
        }
    )
    assert summary.id == EXAMPLE_ORG_ID
    assert summary.slug == "lsst"
    assert summary.title == "Rubin Observatory"
    assert summary.role is OrgRole.reader
    assert summary.self_url.endswith("/orgs/lsst")
    assert summary.model_dump(mode="json")["id"] == EXAMPLE_ORG_ID


def test_organization_round_trips_public_id() -> None:
    """The full Organization resource carries the Base32 ``id``."""
    org = Organization.model_validate(
        {
            "self_url": "https://example.com/docverse/orgs/lsst",
            "dashboard_template_url": (
                "https://example.com/docverse/orgs/lsst/dashboard-template"
            ),
            "keeper_sync_url": (
                "https://example.com/docverse/orgs/lsst/keeper-sync"
            ),
            "id": EXAMPLE_ORG_ID,
            "slug": "lsst",
            "title": "Rubin Observatory",
            "base_domain": "lsst.io",
            "url_scheme": "subdomain",
            "root_path_prefix": "/",
            "slug_rewrite_rules": None,
            "lifecycle_rules": None,
            "purgatory_retention": 2592000,
            "date_created": "2026-05-01T12:00:00Z",
            "date_updated": "2026-05-01T12:00:00Z",
        }
    )
    assert org.id == EXAMPLE_ORG_ID
    assert org.model_dump(mode="json")["id"] == EXAMPLE_ORG_ID


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("lsst.io", "lsst.io"),
        ("https://lsst.io", "lsst.io"),
        ("http://lsst.io", "lsst.io"),
        ("https://lsst.io/", "lsst.io"),
        ("http://lsst.io/", "lsst.io"),
        ("lsst.io/", "lsst.io"),
        (
            "https://docverse-dev.jsickcodes.workers.dev",
            "docverse-dev.jsickcodes.workers.dev",
        ),
    ],
)
def test_normalize_base_domain_accepts_and_normalizes(
    raw: str, expected: str
) -> None:
    assert normalize_base_domain(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "https://",
        "https:///",
        "lsst.io/path",
        "https://lsst.io/path",
        "lsst .io",
        "lsst\tio",
    ],
)
def test_normalize_base_domain_rejects_invalid(raw: str) -> None:
    with pytest.raises(ValueError, match="base_domain"):
        normalize_base_domain(raw)


def test_organization_create_normalizes_base_domain() -> None:
    payload = OrganizationCreate(
        slug="lsst",
        title="Rubin Observatory",
        base_domain="https://lsst.io/",
    )
    assert payload.base_domain == "lsst.io"


def test_organization_create_rejects_path_segments() -> None:
    with pytest.raises(ValidationError):
        OrganizationCreate(
            slug="lsst",
            title="Rubin Observatory",
            base_domain="https://lsst.io/docs",
        )


def test_organization_update_normalizes_base_domain() -> None:
    payload = OrganizationUpdate(base_domain="http://lsst.io/")
    assert payload.base_domain == "lsst.io"


def test_organization_update_allows_none_base_domain() -> None:
    payload = OrganizationUpdate()
    assert payload.base_domain is None


def test_organization_update_rejects_whitespace() -> None:
    with pytest.raises(ValidationError):
        OrganizationUpdate(base_domain="lsst .io")


def test_organization_create_accepts_typed_lifecycle_rules() -> None:
    payload = OrganizationCreate(
        slug="lsst",
        title="Rubin Observatory",
        base_domain="lsst.io",
        lifecycle_rules=[  # type: ignore[arg-type]
            {"type": "draft_inactivity", "max_days_inactive": 30},
            {"type": "ref_deleted"},
        ],
    )
    assert isinstance(payload.lifecycle_rules, LifecycleRuleSet)
    assert len(payload.lifecycle_rules.root) == 2


def test_organization_create_rejects_unknown_lifecycle_rule_type() -> None:
    with pytest.raises(ValidationError):
        OrganizationCreate(
            slug="lsst",
            title="Rubin Observatory",
            base_domain="lsst.io",
            lifecycle_rules=[  # type: ignore[arg-type]
                {"type": "purgatory_eviction", "enabled": True},
            ],
        )


def test_organization_update_rejects_duplicate_lifecycle_rule_types() -> None:
    with pytest.raises(ValidationError):
        OrganizationUpdate(
            lifecycle_rules=[  # type: ignore[arg-type]
                {"type": "draft_inactivity", "max_days_inactive": 30},
                {"type": "draft_inactivity", "max_days_inactive": 60},
            ],
        )


def test_organization_update_accepts_none_lifecycle_rules() -> None:
    payload = OrganizationUpdate(lifecycle_rules=None)
    assert payload.lifecycle_rules is None
