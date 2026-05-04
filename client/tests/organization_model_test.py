"""Tests for organization client models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from docverse.client.models import OrganizationCreate, OrganizationUpdate
from docverse.client.models.organizations import normalize_base_domain


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
