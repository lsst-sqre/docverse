"""Tests for the published-URL and hostname helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from docverse.models import UrlScheme
from docverse_server.domain.organization import Organization
from docverse_server.domain.published_url import compute_project_hostname


def _org(
    *,
    base_domain: str = "example.org",
    url_scheme: UrlScheme = UrlScheme.subdomain,
    root_path_prefix: str = "/",
) -> Organization:
    now = datetime.now(tz=UTC)
    return Organization(
        id=1,
        slug="test-org",
        title="Test Org",
        base_domain=base_domain,
        url_scheme=url_scheme,
        root_path_prefix=root_path_prefix,
        slug_rewrite_rules=None,
        lifecycle_rules=None,
        purgatory_retention=timedelta(days=30),
        date_created=now,
        date_updated=now,
    )


def test_compute_project_hostname_subdomain_scheme() -> None:
    """Subdomain orgs serve each project on its own hostname."""
    org = _org(url_scheme=UrlScheme.subdomain)
    assert compute_project_hostname(org, "my-project") == (
        "my-project.example.org"
    )


def test_compute_project_hostname_path_prefix_scheme() -> None:
    """Path-prefix orgs serve every project on the base domain."""
    org = _org(url_scheme=UrlScheme.path_prefix, root_path_prefix="/docs/")
    assert compute_project_hostname(org, "my-project") == "example.org"


def test_compute_project_hostname_normalizes_base_domain() -> None:
    """A base domain carrying a scheme or trailing slash is normalized."""
    org = _org(base_domain="https://example.org/")
    assert compute_project_hostname(org, "my-project") == (
        "my-project.example.org"
    )
