"""Helpers for computing the published URL of projects and editions."""

from __future__ import annotations

from docverse.client.models import UrlScheme
from docverse.client.models.organizations import normalize_base_domain

from .dashboard_context import MAIN_SLUG
from .edition import Edition
from .organization import Organization
from .project import Project

__all__ = ["edition_published_url", "project_published_url"]


def project_published_url(org: Organization, project: Project) -> str:
    """Best-effort publishing root for a project.

    Subdomain orgs serve each project under ``project.base_domain``;
    path-prefix orgs serve under ``base_domain + root_path_prefix +
    project_slug``. Always returns a trailing slash.
    """
    base_domain = normalize_base_domain(org.base_domain)
    if org.url_scheme == UrlScheme.subdomain:
        return f"https://{project.slug}.{base_domain}/"
    prefix = org.root_path_prefix or "/"
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    return f"https://{base_domain}{prefix}{project.slug}/"


def edition_published_url(project_url: str, edition: Edition) -> str:
    """Public URL for one edition under its project's URL space."""
    if edition.slug == MAIN_SLUG:
        return project_url
    return f"{project_url}v/{edition.slug}/"
