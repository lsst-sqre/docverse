"""Helpers for computing the published URL of projects and editions."""

from __future__ import annotations

from docverse.models import UrlScheme
from docverse.models.organizations import normalize_base_domain

from .dashboard_context import MAIN_SLUG
from .edition import Edition
from .organization import Organization
from .project import Project

__all__ = [
    "compute_project_hostname",
    "edition_published_url",
    "project_published_url",
]


def compute_project_hostname(org: Organization, project_slug: str) -> str:
    """Hostname that serves a project's documentation.

    This is the hostname the Cloudflare Worker answers on for the
    project, and therefore the scope of a hostname-wide CDN cache purge.

    Parameters
    ----------
    org
        Organization owning the project.
    project_slug
        Slug of the project.

    Returns
    -------
    str
        ``{project_slug}.{base_domain}`` for ``subdomain`` orgs, or the
        normalized ``base_domain`` for ``path_prefix`` orgs (where every
        project shares one hostname). Never includes a scheme or path.

    Notes
    -----
    ``slug_rewrite_rules`` are deliberately not involved: those rules
    derive *edition* slugs from git refs, not project hostnames.
    """
    base_domain = normalize_base_domain(org.base_domain)
    if org.url_scheme == UrlScheme.subdomain:
        return f"{project_slug}.{base_domain}"
    return base_domain


def project_published_url(org: Organization, project: Project) -> str:
    """Best-effort publishing root for a project.

    Subdomain orgs serve each project under ``project.base_domain``;
    path-prefix orgs serve under ``base_domain + root_path_prefix +
    project_slug``. Always returns a trailing slash.
    """
    hostname = compute_project_hostname(org, project.slug)
    if org.url_scheme == UrlScheme.subdomain:
        return f"https://{hostname}/"
    prefix = org.root_path_prefix or "/"
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"
    if not prefix.startswith("/"):
        prefix = f"/{prefix}"
    return f"https://{hostname}{prefix}{project.slug}/"


def edition_published_url(project_url: str, edition: Edition) -> str:
    """Public URL for one edition under its project's URL space.

    Callers must pass the canonical ``Edition`` row loaded from the
    database, never an edition synthesized from a request-supplied
    slug. The URL is built from ``edition.slug``, which is the
    creation-time casing the store is case-preserving about.
    """
    if edition.slug == MAIN_SLUG:
        return project_url
    return f"{project_url}v/{edition.slug}/"
