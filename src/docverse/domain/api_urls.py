"""Pure builders for Docverse API resource URLs.

The arq worker has no FastAPI ``request`` to call ``url_for`` on, so it
cannot mint HATEOAS links the way request handlers do. These functions
compose absolute resource URLs from a base API URL — resolved via
Repertoire ``DiscoveryClient.url_for_internal("docverse")`` — plus the
resource identifiers.

The path templates mirror the live FastAPI routes ``get_edition`` /
``get_build`` / ``get_queue_job`` minus the application path prefix
(``config.path_prefix``), which the Repertoire-supplied base already
carries. ``tests/domain/api_urls_test.py`` pins these templates to those
routes so a route rename fails the test rather than silently shipping a
stale link.
"""

from __future__ import annotations

__all__ = ["build_url", "edition_url", "queue_job_url"]


def edition_url(base: str, *, org: str, project: str, edition: str) -> str:
    """Compose the URL of an edition resource.

    Parameters
    ----------
    base
        Base Docverse API URL (already including any path prefix).
    org
        Organization slug.
    project
        Project slug.
    edition
        Edition slug.
    """
    return (
        f"{base.rstrip('/')}/orgs/{org}/projects/{project}/editions/{edition}"
    )


def build_url(base: str, *, org: str, project: str, build: str) -> str:
    """Compose the URL of a build resource.

    Provided alongside :func:`edition_url` / :func:`queue_job_url` for
    symmetry and future request-less (worker) use. The request-time
    ``QueueJob.build_url`` is minted via ``request.url_for``, so this builder
    currently has no production caller — only its drift-guard test pins it to
    the live ``get_build`` route.

    Parameters
    ----------
    base
        Base Docverse API URL (already including any path prefix).
    org
        Organization slug.
    project
        Project slug.
    build
        Build public identifier.
    """
    return f"{base.rstrip('/')}/orgs/{org}/projects/{project}/builds/{build}"


def queue_job_url(base: str, *, job: str) -> str:
    """Compose the URL of a queue-job resource.

    Parameters
    ----------
    base
        Base Docverse API URL (already including any path prefix).
    job
        Queue-job public identifier.
    """
    return f"{base.rstrip('/')}/queue/jobs/{job}"
