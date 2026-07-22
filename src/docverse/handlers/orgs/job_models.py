"""Handler-level response models for org-scoped job endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from starlette.requests import Request

from docverse.client.models import BuildProcessingProgress
from docverse.client.models import QueueJob as _QueueJobBase
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import QueueJob as QueueJobDomain

if TYPE_CHECKING:
    from docverse.factory import Factory


class QueueJob(_QueueJobBase):
    """Queue job response model with HATEOAS self and subject links."""

    @classmethod
    async def from_domain(
        cls,
        domain: QueueJobDomain,
        request: Request,
        factory: Factory,
        *,
        org_slug: str,
        run_public_id_cache: dict[int, str | None] | None = None,
        project_slug_cache: dict[int, str | None] | None = None,
    ) -> Self:
        """Create from a domain object, adding the HATEOAS URLs.

        ``self_url`` is the job's canonical org-scoped URL
        (``/orgs/{org}/jobs/{id}``). ``org_slug`` supplies the org segment
        and is required: every caller is org-scoped and already holds the
        slug it resolved in ``require_reader``.

        ``build_url`` / ``edition_url`` / ``subject_url`` are resolved at
        request time from the job's stored identifiers via the factory
        stores; each is ``None`` when the job targets no such resource or
        it could not be resolved (best-effort back-reference). Must be
        called inside an open session — it issues store reads.

        ``run_public_id_cache`` is an optional caller-owned dict, keyed by the
        integer keeper-sync run FK, used to memoize run public-id lookups
        across a page of jobs. List endpoints where many jobs share a run
        (e.g. the run-scoped jobs listing) pass one in to avoid an N+1
        run-store query per job; single-job callers omit it.

        ``project_slug_cache`` is an analogous optional caller-owned dict,
        keyed by the integer project FK, memoizing project-slug lookups
        across a page of jobs so the subject-URL resolution does not issue a
        per-job project-store query; single-job callers omit it.
        """
        job_id_str = serialize_base32_id(domain.public_id)
        # JSONB progress is stored untyped; validate it into the typed
        # model here. Non-build kinds round-trip via ``extra="allow"``.
        progress = (
            BuildProcessingProgress.model_validate(domain.progress)
            if domain.progress is not None
            else None
        )
        build_url, edition_url, subject_url = await _resolve_subject_urls(
            domain,
            request,
            factory,
            org_slug=org_slug,
            project_slug_cache=project_slug_cache,
        )
        keeper_sync_run_id = await _resolve_keeper_sync_run_public_id(
            domain, factory, cache=run_public_id_cache
        )
        return cls(
            self_url=str(
                request.url_for("get_org_job", org=org_slug, job=job_id_str)
            ),
            id=job_id_str,
            kind=domain.kind,
            status=domain.status,
            keeper_sync_run_id=keeper_sync_run_id,
            subject_label=domain.subject_label,
            subject_url=subject_url,
            build_url=build_url,
            edition_url=edition_url,
            phase=domain.phase,
            progress=progress,
            errors=domain.errors,
            date_created=domain.date_created,
            date_started=domain.date_started,
            date_completed=domain.date_completed,
        )


async def _resolve_keeper_sync_run_public_id(
    domain: QueueJobDomain,
    factory: Factory,
    *,
    cache: dict[int, str | None] | None = None,
) -> str | None:
    """Resolve a job's keeper-sync run FK to the run's Base32 public id.

    Returns ``None`` for jobs not attributed to a run, or when the
    attributed run row cannot be resolved (best-effort back-reference).
    The raw integer FK is never surfaced in the API.

    When ``cache`` is supplied it memoizes the FK-to-public-id resolution:
    the run store is queried at most once per distinct run across a page of
    jobs, collapsing the otherwise N+1 lookup on the run-scoped jobs listing.
    """
    run_fk = domain.keeper_sync_run_id
    if run_fk is None:
        return None
    if cache is not None and run_fk in cache:
        return cache[run_fk]
    run = await factory.create_keeper_sync_run_store().get(run_fk)
    resolved = serialize_base32_id(run.public_id) if run is not None else None
    if cache is not None:
        cache[run_fk] = resolved
    return resolved


async def _resolve_project_slug(
    project_id: int,
    factory: Factory,
    *,
    cache: dict[int, str | None] | None = None,
) -> str | None:
    """Resolve a project's integer FK to its slug.

    Returns ``None`` when the project row cannot be resolved (best-effort
    back-reference).

    When ``cache`` is supplied it memoizes the FK-to-slug resolution: the
    project store is queried at most once per distinct project across a page
    of jobs, collapsing the otherwise N+1 lookup on the jobs listing.
    """
    if cache is not None and project_id in cache:
        return cache[project_id]
    project = await factory.create_project_store().get_by_id(project_id)
    resolved = project.slug if project is not None else None
    if cache is not None:
        cache[project_id] = resolved
    return resolved


async def _resolve_subject_urls(
    domain: QueueJobDomain,
    request: Request,
    factory: Factory,
    *,
    org_slug: str,
    project_slug_cache: dict[int, str | None] | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve ``(build_url, edition_url, subject_url)`` for a queue job.

    Returns ``(None, None, None)`` immediately for jobs that target no
    build or edition (e.g. keeper-sync run/project jobs), so list
    endpoints incur no extra queries for them. ``org_slug`` is the org the
    job belongs to, already resolved by the caller, so no org-store query is
    issued here. The project slug and the build public-id / edition slug are
    resolved via the factory stores; any identifier that cannot be resolved
    degrades its URL to ``None``.

    When ``project_slug_cache`` is supplied it memoizes the project FK -> slug
    resolution across a page of jobs, so a listing where many jobs share a
    project issues at most one project-store query per distinct project
    rather than one per job.
    """
    if domain.build_id is None and domain.edition_id is None:
        return None, None, None
    if domain.project_id is None:
        return None, None, None

    project_slug = await _resolve_project_slug(
        domain.project_id, factory, cache=project_slug_cache
    )
    if project_slug is None:
        return None, None, None

    build_url: str | None = None
    if domain.build_id is not None:
        build = await factory.create_build_store().get_by_id(domain.build_id)
        # Exclude soft-deleted builds here, unlike the edition side which
        # filters date_deleted at the store (EditionStore.get_by_id):
        # BuildStore.get_by_id can't, because the build_processing /
        # publish_edition / dashboard workers rely on fetching soft-deleted
        # builds. A soft-deleted build 404s from get_build, so it must not
        # yield a build_url/subject_url.
        if build is not None and build.date_deleted is None:
            build_url = str(
                request.url_for(
                    "get_build",
                    org=org_slug,
                    project=project_slug,
                    build=serialize_base32_id(build.public_id),
                )
            )

    edition_url: str | None = None
    if domain.edition_id is not None:
        edition = await factory.create_edition_store().get_by_id(
            domain.edition_id
        )
        if edition is not None:
            edition_url = str(
                request.url_for(
                    "get_edition",
                    org=org_slug,
                    project=project_slug,
                    edition=edition.slug,
                )
            )

    # The "subject" is the resource the job primarily processes: the
    # edition for publish jobs, otherwise the build for build_processing.
    subject_url = edition_url or build_url
    return build_url, edition_url, subject_url
