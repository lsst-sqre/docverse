"""Handler-level response models for queue endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from docverse.client.models import BuildProcessingProgress
from docverse.client.models import QueueJob as _QueueJobBase
from starlette.requests import Request

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
    ) -> Self:
        """Create from a domain object, adding the HATEOAS URLs.

        ``build_url`` / ``edition_url`` / ``subject_url`` are resolved at
        request time from the job's stored identifiers via the factory
        stores; each is ``None`` when the job targets no such resource or
        it could not be resolved (best-effort back-reference). Must be
        called inside an open session — it issues store reads.
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
            domain, request, factory
        )
        return cls(
            self_url=str(request.url_for("get_queue_job", job=job_id_str)),
            id=job_id_str,
            kind=domain.kind,
            status=domain.status,
            keeper_sync_run_id=domain.keeper_sync_run_id,
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


async def _resolve_subject_urls(
    domain: QueueJobDomain,
    request: Request,
    factory: Factory,
) -> tuple[str | None, str | None, str | None]:
    """Resolve ``(build_url, edition_url, subject_url)`` for a queue job.

    Returns ``(None, None, None)`` immediately for jobs that target no
    build or edition (e.g. keeper-sync run/project jobs), so list
    endpoints incur no extra queries for them. Otherwise the org/project
    slugs and the build public-id / edition slug are resolved via the
    factory stores; any identifier that cannot be resolved degrades its
    URL to ``None``.
    """
    if domain.build_id is None and domain.edition_id is None:
        return None, None, None
    if domain.project_id is None:
        return None, None, None

    org = await factory.create_org_store().get_by_id(domain.org_id)
    project = await factory.create_project_store().get_by_id(domain.project_id)
    if org is None or project is None:
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
                    org=org.slug,
                    project=project.slug,
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
                    org=org.slug,
                    project=project.slug,
                    edition=edition.slug,
                )
            )

    # The "subject" is the resource the job primarily processes: the
    # edition for publish jobs, otherwise the build for build_processing.
    subject_url = edition_url or build_url
    return build_url, edition_url, subject_url
