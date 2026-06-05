"""Handler-level response models for queue endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import BuildProcessingProgress
from docverse.client.models import QueueJob as _QueueJobBase
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import QueueJob as QueueJobDomain


class QueueJob(_QueueJobBase):
    """Queue job response model with HATEOAS self_url."""

    @classmethod
    def from_domain(cls, domain: QueueJobDomain, request: Request) -> Self:
        """Create from a domain object, adding the self_url."""
        job_id_str = serialize_base32_id(domain.public_id)
        # JSONB progress is stored untyped; validate it into the typed
        # model here. Non-build kinds round-trip via ``extra="allow"``.
        progress = (
            BuildProcessingProgress.model_validate(domain.progress)
            if domain.progress is not None
            else None
        )
        return cls(
            self_url=str(request.url_for("get_queue_job", job=job_id_str)),
            id=job_id_str,
            kind=domain.kind,
            status=domain.status,
            keeper_sync_run_id=domain.keeper_sync_run_id,
            subject_label=domain.subject_label,
            phase=domain.phase,
            progress=progress,
            errors=domain.errors,
            date_created=domain.date_created,
            date_started=domain.date_started,
            date_completed=domain.date_completed,
        )
