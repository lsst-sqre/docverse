"""Handler-level response models for queue endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import QueueJob as _QueueJobBase
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.queue import QueueJob as QueueJobDomain


class QueueJob(_QueueJobBase):
    """Queue job response model with HATEOAS self_url."""

    @classmethod
    def from_domain(cls, domain: QueueJobDomain, request: Request) -> Self:
        """Create from a domain object, adding the self_url."""
        job_id_str = serialize_base32_id(domain.public_id)
        return cls(
            self_url=str(request.url_for("get_queue_job", job=job_id_str)),
            id=domain.public_id,
            kind=domain.kind,
            status=domain.status,
            phase=domain.phase,
            progress=domain.progress,
            errors=domain.errors,
            date_created=domain.date_created,
            date_started=domain.date_started,
            date_completed=domain.date_completed,
        )
