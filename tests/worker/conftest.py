"""Shared fixtures and helpers for ``tests/worker``.

The ``make_worker_ctx`` helper here matches the dependency wiring
``docverse.worker.main.startup`` performs in production: it builds a
``WorkerFactoryBuilder`` capturing the supplied process-lifetime deps
(or sensible test defaults) and returns a ctx dict with the same shape
each worker function expects (``factory_builder`` plus the ``http_client``
and ``arq_queue`` entries that the production ``shutdown`` would close).
"""

from __future__ import annotations

from typing import Any

import httpx
from cryptography.fernet import Fernet
from pydantic import SecretStr
from rubin.repertoire import DiscoveryClient
from safir.arq import MockArqQueue

from docverse.config import Configuration
from docverse.metrics.events import DocverseEvents
from docverse.services.credential_encryptor import CredentialEncryptor
from docverse.worker.main import WorkerFactoryBuilder

__all__ = ["make_worker_ctx"]


_config = Configuration()


def make_worker_ctx(
    *,
    http_client: httpx.AsyncClient,
    arq_queue: MockArqQueue | None = None,
    job_id: str | None = None,
    encryptor: CredentialEncryptor | None = None,
    discovery: DiscoveryClient | None = None,
    github_app_id: int | None = None,
    github_app_private_key: SecretStr | None = None,
    github_webhook_secret: SecretStr | None = None,
    events: DocverseEvents | None = None,
) -> dict[str, Any]:
    """Build a worker ctx dict that mirrors ``worker.main.startup``.

    Defaults are filled in for every dep so individual tests only need
    to override the ones that drive their assertion (e.g.,
    ``arq_queue=mock_arq`` to read enqueued jobs back, or the GitHub-App
    secrets when exercising ``dashboard_sync``). Pass ``events`` (an
    initialized :class:`~docverse.metrics.events.DocverseEvents`) when a
    test asserts on published metrics; production's ``_startup`` always
    sets ``ctx["events"]``, but tests that do not care about metrics may
    leave it unset and the emitting worker simply skips publication.
    """
    if encryptor is None:
        encryptor = CredentialEncryptor(
            current_key=Fernet.generate_key().decode()
        )
    if discovery is None:
        discovery = DiscoveryClient(http_client)
    if arq_queue is None:
        arq_queue = MockArqQueue(default_queue_name=_config.arq_queue_name)
    builder = WorkerFactoryBuilder(
        encryptor=encryptor,
        http_client=http_client,
        arq_queue=arq_queue,
        discovery=discovery,
        github_app_id=github_app_id,
        github_app_private_key=github_app_private_key,
        github_webhook_secret=github_webhook_secret,
        default_queue_name=_config.arq_queue_name,
    )
    ctx: dict[str, Any] = {
        "factory_builder": builder,
        "http_client": http_client,
        "arq_queue": arq_queue,
    }
    if job_id is not None:
        ctx["job_id"] = job_id
    if events is not None:
        ctx["events"] = events
    return ctx
