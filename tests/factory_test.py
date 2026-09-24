"""Unit tests for the service factories."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Self

import httpx
import pytest
import structlog
from cryptography.fernet import Fernet
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import CredentialProvider, OrganizationCreate
from docverse_server.factory import Factory
from docverse_server.services.credential_encryptor import CredentialEncryptor
from docverse_server.services.keeper_sync.copier import (
    DEFAULT_COPY_CONCURRENCY,
)
from docverse_server.services.keeper_sync.service import (
    DEFAULT_COPY_RETRY_DELAY_SECONDS,
)
from docverse_server.storage._http_retry import (
    DEFAULT_MAX_ATTEMPTS,
    MAX_BACKOFF_SECONDS,
)
from docverse_server.storage.ltd import LtdClient, LtdS3Source
from docverse_server.storage.objectstore import MockObjectStore
from docverse_server.storage.queue_backend import (
    ArqQueueBackend,
    NullQueueBackend,
)


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("docverse")  # type: ignore[no-any-return]


class _StubLtdSource:
    """Async-CM stand-in for ``LtdS3Source`` that never touches S3.

    Serves ``objects`` (key to body) when given, and an empty bucket
    otherwise. Counts its context entries and exits, and records every
    prefix it lists, so a test can tell which source a copier read from
    and whether the factory opened or closed it.
    """

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self._objects = objects or {}
        self.enters = 0
        self.exits = 0
        self.listed: list[str] = []

    async def __aenter__(self) -> Self:
        self.enters += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.exits += 1

    async def list_keys(self, *, prefix: str) -> list[str]:
        self.listed.append(prefix)
        return [key for key in self._objects if key.startswith(prefix)]

    async def download_object(self, *, key: str) -> bytes:
        return self._objects.get(key, b"")


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replace ``asyncio.sleep`` with a recorder and return the log."""
    delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    return delays


def _always_throttled(attempts: list[int]) -> httpx.MockTransport:
    """Answer every presigned PUT with a ``503`` asking for a 30 s wait.

    The ``Retry-After`` sits above the shared 10 s ceiling, so the
    recorded sleeps show which ceiling the store was built with and the
    attempt count shows which attempt budget.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(len(attempts) + 1)
        return httpx.Response(503, headers={"Retry-After": "30"})

    return httpx.MockTransport(handler)


async def _seed_minio_service(session: AsyncSession, factory: Factory) -> int:
    """Seed an org whose ``minio`` object-store service resolves for real.

    Returns the org id. ``create_objectstore_for_org`` then walks the
    genuine service-row and credential-decryption path, so a test sees
    the store the factory actually builds rather than a stand-in.
    """
    async with session.begin():
        org = await factory.create_org_store().create(
            OrganizationCreate(
                slug="budget-org",
                title="Budget Org",
                base_domain="budget.example.com",
            )
        )
        await factory.create_credential_service().create(
            org_slug="budget-org",
            label="minio-cred",
            provider=CredentialProvider.s3,
            credentials={
                "access_key_id": "key-id",
                "secret_access_key": "secret",
            },
        )
        await factory.create_service_store().create(
            organization_id=org.id,
            label="minio",
            category="object_storage",
            provider="minio",
            config={
                "endpoint_url": "https://minio.example.com",
                "bucket": "docs",
            },
            credential_label="minio-cred",
        )
    return org.id


@pytest.mark.asyncio
async def test_factory_without_arq_queue_uses_null_backend(
    db_session: AsyncSession,
) -> None:
    """Factory defaults to NullQueueBackend when no arq queue is given."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    assert isinstance(factory.create_queue_backend(), NullQueueBackend)


@pytest.mark.asyncio
async def test_factory_with_arq_queue_uses_arq_backend(
    db_session: AsyncSession,
) -> None:
    """Factory uses ArqQueueBackend when an arq queue is provided."""
    arq_queue = MockArqQueue(default_queue_name="docverse:queue")
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=arq_queue,
        default_queue_name="docverse:queue",
    )
    assert isinstance(factory.create_queue_backend(), ArqQueueBackend)


@pytest.mark.asyncio
async def test_factory_creates_ltd_client_when_http_client_set(
    db_session: AsyncSession,
) -> None:
    """LtdClient construction needs the shared httpx.AsyncClient."""
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            default_queue_name="docverse:queue",
        )
        client = factory.create_ltd_client()
        assert isinstance(client, LtdClient)


@pytest.mark.asyncio
async def test_factory_create_ltd_client_without_http_raises(
    db_session: AsyncSession,
) -> None:
    """No HTTP client -> the LTD-side accessor must error early."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    with pytest.raises(RuntimeError, match="HTTP client is required"):
        factory.create_ltd_client()


@pytest.mark.asyncio
async def test_factory_create_ltd_s3_source_returns_unopened(
    db_session: AsyncSession,
) -> None:
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    source = factory.create_ltd_s3_source()
    assert isinstance(source, LtdS3Source)


@pytest.mark.asyncio
async def test_copier_reuses_the_shared_ltd_source(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every copier reads LTD through the factory's shared, open source.

    The sync worker opens one ``LtdS3Source`` per process so LTD
    downloads reuse its connections across builds and manifest hashes.
    A copier that opened a source of its own, or closed the shared one
    on exit, would re-dial S3 per build and leave each closed connection
    pinning a Cloud NAT port through TIME_WAIT (PRD #698).
    """
    shared = _StubLtdSource({"proj/builds/1/index.html": b"<html></html>"})
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
        ltd_s3_source=shared,
    )

    async def _fake_objectstore(
        *, org_id: int, service_label: str, **options: object
    ) -> MockObjectStore:
        return MockObjectStore()

    def _own_source(**kwargs: object) -> _StubLtdSource:
        msg = "copier built its own LTD source despite a shared one"
        raise AssertionError(msg)

    monkeypatch.setattr(
        factory, "create_objectstore_for_org", _fake_objectstore
    )
    monkeypatch.setattr(factory, "create_ltd_s3_source", _own_source)

    for _ in range(2):
        async with factory.create_build_content_copier_for_org(
            org_id=1, service_label="r2"
        ) as copier:
            await copier.compute_manifest_hash(source_prefix="proj/builds/1/")

    assert factory.ltd_s3_source is shared
    assert shared.listed == ["proj/builds/1/", "proj/builds/1/"]
    assert shared.enters == 0
    assert shared.exits == 0


@pytest.mark.asyncio
async def test_copier_opens_its_own_source_when_none_shared(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a shared source each copier opens and closes its own.

    Directly constructed factories (tests, scripts) hold no shared
    source, so every copier keeps today's lifetime: a fresh source from
    ``create_ltd_s3_source``, opened on entry and closed on exit.
    """
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )
    created: list[_StubLtdSource] = []

    async def _fake_objectstore(
        *, org_id: int, service_label: str, **options: object
    ) -> MockObjectStore:
        return MockObjectStore()

    def _create_ltd_s3_source(**kwargs: object) -> _StubLtdSource:
        source = _StubLtdSource()
        created.append(source)
        return source

    monkeypatch.setattr(
        factory, "create_objectstore_for_org", _fake_objectstore
    )
    monkeypatch.setattr(factory, "create_ltd_s3_source", _create_ltd_s3_source)

    for _ in range(2):
        async with factory.create_build_content_copier_for_org(
            org_id=1, service_label="r2"
        ) as copier:
            await copier.compute_manifest_hash(source_prefix="proj/builds/1/")
            assert created[-1].enters == 1
            assert created[-1].exits == 0

    assert factory.ltd_s3_source is None
    assert len(created) == 2
    assert created[0] is not created[1]
    for source in created:
        assert source.listed == ["proj/builds/1/"]
        assert source.enters == 1
        assert source.exits == 1


@pytest.mark.asyncio
async def test_copier_uses_factory_copy_concurrency(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The configured fan-out bound reaches ``BuildContentCopier``.

    The bound used to be a literal ``8`` in ``factory.py`` duplicating
    the copier's own default, so no operator knob could move the sync
    worker's real memory ceiling (#517).
    """
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
        keeper_sync_copy_concurrency=3,
    )

    async def _fake_objectstore(
        *, org_id: int, service_label: str, **options: object
    ) -> MockObjectStore:
        return MockObjectStore()

    monkeypatch.setattr(
        factory, "create_objectstore_for_org", _fake_objectstore
    )
    monkeypatch.setattr(
        factory, "create_ltd_s3_source", lambda **kwargs: _StubLtdSource()
    )

    async with factory.create_build_content_copier_for_org(
        org_id=1, service_label="r2"
    ) as copier:
        assert copier.max_concurrent == 3


@pytest.mark.asyncio
async def test_keeper_sync_service_uses_factory_copy_retry_delay(
    db_session: AsyncSession,
) -> None:
    """The configured build-copy retry delay reaches the sync service.

    ``sync_build`` waits this long before re-running a copy that failed
    on a transport error (PRD #685), so the operator knob has to arrive
    on the service the keeper-sync worker actually runs.
    """
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            default_queue_name="docverse:queue",
            keeper_sync_copy_retry_delay_seconds=4.5,
        )
        service = factory.create_keeper_sync_service(
            org_id=1, service_label="r2"
        )
    assert service.copy_retry_delay_seconds == 4.5


@pytest.mark.asyncio
async def test_keeper_sync_service_copy_retry_delay_defaults(
    db_session: AsyncSession,
) -> None:
    """A Factory built without the knob keeps the service's own default."""
    async with httpx.AsyncClient() as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            http_client=http_client,
            default_queue_name="docverse:queue",
        )
        service = factory.create_keeper_sync_service(
            org_id=1, service_label="r2"
        )
    assert service.copy_retry_delay_seconds == (
        DEFAULT_COPY_RETRY_DELAY_SECONDS
    )


@pytest.mark.asyncio
async def test_copier_concurrency_defaults_to_copier_fallback(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Factory built without the knob keeps the copier's own default."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        default_queue_name="docverse:queue",
    )

    async def _fake_objectstore(
        *, org_id: int, service_label: str, **options: object
    ) -> MockObjectStore:
        return MockObjectStore()

    monkeypatch.setattr(
        factory, "create_objectstore_for_org", _fake_objectstore
    )
    monkeypatch.setattr(
        factory, "create_ltd_s3_source", lambda **kwargs: _StubLtdSource()
    )

    async with factory.create_build_content_copier_for_org(
        org_id=1, service_label="r2"
    ) as copier:
        assert copier.max_concurrent == DEFAULT_COPY_CONCURRENCY


@pytest.mark.asyncio
async def test_copier_destination_uses_keeper_sync_upload_budget(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The keeper-sync copier's uploads spend the configured budget.

    Build-content copies are what an R2 connect outage fails, so the
    worker hands the copier's destination store a larger attempt budget
    and ceiling than the shared defaults (PRD #685). The copy here keeps
    getting ``503 Retry-After: 30``: three attempts with two 25 s sleeps
    between them proves both knobs reached the store the copier uploads
    through.
    """
    delays = _record_sleeps(monkeypatch)
    attempts: list[int] = []
    async with httpx.AsyncClient(
        transport=_always_throttled(attempts)
    ) as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            credential_encryptor=CredentialEncryptor(
                current_key=Fernet.generate_key().decode()
            ),
            http_client=http_client,
            default_queue_name="docverse:queue",
            keeper_sync_upload_max_attempts=3,
            keeper_sync_upload_max_backoff_seconds=25.0,
        )
        org_id = await _seed_minio_service(db_session, factory)
        monkeypatch.setattr(
            factory,
            "create_ltd_s3_source",
            lambda **kwargs: _StubLtdSource(
                {"proj/builds/1/index.html": b"<html></html>"}
            ),
        )

        async with factory.create_build_content_copier_for_org(
            org_id=org_id, service_label="minio"
        ) as copier:
            with pytest.raises(httpx.HTTPStatusError):
                await copier.copy_build(
                    source_prefix="proj/builds/1/",
                    dest_prefix="proj/__builds/1/",
                )

    assert attempts == [1, 2, 3]
    assert delays == [25.0, 25.0]


@pytest.mark.asyncio
async def test_objectstore_for_org_keeps_the_shared_upload_budget(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every other store the factory builds keeps the shared budget.

    The API's build uploads, dashboard renders and build processing all
    call ``create_objectstore_for_org`` directly, so the keeper-sync
    budget — set on this factory too — must not leak into them.
    """
    delays = _record_sleeps(monkeypatch)
    attempts: list[int] = []
    async with httpx.AsyncClient(
        transport=_always_throttled(attempts)
    ) as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            credential_encryptor=CredentialEncryptor(
                current_key=Fernet.generate_key().decode()
            ),
            http_client=http_client,
            default_queue_name="docverse:queue",
            keeper_sync_upload_max_attempts=3,
            keeper_sync_upload_max_backoff_seconds=25.0,
        )
        org_id = await _seed_minio_service(db_session, factory)
        async with db_session.begin():
            store = await factory.create_objectstore_for_org(
                org_id=org_id, service_label="minio"
            )
        async with store:
            with pytest.raises(httpx.HTTPStatusError):
                await store.upload_object(
                    key="proj/__dashboard.html",
                    data=b"<html></html>",
                    content_type="text/html",
                )

    assert len(attempts) == DEFAULT_MAX_ATTEMPTS
    assert delays == [MAX_BACKOFF_SECONDS] * (DEFAULT_MAX_ATTEMPTS - 1)


def _recording_transport(puts: list[str]) -> httpx.MockTransport:
    """Accept every request, recording each PUT's URL path."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            puts.append(request.url.path)
        return httpx.Response(200)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_copier_destination_puts_over_the_copy_client(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build-content copies PUT over the worker's dedicated copy client.

    A copy burst used to share the worker's one client with discovery,
    LTD API, GitHub and Cloudflare calls, and ran that client's pool
    near its ceiling (PRD #685). With a copy client given, the copier's
    presigned PUTs go over it and none reach the shared client.
    """
    shared_puts: list[str] = []
    copy_puts: list[str] = []
    async with (
        httpx.AsyncClient(
            transport=_recording_transport(shared_puts)
        ) as http_client,
        httpx.AsyncClient(
            transport=_recording_transport(copy_puts)
        ) as copy_http_client,
    ):
        factory = Factory(
            session=db_session,
            logger=_logger(),
            credential_encryptor=CredentialEncryptor(
                current_key=Fernet.generate_key().decode()
            ),
            http_client=http_client,
            copy_http_client=copy_http_client,
            default_queue_name="docverse:queue",
        )
        org_id = await _seed_minio_service(db_session, factory)
        monkeypatch.setattr(
            factory,
            "create_ltd_s3_source",
            lambda **kwargs: _StubLtdSource(
                {"proj/builds/1/index.html": b"<html></html>"}
            ),
        )

        async with factory.create_build_content_copier_for_org(
            org_id=org_id, service_label="minio"
        ) as copier:
            await copier.copy_build(
                source_prefix="proj/builds/1/",
                dest_prefix="proj/__builds/1/",
            )

    assert copy_puts == ["/docs/proj/__builds/1/index.html"]
    assert shared_puts == []


@pytest.mark.asyncio
async def test_copier_destination_falls_back_to_the_shared_client(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a copy client, the copier PUTs over the shared client.

    Only the arq worker builds a dedicated copy client; every directly
    constructed factory (tests, scripts) keeps uploading as before.
    """
    shared_puts: list[str] = []
    async with httpx.AsyncClient(
        transport=_recording_transport(shared_puts)
    ) as http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            credential_encryptor=CredentialEncryptor(
                current_key=Fernet.generate_key().decode()
            ),
            http_client=http_client,
            default_queue_name="docverse:queue",
        )
        org_id = await _seed_minio_service(db_session, factory)
        monkeypatch.setattr(
            factory,
            "create_ltd_s3_source",
            lambda **kwargs: _StubLtdSource(
                {"proj/builds/1/index.html": b"<html></html>"}
            ),
        )

        async with factory.create_build_content_copier_for_org(
            org_id=org_id, service_label="minio"
        ) as copier:
            await copier.copy_build(
                source_prefix="proj/builds/1/",
                dest_prefix="proj/__builds/1/",
            )

    assert shared_puts == ["/docs/proj/__builds/1/index.html"]


@pytest.mark.asyncio
async def test_objectstore_for_org_keeps_the_shared_client(
    db_session: AsyncSession,
) -> None:
    """Every other store the factory builds uploads over the shared client.

    Dashboard renders and build processing call
    ``create_objectstore_for_org`` directly; the copy client is sized for
    keeper-sync copies alone, so it must not leak into them.
    """
    shared_puts: list[str] = []
    copy_puts: list[str] = []
    async with (
        httpx.AsyncClient(
            transport=_recording_transport(shared_puts)
        ) as http_client,
        httpx.AsyncClient(
            transport=_recording_transport(copy_puts)
        ) as copy_http_client,
    ):
        factory = Factory(
            session=db_session,
            logger=_logger(),
            credential_encryptor=CredentialEncryptor(
                current_key=Fernet.generate_key().decode()
            ),
            http_client=http_client,
            copy_http_client=copy_http_client,
            default_queue_name="docverse:queue",
        )
        org_id = await _seed_minio_service(db_session, factory)
        async with db_session.begin():
            store = await factory.create_objectstore_for_org(
                org_id=org_id, service_label="minio"
            )
        async with store:
            await store.upload_object(
                key="proj/__dashboard.html",
                data=b"<html></html>",
                content_type="text/html",
            )

    assert shared_puts == ["/docs/proj/__dashboard.html"]
    assert copy_puts == []


@pytest.mark.asyncio
async def test_copier_destination_uploads_under_the_upload_limiter(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build-content copies hold the worker's upload limiter per PUT.

    Each copier fans a build out over ``keeper_sync_copy_concurrency``
    objects and the sync pool runs ``keeper_sync_max_jobs`` copiers at
    once, so without one process-wide limiter a backfill had far more
    presigned PUTs (and TCP connects) in flight than the node's NAT
    ports could carry. A single-slot limiter must hold a three-object
    build to one PUT at a time even though the copier alone would run
    all three together. The handler holds each PUT open across a real
    ``await`` so PUTs the limiter did not hold back pile up in the peak.
    """
    puts: list[str] = []
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        if request.method == "PUT":
            puts.append(request.url.path)
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
        return httpx.Response(200)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    ) as copy_http_client:
        factory = Factory(
            session=db_session,
            logger=_logger(),
            credential_encryptor=CredentialEncryptor(
                current_key=Fernet.generate_key().decode()
            ),
            copy_http_client=copy_http_client,
            default_queue_name="docverse:queue",
            keeper_sync_copy_concurrency=8,
            keeper_sync_upload_limiter=asyncio.Semaphore(1),
        )
        org_id = await _seed_minio_service(db_session, factory)
        objects = {
            f"proj/builds/1/page-{index}.html": b"<html></html>"
            for index in range(3)
        }
        monkeypatch.setattr(
            factory,
            "create_ltd_s3_source",
            lambda **kwargs: _StubLtdSource(objects),
        )

        async with factory.create_build_content_copier_for_org(
            org_id=org_id, service_label="minio"
        ) as copier:
            assert copier.max_concurrent == 8
            await copier.copy_build(
                source_prefix="proj/builds/1/",
                dest_prefix="proj/__builds/1/",
            )

    assert len(puts) == 3
    assert peak == 1


@pytest.mark.asyncio
async def test_other_stores_get_no_upload_limiter(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the keeper-sync copier's store uploads under the limiter.

    The limiter caps the sync worker's presigned PUTs to R2 so a backfill
    burst cannot exhaust the node's NAT ports; API build uploads,
    dashboard renders and build processing call
    ``create_objectstore_for_org`` directly and must keep uploading
    unbounded, even on a factory that holds the worker's limiter.
    """
    recorded: list[dict[str, object]] = []

    def _recording_create_objectstore(**kwargs: object) -> MockObjectStore:
        recorded.append(kwargs)
        return MockObjectStore()

    monkeypatch.setattr(
        "docverse_server.factory.create_objectstore",
        _recording_create_objectstore,
    )
    factory = Factory(
        session=db_session,
        logger=_logger(),
        credential_encryptor=CredentialEncryptor(
            current_key=Fernet.generate_key().decode()
        ),
        default_queue_name="docverse:queue",
        keeper_sync_upload_limiter=asyncio.Semaphore(1),
    )
    org_id = await _seed_minio_service(db_session, factory)
    async with db_session.begin():
        await factory.create_objectstore_for_org(
            org_id=org_id, service_label="minio"
        )

    assert len(recorded) == 1
    assert "upload_limiter" in recorded[0]
    assert recorded[0]["upload_limiter"] is None
