"""Tests that both stores still satisfy the ``ObjectStore`` protocol.

``ObjectStore`` is ``@runtime_checkable``, which makes ``isinstance``
a real (if shallow) conformance check: it fails the moment an
implementation is missing a method the protocol declares. Adding a
method to the protocol is therefore a breaking change for every
implementation, and this module is what says so out loud — the
``_returns_object_store`` helpers add the static half, since mypy
checks the structural match including signatures.
"""

from __future__ import annotations

import structlog

from docverse_server.storage.objectstore import (
    MockObjectStore,
    ObjectStore,
    S3ObjectStore,
)


def _make_s3_store() -> ObjectStore:
    """Return an unopened S3 store, typed as the protocol."""
    return S3ObjectStore(
        endpoint_url="https://account.r2.cloudflarestorage.com",
        bucket="docs",
        access_key_id="key-id",
        secret_access_key="secret-key",
        region="auto",
        logger=structlog.get_logger("test"),
    )


def _make_mock_store() -> ObjectStore:
    """Return an in-memory store, typed as the protocol."""
    return MockObjectStore()


def test_s3_object_store_satisfies_the_protocol() -> None:
    """The production store implements every protocol method."""
    assert isinstance(_make_s3_store(), ObjectStore)


def test_mock_object_store_satisfies_the_protocol() -> None:
    """The in-memory store keeps pace with the protocol."""
    assert isinstance(_make_mock_store(), ObjectStore)
