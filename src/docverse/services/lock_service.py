"""Service for cross-job serialization via Postgres advisory locks.

Docverse background jobs coordinate on shared resources (a project's
builds, an edition's pointer, a project's dashboard render) using
``pg_advisory_lock`` on a 64-bit lock id. The high 16 bits encode a
:class:`LockClass` so a project-level lock cannot collide with an
edition-level lock that happens to hash to the same 48-bit value; the
low 48 bits are a ``blake2b`` digest of the resource tuple, so the id
is deterministic across Python processes and worker replicas.

See SQR-112 §Cross-job serialization for the design rationale.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class LockClass(IntEnum):
    """Advisory-lock class prefixes encoded in the high 16 bits.

    Values are stable on-disk identifiers: changing them would
    repartition the lock ID space and collide with locks held by
    already-running workers.
    """

    BUILD_PROCESSING = 0x0000
    EDITION_UPDATE = 0x0001
    PROJECT = 0x0002


def compute_lock_id(lock_class: LockClass, **parts: Any) -> int:
    """Compute a deterministic signed 64-bit advisory-lock id.

    ``parts`` values are ``|``-joined as UTF-8 and hashed with
    ``blake2b(digest_size=6)``; the 48-bit digest is OR'd with
    ``lock_class.value << 48`` and the resulting unsigned 64-bit value
    is reinterpreted as a signed ``bigint`` compatible with
    ``pg_advisory_lock``. blake2b is chosen over the built-in ``hash()``
    because it is deterministic across interpreter restarts and worker
    replicas.

    Parts are serialized in kwargs insertion order, so callers must
    pass them in a stable canonical order.

    Parameters
    ----------
    lock_class
        The class prefix to encode in the high 16 bits.
    **parts
        Resource-identifying fields (ints/strings) to hash into the
        low 48 bits, in a stable canonical order.

    Returns
    -------
    int
        A signed 64-bit int suitable for ``pg_advisory_lock(bigint)``.
    """
    joined = "|".join(str(value) for value in parts.values())
    digest = hashlib.blake2b(
        joined.encode("utf-8"), digest_size=6
    ).digest()
    hash_int = int.from_bytes(digest, "big")
    unsigned = (int(lock_class.value) << 48) | hash_int
    if unsigned >= (1 << 63):
        return unsigned - (1 << 64)
    return unsigned


@dataclass(frozen=True, slots=True)
class LockKey:
    """A resolved advisory-lock identifier.

    Attributes
    ----------
    lock_class
        The :class:`LockClass` encoded in the high 16 bits.
    lock_id
        The signed 64-bit id suitable for ``pg_advisory_lock(bigint)``.
    label
        A short human-readable label used in log messages.
    """

    lock_class: LockClass
    lock_id: int
    label: str

    @classmethod
    def for_build_processing(
        cls, org_id: int, project_id: int, git_ref: str
    ) -> LockKey:
        """Build the lock key for a ``build_processing`` job.

        Serializes builds sharing the same ``(org, project, git_ref)``.
        """
        lock_class = LockClass.BUILD_PROCESSING
        lock_id = compute_lock_id(
            lock_class,
            org_id=org_id,
            project_id=project_id,
            git_ref=git_ref,
        )
        label = (
            f"build_processing(org={org_id},project={project_id},"
            f"ref={git_ref})"
        )
        return cls(lock_class=lock_class, lock_id=lock_id, label=label)

    @classmethod
    def for_edition_update(
        cls, org_id: int, project_id: int, edition_id: int
    ) -> LockKey:
        """Build the lock key for an edition-pointer update.

        Serializes updates to a single edition (the edition's pointer
        and its per-edition metadata JSON).
        """
        lock_class = LockClass.EDITION_UPDATE
        lock_id = compute_lock_id(
            lock_class,
            org_id=org_id,
            project_id=project_id,
            edition_id=edition_id,
        )
        label = (
            f"edition_update(org={org_id},project={project_id},"
            f"edition={edition_id})"
        )
        return cls(lock_class=lock_class, lock_id=lock_id, label=label)

    @classmethod
    def for_project(cls, org_id: int, project_id: int) -> LockKey:
        """Build the lock key for a project-scoped job.

        Serializes per-project work such as dashboard renders.
        """
        lock_class = LockClass.PROJECT
        lock_id = compute_lock_id(
            lock_class,
            org_id=org_id,
            project_id=project_id,
        )
        label = f"project(org={org_id},project={project_id})"
        return cls(lock_class=lock_class, lock_id=lock_id, label=label)


class LockService:
    """Acquire and release Postgres advisory locks for a session."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    @asynccontextmanager
    async def acquire(
        self, lock_key: LockKey
    ) -> AsyncGenerator[None]:
        """Acquire ``lock_key`` for the lifetime of the context block.

        Blocks the caller until the lock is granted via
        ``SELECT pg_advisory_lock(:lock_id)``. On exit (normal or
        exceptional) issues ``SELECT pg_advisory_unlock(:lock_id)``. If
        the session's underlying connection dies before unlock runs
        (e.g. a crashed worker), Postgres releases the lock when the
        connection closes — session lifetime is the ultimate owner.
        """
        self._logger.debug(
            "Acquiring advisory lock",
            lock_id=lock_key.lock_id,
            lock_label=lock_key.label,
            lock_class=lock_key.lock_class.name,
        )
        await self._session.execute(
            text("SELECT pg_advisory_lock(:lock_id)"),
            {"lock_id": lock_key.lock_id},
        )
        self._logger.debug(
            "Acquired advisory lock",
            lock_id=lock_key.lock_id,
            lock_label=lock_key.label,
        )
        try:
            yield
        finally:
            try:
                await self._session.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": lock_key.lock_id},
                )
            except Exception:
                # Unlock is best-effort: on session or connection
                # failure the DB releases the lock when the connection
                # closes. Log and continue so the original exception
                # (if any) still propagates.
                self._logger.warning(
                    "Failed to release advisory lock",
                    lock_id=lock_key.lock_id,
                    lock_label=lock_key.label,
                    exc_info=True,
                )
            else:
                self._logger.debug(
                    "Released advisory lock",
                    lock_id=lock_key.lock_id,
                    lock_label=lock_key.label,
                )
