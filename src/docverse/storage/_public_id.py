"""Collision-safe insertion of rows carrying a time-ordered ``public_id``.

Time-ordered resource IDs (see :mod:`docverse.domain.base32id`) reserve only
`~17` random low bits, so two rows minted in the same millisecond can collide
on the ``public_id`` unique constraint. A naive ``session.flush()`` that hits
that collision raises ``IntegrityError`` and — because per ``CLAUDE.md`` the
request handler owns the surrounding ``session.begin()`` transaction — would
poison that outer transaction, surfacing a collision as a 500 rather than a
re-mint.

`insert_with_time_ordered_public_id` isolates each insert attempt inside a
SAVEPOINT (``session.begin_nested()``). A ``public_id`` collision rolls back
only the savepoint, leaving the outer transaction intact, and the helper
re-mints and retries. Any other integrity violation is a genuine error and is
re-raised untouched.
"""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.domain.base32id import generate_resource_id

__all__ = ["insert_with_time_ordered_public_id"]

MAX_PUBLIC_ID_ATTEMPTS = 5
"""Bound on re-mint attempts before giving up.

Same-millisecond collisions across ~131k random values are astronomically
unlikely to recur even once; five attempts is a generous safety margin whose
exhaustion signals a real problem rather than an unlucky draw.
"""


async def insert_with_time_ordered_public_id[Row](
    session: AsyncSession,
    make_row: Callable[[int], Row],
    *,
    max_attempts: int = MAX_PUBLIC_ID_ATTEMPTS,
) -> Row:
    """Insert a row minted with a time-ordered ``public_id``, retrying on
    a ``public_id`` collision.

    Parameters
    ----------
    session
        The active session. Its surrounding transaction (owned by the
        handler) is left intact on a collision because each attempt runs
        inside its own SAVEPOINT.
    make_row
        Callable that, given a freshly minted integer ``public_id``, returns
        the ORM row to insert. It is called once per attempt so any derived
        values (e.g. object-store keys embedding the ID) are recomputed for
        the retried ID.
    max_attempts
        Maximum number of mint-and-insert attempts before raising.

    Returns
    -------
    Row
        The successfully inserted (and flushed) ORM row.

    Raises
    ------
    RuntimeError
        If ``max_attempts`` consecutive ``public_id`` collisions occur.
    """
    last_error: IntegrityError | None = None
    for _ in range(max_attempts):
        row = make_row(generate_resource_id())
        try:
            async with session.begin_nested():
                session.add(row)
                await session.flush()
        except IntegrityError as exc:
            if not _is_public_id_conflict(exc):
                raise
            last_error = exc
            continue
        else:
            return row
    msg = (
        f"Exhausted {max_attempts} attempts minting a unique public_id "
        "for a time-ordered resource"
    )
    raise RuntimeError(msg) from last_error


def _is_public_id_conflict(exc: IntegrityError) -> bool:
    """Return True when ``exc`` is a ``public_id`` unique-constraint violation.

    The retry loop only re-mints for ``public_id`` collisions; any other
    integrity violation (a real bug or a different constraint) must propagate.
    """
    for candidate in (exc.orig, getattr(exc.orig, "__cause__", None)):
        name = getattr(candidate, "constraint_name", None)
        if name is not None:
            return "public_id" in name
    return "public_id" in str(exc.orig)
