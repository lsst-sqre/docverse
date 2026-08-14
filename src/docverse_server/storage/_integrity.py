"""Classification of database integrity errors by SQLSTATE.

Several storage paths need to tell "this integrity error is the benign
outcome I know how to absorb" apart from "this integrity error is a bug".
Both questions are answered from the *driver* exception underneath
SQLAlchemy's :class:`~sqlalchemy.exc.IntegrityError` wrapper, so the
inspection logic lives here once rather than being re-derived per call
site:

* :func:`is_unique_violation` gates on the Postgres SQLSTATE, so a
  *different* integrity error that merely mentions the same identifier
  (e.g. a NOT NULL violation, SQLSTATE ``23502``) is never mistaken for a
  uniqueness conflict.
* :func:`violated_constraint_name` names the constraint or unique index
  the server attributed the conflict to, which is what lets a caller
  absorb one specific conflict and re-raise every other.
"""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError

__all__ = [
    "UNIQUE_VIOLATION_SQLSTATE",
    "is_unique_violation",
    "violated_constraint_name",
]

UNIQUE_VIOLATION_SQLSTATE = "23505"
"""Postgres SQLSTATE for ``unique_violation``."""


def is_unique_violation(exc: IntegrityError) -> bool:
    """Return True when ``exc`` wraps a Postgres ``unique_violation``.

    Checks the SQLSTATE on the driver exception (asyncpg exposes it as
    ``sqlstate``; other DBAPIs as ``pgcode``) against
    :data:`UNIQUE_VIOLATION_SQLSTATE`. Walks both ``exc.orig`` and its
    ``__cause__`` because the driver exception may be wrapped.
    """
    for candidate in (exc.orig, getattr(exc.orig, "__cause__", None)):
        sqlstate = getattr(candidate, "sqlstate", None) or getattr(
            candidate, "pgcode", None
        )
        if sqlstate == UNIQUE_VIOLATION_SQLSTATE:
            return True
    return False


def violated_constraint_name(exc: IntegrityError) -> str | None:
    """Return the constraint (or unique index) name ``exc`` was raised for.

    asyncpg surfaces the server's ``constraint_name`` error field on the
    driver exception; for a violated *unique index* — the shape of the
    ``queue_jobs`` active-job mutexes — that field carries the index
    name. Returns ``None`` when the driver did not supply one, in which
    case callers fall back to matching against ``str(exc.orig)``.
    """
    for candidate in (exc.orig, getattr(exc.orig, "__cause__", None)):
        name = getattr(candidate, "constraint_name", None)
        if name is not None:
            return str(name)
    return None
