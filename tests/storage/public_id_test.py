"""Tests for the ``public_id`` collision classifier.

Exercises :func:`docverse.storage._public_id._is_public_id_conflict`, which
gates the re-mint retry loop: only a Postgres ``unique_violation`` (SQLSTATE
``23505``) on a ``public_id`` constraint is a re-mintable collision. Any other
integrity error — even one whose message mentions ``public_id`` — must
propagate rather than be silently retried and masked as an "exhausted
attempts" error.
"""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError

from docverse.storage._public_id import _is_public_id_conflict


class _FakeOrigError(Exception):
    """Stand-in for the driver exception wrapped by ``IntegrityError.orig``.

    Mirrors the asyncpg surface: a ``sqlstate`` code and an optional
    ``constraint_name``, plus a string message for the fallback path.
    """

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        constraint_name: str | None = None,
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.constraint_name = constraint_name


def _integrity_error(orig: _FakeOrigError) -> IntegrityError:
    return IntegrityError(statement=None, params=None, orig=orig)


_DUP_MSG = 'duplicate key violates unique constraint "builds_public_id_key"'


def test_unique_violation_on_public_id_constraint_is_conflict() -> None:
    """23505 + a public_id constraint name is a re-mintable collision."""
    exc = _integrity_error(
        _FakeOrigError(
            _DUP_MSG,
            sqlstate="23505",
            constraint_name="builds_public_id_key",
        )
    )
    assert _is_public_id_conflict(exc) is True


def test_unique_violation_on_public_id_via_message_fallback() -> None:
    """23505 with no constraint_name falls back to the message match."""
    exc = _integrity_error(
        _FakeOrigError(_DUP_MSG, sqlstate="23505", constraint_name=None)
    )
    assert _is_public_id_conflict(exc) is True


def test_unique_violation_on_other_constraint_is_not_conflict() -> None:
    """A unique violation on a different constraint must propagate."""
    exc = _integrity_error(
        _FakeOrigError(
            'duplicate key value violates unique constraint "builds_slug_key"',
            sqlstate="23505",
            constraint_name="builds_slug_key",
        )
    )
    assert _is_public_id_conflict(exc) is False


def test_not_null_violation_mentioning_public_id_is_not_conflict() -> None:
    """A non-unique error mentioning public_id must NOT be retried.

    This is the case the tightened classifier guards against: a NOT NULL
    violation (SQLSTATE 23502) on public_id would otherwise be swallowed by
    the retry loop and surfaced as an "exhausted attempts" RuntimeError,
    masking the real bug.
    """
    exc = _integrity_error(
        _FakeOrigError(
            'null value in column "public_id" violates not-null constraint',
            sqlstate="23502",
            constraint_name=None,
        )
    )
    assert _is_public_id_conflict(exc) is False


def test_missing_sqlstate_is_not_conflict() -> None:
    """Without a 23505 SQLSTATE the error is not classified as a collision."""
    exc = _integrity_error(
        _FakeOrigError(
            "some public_id related error with no sqlstate",
            sqlstate=None,
            constraint_name=None,
        )
    )
    assert _is_public_id_conflict(exc) is False
