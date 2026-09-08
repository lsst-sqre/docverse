"""Widen ``builds_status_check`` for ``superseded`` and ``cancelled``.

Stranded-builds PRD #577 / DM-56012. ``builds.status`` is a non-native
enum, so the values it may hold live in the ``builds_status_check`` CHECK
constraint created by ``c2d3e4f5a6b7`` rather than in a Postgres type.
Adding the two terminal statuses to :class:`~docverse.models.BuildStatus`
is therefore inert until this constraint is replaced: the stale-skip path
writing ``superseded`` would fail on the constraint instead.

The upgrade then backfills the rows this branch's code can no longer
produce but earlier code left behind: builds soft-deleted while still
``pending`` or ``processing``. The branch's invariant is that a deleted
build is finished, however it was deleted, but nothing reaches those
rows to enforce it — the stranded-build sweep and every API path filter
``date_deleted IS NULL``, and no job will ever deliver for them. Left
alone they would claim forever that a worker is on them, and DM-54691's
purgatory restore would hand one back as a live build. The backfill is
safe to fold into this revision rather than a follow-up: the revision
has never been deployed anywhere, and the ``cancelled`` it writes only
becomes a legal value one statement earlier.

``date_completed`` is set unconditionally rather than coalesced.
``BuildStore.transition_status`` stamps it on entry to a terminal status
and creates every row ``pending`` with no stamp, so a ``pending`` or
``processing`` row cannot already carry one.

The downgrade rewrites ``superseded`` and ``cancelled`` rows to ``failed``
*before* restoring the five-value constraint. Both new statuses mean
"this build will never be published", which is what ``failed`` already
signals to every pre-#577 reader, and folding first is what keeps the
rollback from failing on its own data. A backfilled row therefore comes
back as ``failed`` rather than the ``pending`` or ``processing`` it held
before the upgrade — the right answer for a row that was already deleted
and will never be built.

No column changes: ``status`` is already ``VARCHAR(32)``, wide enough for
both new values.

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-09-02 00:00:00.000000+00:00
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b3c4d5e6f7a8"
down_revision: str | None = "a2b3c4d5e6f7"
branch_labels: str | None = None
depends_on: str | None = None

_FIVE_VALUES = "'pending', 'uploaded', 'processing', 'completed', 'failed'"

_SEVEN_VALUES = (
    "'pending', 'uploaded', 'processing', 'completed', 'failed',"
    " 'superseded', 'cancelled'"
)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE builds DROP CONSTRAINT IF EXISTS builds_status_check"
    )
    op.execute(
        "ALTER TABLE builds ADD CONSTRAINT builds_status_check"
        f" CHECK (status IN ({_SEVEN_VALUES}))"
    )
    op.execute(
        "UPDATE builds SET status = 'cancelled', date_completed = now()"
        " WHERE date_deleted IS NOT NULL"
        " AND status IN ('pending', 'processing')"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE builds SET status = 'failed'"
        " WHERE status IN ('superseded', 'cancelled')"
    )
    op.execute(
        "ALTER TABLE builds DROP CONSTRAINT IF EXISTS builds_status_check"
    )
    op.execute(
        "ALTER TABLE builds ADD CONSTRAINT builds_status_check"
        f" CHECK (status IN ({_FIVE_VALUES}))"
    )
