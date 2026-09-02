"""Widen ``builds_status_check`` for ``superseded`` and ``cancelled``.

Stranded-builds PRD #577 / DM-56012. ``builds.status`` is a non-native
enum, so the values it may hold live in the ``builds_status_check`` CHECK
constraint created by ``c2d3e4f5a6b7`` rather than in a Postgres type.
Adding the two terminal statuses to :class:`~docverse.models.BuildStatus`
is therefore inert until this constraint is replaced: the stale-skip path
writing ``superseded`` would fail on the constraint instead.

The downgrade rewrites ``superseded`` and ``cancelled`` rows to ``failed``
*before* restoring the five-value constraint. Both new statuses mean
"this build will never be published", which is what ``failed`` already
signals to every pre-#577 reader, and folding first is what keeps the
rollback from failing on its own data.

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
