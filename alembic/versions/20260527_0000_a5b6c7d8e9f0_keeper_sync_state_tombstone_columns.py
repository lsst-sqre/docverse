"""Add tombstone columns to ``keeper_sync_state``.

Foundational data layer for sync tombstones (PRD #332 / DM-54914).
A sync tombstone is a permanent veto recorded on the existing
``keeper_sync_state`` table that tells keeper-sync "this LTD resource
has been deleted on the Docverse side; do not re-migrate it." Three
new nullable columns capture the veto:

- ``date_tombstoned`` — when the tombstone was recorded. ``NULL`` means
  the row is not tombstoned.
- ``tombstone_reason`` — short string enum. Constrained via a
  ``CheckConstraint`` matching the existing ``resource_type`` pattern
  (``NULL`` or one of ``manual_delete`` / ``lifecycle_delete`` /
  ``lifecycle_preemptive``).
- ``tombstone_note`` — short free-text the writer can attach for the
  admin UI.

All three columns are nullable so every existing row is NULL-backfilled
to "not tombstoned"; no operator intervention is required to deploy.

Revision ID: a5b6c7d8e9f0
Revises: z4a5b6c7d8e9
Create Date: 2026-05-27 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a5b6c7d8e9f0"
down_revision: str | None = "z4a5b6c7d8e9"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "keeper_sync_state",
        sa.Column(
            "date_tombstoned", sa.DateTime(timezone=True), nullable=True
        ),
    )
    op.add_column(
        "keeper_sync_state",
        sa.Column("tombstone_reason", sa.String(32), nullable=True),
    )
    op.add_column(
        "keeper_sync_state",
        sa.Column("tombstone_note", sa.String(512), nullable=True),
    )
    op.create_check_constraint(
        "ck_keeper_sync_state_tombstone_reason",
        "keeper_sync_state",
        "tombstone_reason IS NULL OR tombstone_reason IN "
        "('manual_delete', 'lifecycle_delete', 'lifecycle_preemptive')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_keeper_sync_state_tombstone_reason",
        "keeper_sync_state",
        type_="check",
    )
    op.drop_column("keeper_sync_state", "tombstone_note")
    op.drop_column("keeper_sync_state", "tombstone_reason")
    op.drop_column("keeper_sync_state", "date_tombstoned")
