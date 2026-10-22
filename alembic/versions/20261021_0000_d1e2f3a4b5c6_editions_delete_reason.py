"""Add ``editions.delete_reason``.

First slice of the lifecycle-revive work (PRD #830 / DM-56771). A
keeper-synced edition's soft-delete reason lives on its
``keeper_sync_state`` tombstone, but a native (direct-upload) edition
has no state row, so nothing recorded *why* it was deleted. This column
records it on the edition itself — ``manual_delete`` or
``lifecycle_delete``, constrained like
``ck_keeper_sync_state_tombstone_reason`` — and the ``EditionStore``
soft-delete chokepoints stamp it in the same flush as ``date_deleted``.

The data step backfills existing soft-deleted editions from the
tombstone on their keeper-sync state row, joined on
``(org_id, resource_type='edition', docverse_id)`` through the edition's
project for the org. When more than one state row names the same
edition, the most recent tombstone wins, since it is the one that
describes the delete. A ``lifecycle_preemptive`` tombstone is folded
into ``lifecycle_delete``: it is a lifecycle deletion all the same, and
the edition column has no preemptive value. Rows with nothing to copy
from stay ``NULL``, and no default is written — every consumer reads
``NULL`` as revivable, the same as ``lifecycle_delete``.

Revision ID: d1e2f3a4b5c6
Revises: c0d1e2f3a4b5
Create Date: 2026-10-21 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d1e2f3a4b5c6"
down_revision: str | None = "c0d1e2f3a4b5"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "editions",
        sa.Column("delete_reason", sa.String(length=32), nullable=True),
    )
    op.create_check_constraint(
        "ck_editions_delete_reason",
        "editions",
        "delete_reason IS NULL OR delete_reason IN "
        "('manual_delete', 'lifecycle_delete')",
    )
    op.execute(
        """
        UPDATE editions AS e
        SET delete_reason = CASE t.tombstone_reason
            WHEN 'manual_delete' THEN 'manual_delete'
            ELSE 'lifecycle_delete'
        END
        FROM projects AS p, (
            SELECT DISTINCT ON (s.org_id, s.docverse_id)
                s.org_id, s.docverse_id, s.tombstone_reason
            FROM keeper_sync_state AS s
            WHERE s.resource_type = 'edition'
                AND s.docverse_id IS NOT NULL
                AND s.tombstone_reason IS NOT NULL
            ORDER BY s.org_id, s.docverse_id,
                s.date_tombstoned DESC NULLS LAST, s.id DESC
        ) AS t
        WHERE e.date_deleted IS NOT NULL
            AND e.project_id = p.id
            AND t.org_id = p.org_id
            AND t.docverse_id = e.id
        """
    )


def downgrade() -> None:
    op.drop_constraint("ck_editions_delete_reason", "editions", type_="check")
    op.drop_column("editions", "delete_reason")
