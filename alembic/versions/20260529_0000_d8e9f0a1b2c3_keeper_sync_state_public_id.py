"""Add a public Base32 ``public_id`` to ``keeper_sync_state``.

Part of the time-ordered resource-ID work (PRD #448 / DM-55549). Every
externally-addressable resource gets a stable Crockford Base32
``public_id`` so the API never leaks the raw integer primary key.
Keeper-sync state rows — the rows that back tombstones — are next: this
migration adds the ``BIGINT`` column and backfills it, and the following
slice switches the tombstone API surface over to addressing rows by this
public id instead of the raw primary key.

The column mirrors the shape already on ``builds``, ``queue_jobs``, and
``keeper_sync_runs`` — ``BIGINT``, unique, not-null,
``autoincrement=False`` — because the value is minted in application code
(or, here, by this backfill), never by a sequence.

Backfill story
--------------
The PRD names ``date_created`` as the backfill source, but
``keeper_sync_state`` has no creation timestamp at all — only the
nullable ``date_last_synced``, ``date_rebuilt_seen``, and
``date_tombstoned``. Existing rows are therefore re-minted from
``COALESCE(date_tombstoned, date_last_synced, now())`` in ascending order
via ``mint_time_ordered_resource_ids``, which yields strictly increasing
IDs that sort in that order even when several rows share a millisecond.
Rows where every timestamp is NULL fall back to ``now()`` and simply
order last, receiving strictly increasing IDs among themselves. State IDs
appear in no object-store keys, so re-assigning them to existing rows is
safe — nothing downstream is keyed off the value.

The column is added nullable, backfilled, then the unique constraint is
created and the column flipped to ``NOT NULL``, so the migration succeeds
against a populated table without a two-phase deploy.

Revision ID: d8e9f0a1b2c3
Revises: c7d8e9f0a1b2
Create Date: 2026-05-29 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op
from docverse.domain.base32id import mint_time_ordered_resource_ids

# revision identifiers, used by Alembic.
revision: str = "d8e9f0a1b2c3"
down_revision: str | None = "c7d8e9f0a1b2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "keeper_sync_state",
        sa.Column(
            "public_id", sa.BigInteger(), autoincrement=False, nullable=True
        ),
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id,"
            " COALESCE(date_tombstoned, date_last_synced, now()) AS sort_ts"
            " FROM keeper_sync_state"
            " ORDER BY COALESCE(date_tombstoned, date_last_synced, now())"
            " ASC, id ASC"
        )
    ).all()
    if rows:
        public_ids = mint_time_ordered_resource_ids(
            [row.sort_ts for row in rows]
        )
        for row, public_id in zip(rows, public_ids, strict=True):
            bind.execute(
                sa.text(
                    "UPDATE keeper_sync_state SET public_id = :public_id"
                    " WHERE id = :id"
                ),
                {"public_id": public_id, "id": row.id},
            )

    op.create_unique_constraint(
        "keeper_sync_state_public_id_key", "keeper_sync_state", ["public_id"]
    )
    op.alter_column("keeper_sync_state", "public_id", nullable=False)


def downgrade() -> None:
    op.drop_constraint(
        "keeper_sync_state_public_id_key",
        "keeper_sync_state",
        type_="unique",
    )
    op.drop_column("keeper_sync_state", "public_id")
