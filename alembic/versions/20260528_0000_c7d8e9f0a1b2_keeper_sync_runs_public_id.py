"""Add a public Base32 ``public_id`` to ``keeper_sync_runs``.

Part of the time-ordered resource-ID work (PRD #448 / DM-55549). Every
externally-addressable resource gets a stable Crockford Base32
``public_id`` so the API never leaks the raw integer primary key.
Keeper-sync runs are next: this migration adds the ``BIGINT`` column and
backfills it, and the following slice switches the API surface over.

The column mirrors the shape already on ``builds`` and ``queue_jobs`` —
``BIGINT``, unique, not-null, ``autoincrement=False`` — because the value
is minted in application code (or, here, by this backfill), never by a
sequence.

Backfill story
--------------
The PRD names ``date_created`` as the backfill source, but
``keeper_sync_runs`` has no such column; its creation timestamp is
``date_started`` (NOT NULL, defaulted to ``now()``). Existing rows are
therefore re-minted from ``date_started`` in ascending order via
``mint_time_ordered_resource_ids``, which yields strictly increasing IDs
that sort in creation order even when several rows share a millisecond.
Run IDs appear in no object-store keys, so re-assigning them to existing
rows is safe — nothing downstream is keyed off the value.

The column is added nullable, backfilled, then the unique constraint is
created and the column flipped to ``NOT NULL``, so the migration succeeds
against a populated table without a two-phase deploy.

Revision ID: c7d8e9f0a1b2
Revises: b6c7d8e9f0a1
Create Date: 2026-05-28 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op
from docverse.domain.base32id import mint_time_ordered_resource_ids

# revision identifiers, used by Alembic.
revision: str = "c7d8e9f0a1b2"
down_revision: str | None = "b6c7d8e9f0a1"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "keeper_sync_runs",
        sa.Column(
            "public_id", sa.BigInteger(), autoincrement=False, nullable=True
        ),
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id, date_started FROM keeper_sync_runs"
            " ORDER BY date_started ASC, id ASC"
        )
    ).all()
    if rows:
        public_ids = mint_time_ordered_resource_ids(
            [row.date_started for row in rows]
        )
        for row, public_id in zip(rows, public_ids, strict=True):
            bind.execute(
                sa.text(
                    "UPDATE keeper_sync_runs SET public_id = :public_id"
                    " WHERE id = :id"
                ),
                {"public_id": public_id, "id": row.id},
            )

    op.create_unique_constraint(
        "keeper_sync_runs_public_id_key", "keeper_sync_runs", ["public_id"]
    )
    op.alter_column("keeper_sync_runs", "public_id", nullable=False)


def downgrade() -> None:
    op.drop_constraint(
        "keeper_sync_runs_public_id_key",
        "keeper_sync_runs",
        type_="unique",
    )
    op.drop_column("keeper_sync_runs", "public_id")
