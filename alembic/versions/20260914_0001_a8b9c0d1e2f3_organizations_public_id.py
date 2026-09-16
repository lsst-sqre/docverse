"""Add a public Base32 ``public_id`` to ``organizations``.

The sibling of the ``projects`` backfill in ``f7a8b9c0d1e2`` (PRD #634 /
DM-56083). Organizations, like projects, are addressed on the wire by
slug; a stable Crockford Base32 ``public_id`` gives each one an
identifier that outlives a slug rename and gives the conditional-GET
work a durable value to hash into an ETag.

The column mirrors the shape already on ``builds``, ``queue_jobs``, and
``keeper_sync_runs`` — ``BIGINT``, unique, not-null,
``autoincrement=False`` — because the value is minted in application code
(or, here, by this backfill), never by a sequence.

Backfill story
--------------
Existing rows are minted from ``date_created`` in ascending order (with
``id`` as the tiebreak) via ``mint_time_ordered_resource_ids``, which
yields strictly increasing IDs that sort in creation order even when
several rows share a millisecond. An organization's ID appears in no
object-store key, so assigning IDs to existing rows is safe.

The column is added nullable, backfilled, then the unique constraint is
created and the column flipped to ``NOT NULL``, so the migration succeeds
against a populated table without a two-phase deploy.

Revision ID: a8b9c0d1e2f3
Revises: f7a8b9c0d1e2
Create Date: 2026-09-14 00:01:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op
from docverse_server.domain.base32id import mint_time_ordered_resource_ids

# revision identifiers, used by Alembic.
revision: str = "a8b9c0d1e2f3"
down_revision: str | None = "f7a8b9c0d1e2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column(
            "public_id", sa.BigInteger(), autoincrement=False, nullable=True
        ),
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id, date_created FROM organizations"
            " ORDER BY date_created ASC, id ASC"
        )
    ).all()
    if rows:
        public_ids = mint_time_ordered_resource_ids(
            [row.date_created for row in rows]
        )
        for row, public_id in zip(rows, public_ids, strict=True):
            bind.execute(
                sa.text(
                    "UPDATE organizations SET public_id = :public_id"
                    " WHERE id = :id"
                ),
                {"public_id": public_id, "id": row.id},
            )

    op.create_unique_constraint(
        "organizations_public_id_key", "organizations", ["public_id"]
    )
    op.alter_column("organizations", "public_id", nullable=False)


def downgrade() -> None:
    op.drop_constraint(
        "organizations_public_id_key",
        "organizations",
        type_="unique",
    )
    op.drop_column("organizations", "public_id")
