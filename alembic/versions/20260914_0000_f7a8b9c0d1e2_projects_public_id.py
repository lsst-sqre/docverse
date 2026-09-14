"""Add a public Base32 ``public_id`` to ``projects``.

Part of the Ook project-listing work (PRD #634 / DM-56083). Projects are
externally addressable by slug, and a slug can one day be renamed; a
stable Crockford Base32 ``public_id`` gives every project an identifier
that survives such a rename, so a consumer polling the listing can keep
following the same resource.

The column mirrors the shape already on ``builds``, ``queue_jobs``, and
``keeper_sync_runs`` — ``BIGINT``, unique, not-null,
``autoincrement=False`` — because the value is minted in application code
(or, here, by this backfill), never by a sequence.

Backfill story
--------------
Existing rows are minted from ``date_created`` in ascending order (with
``id`` as the tiebreak) via ``mint_time_ordered_resource_ids``, which
yields strictly increasing IDs that sort in creation order even when
several rows share a millisecond. A project's ID appears in no
object-store key — builds carry their own ID in ``staging_key`` and
``storage_prefix`` — so assigning IDs to existing rows is safe; nothing
downstream is keyed off the value.

The column is added nullable, backfilled, then the unique constraint is
created and the column flipped to ``NOT NULL``, so the migration succeeds
against a populated table without a two-phase deploy.

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-09-14 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op
from docverse_server.domain.base32id import mint_time_ordered_resource_ids

# revision identifiers, used by Alembic.
revision: str = "f7a8b9c0d1e2"
down_revision: str | None = "e6f7a8b9c0d1"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column(
            "public_id", sa.BigInteger(), autoincrement=False, nullable=True
        ),
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id, date_created FROM projects"
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
                    "UPDATE projects SET public_id = :public_id WHERE id = :id"
                ),
                {"public_id": public_id, "id": row.id},
            )

    op.create_unique_constraint(
        "projects_public_id_key", "projects", ["public_id"]
    )
    op.alter_column("projects", "public_id", nullable=False)


def downgrade() -> None:
    op.drop_constraint(
        "projects_public_id_key",
        "projects",
        type_="unique",
    )
    op.drop_column("projects", "public_id")
