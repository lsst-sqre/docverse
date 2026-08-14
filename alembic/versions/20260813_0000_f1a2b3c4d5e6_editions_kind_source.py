"""Replace ``editions.kind_manually_set`` with ``editions.kind_source``.

Part of the version-heuristic release-kind work (PRD #498 / DM-55772).
The boolean it replaces answered "did a PATCH mention ``kind``?", which
is a proxy for the question the heal paths actually ask: *who owns this
edition's kind?* ``kind_source`` answers that directly — ``derived``
means Docverse recomputes it on every sync and tracked build (in both
directions, so a promotion can be undone by fixing the rules),
``declared`` means an operator decided it and no automated path may
touch it.

The backfill preserves every existing decision: rows an operator had
PATCHed carry ``kind_manually_set = true`` and become ``declared``;
every other row becomes ``derived`` and keeps healing automatically.
Because the old flag was only ever set by the editions PATCH API, that
mapping is exact.

Revision ID: f1a2b3c4d5e6
Revises: f0a1b2c3d4e5
Create Date: 2026-08-13 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f1a2b3c4d5e6"
down_revision: str | None = "f0a1b2c3d4e5"
branch_labels: str | None = None
depends_on: str | None = None

#: ``EditionKind`` and friends are stored as ``VARCHAR`` + CHECK
#: (``native_enum=False``), so the column type here must match the ORM's
#: ``Enum(..., native_enum=False, length=32)`` exactly.
_KIND_SOURCE = sa.Enum(
    "derived",
    "declared",
    name="editionkindsource",
    native_enum=False,
    length=32,
)


def upgrade() -> None:
    op.add_column(
        "editions",
        sa.Column(
            "kind_source",
            _KIND_SOURCE,
            nullable=False,
            server_default=sa.text("'derived'"),
        ),
    )
    op.execute(
        sa.text(
            "UPDATE editions SET kind_source = 'declared'"
            " WHERE kind_manually_set"
        )
    )
    op.drop_column("editions", "kind_manually_set")


def downgrade() -> None:
    op.add_column(
        "editions",
        sa.Column(
            "kind_manually_set",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.execute(
        sa.text(
            "UPDATE editions SET kind_manually_set = true"
            " WHERE kind_source = 'declared'"
        )
    )
    op.drop_column("editions", "kind_source")
