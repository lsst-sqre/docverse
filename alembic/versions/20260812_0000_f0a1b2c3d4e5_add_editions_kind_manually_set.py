"""Add ``kind_manually_set`` to ``editions``.

Part of the version-heuristic release-kind work (PRD #498 / DM-55772).
Two automated paths re-derive an edition's ``kind`` long after the row
was created — keeper-sync's per-sync refresh and the native
build-upload heal — and both promote ``draft`` -> ``release``. That
made an operator's deliberate ``release`` -> ``draft`` demotion
unstable: the very next poll or upload re-derived ``release`` and wrote
it straight back.

This column records "an operator set this kind by hand through the
editions PATCH API", and both automated paths skip flagged rows. It is
``NOT NULL`` with a ``false`` server default, so every pre-existing
edition backfills to "never manually set" and keeps healing
automatically — which is the behaviour the healing story depends on.

Revision ID: f0a1b2c3d4e5
Revises: e9f0a1b2c3d4
Create Date: 2026-08-12 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f0a1b2c3d4e5"
down_revision: str | None = "e9f0a1b2c3d4"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "editions",
        sa.Column(
            "kind_manually_set",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("editions", "kind_manually_set")
