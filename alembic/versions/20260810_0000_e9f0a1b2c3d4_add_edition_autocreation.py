"""Add ``edition_autocreation`` to ``organizations`` and ``projects``.

Part of the version-heuristic release-kind work (PRD #498 / DM-55772).
Semver major/minor aggregate editions (``N`` and ``N.M``) have been
created unconditionally for every stable semver release; this column
makes that behaviour configurable per organization and per project.

The column is nullable JSONB holding ``{"semver_aggregates": <bool>}``.
``NULL`` is the "unset" marker, not a value: the project column wins
whole-object when non-NULL, the organization column is the fallback,
and NULL at both levels means the built-in defaults apply
(``semver_aggregates`` defaults to ``true``). Backfilling either column
with an explicit default would erase the distinction between "unset"
and "explicitly configured", so existing rows are deliberately left
NULL and keep today's behaviour.

Revision ID: e9f0a1b2c3d4
Revises: d8e9f0a1b2c3
Create Date: 2026-08-10 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e9f0a1b2c3d4"
down_revision: str | None = "d8e9f0a1b2c3"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column("edition_autocreation", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("edition_autocreation", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("projects", "edition_autocreation")
    op.drop_column("organizations", "edition_autocreation")
