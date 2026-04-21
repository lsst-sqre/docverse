"""Enforce main-edition invariants on the editions table.

Adds a CHECK constraint ensuring ``(kind = 'main') = (slug = '__main')``.
Combined with the existing ``UNIQUE(project_id, slug)``, this guarantees
at most one main edition per project and forbids non-main editions from
using the reserved ``__main`` slug.

Revision ID: k9l0m1n2o3p4
Revises: j8k9l0m1n2o3
Create Date: 2026-04-21 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "k9l0m1n2o3p4"
down_revision: str | None = "j8k9l0m1n2o3"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_editions_main_slug_kind",
        "editions",
        "(kind = 'main') = (slug = '__main')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_editions_main_slug_kind", "editions", type_="check")
