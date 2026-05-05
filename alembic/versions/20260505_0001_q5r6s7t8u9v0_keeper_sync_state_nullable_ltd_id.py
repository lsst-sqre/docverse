"""Make ``keeper_sync_state.ltd_id`` nullable; partial uniques per type.

LTD products are slug-keyed (LTD's product API has no integer id), but
the original ``keeper_sync_state`` schema modeled all rows under a
single ``(org_id, resource_type, ltd_id)`` UNIQUE constraint and so
needed an integer for project rows. The previous code synthesized one
by hashing the slug — a wart that hid the real schema mismatch. Drop
the hash by making ``ltd_id`` nullable and switching uniqueness to
two partial indexes: project rows are keyed on ``(org_id, ltd_slug)``,
edition / build rows stay keyed on ``(org_id, resource_type, ltd_id)``
because LTD edition and build slugs are only unique within a product
while ``keeper_sync_state`` rows are org-scoped.

``downgrade`` restores the prior single UNIQUE constraint and the
NOT NULL on ``ltd_id``. It does **not** rehydrate hashed integers for
project rows; project rows must be removed (or the database wiped) by
the operator before downgrading. Run this migration on a fresh
database or on one whose project rows you are willing to drop.

Revision ID: q5r6s7t8u9v0
Revises: p4q5r6s7t8u9
Create Date: 2026-05-05 00:01:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "q5r6s7t8u9v0"
down_revision: str | None = "p4q5r6s7t8u9"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.drop_constraint(
        "uq_keeper_sync_state_org_resource_ltd",
        "keeper_sync_state",
        type_="unique",
    )
    op.alter_column("keeper_sync_state", "ltd_id", nullable=True)
    op.create_index(
        "uq_keeper_sync_state_project_org_slug",
        "keeper_sync_state",
        ["org_id", "ltd_slug"],
        unique=True,
        postgresql_where=sa.text("resource_type = 'project'"),
    )
    op.create_index(
        "uq_keeper_sync_state_other_org_resource_ltd",
        "keeper_sync_state",
        ["org_id", "resource_type", "ltd_id"],
        unique=True,
        postgresql_where=sa.text("resource_type IN ('edition', 'build')"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_keeper_sync_state_other_org_resource_ltd",
        table_name="keeper_sync_state",
    )
    op.drop_index(
        "uq_keeper_sync_state_project_org_slug",
        table_name="keeper_sync_state",
    )
    op.alter_column("keeper_sync_state", "ltd_id", nullable=False)
    op.create_unique_constraint(
        "uq_keeper_sync_state_org_resource_ltd",
        "keeper_sync_state",
        ["org_id", "resource_type", "ltd_id"],
    )
