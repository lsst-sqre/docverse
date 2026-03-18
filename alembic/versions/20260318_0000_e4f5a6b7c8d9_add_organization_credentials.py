"""Add organization_credentials table and credential label columns.

Revision ID: e4f5a6b7c8d9
Revises: d3e4f5a6b7c8
Create Date: 2026-03-18 00:00:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e4f5a6b7c8d9"
down_revision: str | None = "d3e4f5a6b7c8"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "organization_credentials",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "organization_id",
            sa.Integer,
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("service_type", sa.String(32), nullable=False),
        sa.Column("encrypted_credential", sa.LargeBinary, nullable=False),
        sa.Column(
            "date_created",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "date_updated",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "organization_id", "label", name="uq_org_credential_label"
        ),
    )

    op.add_column(
        "organizations",
        sa.Column(
            "publishing_credential_label", sa.String(128), nullable=True
        ),
    )
    op.add_column(
        "organizations",
        sa.Column("staging_credential_label", sa.String(128), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("organizations", "staging_credential_label")
    op.drop_column("organizations", "publishing_credential_label")
    op.drop_table("organization_credentials")
