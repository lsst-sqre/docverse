"""Add three-layer infrastructure model (credentials, services, slots).

Replaces the single organization_credentials table (which bundled
secrets and config) with:
- organization_credentials: provider-level encrypted secrets only
- organization_services: non-secret config + credential reference
- Organization slot columns: publishing_store_label, staging_store_label,
  cdn_service_label, dns_service_label

Revision ID: f4a5b6c7d8e9
Revises: e4f5a6b7c8d9
Create Date: 2026-03-19 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f4a5b6c7d8e9"
down_revision: str | None = "e4f5a6b7c8d9"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # --- Recreate organization_credentials with new schema ---
    # Drop old table (pre-production, no data to migrate)
    op.drop_table("organization_credentials")

    # Create new credentials table with provider-level auth
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
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("encrypted_credentials", sa.LargeBinary, nullable=False),
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

    # --- Create organization_services table ---
    op.create_table(
        "organization_services",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "organization_id",
            sa.Integer,
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column(
            "config",
            postgresql.JSONB,
            nullable=False,
            server_default="{}",
        ),
        sa.Column("credential_label", sa.String(128), nullable=False),
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
            "organization_id", "label", name="uq_org_service_label"
        ),
    )

    # --- Update organization slot columns ---
    # Remove old credential label columns
    op.drop_column("organizations", "publishing_credential_label")
    op.drop_column("organizations", "staging_credential_label")

    # Add new service slot columns
    op.add_column(
        "organizations",
        sa.Column("publishing_store_label", sa.String(128), nullable=True),
    )
    op.add_column(
        "organizations",
        sa.Column("staging_store_label", sa.String(128), nullable=True),
    )
    op.add_column(
        "organizations",
        sa.Column("cdn_service_label", sa.String(128), nullable=True),
    )
    op.add_column(
        "organizations",
        sa.Column("dns_service_label", sa.String(128), nullable=True),
    )


def downgrade() -> None:
    # Remove new slot columns
    op.drop_column("organizations", "dns_service_label")
    op.drop_column("organizations", "cdn_service_label")
    op.drop_column("organizations", "staging_store_label")
    op.drop_column("organizations", "publishing_store_label")

    # Restore old credential label columns
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

    # Drop services table
    op.drop_table("organization_services")

    # Recreate old credentials table
    op.drop_table("organization_credentials")
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
