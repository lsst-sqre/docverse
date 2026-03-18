"""Rename build status 'uploading' to 'pending', add 'uploaded'.

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-03-16 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c2d3e4f5a6b7"
down_revision: str | None = "b1c2d3e4f5a6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Migrate existing rows from 'uploading' to 'pending'
    op.execute(
        "UPDATE builds SET status = 'pending' WHERE status = 'uploading'"
    )

    # Drop the old CHECK constraint and create a new one with updated values.
    # The constraint name follows SQLAlchemy's naming for non-native enums.
    op.execute(
        "ALTER TABLE builds DROP CONSTRAINT IF EXISTS"
        " ck_builds_status_buildstatus"
    )
    # Also try the generic name pattern SQLAlchemy may generate
    op.execute(
        "ALTER TABLE builds DROP CONSTRAINT IF EXISTS builds_status_check"
    )
    op.execute(
        "ALTER TABLE builds ADD CONSTRAINT builds_status_check"
        " CHECK (status IN ('pending', 'uploaded', 'processing',"
        " 'completed', 'failed'))"
    )


def downgrade() -> None:
    # Migrate rows back from 'pending' to 'uploading'
    op.execute(
        "UPDATE builds SET status = 'uploading' WHERE status = 'pending'"
    )

    op.execute(
        "ALTER TABLE builds DROP CONSTRAINT IF EXISTS builds_status_check"
    )
    op.execute(
        "ALTER TABLE builds ADD CONSTRAINT builds_status_check"
        " CHECK (status IN ('uploading', 'processing',"
        " 'completed', 'failed'))"
    )
