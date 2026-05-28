"""Add partial index backing the keeper-sync tombstones listing.

The admin ``GET /orgs/{org}/keeper-sync/tombstones`` endpoint pages one
org's tombstoned ``keeper_sync_state`` rows ordered by
``date_tombstoned DESC, id DESC`` (see
``KeeperSyncStateDateTombstonedCursor``). Without a supporting index that
query filters ``org_id`` off ``idx_keeper_sync_state_org_id`` and then
sorts the org's matching rows in memory.

The partial index ``(org_id, date_tombstoned) WHERE date_tombstoned IS
NOT NULL`` serves both the filter and the ordering directly. The partial
predicate keeps the index small — only tombstoned rows, the rare case,
are indexed — and PostgreSQL scans the ``date_tombstoned`` key backward
to satisfy the ``DESC`` ordering, so no explicit ``DESC`` modifier is
needed. The optional ``resource_type`` / ``tombstone_reason`` filters
are applied as cheap residual predicates on the rows the index returns.

Revision ID: b6c7d8e9f0a1
Revises: a5b6c7d8e9f0
Create Date: 2026-05-27 00:01:00.000000+00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b6c7d8e9f0a1"
down_revision: str | None = "a5b6c7d8e9f0"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_index(
        "idx_keeper_sync_state_org_date_tombstoned",
        "keeper_sync_state",
        ["org_id", "date_tombstoned"],
        postgresql_where=sa.text("date_tombstoned IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_keeper_sync_state_org_date_tombstoned",
        table_name="keeper_sync_state",
    )
