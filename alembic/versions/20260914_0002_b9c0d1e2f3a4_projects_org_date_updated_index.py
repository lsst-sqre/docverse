"""Index ``projects`` on ``(org_id, date_updated, id)``, dropping the prefix.

Part of the Ook project-listing work (PRD #634 / DM-56083). The project
listing gained two ways of reading an organization by the clock rather
than by slug: ``order=date_updated`` (newest-touched first, ``id`` as the
tiebreak) and the ``updated_since`` filter a poller uses to ask for only
what changed. Both narrow to one ``org_id`` and then range over
``date_updated``.

``idx_projects_org_id`` stopped at the organization, so without this
index every poll would have Postgres fetch the org's whole project set
and sort or filter it in memory. The composite is ordered to match how
the queries consume it: ``org_id`` as the equality prefix,
``date_updated`` as the range key, and ``id`` last so the keyset
cursor's tiebreak is satisfied from the index rather than a heap
lookup. A plain btree serves the cursor's ``DESC`` ordering as well as
an ``ASC`` scan — Postgres reads either direction — so no explicit
``DESC`` ordering is declared.

``idx_projects_org_id`` is then dropped, because the composite is a
superset of it: ``org_id`` leads the new index, so Postgres answers an
``org_id``-only lookup from it just as well. Keeping both would cost
writes rather than buy reads. Indexing ``date_updated`` takes every
clock stamp off PostgreSQL's HOT path, so each ``__main`` republish
(fleet-wide per keeper-sync tick), each GitHub-binding write, and each
PATCH now writes an entry into *every* index on the row — and the
redundant prefix would take one of those entries on the hottest write
path in the table for nothing.

Revision ID: b9c0d1e2f3a4
Revises: a8b9c0d1e2f3
Create Date: 2026-09-14 00:02:00.000000+00:00
"""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b9c0d1e2f3a4"
down_revision: str | None = "a8b9c0d1e2f3"
branch_labels: str | None = None
depends_on: str | None = None

INDEX_NAME = "idx_projects_org_date_updated"

#: Single-column prefix index this migration replaces.
ORG_ID_INDEX_NAME = "idx_projects_org_id"


def upgrade() -> None:
    op.create_index(
        INDEX_NAME,
        "projects",
        ["org_id", "date_updated", "id"],
    )
    op.drop_index(ORG_ID_INDEX_NAME, table_name="projects")


def downgrade() -> None:
    op.create_index(ORG_ID_INDEX_NAME, "projects", ["org_id"])
    op.drop_index(INDEX_NAME, table_name="projects")
