"""Make ``edition_build_history`` positions unique per edition.

Edition-reconciliation PRD #612 / DM-54683, from the review of PR #621
(issue #626).

``position`` is what "newest" means for an edition: every writer of
``publish_status`` resolves a ``(edition_id, build_id)`` pair through the
lowest-position row, and the reconciliation planner reads the same row
from the batched lookup. Nothing ever made those positions unique.
``EditionBuildHistoryStore.record`` bumps an edition's positions and then
inserts at position 1 as two statements, so under READ COMMITTED an API
rollback (which takes no ``EDITION_UPDATE`` advisory lock) racing a
worker's tracking update could each bump a history the other's insert was
not yet visible in and both commit a row at position 1. The two readers
could then pick different rows for one pair, the publish would mark one
``published`` while the planner read the other as pending with no live
job, and the reconciler would re-drive that publish on every tick — the
loop ``cdd631c`` closed, reopened from underneath.

The constraint is **deferrable**. ``record()``'s bump rewrites a whole
edition's rows in one ``UPDATE``, and an immediate unique index checks
each rewritten row as it goes: it would reject the bump the moment it
turned a 1 into a 2 that a row it had not reached yet still held, and
which row a seq scan reaches first is the heap's business.
``INITIALLY IMMEDIATE`` keeps the check at the end of every statement —
no caller has to opt in, and nobody gets a violation deferred to commit —
while letting the statement's own intermediate states alone.

Ties that already exist are renumbered rather than refused. The table has
run unconstrained for its whole life, so the upgrade cannot assume a
clean read; it renumbers each *tied* edition densely from 1 in
``(position ASC, id DESC)`` order, which is the order the readers were
already resolving ties in, so no edition's history is reordered relative
to what the running code saw. Editions with no tie are left alone, gaps
included: this is a repair, not a normalization pass over every row.

``idx_ebh_edition_position`` is dropped because the constraint's own
index covers exactly the same columns; keeping both would pay for two
identical btrees on a table written once per build.

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-09-11 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e6f7a8b9c0d1"
down_revision: str | None = "d5e6f7a8b9c0"
branch_labels: str | None = None
depends_on: str | None = None

CONSTRAINT_NAME = "uq_ebh_edition_position"

INDEX_NAME = "idx_ebh_edition_position"

_RENUMBER_TIES = sa.text(
    """
    UPDATE edition_build_history AS h
    SET position = renumbered.new_position
    FROM (
        SELECT
            id,
            row_number() OVER (
                PARTITION BY edition_id
                ORDER BY position ASC, id DESC
            ) AS new_position
        FROM edition_build_history
        WHERE edition_id IN (
            SELECT edition_id
            FROM edition_build_history
            GROUP BY edition_id, position
            HAVING count(*) > 1
        )
    ) AS renumbered
    WHERE h.id = renumbered.id
      AND h.position <> renumbered.new_position
    """
)
"""Collapse every tied edition onto a dense 1..N numbering.

Scoped to editions that actually have a tie, so a table with none is
untouched and the upgrade is a no-op it can be re-run over. The ordering
matches the readers' ``ORDER BY position ASC, id DESC``.
"""


def upgrade() -> None:
    op.execute(_RENUMBER_TIES)
    op.create_unique_constraint(
        CONSTRAINT_NAME,
        "edition_build_history",
        ["edition_id", "position"],
        deferrable=True,
        initially="IMMEDIATE",
    )
    op.drop_index(INDEX_NAME, table_name="edition_build_history")


def downgrade() -> None:
    op.create_index(
        INDEX_NAME,
        "edition_build_history",
        ["edition_id", "position"],
    )
    op.drop_constraint(
        CONSTRAINT_NAME, "edition_build_history", type_="unique"
    )
