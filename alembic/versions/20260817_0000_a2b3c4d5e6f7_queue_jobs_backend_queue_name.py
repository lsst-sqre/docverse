"""Record the enqueueing queue's name on ``queue_jobs``.

Review follow-up to the abandoned reaper sweep (PRD #538 / DM-55807).
arq resolves a job's *status* through its queue's sorted set, so a
lookup aimed at the wrong queue answers "no record" — the exact answer
the abandoned sweep reads as "arq lost this job". Verification used to
compensate by walking a hand-listed tuple of Docverse's pool queues,
which silently misses any pool queue added later. Recording the queue
each job was enqueued onto lets the sweep probe that one queue directly.

Nullable with no backfill: rows written before this migration genuinely
do not know which pool holds their job, and there is nothing in the
database to derive it from. ``ArqQueueBackend.get_job_metadata`` keeps
the multi-queue walk for exactly those rows, so they stay verifiable
until they age out.

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-08-17 00:00:00.000000+00:00
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a2b3c4d5e6f7"
down_revision: str | None = "f1a2b3c4d5e6"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "queue_jobs",
        sa.Column("backend_queue_name", sa.String(length=256), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("queue_jobs", "backend_queue_name")
