"""Null redundant github.com ``source_url`` values on ``projects``.

The structured ``github`` binding is now the single source of truth for
GitHub-backed projects, and a project's effective source URL is derived
from that binding on read. The free-form ``source_url`` column therefore
stores only non-GitHub URLs (or NULL); a stored github.com value is
redundant and can drift out of agreement with the binding. This
data-only migration nulls every ``source_url`` that parses as a
github.com repository URL — exactly the values the ``ProjectCreate`` /
``ProjectUpdate`` validators now reject on the way in.

The parse mirrors
:func:`docverse.client.models.projects.parse_github_url` (host is
``github.com``, case-insensitive; at least two non-empty path segments)
so the rows nulled here are precisely the rows the API would now refuse
to accept. A copy of the predicate lives in this module so the migration
stays pinned to today's semantics even if the app helper later changes.

The downgrade best-effort re-derives ``source_url`` from the binding so
a roll-back restores the pre-migration shape for GitHub-bound rows; path
tails and ``.git`` suffixes that the upgrade discarded cannot be
recovered.

Revision ID: y3z4a5b6c7d8
Revises: x2y3z4a5b6c7
Create Date: 2026-05-25 00:01:00.000000+00:00
"""

from __future__ import annotations

from urllib.parse import urlparse

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "y3z4a5b6c7d8"
down_revision: str | None = "x2y3z4a5b6c7"
branch_labels: str | None = None
depends_on: str | None = None


def _is_github_url(url: str) -> bool:
    """Return ``True`` when ``url`` is a github.com repository URL.

    Mirrors ``parse_github_url``: the host must be ``github.com``
    (case-insensitive) and the path must carry at least two non-empty
    segments. Deeper paths (``/tree/main/docs``) and a ``.git`` suffix
    still count as a GitHub repository URL.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host != "github.com":
        return False
    segments = [s for s in parsed.path.split("/") if s]
    min_segments = 2
    return len(segments) >= min_segments


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id, source_url FROM projects WHERE source_url IS NOT NULL"
        )
    ).fetchall()
    redundant_ids = [row.id for row in rows if _is_github_url(row.source_url)]
    for project_id in redundant_ids:
        bind.execute(
            sa.text("UPDATE projects SET source_url = NULL WHERE id = :id"),
            {"id": project_id},
        )


def downgrade() -> None:
    # Best-effort: re-derive the canonical github.com URL for every row
    # that carries a binding but no stored source_url. Path tails and
    # ``.git`` suffixes the upgrade discarded cannot be recovered.
    op.execute(
        "UPDATE projects"
        " SET source_url = 'https://github.com/'"
        " || github_owner || '/' || github_repo"
        " WHERE github_owner IS NOT NULL"
        " AND github_repo IS NOT NULL"
        " AND source_url IS NULL"
    )
