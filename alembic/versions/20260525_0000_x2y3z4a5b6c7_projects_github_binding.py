"""Add structured GitHub binding to ``projects`` and rename ``doc_repo``.

Formalises a project's GitHub coordinates ahead of the ``ref_deleted``
lifecycle-rule work (PRD #346 / DM-54913). Five new nullable columns
land on ``projects`` for the operator-supplied owner/repo pair and the
opportunistically-captured numeric ids + installation id, the free-form
``doc_repo`` URL column is renamed to nullable ``source_url`` (existing
values flow through unchanged), and a both-or-neither check constraint
plus two webhook-lookup indexes round out the structural shape.

The data step parses every existing ``doc_repo`` value as a
``github.com`` URL and back-fills ``github_owner`` / ``github_repo``
from the first two path segments. Any URL whose host is not
``github.com`` aborts the migration loudly with the offending project
ids surfaced in the error message. The expectation is zero non-GitHub
rows in production today; the abort is the safety net so a stray row
cannot end up with NULL structured columns silently after the rename.

Revision ID: x2y3z4a5b6c7
Revises: w1x2y3z4a5b6
Create Date: 2026-05-25 00:00:00.000000+00:00
"""

from __future__ import annotations

from urllib.parse import urlparse

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "x2y3z4a5b6c7"
down_revision: str | None = "w1x2y3z4a5b6"
branch_labels: str | None = None
depends_on: str | None = None


def _parse_github_doc_repo(url: str) -> tuple[str, str] | None:
    """Parse a ``github.com`` URL into ``(owner, repo)``.

    Returns ``None`` if the host is not ``github.com`` (case-insensitive)
    or if the path does not contain two non-empty segments. The migration
    treats both cases as parse failures so the abort path lists the
    offending project id either way.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host != "github.com":
        return None
    segments = [s for s in parsed.path.split("/") if s]
    min_segments = 2
    if len(segments) < min_segments:
        return None
    owner = segments[0]
    # Trim the conventional ``.git`` suffix so the parsed repo matches
    # what GitHub returns for the repo name.
    repo = segments[1].removesuffix(".git")
    return owner, repo


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("github_owner", sa.String(39), nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("github_repo", sa.String(100), nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("github_owner_id", sa.BigInteger, nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("github_repo_id", sa.BigInteger, nullable=True),
    )
    op.add_column(
        "projects",
        sa.Column("github_installation_id", sa.BigInteger, nullable=True),
    )

    # Backfill structured GitHub columns from the existing free-form
    # ``doc_repo`` URLs before the rename. Any non-github.com row aborts
    # the migration with an operator-readable list of project ids.
    bind = op.get_bind()
    rows = bind.execute(
        sa.text("SELECT id, doc_repo FROM projects")
    ).fetchall()
    invalid_ids: list[int] = []
    parsed: list[tuple[int, str, str]] = []
    for row in rows:
        result = _parse_github_doc_repo(row.doc_repo)
        if result is None:
            invalid_ids.append(row.id)
            continue
        owner, repo = result
        parsed.append((row.id, owner, repo))
    if invalid_ids:
        invalid_ids.sort()
        message = (
            "Cannot rename projects.doc_repo to source_url: the following"
            " project ids have a doc_repo value that could not be parsed as"
            " a https://github.com/<owner>/<repo> URL (the host is not"
            " github.com, or the path has fewer than two segments):"
            f" {invalid_ids}. Resolve these rows (either fix the URL or"
            " migrate them to a non-GitHub host once the source_url column"
            " lands in a later release) and re-run the migration."
        )
        raise RuntimeError(message)
    for project_id, owner, repo in parsed:
        bind.execute(
            sa.text(
                "UPDATE projects SET github_owner = :owner,"
                " github_repo = :repo WHERE id = :id"
            ),
            {"owner": owner, "repo": repo, "id": project_id},
        )

    # Rename ``doc_repo`` to ``source_url`` and drop its NOT NULL.
    op.alter_column(
        "projects",
        "doc_repo",
        new_column_name="source_url",
        existing_type=sa.String(512),
        nullable=True,
    )

    op.create_check_constraint(
        "ck_projects_github_owner_repo_both_or_neither",
        "projects",
        "(github_owner IS NULL) = (github_repo IS NULL)",
    )

    op.execute(
        "CREATE INDEX idx_projects_github_owner_repo"
        " ON projects (lower(github_owner), lower(github_repo))"
    )
    op.create_index(
        "idx_projects_github_repo_id",
        "projects",
        ["github_repo_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_projects_github_repo_id", table_name="projects")
    op.drop_index("idx_projects_github_owner_repo", table_name="projects")
    op.drop_constraint(
        "ck_projects_github_owner_repo_both_or_neither",
        "projects",
        type_="check",
    )
    op.alter_column(
        "projects",
        "source_url",
        new_column_name="doc_repo",
        existing_type=sa.String(512),
        nullable=False,
    )
    op.drop_column("projects", "github_installation_id")
    op.drop_column("projects", "github_repo_id")
    op.drop_column("projects", "github_owner_id")
    op.drop_column("projects", "github_repo")
    op.drop_column("projects", "github_owner")
