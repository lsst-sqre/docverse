"""SQLAlchemy ORM model for the ``projects`` table."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SqlProject(Base):
    """ORM model for the ``projects`` table."""

    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # Stable time-ordered Crockford Base32 identifier exposed on the wire
    # as ``id``. Minted in application code (never by a sequence), it
    # survives a slug rename, so a consumer polling the project listing
    # can keep following the same resource.
    public_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False, autoincrement=False
    )

    slug: Mapped[str] = mapped_column(String(128), nullable=False)

    title: Mapped[str] = mapped_column(String(256), nullable=False)

    org_id: Mapped[int] = mapped_column(Integer, nullable=False)

    # Free-form URL to the documentation source repository. Nullable so
    # projects can be created GitHub-binding-only (with ``github_owner``
    # / ``github_repo`` populated) without a redundant URL string.
    source_url: Mapped[str | None] = mapped_column(String(512), nullable=True)

    # Structured GitHub coordinates for the documentation source repo.
    # ``github_owner`` / ``github_repo`` are the operator-supplied source
    # of truth; the ``_id`` and ``installation_id`` columns are captured
    # opportunistically and may stay NULL forever for projects whose
    # GitHub App is not installed. The check constraint enforces that
    # ``github_owner`` and ``github_repo`` are populated together.
    github_owner: Mapped[str | None] = mapped_column(String(39), nullable=True)
    github_repo: Mapped[str | None] = mapped_column(String(100), nullable=True)
    github_owner_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    github_repo_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    github_installation_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    slug_rewrite_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    lifecycle_rules: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    edition_autocreation: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )

    date_created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # The project's clock, and the one rule every writer obeys: this
    # ``onupdate`` fires on any UPDATE of this row that does not name
    # the column, so a write that changes something advances the clock
    # without the writer saying so (PRD #634, tasks #644 / #651). That
    # clock is a change signal for pollers such as Ook — it feeds the
    # ``updated_since`` filter, the listing's ``clock_sum`` watermark,
    # and the ETag on every project endpoint — not a "last operator
    # edit" marker.
    #
    # Two consequences for writers, and no third idiom:
    #
    # * Never stamp ``date_updated=func.now()`` explicitly. It emits
    #   the same SQL the default already emits, and a writer that
    #   copies the line stops thinking about whether its write is a
    #   change at all. The one exception lives outside this table's own
    #   writers: ``EditionStore.set_current_build`` updates this row
    #   from the edition side with no other column to carry the
    #   ``onupdate``, so it names the column deliberately.
    # * A write that may be a no-op — a redelivered webhook, a resolve
    #   re-reading ids it already stored — narrows its ``WHERE`` with
    #   ``IS DISTINCT FROM`` on the columns it sets, so the row is
    #   never reached and the clock never moves. Pinning the column to
    #   its own value would suppress the clock for *real* changes too;
    #   the predicate distinguishes the two.
    date_updated: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    date_deleted: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("org_id", "slug", name="uq_projects_org_slug"),
        CheckConstraint(
            "(github_owner IS NULL) = (github_repo IS NULL)",
            name="ck_projects_github_owner_repo_both_or_neither",
        ),
        # Serves ``order=date_updated`` and the ``updated_since`` filter
        # on the project listing: ``org_id`` is the equality prefix,
        # ``date_updated`` the range key, and ``id`` the keyset cursor's
        # tiebreak, so a poll on one org is an index range scan rather
        # than a sort over the org's whole project set.
        #
        # It is also the only index an ``org_id``-only lookup needs:
        # ``org_id`` leads it, so PostgreSQL scans this index for those
        # too. A separate single-column ``org_id`` index would be pure
        # write amplification now that ``date_updated`` is indexed —
        # every clock stamp already has to touch this one.
        Index(
            "idx_projects_org_date_updated",
            "org_id",
            "date_updated",
            "id",
        ),
        Index(
            "idx_projects_slug_trgm",
            "slug",
            postgresql_using="gin",
            postgresql_ops={"slug": "gin_trgm_ops"},
        ),
        Index(
            "idx_projects_title_trgm",
            "title",
            postgresql_using="gin",
            postgresql_ops={"title": "gin_trgm_ops"},
        ),
        Index(
            "idx_projects_github_owner_repo",
            text("lower(github_owner)"),
            text("lower(github_repo)"),
        ),
        Index(
            "idx_projects_github_repo_id",
            "github_repo_id",
        ),
    )
