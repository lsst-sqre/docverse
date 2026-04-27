"""Pydantic models for dashboard-template binding resources."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "DashboardTemplateBinding",
    "DashboardTemplateBindingCreate",
    "normalize_github_ref",
]

_REF_PREFIXES = ("refs/heads/", "refs/tags/")


def normalize_github_ref(value: str) -> str:
    """Strip a leading ``refs/heads/`` or ``refs/tags/`` prefix from a ref.

    GitHub push payloads carry refs in their fully-qualified form
    (``refs/heads/main``, ``refs/tags/v1.0``), but operators register
    bindings using the bare branch or tag name (``main``, ``v1.0``).
    This helper is the canonical normalizer used at both write seams:
    the ``DashboardTemplateBindingCreate`` validator (so PUT bodies
    from operators land in canonical form) and the push-event processor
    (so webhook lookups match bindings stored in canonical form).

    Refs that don't start with one of the known prefixes — bare names,
    commit SHAs, ``refs/pull/...``, ``refs/remotes/...`` — pass through
    unchanged. ``refs/heads/`` or ``refs/tags/`` alone normalize to the
    empty string, which fails the binding's ``min_length=1`` constraint
    on the model side.
    """
    for prefix in _REF_PREFIXES:
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


class DashboardTemplateBindingCreate(BaseModel):
    """Request model for creating or updating a dashboard-template binding."""

    model_config = ConfigDict(extra="forbid")

    github_owner: Annotated[
        str,
        Field(
            min_length=1,
            max_length=39,
            pattern=r"^[A-Za-z0-9](?:-?[A-Za-z0-9])*$",
            description="GitHub owner (user or organization login).",
        ),
    ]

    github_repo: Annotated[
        str,
        Field(
            min_length=1,
            max_length=100,
            pattern=r"^[A-Za-z0-9._-]+$",
            description="GitHub repository name.",
        ),
    ]

    github_ref: Annotated[
        str,
        Field(
            min_length=1,
            max_length=256,
            description=(
                "Git ref (branch, tag, or commit SHA) to sync from."
                " Stored in bare form (``main``, ``v1.0``); a leading"
                " ``refs/heads/`` or ``refs/tags/`` prefix on input is"
                " stripped automatically so GitHub push events match"
                " operator-supplied bindings."
            ),
        ),
    ]

    @field_validator("github_ref", mode="before")
    @classmethod
    def _strip_ref_prefix(cls, value: object) -> object:
        if isinstance(value, str):
            return normalize_github_ref(value)
        return value

    root_path: Annotated[
        str,
        Field(
            min_length=1,
            max_length=512,
            description=(
                "Path within the repository where the template lives."
                " Defaults to ``/`` (repo root)."
            ),
        ),
    ] = "/"


class DashboardTemplateBinding(BaseModel):
    """Response model for a dashboard-template binding."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this binding resource.")

    web_url: str = Field(
        description=(
            "URL of the template's location in the GitHub browse UI. "
            "Always present once a binding exists; derived from the "
            "binding's source coordinates rather than the synced "
            "content row, so it is meaningful before the first sync."
        )
    )

    github_owner: str = Field(description="GitHub owner (user or org).")

    github_repo: str = Field(description="GitHub repository name.")

    github_ref: str = Field(description="Git ref (branch, tag, or SHA).")

    root_path: str = Field(
        description="Path within the repo where the template lives."
    )

    commit_sha: str | None = Field(
        default=None,
        description=(
            "Commit SHA of the most-recently-synced template content. "
            "``None`` until the first successful sync."
        ),
    )

    last_sync_status: str = Field(
        description="One of ``pending``, ``succeeded``, ``failed``."
    )

    last_sync_error: str | None = Field(
        default=None,
        description="Operator-readable error from the most recent sync.",
    )

    last_sync_queue_job_url: str | None = Field(
        default=None,
        description=(
            "URL of the most-recently-enqueued ``dashboard_sync`` queue "
            "job, or ``None`` if no sync has been enqueued for this "
            "binding yet (or the referenced job has been pruned)."
        ),
    )

    github_owner_id: int | None = Field(
        default=None,
        description=(
            "GitHub's stable numeric ID for the owner. Captured on first "
            "successful sync; ``None`` for un-synced bindings. Informational "
            "only — the public API remains keyed on ``github_owner``."
        ),
    )

    github_repo_id: int | None = Field(
        default=None,
        description=(
            "GitHub's stable numeric ID for the repository. Captured on "
            "first successful sync; ``None`` for un-synced bindings. "
            "Informational only — the public API remains keyed on "
            "``github_repo``."
        ),
    )

    github_installation_id: int | None = Field(
        default=None,
        description=(
            "GitHub App installation ID for the repository. Captured on "
            "first successful sync; ``None`` for un-synced bindings or when "
            "the GitHub App is not installed. Informational only."
        ),
    )

    date_created: datetime = Field(
        description="Timestamp when the binding was created."
    )

    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
