"""Domain model for dashboard GitHub template bindings."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DashboardGitHubTemplateBinding(BaseModel):
    """A configuration row binding an org or project to a GitHub source.

    A binding with ``project_id`` set is a project-specific override; a
    binding with ``project_id`` ``None`` is the org default. Bindings
    point at a synced :class:`DashboardGitHubTemplate` row via
    ``github_template_id`` once the first sync has succeeded.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the binding.")

    org_id: int = Field(description="ID of the owning organization.")

    project_id: int | None = Field(
        default=None,
        description="Project ID for an override; ``None`` for org default.",
    )

    github_owner: str = Field(description="GitHub owner (user or org).")
    github_repo: str = Field(description="GitHub repository name.")
    github_ref: str = Field(description="Git ref (branch, tag, or SHA).")
    root_path: str = Field(
        description="Path within the repo where the template lives."
    )

    github_template_id: int | None = Field(
        default=None,
        description=(
            "ID of the synced GitHub template row, or ``None`` until first "
            "sync."
        ),
    )

    last_sync_status: str = Field(
        description="One of ``pending``, ``succeeded``, ``failed``."
    )
    last_sync_error: str | None = Field(
        default=None,
        description="Operator-readable error from the most recent sync.",
    )

    date_created: datetime = Field(
        description="Timestamp when the binding was created."
    )
    date_updated: datetime = Field(
        description="Timestamp of the most recent update."
    )
