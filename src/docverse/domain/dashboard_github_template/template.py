"""Domain model for a dashboard GitHub template."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class DashboardGitHubTemplate(BaseModel):
    """One synced template tree, keyed by its GitHub source location.

    The bytes of ``template.toml`` are stored on the row itself; the
    individual file blobs live in the
    :class:`DashboardGitHubTemplateFile` rows that reference this row.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Unique identifier for the template row.")

    github_owner: str = Field(description="GitHub owner (user or org).")
    github_repo: str = Field(description="GitHub repository name.")
    github_ref: str = Field(description="Git ref (branch, tag, or SHA).")
    root_path: str = Field(
        description="Path within the repo where the template lives."
    )

    github_owner_id: int | None = Field(
        default=None,
        description=(
            "Stable GitHub numeric ID of the owner, or ``None`` until first "
            "successful sync captures it."
        ),
    )
    github_repo_id: int | None = Field(
        default=None,
        description=(
            "Stable GitHub numeric ID of the repository, or ``None`` until "
            "first successful sync captures it."
        ),
    )

    commit_sha: str = Field(
        description="Git commit SHA captured from the sync source."
    )
    etag: str = Field(
        description="GitHub ETag (or tree SHA) used for change detection."
    )

    template_toml: bytes = Field(
        description="Raw bytes of the ``template.toml`` file."
    )

    date_synced: datetime = Field(
        description="Timestamp of the most recent successful sync."
    )
