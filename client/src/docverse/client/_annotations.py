"""Auto-detection of build provenance annotations from CI environments."""

from __future__ import annotations

import os

from .models.builds import BuildAnnotations

__all__ = [
    "detect_github_actions_annotations",
    "merge_annotations",
]


def detect_github_actions_annotations() -> BuildAnnotations | None:
    """Detect build provenance from GitHub Actions environment variables.

    Returns a populated ``BuildAnnotations`` if running inside GitHub
    Actions (``GITHUB_ACTIONS == "true"``), otherwise ``None``.
    """
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return None

    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repository = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")

    run_url: str | None = None
    if repository and run_id:
        run_url = f"{server_url}/{repository}/actions/runs/{run_id}"

    return BuildAnnotations(
        commit_sha=os.environ.get("GITHUB_SHA"),
        github_repository=repository,
        github_run_id=run_id,
        github_run_url=run_url,
        github_run_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
        github_workflow=os.environ.get("GITHUB_WORKFLOW"),
        github_actor=os.environ.get("GITHUB_ACTOR"),
        github_event_name=os.environ.get("GITHUB_EVENT_NAME"),
        ci_platform="github-actions",
    )


def merge_annotations(
    auto: BuildAnnotations | None,
    manual: dict[str, str] | None,
) -> BuildAnnotations | None:
    """Merge auto-detected and manual annotations.

    Manual entries take precedence over auto-detected values.  Returns
    ``None`` if both inputs are empty/None.
    """
    result: dict[str, str] = {}
    if auto is not None:
        result.update(
            {k: v for k, v in auto.model_dump().items() if v is not None}
        )
    if manual:
        result.update(manual)
    if not result:
        return None
    return BuildAnnotations.model_validate(result)
