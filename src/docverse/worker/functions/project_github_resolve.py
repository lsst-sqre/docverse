"""Project GitHub binding resolver worker function.

Resolves a project's GitHub App installation id and the numeric
owner / repo ids opportunistically, after a project create or update
that supplied a ``github`` sub-object. A failure (no installation,
transient HTTP, etc.) is logged but never re-raised — the columns
stay NULL and a later install of the GitHub App will backfill them
through the ``installation`` webhook (PRD #346 user stories 12 / 13).
"""

from __future__ import annotations

from typing import Any

import gidgethub
import httpx
import jwt.exceptions
import sentry_sdk
import structlog
from safir.dependencies.db_session import db_session_dependency

__all__ = ["project_github_resolve"]


async def project_github_resolve(
    ctx: dict[str, Any], payload: dict[str, Any]
) -> str:
    """Resolve and persist a project's opportunistic GitHub ids.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``).
    payload
        Job payload with ``project_id``.

    Returns
    -------
    str
        ``"completed"`` on a successful resolve, ``"skipped"`` when the
        project has no GitHub binding (or has been deleted), or
        ``"failed"`` when GitHub returned an error or the columns
        could not be written.
    """
    project_id: int = payload["project_id"]
    logger = structlog.get_logger(
        "docverse.worker.project_github_resolve"
    ).bind(project_id=project_id)

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        project_store = factory.create_project_store()

        async with session.begin():
            project = await project_store.get_by_id(project_id)
        if project is None:
            logger.info("Skipping resolve: project not found")
            return "skipped"

        owner = project.github_owner
        repo = project.github_repo
        if owner is None or repo is None:
            logger.info("Skipping resolve: project has no GitHub binding")
            return "skipped"

        logger = logger.bind(github_owner=owner, github_repo=repo)

        try:
            app_client = factory.create_github_app_client()
            metadata = await app_client.resolve_repository_metadata(
                owner=owner, repo=repo
            )
        except (
            httpx.HTTPError,
            gidgethub.GitHubException,
            jwt.exceptions.InvalidKeyError,
        ) as exc:
            sentry_sdk.capture_exception(exc)
            logger.warning(
                "Failed to resolve project GitHub metadata",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return "failed"

        async with session.begin():
            updated = await project_store.update_github_metadata(
                project_id=project_id,
                expected_owner=owner,
                expected_repo=repo,
                installation_id=metadata.installation_id,
                owner_id=metadata.owner_id,
                repo_id=metadata.repo_id,
            )
            await session.commit()

        if not updated:
            logger.info(
                "Skipping persist: project binding changed during resolve"
            )
            return "skipped"

        logger.info(
            "Resolved project GitHub metadata",
            github_installation_id=metadata.installation_id,
            github_owner_id=metadata.owner_id,
            github_repo_id=metadata.repo_id,
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
