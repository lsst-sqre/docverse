"""Service stamping keeper-sync push hints from a GitHub ``push`` event."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import structlog

from docverse.models.dashboard_template import normalize_github_ref
from docverse_server.domain.organization import Organization
from docverse_server.domain.project import Project
from docverse_server.services.github_payload import repository_coordinates
from docverse_server.services.keeper_sync.push_hints import (
    ANNOTATION_GITHUB_PUSHED_REFS,
    stamp_pushed_ref,
)
from docverse_server.storage.keeper_sync import (
    KeeperSyncStateStore,
    ResourceType,
)
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

__all__ = [
    "KeeperSyncPushProcessor",
    "KeeperSyncPushSkip",
    "StampedProject",
]

_TRACKED_REF_PREFIXES = ("refs/heads/", "refs/tags/")
"""The fully-qualified ref prefixes of a push keeper-sync acts on."""


class KeeperSyncPushSkip(StrEnum):
    """Why a project bound to a pushed repository was not stamped.

    Logged as ``reason`` on ``Skipped project for keeper-sync push``.
    """

    sync_disabled = "sync_disabled"
    """The project's organization has keeper-sync off, or no config."""

    out_of_scope = "out_of_scope"
    """The project's slug is outside its organization's sync scope."""

    no_state = "no_state"
    """The project has no live ``keeper_sync_state`` project row: it
    was never synced from LTD, or its row is tombstoned."""


@dataclass(frozen=True, slots=True)
class StampedProject:
    """One project a ``push`` delivery stamped with a push hint."""

    org_slug: str
    """Slug of the organization that owns the project."""

    project_slug: str
    """The project's slug, which is also its LTD product slug."""

    github_ref: str
    """The ref stamped, normalized (``main``, not ``refs/heads/main``)."""


class KeeperSyncPushProcessor:
    """Stamp a ``push`` onto the LTD-synced projects of its repository.

    A push cannot sync anything itself: the repository's CI builds and
    uploads the docs to LTD Keeper afterwards. It is a hint that LTD is
    about to change, so this processor records it — the pushed ref and
    the delivery's time, in the ``github_pushed_refs`` annotation of
    each project's ``keeper_sync_state`` row (see
    :mod:`~docverse_server.services.keeper_sync.push_hints`) — and the
    keeper-sync tier crons poll the project on their fast path while the
    stamp is inside its window (PRD #803).

    Only a push that updates a branch or a tag is stamped; a push that
    deleted its ref is the ``delete`` event's business. The projects are
    found by ``repository.id``, with the ``(owner, name)`` fallback for
    projects whose numeric id is still unresolved, as the other
    repository-keyed processors find them
    (:meth:`ProjectStore.list_by_github_repo`). Of those, a project is
    stamped only when its organization has keeper-sync enabled, its slug
    is in the organization's sync scope
    (:meth:`~docverse.models.KeeperSyncConfig.is_in_scope`), and it has
    a live project state row. A repository bound to several such
    projects stamps every one.

    Everything else — the hot path switched off, a ref that is neither a
    branch nor a tag, a deletion, a payload without a repository, a
    repository with no synced project — is logged at info and ignored,
    so the handler still answers 200. Nothing here calls GitHub.

    The caller (the webhook handler) owns the surrounding transaction;
    the processor only writes through
    :meth:`KeeperSyncStateStore.upsert` and never opens its own
    ``session.begin()``. Each state row is read ``FOR UPDATE`` before
    its annotations are merged and written back whole, so two
    deliveries stamping the same project — a branch and a tag pushed
    together, say — cannot drop each other's ref.
    """

    def __init__(
        self,
        *,
        project_store: ProjectStore,
        org_store: OrganizationStore,
        state_store: KeeperSyncStateStore,
        logger: structlog.stdlib.BoundLogger,
        enabled: bool,
        window: timedelta,
    ) -> None:
        self._project_store = project_store
        self._org_store = org_store
        self._state_store = state_store
        self._logger = logger
        self._enabled = enabled
        self._window = window

    async def process(
        self,
        payload: Mapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> list[StampedProject]:
        """Stamp the push on every eligible project and return them.

        Parameters
        ----------
        payload
            The ``push`` delivery's payload.
        now
            The push time to stamp; the current time when omitted.

        Returns
        -------
        list of StampedProject
            The projects stamped, in project id order. Empty when the
            push was ignored.
        """
        coordinates = repository_coordinates(payload.get("repository"))
        owner, repo_name = coordinates.owner, coordinates.name
        raw_ref = payload.get("ref")
        logger = self._logger.bind(
            github_owner=owner,
            github_repo=repo_name,
            github_repo_id=coordinates.repo_id,
            github_ref_raw=raw_ref,
        )
        if not self._enabled:
            logger.info("Keeper-sync push hot path is disabled, not stamping")
            return []
        ref = _tracked_ref(raw_ref)
        if ref is None:
            logger.info("Ignoring push to a ref keeper-sync does not track")
            return []
        logger = logger.bind(github_ref=ref)
        if payload.get("deleted") is True:
            logger.info("Ignoring push that deleted its ref")
            return []
        if not (owner and repo_name):
            logger.info("Ignoring push without a repository owner and name")
            return []

        projects = await self._project_store.list_by_github_repo(
            repo_id=coordinates.repo_id, owner=owner, repo=repo_name
        )
        if not projects:
            logger.info("No projects match push for keeper-sync")
            return []

        stamped_at = now if now is not None else datetime.now(tz=UTC)
        orgs: dict[int, Organization | None] = {}
        stamped: list[StampedProject] = []
        # In id order, so two deliveries for one repository lock its
        # projects' state rows in the same order and cannot deadlock.
        for project in sorted(projects, key=lambda p: p.id):
            if project.org_id not in orgs:
                orgs[project.org_id] = await self._org_store.get_by_id(
                    project.org_id
                )
            org = orgs[project.org_id]
            if org is None:
                continue
            if await self._stamp_project(
                org=org,
                project=project,
                ref=ref,
                stamped_at=stamped_at,
                logger=logger,
            ):
                stamped.append(
                    StampedProject(
                        org_slug=org.slug,
                        project_slug=project.slug,
                        github_ref=ref,
                    )
                )
        logger.info(
            "Processed push for keeper-sync",
            projects_matched=len(projects),
            projects_stamped=len(stamped),
        )
        return stamped

    async def _stamp_project(
        self,
        *,
        org: Organization,
        project: Project,
        ref: str,
        stamped_at: datetime,
        logger: structlog.stdlib.BoundLogger,
    ) -> bool:
        """Stamp one project if it is eligible; return whether it was."""
        project_logger = logger.bind(
            org=org.slug, project=project.slug, project_id=project.id
        )
        skip = _scope_skip(org, project)
        state = None
        if skip is None:
            state = await self._state_store.get(
                org_id=org.id,
                resource_type=ResourceType.project,
                ltd_slug=project.slug,
                for_update=True,
            )
            if state is None:
                skip = KeeperSyncPushSkip.no_state
        if state is None:
            project_logger.info(
                "Skipped project for keeper-sync push", reason=skip
            )
            return False
        annotations = stamp_pushed_ref(
            state, ref=ref, now=stamped_at, window=self._window
        )
        await self._state_store.upsert(
            org_id=org.id,
            resource_type=ResourceType.project,
            ltd_slug=project.slug,
            annotations=annotations,
        )
        project_logger.info(
            "Stamped keeper-sync push hint",
            pushed_at=stamped_at.isoformat(),
            pushed_refs=len(annotations[ANNOTATION_GITHUB_PUSHED_REFS]),
        )
        return True


def _scope_skip(
    org: Organization, project: Project
) -> KeeperSyncPushSkip | None:
    """Return why the org's sync config rules a project out, if it does."""
    sync_config = org.keeper_sync_config
    if sync_config is None or not sync_config.enabled:
        return KeeperSyncPushSkip.sync_disabled
    if not sync_config.is_in_scope(project.slug):
        return KeeperSyncPushSkip.out_of_scope
    return None


def _tracked_ref(raw_ref: object) -> str | None:
    """Return a pushed branch or tag's normalized ref, else ``None``."""
    if not isinstance(raw_ref, str) or not raw_ref.startswith(
        _TRACKED_REF_PREFIXES
    ):
        return None
    return normalize_github_ref(raw_ref) or None
