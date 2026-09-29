"""Tests for the shared GitHub webhook payload readers.

The unit tests pin :func:`repository_coordinates` and
:func:`coerce_int` themselves. The last test pins what sharing them
buys: the ``repository.edited`` and ``delete`` processors reach the
same project for the same ``repository`` block, where they used to read
the block with two different fallback rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
import structlog
from safir.arq import MockArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.models import (
    EditionCreate,
    EditionKind,
    OrganizationCreate,
    ProjectCreate,
    TrackingMode,
)
from docverse.models.projects import ProjectGitHubBindingCreate
from docverse_server.config import Configuration
from docverse_server.factory import Factory
from docverse_server.services.default_branch import DefaultBranchService
from docverse_server.services.default_branch_processor import (
    DefaultBranchEventProcessor,
)
from docverse_server.services.github_payload import (
    RepositoryCoordinates,
    coerce_int,
    repository_coordinates,
)
from docverse_server.services.ref_deleted_processor import (
    RefDeletedWebhookProcessor,
)
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.organization_store import OrganizationStore
from docverse_server.storage.project_store import ProjectStore

_config = Configuration()


def _logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger("test")  # type: ignore[no-any-return]


def test_repository_coordinates_reads_a_full_block() -> None:
    """``owner.login``, ``name``, and ``id`` are read as given."""
    coordinates = repository_coordinates(
        {
            "id": 12345,
            "name": "docs",
            "full_name": "other/elsewhere",
            "owner": {"login": "acme", "name": "Acme Corp"},
        }
    )

    assert coordinates == RepositoryCoordinates(
        owner="acme", name="docs", repo_id=12345
    )


def test_repository_coordinates_falls_back_to_full_name() -> None:
    """A block carrying only ``full_name`` is split into owner and name."""
    assert repository_coordinates(
        {"full_name": "acme/docs"}
    ) == RepositoryCoordinates(owner="acme", name="docs", repo_id=None)


def test_repository_coordinates_splits_full_name_once() -> None:
    """Only the first ``/`` separates the owner from the name."""
    coordinates = repository_coordinates({"full_name": "acme/docs/extra"})

    assert coordinates.owner == "acme"
    assert coordinates.name == "docs/extra"


def test_repository_coordinates_empty_name_falls_back() -> None:
    """An empty-string ``name`` takes the ``full_name`` split."""
    coordinates = repository_coordinates(
        {"name": "", "full_name": "acme/docs", "owner": {"login": "acme"}}
    )

    assert coordinates.name == "docs"


def test_repository_coordinates_non_string_name_falls_back() -> None:
    """A non-string ``name`` takes the ``full_name`` split."""
    coordinates = repository_coordinates(
        {"name": 42, "full_name": "acme/docs", "owner": {"login": "acme"}}
    )

    assert coordinates.name == "docs"


def test_repository_coordinates_owner_name_when_login_is_empty() -> None:
    """``owner.name`` stands in for an empty ``owner.login``."""
    coordinates = repository_coordinates(
        {
            "name": "docs",
            "full_name": "other/docs",
            "owner": {"login": "", "name": "acme"},
        }
    )

    assert coordinates.owner == "acme"


@pytest.mark.parametrize(
    "owner_block",
    [{"login": "", "name": ""}, {"login": 7, "name": None}, "acme", None],
    ids=["empty-strings", "non-strings", "string-block", "null-block"],
)
def test_repository_coordinates_unusable_owner_falls_back(
    owner_block: object,
) -> None:
    """Without a usable ``owner.login`` or ``owner.name``, ``full_name``."""
    coordinates = repository_coordinates(
        {"name": "docs", "full_name": "acme/docs", "owner": owner_block}
    )

    assert coordinates.owner == "acme"


def test_repository_coordinates_no_usable_names_are_none() -> None:
    """Nothing usable to read or split leaves owner and name ``None``."""
    coordinates = repository_coordinates(
        {"name": "", "full_name": "no-slash", "owner": {"login": ""}}
    )

    assert coordinates == RepositoryCoordinates(
        owner=None, name=None, repo_id=None
    )


@pytest.mark.parametrize(
    "repository",
    [None, "acme/docs", ["acme", "docs"], 12345],
    ids=["null", "string", "list", "int"],
)
def test_repository_coordinates_non_mapping_is_all_none(
    repository: object,
) -> None:
    """A ``repository`` that is not a mapping yields no coordinates."""
    assert repository_coordinates(repository) == RepositoryCoordinates(
        owner=None, name=None, repo_id=None
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (12345, 12345),
        (0, 0),
        (True, None),
        (False, None),
        ("12345", None),
        (12345.0, None),
        (None, None),
    ],
    ids=["int", "zero", "true", "false", "str", "float", "none"],
)
def test_coerce_int_accepts_only_a_non_bool_int(
    value: object, expected: int | None
) -> None:
    """``isinstance(True, int)`` must not leak a truth value as an id."""
    assert coerce_int(value) == expected


@pytest.mark.parametrize(
    ("repo_id", "expected"),
    [(12345, 12345), (True, None), ("12345", None)],
    ids=["int", "bool", "str"],
)
def test_repository_coordinates_repo_id_is_a_non_bool_int(
    repo_id: object, expected: int | None
) -> None:
    """``repository.id`` goes through :func:`coerce_int`."""
    coordinates = repository_coordinates(
        {"id": repo_id, "name": "docs", "owner": {"login": "acme"}}
    )

    assert coordinates.repo_id == expected


@dataclass
class _StubPublishingService:
    """Records ``unpublish`` calls in place of the CDN publisher."""

    calls: list[tuple[int, str, str]] = field(default_factory=list)

    async def unpublish(
        self, *, org_id: int, project_slug: str, edition_slug: str
    ) -> None:
        self.calls.append((org_id, project_slug, edition_slug))


def _processors(
    db_session: AsyncSession,
) -> tuple[DefaultBranchEventProcessor, RefDeletedWebhookProcessor]:
    """Wire the ``repository.edited`` and ``delete`` processors."""
    factory = Factory(
        session=db_session,
        logger=_logger(),
        arq_queue=MockArqQueue(),
        default_queue_name=_config.arq_queue_name,
    )
    publishing = _StubPublishingService()
    default_branch = DefaultBranchEventProcessor(
        project_store=factory.create_project_store(),
        org_store=factory.create_org_store(),
        default_branch_service=DefaultBranchService(
            project_store=factory.create_project_store(),
            edition_store=factory.create_edition_store(),
            build_store=factory.create_build_store(),
            edition_service=factory.create_edition_service(),
            publishing_service=publishing,  # type: ignore[arg-type]
            lock_service=factory.create_lock_service(),
            logger=_logger(),
        ),
        logger=_logger(),
    )
    ref_deleted = RefDeletedWebhookProcessor(
        project_store=factory.create_project_store(),
        edition_store=factory.create_edition_store(),
        edition_service=factory.create_edition_service(),
        org_store=factory.create_org_store(),
        publishing_service=publishing,  # type: ignore[arg-type]
        logger=_logger(),
    )
    return default_branch, ref_deleted


async def _seed_project(
    db_session: AsyncSession, *, repo_id: int | None
) -> None:
    """Seed ``acme/docs`` as project ``docs`` with a ``feature`` draft.

    The project's numeric repository id is resolved only when
    ``repo_id`` is given; otherwise only the name fallback reaches it.
    """
    logger = _logger()
    async with db_session.begin():
        org = await OrganizationStore(
            session=db_session, logger=logger
        ).create(
            OrganizationCreate(
                slug="gp-org", title="Org", base_domain="gp-org.example.com"
            )
        )
        project_store = ProjectStore(session=db_session, logger=logger)
        project = await project_store.create(
            org_id=org.id,
            data=ProjectCreate(
                slug="docs",
                title="Docs",
                github=ProjectGitHubBindingCreate(owner="acme", repo="docs"),
            ),
            github_owner="acme",
            github_repo="docs",
        )
        if repo_id is not None:
            await project_store.apply_installation_scope(
                installation_id=99,
                owner="acme",
                owner_id=999,
                repo="docs",
                repo_id=repo_id,
            )
        await EditionStore(session=db_session, logger=logger).create(
            project_id=project.id,
            data=EditionCreate(
                slug="feature",
                title="feature",
                kind=EditionKind.draft,
                tracking_mode=TrackingMode.git_ref,
                tracking_params={"git_ref": "feature"},
            ),
        )
        await db_session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("repository", "project_repo_id"),
    [
        (
            {
                "id": 12345,
                "name": "docs",
                "full_name": "acme/docs",
                "owner": {"login": "acme"},
            },
            12345,
        ),
        ({"full_name": "acme/docs"}, None),
        (
            {"name": "", "full_name": "acme/docs", "owner": {"login": ""}},
            None,
        ),
        ({"name": "docs", "owner": {"login": "", "name": "acme"}}, None),
        (
            {"name": "docs", "full_name": "acme/docs", "owner": {"login": 7}},
            None,
        ),
        (
            {
                "id": True,
                "name": "docs",
                "full_name": "acme/docs",
                "owner": {"login": "acme"},
            },
            None,
        ),
    ],
    ids=[
        "full-block",
        "full-name-only",
        "empty-name",
        "owner-name",
        "non-string-login",
        "bool-id",
    ],
)
async def test_edited_and_delete_resolve_the_same_project(
    db_session: AsyncSession,
    repository: dict[str, Any],
    project_repo_id: int | None,
) -> None:
    """Both processors reach project ``docs`` from the same block.

    ``repository.edited`` names it as a convergence target; ``delete``
    reaches it by retiring its draft on the deleted ref.
    """
    await _seed_project(db_session, repo_id=project_repo_id)
    default_branch, ref_deleted = _processors(db_session)

    async with db_session.begin():
        targets = await default_branch.resolve_targets(
            {
                "action": "edited",
                "changes": {"default_branch": {"from": "master"}},
                "repository": {**repository, "default_branch": "main"},
            }
        )
    async with db_session.begin():
        result = await ref_deleted.process(
            {"ref": "feature", "ref_type": "branch", "repository": repository}
        )
        await db_session.commit()

    edited = [(t.org_slug, t.project.slug) for t in targets]
    deleted = [(p.org_slug, p.project_slug) for p in result.affected_projects]
    assert edited == deleted == [("gp-org", "docs")]
