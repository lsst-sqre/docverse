"""Read-only grouped-aggregate census of active Docverse resources.

The store backs the daily ``inventory_census`` worker (SQR-112 D8). It
runs only read-only grouped aggregates — ``COUNT`` of non-deleted
projects/editions/builds and ``SUM`` of active-build ``total_size_bytes``
— and takes no advisory locks, so a census pass never contends with the
publishing or maintenance flows for row locks. Modelled on
:meth:`docverse.storage.lifecycle_eval_run_store.LifecycleEvalRunStore.aggregate_activity`:
one ``GROUP BY`` query per resource, assembled into per-project rows and
rolled up to per-org rows in Python. The per-resource queries are kept
separate so joining editions and builds onto projects in a single query
cannot fan the build-byte ``SUM`` out across the edition rows.
"""

from __future__ import annotations

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse.dbschema.build import SqlBuild
from docverse.dbschema.edition import SqlEdition
from docverse.dbschema.organization import SqlOrganization
from docverse.dbschema.project import SqlProject
from docverse.domain.inventory_census import (
    InventoryCensus,
    OrgInventoryCensus,
    ProjectInventoryCensus,
)

__all__ = ["InventoryCensusStore"]


class InventoryCensusStore:
    """Read-only grouped-aggregate census of active resources."""

    def __init__(
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session = session
        self._logger = logger

    async def aggregate_inventory(self) -> InventoryCensus:
        """Census active orgs/projects/editions/builds in one snapshot.

        Every org yields one :class:`OrgInventoryCensus` row (even with
        zero projects); every non-deleted project yields one
        :class:`ProjectInventoryCensus` row. Soft-deleted
        projects/editions/builds are excluded everywhere — and editions
        and builds belonging to a soft-deleted project drop out by
        virtue of that project not producing a row. All counts and the
        byte sum are absolute gauges (SQR-112 D8), queried downstream
        with ``last()``.
        """
        # Every org, including those with no projects, so each yields a
        # census row.
        org_rows = (
            await self._session.execute(
                select(SqlOrganization.id, SqlOrganization.slug).order_by(
                    SqlOrganization.slug
                )
            )
        ).all()

        # Non-deleted projects with their owning org's slug.
        project_rows = (
            await self._session.execute(
                select(
                    SqlProject.id,
                    SqlProject.slug,
                    SqlProject.org_id,
                    SqlOrganization.slug,
                )
                .join(SqlOrganization, SqlOrganization.id == SqlProject.org_id)
                .where(SqlProject.date_deleted.is_(None))
                .order_by(SqlProject.id)
            )
        ).all()

        # Non-deleted edition counts per project. Editions of a
        # soft-deleted project are dropped below because that project
        # contributes no row to assemble them onto.
        edition_count_rows = (
            await self._session.execute(
                select(SqlEdition.project_id, func.count(SqlEdition.id))
                .where(SqlEdition.date_deleted.is_(None))
                .group_by(SqlEdition.project_id)
            )
        ).all()
        edition_counts: dict[int, int] = dict(edition_count_rows)

        # Non-deleted build counts + byte sums per project. ``SUM`` skips
        # NULL ``total_size_bytes`` (an unprocessed build still counts
        # toward ``build_count`` but adds nothing to the footprint); the
        # ``coalesce`` yields 0 for a project whose builds are all
        # NULL-sized.
        build_rows = (
            await self._session.execute(
                select(
                    SqlBuild.project_id,
                    func.count(SqlBuild.id),
                    func.coalesce(func.sum(SqlBuild.total_size_bytes), 0),
                )
                .where(SqlBuild.date_deleted.is_(None))
                .group_by(SqlBuild.project_id)
            )
        ).all()
        build_stats: dict[int, tuple[int, int]] = {
            project_id: (count, int(total_bytes))
            for project_id, count, total_bytes in build_rows
        }

        projects: list[ProjectInventoryCensus] = []
        projects_by_org: dict[int, list[ProjectInventoryCensus]] = {}
        for project_id, project_slug, org_id, org_slug in project_rows:
            build_count, total_build_bytes = build_stats.get(
                project_id, (0, 0)
            )
            project = ProjectInventoryCensus(
                org_id=org_id,
                org_slug=org_slug,
                project_id=project_id,
                project_slug=project_slug,
                edition_count=edition_counts.get(project_id, 0),
                build_count=build_count,
                total_build_bytes=total_build_bytes,
            )
            projects.append(project)
            projects_by_org.setdefault(org_id, []).append(project)

        orgs: list[OrgInventoryCensus] = []
        for org_id, org_slug in org_rows:
            org_projects = projects_by_org.get(org_id, [])
            orgs.append(
                OrgInventoryCensus(
                    org_id=org_id,
                    org_slug=org_slug,
                    project_count=len(org_projects),
                    edition_count=sum(p.edition_count for p in org_projects),
                    build_count=sum(p.build_count for p in org_projects),
                    total_build_bytes=sum(
                        p.total_build_bytes for p in org_projects
                    ),
                )
            )

        return InventoryCensus(orgs=orgs, projects=projects)
