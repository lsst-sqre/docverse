"""Read-only grouped-aggregate census of active Docverse resources.

The store backs the daily ``inventory_census`` worker (SQR-112 D8). It
runs only read-only grouped aggregates — ``COUNT`` of non-deleted
projects/editions/builds, ``SUM`` of active-build ``total_size_bytes``,
and the matching pair over the builds waiting on the ``purgatory_cleanup``
sweep — and takes no advisory locks, so a census pass never contends with
the publishing or maintenance flows for row locks. Modelled on
:meth:`docverse_server.storage.lifecycle_eval_run_store.LifecycleEvalRunStore.aggregate_activity`:
one ``GROUP BY`` query per resource, assembled into per-project rows and
rolled up to per-org rows in Python. The per-resource queries are kept
separate so joining editions and builds onto projects in a single query
cannot fan the build-byte ``SUM`` out across the edition rows.
"""

from __future__ import annotations

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from docverse_server.dbschema.build import SqlBuild
from docverse_server.dbschema.edition import SqlEdition
from docverse_server.dbschema.organization import SqlOrganization
from docverse_server.dbschema.project import SqlProject
from docverse_server.domain.inventory_census import (
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
        projects/editions/builds are excluded from the active counts —
        and editions and builds belonging to a soft-deleted project drop
        out by virtue of that project not producing a row. The purgatory
        pair counts the other side of that line: builds that are
        soft-deleted but not yet purged, so their storage is still on the
        bucket. Those reach the org row from every project of the org,
        deleted or not. All counts and the byte sums are absolute gauges
        (SQR-112 D8), queried downstream with ``last()``.
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
            (
                await self._session.execute(
                    select(SqlEdition.project_id, func.count(SqlEdition.id))
                    .where(SqlEdition.date_deleted.is_(None))
                    .group_by(SqlEdition.project_id)
                )
            )
            .tuples()
            .all()
        )
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

        # Soft-deleted, not-yet-purged build counts + byte sums, keyed by
        # project and rolled up by org in the same pass. This one joins
        # ``projects`` and deliberately does not filter on
        # ``SqlProject.date_deleted``: deleting a project is precisely
        # what sends its builds' storage into purgatory, so dropping
        # those rows would hide the largest reclaim the sweep ever has to
        # make. They reach the org roll-up below; a deleted project still
        # contributes no project row to hang them on.
        purgatory_rows = (
            await self._session.execute(
                select(
                    SqlBuild.project_id,
                    SqlProject.org_id,
                    func.count(SqlBuild.id),
                    func.coalesce(func.sum(SqlBuild.total_size_bytes), 0),
                )
                .join(SqlProject, SqlProject.id == SqlBuild.project_id)
                .where(
                    SqlBuild.date_deleted.is_not(None),
                    SqlBuild.date_purged.is_(None),
                )
                .group_by(SqlBuild.project_id, SqlProject.org_id)
            )
        ).all()
        purgatory_stats: dict[int, tuple[int, int]] = {}
        org_purgatory_stats: dict[int, tuple[int, int]] = {}
        for project_id, org_id, count, total_bytes in purgatory_rows:
            purgatory_stats[project_id] = (count, int(total_bytes))
            org_count, org_bytes = org_purgatory_stats.get(org_id, (0, 0))
            org_purgatory_stats[org_id] = (
                org_count + count,
                org_bytes + int(total_bytes),
            )

        projects: list[ProjectInventoryCensus] = []
        projects_by_org: dict[int, list[ProjectInventoryCensus]] = {}
        for project_id, project_slug, org_id, org_slug in project_rows:
            build_count, total_build_bytes = build_stats.get(
                project_id, (0, 0)
            )
            purgatory_build_count, purgatory_bytes = purgatory_stats.get(
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
                purgatory_build_count=purgatory_build_count,
                purgatory_bytes=purgatory_bytes,
            )
            projects.append(project)
            projects_by_org.setdefault(org_id, []).append(project)

        orgs: list[OrgInventoryCensus] = []
        for org_id, org_slug in org_rows:
            org_projects = projects_by_org.get(org_id, [])
            # The purgatory pair comes from the org-keyed roll-up rather
            # than a sum over ``org_projects``, which would drop the
            # builds of the org's soft-deleted projects.
            org_purgatory_count, org_purgatory_bytes = org_purgatory_stats.get(
                org_id, (0, 0)
            )
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
                    purgatory_build_count=org_purgatory_count,
                    purgatory_bytes=org_purgatory_bytes,
                )
            )

        return InventoryCensus(orgs=orgs, projects=projects)
