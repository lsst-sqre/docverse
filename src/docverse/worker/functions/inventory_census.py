"""arq cron worker that publishes the daily ``resource_inventory`` gauge.

Daily cron entrypoint for the SQR-112 D8 resource-inventory census. Each
firing takes one read-only snapshot of every org's and non-deleted
project's active resource counts (via :class:`InventoryCensusService`,
which runs grouped aggregates with no advisory locks) and publishes one
org-scoped ``resource_inventory`` event per org plus one project-scoped
event per project. The counts are self-contained absolute gauges queried
downstream with ``last()``; the publish is best-effort — production runs
``raise_on_error=False`` so a metrics-backend outage can never fail the
job, and there is no defensive try/except at the call site.
"""

from __future__ import annotations

from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.domain.inventory_census import InventoryCensus
from docverse.metrics import ResourceInventoryEvent

__all__ = ["inventory_census"]


async def inventory_census(ctx: dict[str, Any]) -> str:
    """Publish one ``resource_inventory`` gauge per org and per project."""
    logger = structlog.get_logger("docverse.worker.inventory_census")

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        service = factory.create_inventory_census_service()
        async with session.begin():
            census = await service.take_census()
        await _publish_resource_inventory(ctx=ctx, census=census)
        logger.info(
            "Inventory census completed",
            org_count=len(census.orgs),
            project_count=len(census.projects),
        )
        return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)


async def _publish_resource_inventory(
    *, ctx: dict[str, Any], census: InventoryCensus
) -> None:
    """Emit one org-scoped row per org and one project-scoped row per project.

    Published after the read transaction closes. Best-effort: production
    runs ``raise_on_error=False`` so a metrics outage never fails the job
    (no defensive try/except at the call site).
    """
    events = ctx.get("events")
    if events is None:
        return
    for org in census.orgs:
        await events.resource_inventory.publish(
            ResourceInventoryEvent(
                organization=org.org_slug,
                project=None,
                project_count=org.project_count,
                edition_count=org.edition_count,
                build_count=org.build_count,
                total_build_bytes=org.total_build_bytes,
            )
        )
    for project in census.projects:
        await events.resource_inventory.publish(
            ResourceInventoryEvent(
                organization=project.org_slug,
                project=project.project_slug,
                project_count=None,
                edition_count=project.edition_count,
                build_count=project.build_count,
                total_build_bytes=project.total_build_bytes,
            )
        )
