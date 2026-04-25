"""Dashboard template sync worker function.

Fetches a dashboard-template tree from GitHub, upserts the content +
files, and fans out ``dashboard_build`` jobs for every project whose
resolved template points at the synced content.
"""

from __future__ import annotations

import traceback
from typing import Any

import structlog
from safir.dependencies.db_session import db_session_dependency

from docverse.exceptions import NotFoundError
from docverse.services.dashboard_templates.sync import DashboardSyncStatus
from docverse.services.lock_service import LockKey


async def dashboard_sync(ctx: dict[str, Any], payload: dict[str, Any]) -> str:
    """Sync one dashboard-template binding from GitHub.

    Parameters
    ----------
    ctx
        arq worker context (``factory_builder``, ``http_client``,
        ``arq_queue``).
    payload
        Job payload with ``binding_id``, ``queue_job_id``,
        ``queue_job_public_id``.

    Returns
    -------
    str
        ``"completed"`` on success or ``"failed"`` if the sync raised.
    """
    logger = structlog.get_logger("docverse.worker.dashboard_sync").bind(
        binding_id=payload["binding_id"],
        queue_job_id=payload["queue_job_public_id"],
    )
    binding_id: int = payload["binding_id"]
    queue_job_id: int = payload["queue_job_id"]

    async for session in db_session_dependency():
        factory = ctx["factory_builder"](session=session, logger=logger)
        queue_job_store = factory.create_queue_job_store()
        binding_store = (
            factory.create_dashboard_github_template_binding_store()
        )
        lock_service = factory.create_lock_service()

        # Load the binding before the lock so we know the content key
        # to serialize on. A binding that was deleted between enqueue
        # and dequeue fails fast before we try to acquire anything.
        async with session.begin():
            binding = await binding_store.get_by_id(binding_id)
        if binding is None:
            async with session.begin():
                await queue_job_store.start(queue_job_id)
                await queue_job_store.fail(
                    queue_job_id,
                    errors={
                        "message": (
                            f"Dashboard template binding {binding_id} "
                            "not found"
                        ),
                        "type": NotFoundError.__name__,
                    },
                )
            logger.warning("Dashboard sync binding missing")
            return "failed"

        lock_key = LockKey.for_dashboard_template(
            owner=binding.github_owner,
            repo=binding.github_repo,
            ref=binding.github_ref,
            root_path=binding.root_path,
        )
        async with lock_service.acquire(lock_key):
            # Autobegin-then-commit dance inside the lock mirrors the
            # pattern from PR #224: the lock is held on a dedicated
            # connection, but the caller's session needs an explicit
            # begin/commit around each DB write so progress updates
            # are visible to observers outside the lock hold.
            async with session.begin():
                await queue_job_store.start(queue_job_id)
                await queue_job_store.update_phase(
                    queue_job_id,
                    "fetching",
                    progress={"message": "Fetching template tree from GitHub"},
                )

            try:
                async with session.begin():
                    await queue_job_store.update_phase(
                        queue_job_id,
                        "writing",
                        progress={
                            "message": (
                                "Writing template content to the database"
                            )
                        },
                    )
                    syncer = factory.create_dashboard_template_syncer()
                    sync_result = await syncer.sync(binding_id)
            except Exception as exc:
                logger.exception("Dashboard sync failed unexpectedly")
                async with session.begin():
                    await queue_job_store.fail(
                        queue_job_id,
                        errors={
                            "message": str(exc),
                            "type": type(exc).__name__,
                            "traceback": traceback.format_exc(),
                        },
                    )
                return "failed"

            if sync_result.status is DashboardSyncStatus.failed:
                logger.warning(
                    "Dashboard sync marked binding failed",
                    reason=sync_result.error,
                )
                async with session.begin():
                    await queue_job_store.fail(
                        queue_job_id,
                        errors={
                            "message": sync_result.error or "Sync failed",
                            "type": "DashboardTemplateSyncError",
                        },
                    )
                return "failed"

            fan_out_count = 0
            template_id = sync_result.github_template_id
            if sync_result.changed and template_id is not None:
                async with session.begin():
                    await queue_job_store.update_phase(
                        queue_job_id,
                        "fanning_out",
                        progress={
                            "message": (
                                "Fanning out dashboard rebuilds for dependent "
                                "projects"
                            ),
                        },
                    )
                    fanout = factory.create_dashboard_rebuild_fanout()
                    jobs = await fanout.fan_out(template_id)
                    fan_out_count = len(jobs)

            async with session.begin():
                await queue_job_store.update_phase(
                    queue_job_id,
                    "complete",
                    progress={
                        "message": "Dashboard sync complete",
                        "changed": sync_result.changed,
                        "github_template_id": (sync_result.github_template_id),
                        "fan_out_count": fan_out_count,
                    },
                )
                await queue_job_store.complete(queue_job_id)
            logger.info(
                "Dashboard sync completed",
                changed=sync_result.changed,
                fan_out_count=fan_out_count,
            )
            return "completed"

    msg = "No database session available"
    raise RuntimeError(msg)
