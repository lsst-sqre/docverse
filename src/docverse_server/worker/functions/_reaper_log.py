"""Shared log-payload builders for the three reaper worker functions.

``keeper_sync_reaper``, ``lifecycle_reaper``, and the four run-less
reapers behind :mod:`._runless_reaper` all close a tick the same way:
pair each sweep's name with the rows it claimed, then log one warning
carrying per-row postmortem detail. The payload shape belongs here
rather than pasted into each module so the three reapers cannot drift
apart in the field an operator greps.

The ids are serialized here, deliberately. ``QueueJob.public_id`` is a
:class:`~docverse_server.domain.base32id.Base32Id`, whose serializer
only fires in a Pydantic JSON dump — read straight into a structlog
call it logs as a bare ``int``, which matches nothing an operator has
in hand. Every other queue-job log line (notably the pickup guard's
``Skipping late-delivered queue job``, whose ``queue_job_id`` is the
one an operator arrives with) spells the id in base32, so these
payloads do too and the cross-reference the detail exists for actually
resolves.
"""

from __future__ import annotations

from collections.abc import Sequence

from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.queue import QueueJob

__all__ = ["create_reaped_jobs_payload", "create_reaped_public_ids"]


def create_reaped_jobs_payload(
    by_sweep: Sequence[tuple[str, Sequence[QueueJob]]],
) -> list[dict[str, str | None]]:
    """Build the per-row ``reaped_jobs`` detail for one reaper tick.

    Parameters
    ----------
    by_sweep
        Each sweep's log name paired with the rows that sweep failed,
        in the order the reaper ran them.

    Returns
    -------
    list of dict
        One entry per reaped row carrying its base32 ``public_id``, the
        ``sweep`` that claimed it, and the ``backend_job_id`` that went
        missing (``None`` for the orphan sweeps, which match rows that
        never reached the queue backend at all). Lets a postmortem tell
        which sweep claimed a row without querying the database.
    """
    return [
        {
            "public_id": serialize_base32_id(qj.public_id),
            "sweep": sweep,
            "backend_job_id": qj.backend_job_id,
        }
        for sweep, jobs in by_sweep
        for qj in jobs
    ]


def create_reaped_public_ids(
    by_sweep: Sequence[tuple[str, Sequence[QueueJob]]],
) -> list[str]:
    """List every reaped row's base32 public id, sorted.

    The flat companion to :func:`create_reaped_jobs_payload` for
    reapers that also log a bare id list. Sorting happens on the
    serialized form, which keeps the ids in mint order anyway: a queue
    job's public id is a time-ordered integer inside the fixed 60-bit
    Crockford envelope, so every id encodes to the same width and sorts
    lexically the way it sorts numerically.
    """
    return sorted(
        serialize_base32_id(qj.public_id)
        for _, jobs in by_sweep
        for qj in jobs
    )
