"""arq worker function for the ``excluded_path_scrub_reaper`` cron backstop.

Mirrors :mod:`docverse_server.worker.functions.purgatory_cleanup_reaper`
for ``kind='excluded_path_scrub'`` rows. Like that sweep, the scrub
keeps no parent run row — its record of a run is its ``queue_jobs`` row
(whose ``progress`` is the report) plus the
``excluded_path_scrub_completed`` event — so the reaper only sweeps
stuck ``queue_jobs`` rows and finalises nothing.

What a wedged row costs here is the project's next scrub. ``POST
/orgs/{org}/projects/{project}/builds/scrub-excluded-paths`` hands back
a project's queued or in-progress scrub rather than starting a second
one, so a row left ``in_progress`` by an OOM-killed worker — or left
``queued`` by an API pod that crashed between the SQL commit and
``arq_queue.enqueue``, or by arq losing the job — would answer every
later request with a job that is never going to finish. Reaping the row
to ``failed`` lets the operator's next request queue a fresh scrub.

Failing the row is all the reaper does. It touches no build: a build the
dead job finished keeps its ``excluded_paths_scrubbed`` stamp and its
corrected inventory, and every other build is exactly as the job found
it, so the next real scrub passes the stamped builds by and resumes with
the rest. A build the job died part-way through deleting is unstamped,
with an inventory that still counts what the job removed from it. The
next run lists it again and writes its inventory as listed less what
that run removes, so the dead job's deletes come off as well. Only a
job that died after the last of a build's excluded objects was gone but
before its inventory write committed leaves that build over-counted: the
next run finds no excluded paths in it and does not write it. The report
the job last flushed stays on the failed row's ``progress``.
"""

from __future__ import annotations

from typing import Any

from docverse_server.domain.queue import JobKind

from ._runless_reaper import sweep_runless_kind

__all__ = ["excluded_path_scrub_reaper"]


async def excluded_path_scrub_reaper(ctx: dict[str, Any]) -> str:
    """Cron-driven backstop that fails stuck excluded_path_scrub rows.

    Thin shim over
    :func:`docverse_server.worker.functions._runless_reaper.sweep_runless_kind`;
    see that module for the shared sweep mechanics. Threshold defaults
    to 6 h via ``config.excluded_path_scrub_reaper_threshold_seconds``;
    non-prod can override with
    ``DOCVERSE_EXCLUDED_PATH_SCRUB_REAPER_THRESHOLD_SECONDS``.
    """
    return await sweep_runless_kind(
        ctx,
        kind=JobKind.excluded_path_scrub,
        threshold_attr="excluded_path_scrub_reaper_threshold_seconds",
    )
