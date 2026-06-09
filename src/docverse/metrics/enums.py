"""Dedicated string enums for Sasquatch metrics event payloads.

These enums are intentionally separate from the API client enums in
``docverse.client.models`` (SQR-112 D4/D7): the metrics schema is a
published Avro contract consumed by Sasquatch and must be able to
evolve independently of the HTTP API. Each enum is mapped from its
API-side counterpart at the emission site, so a rename on either side
is an explicit, reviewable change rather than a silent schema break.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "EditionPublishTrigger",
    "MetricsEditionKind",
]


class MetricsEditionKind(StrEnum):
    """Kind of edition, as recorded on ``edition_published`` events.

    Mirrors :class:`docverse.client.models.EditionKind` value-for-value;
    the emission site maps the API enum to this one so the metrics Avro
    schema does not depend on the API model.
    """

    main = "main"
    release = "release"
    draft = "draft"
    major = "major"
    minor = "minor"
    alternate = "alternate"


class EditionPublishTrigger(StrEnum):
    """What caused a ``publish_edition`` job to run.

    ``publish_edition`` is a shared worker reached from more than one
    flow (SQR-112 D7), so the event records which one drove this
    publish:

    - ``build`` — a client-uploaded build's edition-tracking fan-out.
    - ``keeper_sync`` — the LTD Keeper backfill (the publish job's
      ``queue_jobs`` row carries a ``keeper_sync_run_id``).
    """

    build = "build"
    keeper_sync = "keeper_sync"
