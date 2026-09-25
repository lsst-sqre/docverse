"""Tests for the Avro contract of individual metrics event payloads.

A payload's Avro schema is what Sasquatch registers and what Telegraf
turns into InfluxDB fields and tags, so an additive change is pinned
here field by field: a stray rename, or a new field that cannot be
null, on an existing event is a schema break for every dashboard built
on it.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import create_model
from safir.metrics import EventPayload

from docverse_server.metrics import (
    BuildContentCopiedEvent,
    EditionPublishedEvent,
)


def _avro_field_types(model: type[EventPayload]) -> dict[str, Any]:
    """Map each Avro field name of ``model`` to its Avro type, in order.

    Built through a throw-away subclass, the same way
    :meth:`safir.metrics.EventPayload.validate_structure` does, so the
    schema metadata memoized on the probe never leaks onto ``model``.
    """
    probe = cast("type[EventPayload]", create_model("Probe", __base__=model))
    schema = probe.avro_schema_to_python()
    return {field["name"]: field["type"] for field in schema["fields"]}


def test_edition_published_adds_only_a_nullable_ltd_lag() -> None:
    """``edition_published`` grows exactly one field, and it is nullable.

    Every publish that did not come from a fresh keeper-sync visit has
    no LTD rebuild to measure against, so ``ltd_lag`` must accept null;
    the fields that were already there keep their names and order.
    """
    fields = _avro_field_types(EditionPublishedEvent)

    assert list(fields) == [
        "organization",
        "project",
        "edition_kind",
        "trigger",
        "elapsed",
        "ltd_lag",
    ]
    assert isinstance(fields["ltd_lag"], list)
    assert "null" in fields["ltd_lag"]
    # Every union member must still be one InfluxDB can store.
    EditionPublishedEvent.validate_structure()


def test_build_content_copied_adds_only_a_nullable_ltd_lag_seconds() -> None:
    """``build_content_copied`` grows exactly one nullable float field.

    ``ltd_lag_seconds`` is null when LTD reports no ``date_rebuilt`` to
    measure from, and otherwise a float, the same Avro type as the
    ``duration_seconds`` it is read against; the fields that were
    already there keep their names and order.
    """
    fields = _avro_field_types(BuildContentCopiedEvent)

    assert list(fields) == [
        "organization",
        "project",
        "ltd_slug",
        "object_count",
        "total_size_bytes",
        "duration_seconds",
        "peak_concurrent_copies",
        "retried_object_count",
        "exhausted_object_count",
        "build_retry_used",
        "succeeded",
        "ltd_lag_seconds",
    ]
    assert isinstance(fields["ltd_lag_seconds"], list)
    assert set(fields["ltd_lag_seconds"]) == {
        "null",
        fields["duration_seconds"],
    }
    # Every union member must still be one InfluxDB can store.
    BuildContentCopiedEvent.validate_structure()
