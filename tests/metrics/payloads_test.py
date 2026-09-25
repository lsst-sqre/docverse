"""Tests for the Avro contract of individual metrics event payloads.

A payload's Avro schema is what Sasquatch registers and what Telegraf
turns into InfluxDB fields and tags, so an additive change is pinned
here field by field: a stray rename, or a new field that cannot be
null, on an existing event is a schema break for every dashboard built
on it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest
from pydantic import ValidationError, create_model
from safir.metrics import EventPayload

from docverse_server.metrics import (
    ApiRequestEvent,
    BuildContentCopiedEvent,
    DocverseEventBase,
    EditionPublishedEvent,
    GitHubWebhookReceivedEvent,
    HttpStatusClass,
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


def test_api_request_is_not_an_org_scoped_event() -> None:
    """``api_request`` stands outside the org/project base.

    Most API routes address no organization at all (``/orgs``, the
    admin routes, the webhook), so the event derives from
    :class:`~safir.metrics.EventPayload` directly and makes both
    dimensions optional, rather than weakening ``DocverseEventBase``'s
    required ``organization`` for every other event.
    """
    assert issubclass(ApiRequestEvent, EventPayload)
    assert not issubclass(ApiRequestEvent, DocverseEventBase)


def test_api_request_fields() -> None:
    """``api_request`` carries the request's route, outcome, and timing.

    ``route`` is nullable so an unmatched path can still be counted,
    and ``organization``/``project`` are nullable because most routes
    have neither; every other field is always present.
    """
    fields = _avro_field_types(ApiRequestEvent)

    assert list(fields) == [
        "method",
        "route",
        "status_code",
        "status_class",
        "duration",
        "authenticated",
        "organization",
        "project",
    ]
    for nullable in ("route", "organization", "project"):
        assert isinstance(fields[nullable], list)
        assert "null" in fields[nullable]
    for required in (
        "method",
        "status_code",
        "status_class",
        "duration",
        "authenticated",
    ):
        assert not isinstance(fields[required], list)
    # Every union member must still be one InfluxDB can store.
    ApiRequestEvent.validate_structure()


def test_api_request_status_class_is_an_avro_string() -> None:
    """``status_class`` goes over the wire as a string, not an Avro enum.

    Avro enum symbols must not begin with a digit, so ``4xx`` cannot be
    one; the field is a string whose values are
    :class:`~docverse_server.metrics.HttpStatusClass`'s, which is what a
    query's ``WHERE "status_class"='4xx'`` quotes.
    """
    fields = _avro_field_types(ApiRequestEvent)

    assert fields["status_class"] == "string"


def test_api_request_status_class_accepts_only_known_classes() -> None:
    """A value outside :class:`HttpStatusClass` is refused.

    ``status_class`` is an InfluxDB tag, so the closed vocabulary is what
    keeps its cardinality at five even though the Avro type is an open
    string.
    """
    event = _api_request(status_class=HttpStatusClass.client_error)
    assert event.status_class == "4xx"

    with pytest.raises(ValidationError):
        _api_request(status_class="404")


def _api_request(*, status_class: str) -> ApiRequestEvent:
    return ApiRequestEvent(
        method="GET",
        route="/orgs/{org}",
        status_code=404,
        status_class=status_class,
        duration=timedelta(milliseconds=5),
        authenticated=True,
        organization=None,
        project=None,
    )


def test_github_webhook_received_is_not_an_org_scoped_event() -> None:
    """``github_webhook_received`` stands outside the org/project base.

    A delivery is not resolved to any organization when it is recorded
    (and an unsigned or unconfigured one never could be), so the event
    derives from :class:`~safir.metrics.EventPayload` directly, like
    ``api_request``, rather than weakening ``DocverseEventBase``.
    """
    assert issubclass(GitHubWebhookReceivedEvent, EventPayload)
    assert not issubclass(GitHubWebhookReceivedEvent, DocverseEventBase)


def test_github_webhook_received_fields() -> None:
    """``github_webhook_received`` carries a delivery's type and outcome.

    ``event_type`` and ``github_repository`` are nullable because not
    every delivery names them, and ``organization``/``project`` are
    reserved as nullable so a later binding-aware emission is additive;
    every other field is always present.
    """
    fields = _avro_field_types(GitHubWebhookReceivedEvent)

    assert list(fields) == [
        "event_type",
        "outcome",
        "jobs_enqueued",
        "elapsed",
        "github_repository",
        "organization",
        "project",
    ]
    for nullable in (
        "event_type",
        "github_repository",
        "organization",
        "project",
    ):
        assert isinstance(fields[nullable], list)
        assert "null" in fields[nullable]
    for required in ("outcome", "jobs_enqueued", "elapsed"):
        assert not isinstance(fields[required], list)
    # Every union member must still be one InfluxDB can store.
    GitHubWebhookReceivedEvent.validate_structure()
