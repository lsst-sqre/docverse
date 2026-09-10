"""Tests for the message ``ObjectStoreError`` renders.

The class carries no ``to_sentry`` override, so the rendered message is
the entire triage surface: it is what lands in a failing job's
``queue_jobs.errors['message']`` and what titles the Sentry issue. These
tests pin what it has to name and how much of an unbounded failure list
it is allowed to name.
"""

from __future__ import annotations

from docverse_server.storage.objectstore import (
    MAX_REPORTED_OBJECT_FAILURES,
    ObjectStoreError,
)


def test_message_names_bucket_prefix_operation_and_failures() -> None:
    """One failed key renders every identifier the raise site knew."""
    exc = ObjectStoreError(
        bucket="docs",
        prefix="orgs/rubin/builds/01ABCDEF/",
        operation="DeleteObjects",
        failures=["orgs/rubin/builds/01ABCDEF/index.html (AccessDenied)"],
    )

    message = str(exc)
    assert "s3://docs/orgs/rubin/builds/01ABCDEF/" in message
    assert "DeleteObjects" in message
    assert "1 failed key" in message
    assert "index.html (AccessDenied)" in message


def test_message_bounds_the_failure_list_but_not_the_count() -> None:
    """A whole batch failing is summarized, not transcribed."""
    failures = [f"prefix/key-{index} (InternalError)" for index in range(25)]

    exc = ObjectStoreError(
        bucket="docs",
        prefix="prefix/",
        operation="DeleteObjects",
        failures=failures,
    )

    assert exc.failures == failures[:MAX_REPORTED_OBJECT_FAILURES]
    assert exc.failure_count == 25
    message = str(exc)
    assert "25 failed keys" in message
    assert f"(+{25 - MAX_REPORTED_OBJECT_FAILURES} more)" in message
