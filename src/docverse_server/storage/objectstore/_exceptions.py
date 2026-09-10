"""Exceptions raised by object store implementations."""

from __future__ import annotations

from collections.abc import Sequence

from docverse_server.exceptions import DocverseSlackException

__all__ = [
    "MAX_REPORTED_OBJECT_FAILURES",
    "ObjectStoreError",
]

#: Cap on the number of per-key failures named in an
#: :class:`ObjectStoreError` message.
#:
#: An exception message is not a bounded field: ``str(exc)`` is copied
#: verbatim into a failing job's ``queue_jobs.errors['message']`` JSONB
#: and becomes the Sentry issue title. A bulk delete can report a
#: failure for every key in a 1000-key batch, and the failures in a
#: batch share a cause almost by construction (one denied prefix, one
#: expired credential, one bucket in a bad mood), so naming a handful is
#: enough to recognize the pattern while keeping the record a fixed size
#: and the Sentry grouping stable. The count is reported separately and
#: is never truncated.
#:
#: Mirrors :data:`~docverse_server.exceptions.MAX_REPORTED_EDITION_SLUGS`,
#: which bounds the same kind of list for keeper-sync, and is smaller
#: because a single failing key is far more diagnostic than a single
#: failing edition slug.
MAX_REPORTED_OBJECT_FAILURES = 10


class ObjectStoreError(DocverseSlackException):
    """An object store refused part of an operation it was asked for.

    Raised when a store reports a per-key failure that the caller must
    not read as success. The motivating case is a partial
    :meth:`~docverse_server.storage.objectstore.ObjectStore.delete_prefix`:
    its caller stamps ``builds.date_purged`` on the strength of the
    return value, so a batch that quietly left objects behind would
    record a build's content as reclaimed while the bytes are still on
    the store with nothing left pointing at them — unreachable through
    the API and invisible to the next sweep, which skips stamped rows.
    Failing loudly instead leaves the row unstamped and the build in the
    next tick's work list.

    No ``to_sentry`` override: the rendered message already names the
    bucket, prefix, operation, failure count, and a bounded sample of
    the failing keys, which is the whole triage story. Mirrors
    :class:`~docverse_server.storage.ltd.LtdSourceAccessDeniedError`.

    Parameters
    ----------
    bucket
        Bucket the operation ran against.
    prefix
        Key prefix the operation was scoped to.
    operation
        Object store API operation that reported the failures, e.g.
        ``"DeleteObjects"``.
    failures
        Rendered per-key failure descriptions, in the caller's own
        wording (``"<key> (<Code>)"`` for S3). Truncated to
        :data:`MAX_REPORTED_OBJECT_FAILURES` inside the constructor —
        callers pass everything they have and the class enforces the cap.
    failure_count
        How many keys failed in total, which may exceed
        ``len(failures)``. Defaults to the length of ``failures``, so a
        caller that already truncated its own list must pass the real
        total.
    message
        Rendered message, overriding the default built from the fields
        above.
    """

    def __init__(
        self,
        *,
        bucket: str | None = None,
        prefix: str | None = None,
        operation: str | None = None,
        failures: Sequence[str] = (),
        failure_count: int | None = None,
        message: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.operation = operation
        self.failure_count = (
            len(failures) if failure_count is None else failure_count
        )
        self.failures = list(failures[:MAX_REPORTED_OBJECT_FAILURES])
        super().__init__(
            message
            if message is not None
            else self._format_message(
                bucket=bucket,
                prefix=prefix,
                operation=operation,
                failures=self.failures,
                failure_count=self.failure_count,
            )
        )

    @staticmethod
    def _format_message(
        *,
        bucket: str | None,
        prefix: str | None,
        operation: str | None,
        failures: Sequence[str],
        failure_count: int,
    ) -> str:
        target = f"s3://{bucket}/{prefix or ''}" if bucket else prefix
        located = f" under {target}" if target else ""
        called = f" during {operation}" if operation else ""
        noun = "key" if failure_count == 1 else "keys"
        named = f": {', '.join(failures)}" if failures else ""
        elided = failure_count - len(failures)
        more = f" (+{elided} more)" if elided > 0 else ""
        return (
            f"Object store reported {failure_count} failed {noun}"
            f"{called}{located}{named}{more}"
        )
