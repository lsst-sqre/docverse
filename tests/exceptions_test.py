"""Tests for the shared ``DocverseSlackException`` base.

Pins the contract introduced in PRD #338 / slice #340 that every
non-``ClientRequestError`` server-side exception inherits from
``DocverseSlackException`` -- itself a ``safir.slack.blockkit.SlackException``
subclass -- so future enrichment slices (#341-#344) can override
``to_sentry()`` against a single shared base.

The unit tests here assert membership in both bases and that the
inherited default ``to_slack()`` renders without error. Per-subclass
``to_sentry()`` overrides (slice #341 onward) are also exercised here
against representative constructor args without spinning up
``sentry_sdk.init``. The end-to-end Sentry capture is exercised
separately in :mod:`tests.sentry_test`.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from safir.slack.blockkit import SlackException, SlackMessage

from docverse.domain.slug import InvalidSlugError
from docverse.exceptions import (
    DocverseSlackException,
    InvalidBuildStateError,
    InvalidJobStateError,
    JobNotFoundError,
    KeeperSyncInvariantError,
)


def _make_invalid_slug() -> InvalidSlugError:
    return InvalidSlugError("__reserved", "slug must not start with '__'")


def _make_invalid_build_state() -> InvalidBuildStateError:
    return InvalidBuildStateError(
        current_state="processing",
        target_state="completed",
        build_public_id="01ABCDEF",
        project_slug="my-project",
        org_slug="my-org",
        edition_slug="main",
    )


def _make_invalid_job_state() -> InvalidJobStateError:
    return InvalidJobStateError(
        current_state="queued",
        target_state="in_progress",
        job_public_id="ABCD-EFGH-1234",
        queue_name="docverse:queue",
        job_function="build_processing",
    )


def _make_job_not_found() -> JobNotFoundError:
    return JobNotFoundError(
        job_public_id="ABCD-EFGH-1234",
        queue_name="docverse:queue",
        job_function="build_processing",
    )


def _make_keeper_sync_invariant() -> KeeperSyncInvariantError:
    return KeeperSyncInvariantError("state_id=42 has no tombstone")


_FACTORIES: list[tuple[str, Callable[[], DocverseSlackException]]] = [
    ("InvalidJobStateError", _make_invalid_job_state),
    ("InvalidBuildStateError", _make_invalid_build_state),
    ("JobNotFoundError", _make_job_not_found),
    ("InvalidSlugError", _make_invalid_slug),
    ("KeeperSyncInvariantError", _make_keeper_sync_invariant),
]


@pytest.mark.parametrize(
    ("name", "factory"),
    _FACTORIES,
    ids=[name for name, _ in _FACTORIES],
)
def test_migrated_exception_is_docverse_slack_exception(
    name: str, factory: Callable[[], DocverseSlackException]
) -> None:
    """Every migrated exception derives from both new and safir bases."""
    exc = factory()
    assert isinstance(exc, DocverseSlackException)
    assert isinstance(exc, SlackException)


@pytest.mark.parametrize(
    ("name", "factory"),
    _FACTORIES,
    ids=[name for name, _ in _FACTORIES],
)
def test_migrated_exception_renders_default_to_slack(
    name: str, factory: Callable[[], DocverseSlackException]
) -> None:
    """The inherited ``SlackException.to_slack()`` default renders cleanly.

    The default ``to_slack`` is the contract every migrated subclass
    exposes; subclasses that override ``to_sentry`` keep the default
    Slack rendering unless they explicitly override that too.
    """
    exc = factory()
    message = exc.to_slack()
    assert isinstance(message, SlackMessage)
    assert str(exc) == exc.message


_BUILD_STATE_CASES: list[
    tuple[str, Callable[[], InvalidBuildStateError], dict[str, str]]
] = [
    (
        "full-transition",
        lambda: InvalidBuildStateError(
            current_state="processing",
            target_state="completed",
            build_public_id="01ABCDEF",
            project_slug="my-project",
            org_slug="my-org",
            edition_slug="main",
        ),
        {
            "org_slug": "my-org",
            "project_slug": "my-project",
            "build_current_state": "processing",
            "build_target_state": "completed",
        },
    ),
    (
        "no-edition",
        lambda: InvalidBuildStateError(
            current_state="completed",
            target_state="processing",
            build_public_id="02ZYXWVU",
            project_slug="docs-site",
            org_slug="rubin",
        ),
        {
            "org_slug": "rubin",
            "project_slug": "docs-site",
            "build_current_state": "completed",
            "build_target_state": "processing",
        },
    ),
    (
        "not-found",
        lambda: InvalidBuildStateError(
            target_state="processing",
            project_slug="docs-site",
            org_slug="rubin",
            message="Build id=42 not found",
        ),
        {
            "org_slug": "rubin",
            "project_slug": "docs-site",
            "build_target_state": "processing",
        },
    ),
]


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _BUILD_STATE_CASES,
    ids=[case for case, _, _ in _BUILD_STATE_CASES],
)
def test_invalid_build_state_to_sentry_tags(
    case: str,
    factory: Callable[[], InvalidBuildStateError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` surfaces API-facing slugs and state names as tags.

    Tags are low-cardinality (org_slug, project_slug, state names) so
    they can be aggregated in the Sentry UI. ``build_public_id`` is
    intentionally a context value (high cardinality), not a tag.
    """
    info = factory().to_sentry()
    assert info.tags == expected_tags
    assert "build_public_id" not in info.tags


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _BUILD_STATE_CASES,
    ids=[case for case, _, _ in _BUILD_STATE_CASES],
)
def test_invalid_build_state_to_sentry_context(
    case: str,
    factory: Callable[[], InvalidBuildStateError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` exposes the full transition snapshot as a context.

    The ``build_transition`` context carries every API-facing
    identifier the exception was constructed with (even ``None``s) so
    a triager can paste ``build_public_id`` and ``project_slug``
    straight into a build URL without translating row ids.
    """
    exc = factory()
    info = exc.to_sentry()
    context = info.contexts["build_transition"]
    assert context["build_public_id"] == exc.build_public_id
    assert context["project_slug"] == exc.project_slug
    assert context["org_slug"] == exc.org_slug
    assert context["edition_slug"] == exc.edition_slug
    assert context["current_state"] == exc.current_state
    assert context["target_state"] == exc.target_state


def test_invalid_build_state_default_message_is_useful() -> None:
    """Without an explicit ``message`` the default summarises the transition.

    The summary names both states and the API-facing build slug so a
    log line or Slack alert is actionable without unpacking the
    structured fields.
    """
    exc = InvalidBuildStateError(
        current_state="completed",
        target_state="processing",
        build_public_id="01ABCDEF",
        project_slug="my-project",
        org_slug="my-org",
    )
    rendered = str(exc)
    assert "completed" in rendered
    assert "processing" in rendered
    assert "01ABCDEF" in rendered


def test_invalid_build_state_explicit_message_wins() -> None:
    """Passing ``message`` overrides the auto-generated default."""
    exc = InvalidBuildStateError(
        target_state="processing",
        project_slug="my-project",
        org_slug="my-org",
        message="Build id=42 not found",
    )
    assert str(exc) == "Build id=42 not found"


_JOB_STATE_CASES: list[
    tuple[str, Callable[[], InvalidJobStateError], dict[str, str]]
] = [
    (
        "full-transition",
        lambda: InvalidJobStateError(
            current_state="queued",
            target_state="in_progress",
            job_public_id="ABCD-EFGH-1234",
            queue_name="docverse:queue",
            job_function="build_processing",
        ),
        {
            "queue_name": "docverse:queue",
            "job_function": "build_processing",
            "job_current_state": "queued",
            "job_target_state": "in_progress",
        },
    ),
    (
        "keeper-sync-run-transition",
        lambda: InvalidJobStateError(
            current_state="succeeded",
            target_state="in_progress",
            queue_name="docverse:sync-queue",
            message=(
                "Cannot transition keeper sync run 42 from"
                " 'succeeded' to 'in_progress'"
            ),
        ),
        {
            "queue_name": "docverse:sync-queue",
            "job_current_state": "succeeded",
            "job_target_state": "in_progress",
        },
    ),
    (
        "run-not-found",
        lambda: InvalidJobStateError(
            queue_name="docverse:maintenance-queue",
            message="Lifecycle eval run 99 not found",
        ),
        {
            "queue_name": "docverse:maintenance-queue",
        },
    ),
]


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _JOB_STATE_CASES,
    ids=[case for case, _, _ in _JOB_STATE_CASES],
)
def test_invalid_job_state_to_sentry_tags(
    case: str,
    factory: Callable[[], InvalidJobStateError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` surfaces queue name, function, and states as tags.

    Tags are low-cardinality (queue name, function name, state enum
    values) so they can be aggregated in the Sentry UI. ``job_public_id``
    is intentionally a context value (high cardinality), not a tag.
    """
    info = factory().to_sentry()
    assert info.tags == expected_tags
    assert "job_public_id" not in info.tags


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _JOB_STATE_CASES,
    ids=[case for case, _, _ in _JOB_STATE_CASES],
)
def test_invalid_job_state_to_sentry_context(
    case: str,
    factory: Callable[[], InvalidJobStateError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` exposes the full transition snapshot as a context.

    The ``queue_job_transition`` context carries every API-facing
    identifier the exception was constructed with (even ``None``s) so a
    triager can paste ``job_public_id`` straight into a ``/queue/{id}``
    URL without translating internal row ids.
    """
    exc = factory()
    info = exc.to_sentry()
    context = info.contexts["queue_job_transition"]
    assert context["job_public_id"] == exc.job_public_id
    assert context["queue_name"] == exc.queue_name
    assert context["job_function"] == exc.job_function
    assert context["current_state"] == exc.current_state
    assert context["target_state"] == exc.target_state


def test_invalid_job_state_default_message_is_useful() -> None:
    """Without an explicit ``message`` the default summarises the transition.

    The summary names both states and the API-facing job slug so a log
    line or Slack alert is actionable without unpacking the structured
    fields.
    """
    exc = InvalidJobStateError(
        current_state="in_progress",
        target_state="queued",
        job_public_id="ABCD-EFGH-1234",
        queue_name="docverse:queue",
        job_function="build_processing",
    )
    rendered = str(exc)
    assert "in_progress" in rendered
    assert "queued" in rendered
    assert "ABCD-EFGH-1234" in rendered


def test_invalid_job_state_explicit_message_wins() -> None:
    """Passing ``message`` overrides the auto-generated default."""
    exc = InvalidJobStateError(
        current_state="succeeded",
        target_state="in_progress",
        queue_name="docverse:sync-queue",
        message="Cannot transition keeper sync run 42 backwards",
    )
    assert str(exc) == "Cannot transition keeper sync run 42 backwards"


_JOB_NOT_FOUND_CASES: list[
    tuple[str, Callable[[], JobNotFoundError], dict[str, str]]
] = [
    (
        "fully-known",
        lambda: JobNotFoundError(
            job_public_id="ABCD-EFGH-1234",
            queue_name="docverse:queue",
            job_function="build_processing",
        ),
        {"queue_name": "docverse:queue"},
    ),
    (
        "lookup-by-internal-id",
        lambda: JobNotFoundError(
            queue_name="docverse:queue",
            job_function="QueueJobStore._get_row",
            message="Queue job 42 not found",
        ),
        {"queue_name": "docverse:queue"},
    ),
    (
        "queue-unknown",
        lambda: JobNotFoundError(job_public_id="ABCD-EFGH-1234"),
        {},
    ),
]


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _JOB_NOT_FOUND_CASES,
    ids=[case for case, _, _ in _JOB_NOT_FOUND_CASES],
)
def test_job_not_found_to_sentry_tags(
    case: str,
    factory: Callable[[], JobNotFoundError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` surfaces only the queue name as a tag.

    The ``job_public_id`` and ``job_function`` are high-cardinality and
    live in the ``queue_job_lookup`` context instead. ``queue_name``
    earns its place as a tag for aggregation across one queue.
    """
    info = factory().to_sentry()
    assert info.tags == expected_tags
    assert "job_public_id" not in info.tags
    assert "job_function" not in info.tags


@pytest.mark.parametrize(
    ("case", "factory", "expected_tags"),
    _JOB_NOT_FOUND_CASES,
    ids=[case for case, _, _ in _JOB_NOT_FOUND_CASES],
)
def test_job_not_found_to_sentry_context(
    case: str,
    factory: Callable[[], JobNotFoundError],
    expected_tags: dict[str, str],
) -> None:
    """``to_sentry`` exposes the lookup site as a context.

    The ``queue_job_lookup`` context carries every API-facing
    identifier the exception was constructed with (even ``None``s) so a
    triager can paste ``job_public_id`` straight into a ``/queue/{id}``
    URL without translating internal row ids.
    """
    exc = factory()
    info = exc.to_sentry()
    context = info.contexts["queue_job_lookup"]
    assert context["job_public_id"] == exc.job_public_id
    assert context["queue_name"] == exc.queue_name
    assert context["job_function"] == exc.job_function


def test_job_not_found_default_message_uses_public_id() -> None:
    """The default message names the API-facing ``public_id`` when known."""
    exc = JobNotFoundError(
        job_public_id="ABCD-EFGH-1234",
        queue_name="docverse:queue",
    )
    rendered = str(exc)
    assert "ABCD-EFGH-1234" in rendered
    assert "not found" in rendered


def test_job_not_found_explicit_message_wins() -> None:
    """Passing ``message`` overrides the auto-generated default.

    The store layer queries by internal row id and has no
    ``public_id`` to surface; the ``message`` override path lets the
    log/Slack rendering carry the internal id without leaking it into
    Sentry tags or contexts.
    """
    exc = JobNotFoundError(
        queue_name="docverse:queue",
        job_function="QueueJobStore._get_row",
        message="Queue job 42 not found",
    )
    assert str(exc) == "Queue job 42 not found"
