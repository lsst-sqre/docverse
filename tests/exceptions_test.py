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
)
from docverse.services.dashboard_templates import DashboardTemplateSyncError


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


_FACTORIES: list[tuple[str, Callable[[], DocverseSlackException]]] = [
    ("InvalidJobStateError", lambda: InvalidJobStateError("queued -> queued")),
    ("InvalidBuildStateError", _make_invalid_build_state),
    ("JobNotFoundError", lambda: JobNotFoundError("job ABC123 not found")),
    ("InvalidSlugError", _make_invalid_slug),
    (
        "DashboardTemplateSyncError",
        lambda: DashboardTemplateSyncError("sync failed for binding 1"),
    ),
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
