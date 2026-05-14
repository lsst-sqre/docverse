"""Tests for the shared ``DocverseSlackException`` base.

Pins the contract introduced in PRD #338 / slice #340 that every
non-``ClientRequestError`` server-side exception inherits from
``DocverseSlackException`` — itself a ``safir.slack.blockkit.SlackException``
subclass — so future enrichment slices (#341–#344) can override
``to_sentry()`` against a single shared base.

The unit tests here assert membership in both bases and that the
inherited default ``to_slack()`` renders without error. The end-to-end
Sentry capture is exercised separately in :mod:`tests.sentry_test`.
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


_FACTORIES: list[tuple[str, Callable[[], DocverseSlackException]]] = [
    ("InvalidJobStateError", lambda: InvalidJobStateError("queued -> queued")),
    (
        "InvalidBuildStateError",
        lambda: InvalidBuildStateError("uploaded -> uploaded"),
    ),
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

    No constructor overrides in this slice — the default ``to_slack`` is
    the contract every migrated subclass exposes.
    """
    exc = factory()
    message = exc.to_slack()
    assert isinstance(message, SlackMessage)
    assert str(exc) == exc.message
