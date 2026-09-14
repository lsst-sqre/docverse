"""Tests that keep the published docs in step with the code.

``docs/`` has no build and no autodoc, so a page describing a worker's
decision table — or the headers a conditional endpoint answers with —
is prose that can silently fall behind the module it describes. These
tests are the substitute: they read the published Markdown and assert
that every case, counter, field, knob, and parameter the code actually
has is named somewhere on the page.

They deliberately check for *presence* and nothing else. A test cannot
tell whether a sentence explains a bucket correctly, but it can refuse
to let a bucket be added to the planner — or a key to the queue job's
``progress``, or a query parameter to an endpoint — without the page
gaining the word for it, which is the failure mode a docs page of this
kind actually has.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from typing import get_args, get_type_hints

from fastapi import params

from docverse_server.config import Configuration
from docverse_server.domain.edition_reconcile import ReconcileReason, _Skip
from docverse_server.handlers.orgs.projects import get_project, get_projects
from docverse_server.metrics import (
    ConditionalGetEndpoint,
    ConditionalGetEvent,
    ConditionalGetOutcome,
    ConditionalGetPrecondition,
    EditionReconcileCompletedEvent,
)
from docverse_server.services.edition_reconcile import (
    EditionReconcileOutcome,
    _ApplySkip,
)
from docverse_server.storage.pagination import ProjectSortOrder
from docverse_server.worker.functions.edition_reconcile import (
    RECONCILED_DRIFT_MESSAGE,
)

_DOCS = Path(__file__).parents[1] / "docs"

_RECONCILE_PAGE = "edition-reconcile.md"
"""Operations page for the ``edition_reconcile`` loop (PRD #612)."""

_API_PAGE = "api-conventions.md"
"""Reference page for the REST API's cross-cutting conventions."""


def _read(name: str) -> str:
    return (_DOCS / name).read_text()


def _uncoded(names: Iterable[str], page: str) -> list[str]:
    """Names the page does not carry as inline code.

    Stricter than a bare substring search, which a short name like the
    search parameter ``q`` would pass against any page at all. Every
    parameter, field, and enumerated value on the conventions page is
    written as inline code, so requiring the backticks is both the
    house style and the only way these assertions have teeth.
    """
    return sorted(name for name in names if f"`{name}`" not in page)


def _query_parameter_names(endpoint: Callable[..., object]) -> set[str]:
    """Names of the query parameters one FastAPI handler declares.

    Read off the handler's own annotations rather than a hand-kept
    list, so a parameter added to an endpoint is immediately a
    parameter the page is expected to name.
    """
    return {
        name
        for name, hint in get_type_hints(endpoint, include_extras=True).items()
        if any(
            isinstance(metadata, params.Query) for metadata in get_args(hint)
        )
    }


def test_decision_table_names_every_planner_outcome() -> None:
    """Every case ``plan_edition_reconcile`` can reach is documented.

    The union of the republish reasons and the skip buckets *is* the
    planner's decision table, so a new member of either enum that the
    page does not mention is a row an operator would have to read the
    source to discover.
    """
    page = _read(_RECONCILE_PAGE)
    outcomes = {reason.value for reason in ReconcileReason} | {
        skip.value for skip in _Skip
    }
    assert outcomes, "planner exposes no outcomes to document"
    assert not sorted(name for name in outcomes if name not in page)


def test_queue_job_progress_keys_documented() -> None:
    """Every key of the tick's ``queue_jobs.progress`` body is documented."""
    page = _read(_RECONCILE_PAGE)
    keys = set(EditionReconcileOutcome().as_progress())
    assert keys, "outcome renders no progress keys"
    assert not sorted(key for key in keys if key not in page)


def test_metrics_event_fields_documented() -> None:
    """Every field of ``EditionReconcileCompletedEvent`` is documented.

    Only the fields the event declares itself: ``organization`` and
    ``project`` come from the shared payload base and are documented
    once, with the metrics envelope, rather than on every page that
    mentions an event.
    """
    page = _read(_RECONCILE_PAGE)
    fields = set(EditionReconcileCompletedEvent.__annotations__)
    assert fields, "event declares no fields of its own"
    assert not sorted(field for field in fields if field not in page)


def test_apply_time_skip_log_lines_documented() -> None:
    """Both log lines the apply-time re-check emits are documented.

    They are the only record that a planned repair was dropped rather
    than applied — the counters say how many, the lines say which — so
    an operator grepping for one has to be able to find it on the page.
    """
    page = _read(_RECONCILE_PAGE)
    messages = {skip.message for skip in _ApplySkip}
    assert messages, "the re-check reports no skips"
    assert not sorted(message for message in messages if message not in page)


def test_config_knobs_documented() -> None:
    """Every ``edition_reconcile*`` setting is documented."""
    page = _read(_RECONCILE_PAGE)
    knobs = {
        name
        for name in Configuration.model_fields
        if name.startswith("edition_reconcile")
    }
    assert knobs, "configuration exposes no edition_reconcile knobs"
    assert not sorted(name for name in knobs if name not in page)


def test_sentry_message_documented() -> None:
    """The page quotes the Sentry issue title an operator will search."""
    assert RECONCILED_DRIFT_MESSAGE in _read(_RECONCILE_PAGE)


def test_docs_index_links_the_page() -> None:
    """The index points at the operations page."""
    assert _RECONCILE_PAGE in _read("index.md")


def test_conditional_get_endpoints_documented() -> None:
    """Every endpoint that answers a conditional GET is documented.

    :class:`~docverse_server.metrics.ConditionalGetEndpoint` *is* the
    roster of endpoints carrying validators — a handler cannot emit the
    event without a member here — so a member the API conventions page
    does not name is an endpoint whose caching behaviour a client would
    have to read the source to discover.
    """
    page = _read(_API_PAGE)
    endpoints = {endpoint.value for endpoint in ConditionalGetEndpoint}
    assert endpoints, "no endpoint supports conditional GET"
    assert not _uncoded(endpoints, page)


def test_conditional_get_vocabulary_documented() -> None:
    """Both enums a ``conditional_get`` event carries are documented.

    The outcome and the deciding precondition are what an operator
    filters a metrics query on, so every value has to be quotable from
    the page rather than guessed at.
    """
    page = _read(_API_PAGE)
    vocabulary = {outcome.value for outcome in ConditionalGetOutcome} | {
        precondition.value for precondition in ConditionalGetPrecondition
    }
    assert vocabulary, "the event carries no enumerated values"
    assert not _uncoded(vocabulary, page)


def test_conditional_get_event_fields_documented() -> None:
    """Every field of ``ConditionalGetEvent`` is documented.

    Only the fields the event declares itself: ``organization`` and
    ``project`` come from the shared payload base and are documented
    once, with the metrics envelope, rather than on every page that
    mentions an event.
    """
    page = _read(_API_PAGE)
    fields = set(ConditionalGetEvent.__annotations__)
    assert fields, "event declares no fields of its own"
    assert not _uncoded(fields, page)


def test_project_listing_query_parameters_documented() -> None:
    """Every query parameter of the project listing is documented.

    This is the endpoint a consumer polls, so its knobs — the paging
    ones, the search one, and the two this PRD added for mirroring a
    changing project set — are the page's job to name.
    """
    page = _read(_API_PAGE)
    names = _query_parameter_names(get_projects)
    assert names, "the project listing declares no query parameters"
    assert not _uncoded(names, page)


def test_single_project_query_parameters_documented() -> None:
    """Every query parameter of the single-project GET is documented.

    One flag today, and one that changes the endpoint's answer from a
    404 into a representation — precisely the kind of parameter that
    has to be discoverable without reading the handler.
    """
    page = _read(_API_PAGE)
    names = _query_parameter_names(get_project)
    assert names, "the single-project GET declares no query parameters"
    assert not _uncoded(names, page)


def test_project_sort_orders_documented() -> None:
    """Every ordering the project listing accepts is documented.

    ``ProjectSortOrder`` is the accepted set of ``order`` values, so a
    member the page does not name is an ordering a client can only find
    by reading the OpenAPI enum.
    """
    page = _read(_API_PAGE)
    orders = {order.value for order in ProjectSortOrder}
    assert orders, "the project listing accepts no orderings"
    assert not _uncoded(orders, page)
