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

import importlib
import inspect
from collections.abc import Callable, Iterable
from dataclasses import fields
from pathlib import Path
from typing import get_args, get_type_hints

from fastapi import params
from fastapi.routing import APIRoute

from docverse.models import (
    DraftInactivityRule,
    KeeperSyncConfig,
    KeeperSyncScopePreview,
)
from docverse.models.keeper_sync import (
    _MAX_SLUG_PATTERN_LENGTH,
    _MAX_SLUG_PATTERNS,
)
from docverse_server.config import Configuration
from docverse_server.domain.edition_reconcile import ReconcileReason, _Skip
from docverse_server.handlers.orgs.keeper_sync import (
    router as keeper_sync_router,
)
from docverse_server.handlers.orgs.projects import get_project, get_projects
from docverse_server.metrics import (
    BuildContentCopiedEvent,
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
from docverse_server.services.keeper_sync import (
    EditionSyncOutcome,
    ProjectSyncResult,
)
from docverse_server.storage._http_retry import (
    DEFAULT_BASE_BACKOFF_SECONDS,
    RETRYABLE_TRANSPORT_ERRORS,
    backoff_for_attempt,
)
from docverse_server.storage.build_store import BuildStore
from docverse_server.storage.edition_store import EditionStore
from docverse_server.storage.ltd import RETRYABLE_SOURCE_TRANSPORT_ERRORS
from docverse_server.storage.pagination import ProjectSortOrder
from docverse_server.worker.functions.edition_reconcile import (
    RECONCILED_DRIFT_MESSAGE,
)
from docverse_server.worker.functions.keeper_sync import _ScopeCounts
from docverse_server.worker.main import (
    COPY_HTTP_CONNECTION_HEADROOM,
    COPY_HTTP_TIMEOUT,
    copy_http_limits,
)

_DOCS = Path(__file__).parents[1] / "docs"

_RECONCILE_PAGE = "edition-reconcile.md"
"""Operations page for the ``edition_reconcile`` loop (PRD #612)."""

_API_PAGE = "api-conventions.md"
"""Reference page for the REST API's cross-cutting conventions."""

_SCOPE_PAGE = "keeper-sync-scope.md"
"""Operations page for the keeper-sync scope rule (PRD #667)."""

_TRANSPORT_PAGE = "keeper-sync-transport.md"
"""Operations page for keeper-sync transport resilience (PRD #685)."""

_TIMESTAMPS_SECTION = "Timestamps mirror LTD"
"""Scope-page section on keeper-sync's LTD clock stamp (PRD #706)."""

_TRANSPORT_KNOB_PREFIXES = ("keeper_sync_upload_", "keeper_sync_copy_retry_")
"""Name prefixes of the settings that shape a build copy's retries."""


def _read(name: str) -> str:
    return (_DOCS / name).read_text()


def _table_row(page: str, first_cell: str) -> str:
    """Return the Markdown table row whose first cell is ``first_cell``.

    The cell is matched as inline code. This lets a test assert that a
    setting's environment variable and default sit on the *same* row as
    its name, which a page-wide substring search cannot tell apart from
    the three appearing in unrelated places.
    """
    prefix = f"| `{first_cell}` |"
    for line in page.splitlines():
        if line.startswith(prefix):
            return line
    msg = f"no table row starts with {prefix!r}"
    raise AssertionError(msg)


def _uncoded(names: Iterable[str], page: str) -> list[str]:
    """Names the page does not carry as inline code.

    Stricter than a bare substring search, which a short name like the
    search parameter ``q`` would pass against any page at all. Every
    parameter, field, and enumerated value on the conventions page is
    written as inline code, so requiring the backticks is both the
    house style and the only way these assertions have teeth.
    """
    return sorted(name for name in names if f"`{name}`" not in page)


def _section(page: str, heading: str) -> str:
    """Return the body of the page's ``## heading`` section.

    Runs to the next second-level heading, so its own ``###``
    subsections are part of it.
    """
    parts = page.split(f"\n## {heading}\n", 1)
    assert len(parts) == 2, f"the page has no {heading!r} section"
    return parts[1].split("\n## ", 1)[0]


def _stamped_columns(table: str, stamp: Callable[..., object]) -> set[str]:
    """``table.column`` for every column one ``set_sync_dates`` writes.

    The stamp's keyword-only parameters *are* the columns it sets, so
    reading them off the signature means a column added to (or renamed
    in) keeper-sync's clock stamp is one the page has to name.
    """
    return {
        f"{table}.{name}"
        for name, parameter in inspect.signature(stamp).parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }


def _import_name(cls: type) -> str:
    """Return the dotted name an exception class is imported by.

    ``httpx.TimeoutException`` rather than the private module that
    defines it, but ``botocore.exceptions.ConnectionError``, since
    botocore does not re-export its exceptions at the top level.
    """
    package = cls.__module__.split(".", 1)[0]
    if getattr(importlib.import_module(package), cls.__name__, None) is cls:
        return f"{package}.{cls.__name__}"
    return f"{cls.__module__}.{cls.__name__}"


def _route_path(name: str) -> str:
    """URL path of one keeper-sync route, looked up by handler name.

    Read off the router rather than written out, so a page quoting an
    endpoint's URL cannot survive that URL being moved.
    """
    for route in keeper_sync_router.routes:
        if isinstance(route, APIRoute) and route.name == name:
            return route.path
    msg = f"no keeper-sync route named {name!r}"
    raise AssertionError(msg)


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


def test_docs_index_links_the_scope_page() -> None:
    """The index points at the keeper-sync scoping page."""
    assert _SCOPE_PAGE in _read("index.md")


def test_scope_config_fields_documented() -> None:
    """Every config field that shapes the sync scope is documented.

    The slug-bearing fields of :class:`KeeperSyncConfig` *are* the
    scope rule's inputs, so a field added to the config — a second
    exclude form, say — that this page does not name is a knob an
    operator could only find by reading the OpenAPI schema.
    """
    page = _read(_SCOPE_PAGE)
    fields = {name for name in KeeperSyncConfig.model_fields if "slug" in name}
    assert fields, "the config exposes no slug fields"
    assert not _uncoded(fields, page)


def test_scope_preview_response_fields_documented() -> None:
    """Every field of the scope-preview response is documented.

    The page's whole job is teaching an operator to read this body
    before saving a wider scope, so a field it does not name is one
    nobody has been told how to act on.
    """
    page = _read(_SCOPE_PAGE)
    fields = set(KeeperSyncScopePreview.model_fields)
    assert fields, "the preview reports no fields"
    assert not _uncoded(fields, page)


def test_scope_log_counts_documented() -> None:
    """Every count the scope-resolution log events carry is documented.

    :class:`~docverse_server.worker.functions.keeper_sync._ScopeCounts`
    *is* the run-discovery and tier-cron events' payload, and three of
    its names are shared with the preview response — so a count added
    or renamed without the page gaining the word for it is exactly the
    drift issue #680 was filed about.
    """
    page = _read(_SCOPE_PAGE)
    counts = set(_ScopeCounts.__annotations__)
    assert counts, "the scope resolution reports no counts"
    assert not _uncoded(counts, page)


def test_scope_count_identity_documented() -> None:
    """The page states the identity relating the five counts.

    The counts are only comparable across a preview and the run it
    launches because ``in_scope_count`` is the config resolution on
    both sides; the identity is what tells an operator that a
    shortfall between the two is the tombstones and nothing else.
    """
    page = _read(_SCOPE_PAGE)
    assert "`in_scope_count` - `tombstoned_count` = `fan_out_count`" in page


def test_scope_listing_flag_and_filter_documented() -> None:
    """The page names the listing's scope flag and the query that filters it.

    The listing is the one keeper-sync endpoint that reports an
    out-of-scope project rather than 404ing it, so the flag telling the
    two apart — and the query that turns the listing into a
    stale-exclude report — are what makes the "nothing else changes"
    section above actionable rather than merely reassuring.
    """
    page = _read(_SCOPE_PAGE)
    assert _route_path("get_org_keeper_sync_projects") in page
    assert not _uncoded({"in_scope"}, page)
    assert "?in_scope=false" in page


def test_scope_pattern_caps_documented() -> None:
    """The page quotes the two caps a 422 would otherwise explain.

    Both are the mitigation this PRD chose *instead* of a match
    timeout, so an operator writing a wave's patterns has to be able
    to read the limits off the page rather than discover them by
    tripping them.
    """
    page = _read(_SCOPE_PAGE)
    assert str(_MAX_SLUG_PATTERNS) in page
    assert str(_MAX_SLUG_PATTERN_LENGTH) in page


def test_scope_endpoint_paths_documented() -> None:
    """Every endpoint the scope rule governs is documented by URL.

    The preview and the config writes are the wave workflow; the three
    per-project endpoints are the ones that answer 404 once a slug
    falls out of scope, which is the behaviour operators most often
    mistake for data loss.
    """
    page = _read(_SCOPE_PAGE)
    names = {
        "get_org_keeper_sync_config",
        "put_org_keeper_sync_config",
        "patch_org_keeper_sync_config",
        "post_org_keeper_sync_scope_preview",
        "post_org_keeper_sync_run",
        "get_org_keeper_sync_project_status",
        "get_org_keeper_sync_project_editions",
        "post_org_keeper_sync_project_refresh",
    }
    paths = {_route_path(name) for name in names}
    assert not sorted(path for path in paths if path not in page)


def test_timestamps_section_names_every_stamped_column() -> None:
    """The scope page says which columns keeper-sync sets from LTD.

    Read off the two ``set_sync_dates`` stamps, so the page cannot
    keep describing a clock the sync has stopped writing, or miss one
    it has started to.
    """
    section = _section(_read(_SCOPE_PAGE), _TIMESTAMPS_SECTION)
    columns = _stamped_columns(
        "editions", EditionStore.set_sync_dates
    ) | _stamped_columns("builds", BuildStore.set_sync_dates)
    assert columns, "keeper-sync stamps no columns"
    assert not _uncoded(columns, section)


def test_timestamps_section_names_the_backfill_signals() -> None:
    """The section names the backfill's run endpoint and its read-outs.

    The backfill is a full org run, so the section has to name the
    endpoint that launches one; ``restamped_edition_count`` on the
    project's summary log line is how an operator confirms it did
    anything, and ``dates_restamped`` is the per-edition flag it counts.
    """
    section = _section(_read(_SCOPE_PAGE), _TIMESTAMPS_SECTION)
    assert hasattr(ProjectSyncResult, "restamped_edition_count")
    assert "dates_restamped" in {
        field.name for field in fields(EditionSyncOutcome)
    }
    assert _route_path("post_org_keeper_sync_run") in section
    assert not _uncoded(
        {"restamped_edition_count", "dates_restamped"}, section
    )


def test_timestamps_section_covers_what_the_clock_drives() -> None:
    """The section covers ``draft_inactivity`` and the project clock.

    Re-dating a draft to LTD's last rebuild can make the lifecycle rule
    reap it on the next tick, which an operator has to hear about
    before running the backfill; and ``projects.date_updated`` is the
    clock the stamp deliberately leaves alone.
    """
    section = _section(_read(_SCOPE_PAGE), _TIMESTAMPS_SECTION)
    rule_type = DraftInactivityRule.model_fields["type"].default
    assert not _uncoded({rule_type, "projects.date_updated"}, section)


def test_docs_index_links_the_timestamps_section() -> None:
    """The index points at the timestamps section by its anchor."""
    assert "keeper-sync-scope.md#timestamps-mirror-ltd" in _read("index.md")


def test_draft_inactivity_rule_links_the_timestamps_section() -> None:
    """The rule's own documentation points at the timestamps section.

    ``DraftInactivityRule``'s docstring is the rule's schema description
    in the OpenAPI document, so it is where an operator reading about
    ``max_days_inactive`` has to learn that a synced draft is judged on
    LTD's clock.
    """
    doc = DraftInactivityRule.__doc__ or ""
    assert _SCOPE_PAGE in doc
    assert _TIMESTAMPS_SECTION in doc


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


def test_docs_index_links_the_transport_page() -> None:
    """The index points at the keeper-sync transport page."""
    assert _TRANSPORT_PAGE in _read("index.md")


def test_scope_page_related_links_the_transport_page() -> None:
    """The scope page's related reading points at the transport page.

    The two pages are the keeper-sync operations docs; an operator who
    has just launched a wave from the scope page is the one who then
    has to read that wave's copy failures.
    """
    related = _read(_SCOPE_PAGE).split("\n## Related\n", 1)
    assert len(related) == 2, "the scope page has no Related section"
    assert _TRANSPORT_PAGE in related[1]


def test_transport_knobs_documented_with_env_var_and_default() -> None:
    """Every build-copy retry setting is a row naming its env var and default.

    Read off :class:`Configuration` rather than written out, so renaming
    a setting, changing its default, or adding another knob under the
    same prefixes fails here until the page's table says so.
    """
    page = _read(_TRANSPORT_PAGE)
    env_prefix = Configuration.model_config.get("env_prefix", "")
    knobs = {
        name
        for name in Configuration.model_fields
        if name.startswith(_TRANSPORT_KNOB_PREFIXES)
    }
    assert knobs, "configuration exposes no build-copy retry knobs"
    for name in sorted(knobs):
        row = _table_row(page, name)
        env_var = f"{env_prefix}{name}".upper()
        default = Configuration.model_fields[name].default
        assert f"`{env_var}`" in row, name
        assert f"`{default}`" in row, name


def test_transport_page_names_the_upload_cap_and_memory_knobs() -> None:
    """The page names the copy pool's knob and the two memory knobs.

    ``keeper_sync_upload_concurrency`` sizes the copy client's pool;
    ``keeper_sync_max_jobs`` x ``keeper_sync_copy_concurrency`` is the
    buffered-body bound the sync worker's memory limit is sized against.
    """
    page = _read(_TRANSPORT_PAGE)
    assert not _uncoded(
        {
            "keeper_sync_upload_concurrency",
            "keeper_sync_max_jobs",
            "keeper_sync_copy_concurrency",
        },
        page,
    )


def test_transport_page_states_the_process_wide_upload_cap() -> None:
    """The copy-client section covers the cap; the non-goals drop it.

    PRD #685 listed capping in-flight copies across the process among
    the things the layers deliberately do not do. PRD #698 reversed
    that: ``keeper_sync_upload_concurrency`` bounds every presigned PUT
    in the sync worker, so the section describing the copy client has
    to name it, and the non-goals list must stop denying it.
    """
    page = _read(_TRANSPORT_PAGE)
    copy_client = _section(page, "The copy client")
    assert "\n### The worker-wide upload cap\n" in copy_client
    assert not _uncoded({"keeper_sync_upload_concurrency"}, copy_client)
    non_goals = _section(page, "What the layers deliberately do not do")
    assert "Cap in-flight copies" not in non_goals


def test_transport_ride_out_arithmetic_documented() -> None:
    """The page's per-object ride-out sum matches the shipped defaults.

    An object rides out a connect outage until its last attempt starts:
    every backoff sleep plus every earlier attempt's connect timeout.
    Recomputed here from the retry policy, the default budget and the
    copy client's timeout, so a change to any of them has to be carried
    into the page's arithmetic.
    """
    page = _read(_TRANSPORT_PAGE)
    fields = Configuration.model_fields
    max_attempts = fields["keeper_sync_upload_max_attempts"].default
    max_backoff = fields["keeper_sync_upload_max_backoff_seconds"].default
    connect = COPY_HTTP_TIMEOUT.connect
    assert connect is not None
    delays = [
        backoff_for_attempt(
            attempt,
            base_backoff_seconds=DEFAULT_BASE_BACKOFF_SECONDS,
            max_backoff_seconds=max_backoff,
        )
        for attempt in range(1, max_attempts)
    ]
    ride_out = sum(delays) + (max_attempts - 1) * connect
    assert " + ".join(f"{delay:g}" for delay in delays) in page
    assert f"{ride_out:g} s" in page


def test_copy_client_timeouts_documented() -> None:
    """Each copy-client timeout is a row carrying its shipped value."""
    page = _read(_TRANSPORT_PAGE)
    assert not _uncoded({"COPY_HTTP_TIMEOUT"}, page)
    for field in ("connect", "read", "write", "pool"):
        value = getattr(COPY_HTTP_TIMEOUT, field)
        assert value is not None, field
        assert f"| {value:g} s |" in _table_row(page, field), field


def test_copy_client_pool_documented() -> None:
    """The pool rows name the constants they add and the stock result.

    The stock values are derived from the configuration default through
    :func:`copy_http_limits` itself, so the page's "at the defaults"
    column cannot drift from what a worker actually opens.
    """
    page = _read(_TRANSPORT_PAGE)
    limits = copy_http_limits(
        upload_concurrency=Configuration.model_fields[
            "keeper_sync_upload_concurrency"
        ].default,
    )

    connections = _table_row(page, "max_connections")
    assert "`keeper_sync_upload_concurrency`" in connections
    assert "`COPY_HTTP_CONNECTION_HEADROOM`" in connections
    assert f"({COPY_HTTP_CONNECTION_HEADROOM})" in connections
    assert connections.endswith(f"| {limits.max_connections} |")

    keepalive = _table_row(page, "max_keepalive_connections")
    assert keepalive.endswith(f"| {limits.max_keepalive_connections} |")

    expiry = _table_row(page, "keepalive_expiry")
    assert limits.keepalive_expiry is not None
    assert "`COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS`" in expiry
    assert expiry.endswith(f"| {limits.keepalive_expiry:g} s |")


def test_build_content_copied_event_fields_documented() -> None:
    """Every field of ``BuildContentCopiedEvent`` is documented.

    Only the fields the event declares itself: ``organization`` and
    ``project`` come from the shared payload base and are documented
    once, with the metrics envelope, rather than on every page that
    mentions an event.
    """
    page = _read(_TRANSPORT_PAGE)
    fields = set(BuildContentCopiedEvent.__annotations__)
    assert fields, "event declares no fields of its own"
    assert not _uncoded(fields, page)


def test_build_retry_transport_errors_documented() -> None:
    """The build-level retry section names every error class it re-runs.

    Read off both retryable tuples, the R2 upload's httpx classes and
    the LTD download's botocore classes, so widening either one fails
    here until the section that tells an operator what gets re-run says
    so.
    """
    section = _section(_read(_TRANSPORT_PAGE), "The build-level retry")
    classes = (*RETRYABLE_TRANSPORT_ERRORS, *RETRYABLE_SOURCE_TRANSPORT_ERRORS)
    assert not _uncoded({_import_name(cls) for cls in classes}, section)
