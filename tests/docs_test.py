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

import ast
import importlib
import inspect
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, fields
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from types import NoneType
from typing import get_args, get_type_hints
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import APIRouter, params
from fastapi.routing import APIRoute
from safir.metrics import EventManager, EventPayload

from docverse.models import (
    DraftInactivityRule,
    EditionUpdate,
    KeeperSyncConfig,
    KeeperSyncScopePreview,
    ProjectGitHubBinding,
    ProjectGitHubBindingCreate,
)
from docverse.models.keeper_sync import (
    _MAX_SLUG_PATTERN_LENGTH,
    _MAX_SLUG_PATTERNS,
)
from docverse_server.config import Configuration
from docverse_server.domain.edition_reconcile import ReconcileReason, _Skip
from docverse_server.handlers.orgs.editions import router as editions_router
from docverse_server.handlers.orgs.keeper_sync import (
    router as keeper_sync_router,
)
from docverse_server.handlers.orgs.projects import get_project, get_projects
from docverse_server.handlers.webhooks.github import _event_router
from docverse_server.metrics import (
    BuildContentCopiedEvent,
    ConditionalGetEndpoint,
    ConditionalGetEvent,
    ConditionalGetOutcome,
    ConditionalGetPrecondition,
    DocverseEvents,
    EditionReconcileCompletedEvent,
    HttpStatusClass,
)
from docverse_server.services.default_branch import DefaultBranchTrigger
from docverse_server.services.edition_reconcile import (
    EditionReconcileOutcome,
    _ApplySkip,
)
from docverse_server.services.keeper_sync import (
    EditionSyncOutcome,
    ProjectSyncResult,
    TrackingDerivationSource,
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

_METRICS_PAGE = "metrics.md"
"""Catalog of every Sasquatch metrics event Docverse publishes (PRD #713)."""

_METRICS_TOPIC = "lsst.square.metrics.events.docverse"
"""Kafka topic of every Docverse event, and its measurements' prefix.

Safir names the topic ``lsst.square.metrics.events.<application>`` and
each event's Avro schema ``<topic>.<event>``, which Telegraf writes as the
InfluxDB measurement name; ``METRICS_APPLICATION`` is ``docverse`` in
every deployment.
"""

_GITHUB_PAGE = "github-integration.md"
"""Operations page for the GitHub App integration (PRD #721)."""

_GITHUB_KNOB_PREFIXES = ("github_", "git_ref_audit")
"""Name prefixes of the settings the GitHub integration page tabulates."""

_DEFAULT_BRANCH_LOG_MODULES = (
    "docverse_server.services.default_branch",
    "docverse_server.services.default_branch_processor",
)
"""Modules the GitHub page documents every log line of.

Together they *are* the default-branch convergence: the webhook's
payload handling and the one rule every trigger applies.
"""

_GITHUB_LOG_MODULES = (
    *_DEFAULT_BRANCH_LOG_MODULES,
    "docverse_server.worker.functions.git_ref_audit",
    "docverse_server.worker.functions.project_github_resolve",
)
"""Modules whose log lines the GitHub page's log table may quote."""

_KEEPER_SYNC_SERVICE_MODULE = "docverse_server.services.keeper_sync.service"
"""Module that logs keeper-sync's ``__main`` tracking derivation."""

_KEEPER_SYNC_TRACKING_MESSAGE = "Derived keeper-sync edition tracking and kind"
"""The debug line saying where a synced edition's tracking came from."""

_LOG_LEVELS = frozenset({"debug", "info", "warning", "error", "exception"})
"""The structlog methods whose first argument is a log line's message."""

_DOCUMENTED_TYPES: dict[object, str] = {
    str: "string",
    int: "integer",
    float: "float",
    bool: "boolean",
    timedelta: "duration",
}
"""The metrics page's Type-column word for each scalar payload annotation.

Enum-valued fields read ``enum``, and a nullable field appends
``or null``; see :func:`_documented_type`.
"""


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


async def _registered_events() -> dict[str, type[EventPayload]]:
    """Return every event ``DocverseEvents.initialize`` registers, by name.

    Runs the real ``initialize`` against a manager that only records
    what it was asked to create, so the roster is exactly what every
    process registers at startup rather than a hand-kept list that could
    miss the next event added.
    """
    manager = Mock(spec=EventManager)
    manager.create_publisher = AsyncMock(return_value=Mock())
    await DocverseEvents().initialize(manager)
    return {
        call.args[0]: call.args[1]
        for call in manager.create_publisher.call_args_list
    }


def _event_section(page: str, name: str) -> str:
    """Return the body of the metrics catalog's section for one event.

    The catalog gives each event a ``### `name``` heading; the section
    runs to the next heading of the same or a higher level.
    """
    parts = page.split(f"\n### `{name}`\n", 1)
    assert len(parts) == 2, f"the page has no section for {name!r}"
    return re.split(r"\n#{1,3} ", parts[1], maxsplit=1)[0]


def _cells(row: str) -> list[str]:
    """Split one Markdown table row into its stripped cells."""
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def _field_cells(section: str, field: str) -> list[str] | None:
    """Cells of the table row documenting ``field``, or ``None``."""
    prefix = f"| `{field}` |"
    for line in section.splitlines():
        if line.startswith(prefix):
            return _cells(line)
    return None


def _enum_of(annotation: object) -> type[StrEnum] | None:
    """Return the metrics enum a payload field is typed with, if any."""
    for candidate in (annotation, *get_args(annotation)):
        if isinstance(candidate, type) and issubclass(candidate, StrEnum):
            return candidate
    return None


def _documented_type(annotation: object) -> str:
    """Return the metrics page's Type cell for a payload field annotation.

    Read off the payload model rather than the Avro schema, because the
    page describes a field the way its emitter writes it: a
    ``timedelta`` is a ``duration`` (seconds once it is in InfluxDB),
    whatever Avro type carries it.
    """
    members = get_args(annotation)
    if NoneType in members:
        (inner,) = (member for member in members if member is not NoneType)
        return f"{_documented_type(inner)} or null"
    if _enum_of(annotation) is not None:
        return "enum"
    return _DOCUMENTED_TYPES[annotation]


def _phalanx_tags(page: str) -> list[str]:
    """Return the ``influxTags`` list the metrics page quotes from Phalanx.

    The list lives in Phalanx, which this repository cannot read, so the
    page's quotation of it is what the per-event Stored-as cells are
    checked against.
    """
    for block in page.split("```yaml\n")[1:]:
        body = block.split("```", 1)[0]
        if "influxTags:" in body:
            listing = body.split("influxTags:", 1)[1]
            return [
                line.strip().removeprefix("- ")
                for line in listing.splitlines()
                if line.strip().startswith("- ")
            ]
    msg = "the page quotes no influxTags list"
    raise AssertionError(msg)


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


def _route_path(name: str, *, router: APIRouter = keeper_sync_router) -> str:
    """URL path of one route, looked up by handler name.

    Read off the router (the keeper-sync one unless told otherwise)
    rather than written out, so a page quoting an endpoint's URL cannot
    survive that URL being moved.
    """
    for route in router.routes:
        if isinstance(route, APIRoute) and route.name == name:
            return route.path
    msg = f"no route named {name!r}"
    raise AssertionError(msg)


def _subsection(section: str, heading: str) -> str:
    """Return the body of a section's ``### heading`` subsection.

    Runs to the next third-level heading or the end of the section.
    """
    parts = section.split(f"\n### {heading}\n", 1)
    assert len(parts) == 2, f"the section has no {heading!r} subsection"
    return parts[1].split("\n### ", 1)[0]


def _code_rows(text: str) -> list[list[str]]:
    """Cells of every table row whose first cell is inline code."""
    return [
        _cells(line) for line in text.splitlines() if line.startswith("| `")
    ]


def _inline_code(text: str) -> set[str]:
    """Every inline-code span in ``text``, without its backticks."""
    return set(re.findall(r"`([^`]+)`", text))


def _documented_default(default: object) -> str:
    """Return the Default cell a settings table gives a field's default.

    A boolean reads the way its environment variable is spelled, and a
    setting with no default reads "unset".
    """
    if default is None:
        return "unset"
    if isinstance(default, bool):
        return f"`{str(default).lower()}`"
    return f"`{default}`"


def _webhook_subscriptions() -> set[str]:
    """Every event, or ``event.action``, the webhook endpoint dispatches.

    Read off the gidgethub router's registration tables. They are
    private, but they are the only record of what the handler module
    registered, so a callback added for a new event or action is one
    the page has to gain a row for.
    """
    names = set(_event_router._shallow_routes)
    for event, details in _event_router._deep_routes.items():
        for values in details.values():
            names.update(f"{event}.{value}" for value in values)
    return names


@dataclass(frozen=True, slots=True)
class _LogCall:
    """One structlog call: its level and the fields it adds."""

    level: str
    fields: frozenset[str]


def _module_tree(module_name: str) -> ast.Module:
    """Parse a module's source into a syntax tree."""
    return ast.parse(inspect.getsource(importlib.import_module(module_name)))


def _log_calls(module_name: str) -> dict[str, list[_LogCall]]:
    """Every log line a module writes, keyed by its message.

    Read off the module's syntax tree: any ``debug`` / ``info`` /
    ``warning`` / ``error`` / ``exception`` call whose first argument is
    a string literal, with the keyword arguments it passes. Adjacent
    literals are one constant by then, so a message wrapped across
    source lines is matched whole.
    """
    calls: dict[str, list[_LogCall]] = {}
    for node in ast.walk(_module_tree(module_name)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _LOG_LEVELS
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            continue
        calls.setdefault(node.args[0].value, []).append(
            _LogCall(
                level=node.func.attr,
                fields=frozenset(
                    keyword.arg
                    for keyword in node.keywords
                    if keyword.arg is not None
                ),
            )
        )
    return calls


def _bound_log_fields(module_name: str) -> set[str]:
    """Every field a module binds onto a logger with ``.bind(...)``."""
    return {
        keyword.arg
        for node in ast.walk(_module_tree(module_name))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "bind"
        for keyword in node.keywords
        if keyword.arg is not None
    }


def _documented_log_lines(page: str) -> dict[str, _LogCall]:
    """Return the GitHub page's log-line table, keyed by message."""
    table = _subsection(_section(page, "Reading an outcome"), "Log lines")
    return {
        cells[0].strip("`"): _LogCall(
            level=cells[1], fields=frozenset(_inline_code(cells[2]))
        )
        for cells in _code_rows(table)
    }


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


@pytest.mark.asyncio
async def test_metrics_page_covers_every_registered_event() -> None:
    """Every event ``DocverseEvents.initialize`` registers has a section.

    Read off the registration itself, so an event added to the catalog
    without a section here — the one place an operator can look up what
    a measurement holds — fails, and each section has to name the
    measurement its event lands in.
    """
    page = _read(_METRICS_PAGE)
    events = await _registered_events()
    assert events, "DocverseEvents registers no events"
    missing = sorted(
        name for name in events if f"\n### `{name}`\n" not in page
    )
    assert not missing
    unnamed = sorted(
        name
        for name in events
        if f"`{_METRICS_TOPIC}.{name}`" not in _event_section(page, name)
    )
    assert not unnamed


@pytest.mark.asyncio
async def test_metrics_page_types_every_event_field() -> None:
    """Every payload field is a row of its event's table, correctly typed.

    Every field, the shared ``organization`` and ``project`` included:
    on this page, unlike the operations pages, the table is the
    reference. The Type cell is derived from the payload annotation, so
    a field turning nullable, or changing unit, has to be carried here.
    """
    page = _read(_METRICS_PAGE)
    wrong: list[str] = []
    for name, payload in (await _registered_events()).items():
        section = _event_section(page, name)
        for field, info in payload.model_fields.items():
            cells = _field_cells(section, field)
            expected = _documented_type(info.annotation)
            if cells is None or cells[1] != expected:
                wrong.append(f"{name}.{field}: {expected}")
    assert not wrong


@pytest.mark.asyncio
async def test_metrics_page_marks_the_phalanx_tags() -> None:
    """Each field's Stored-as cell agrees with the quoted tag list.

    Telegraf applies the one ``influxTags`` list to every Docverse
    measurement, so a field is a tag exactly when its name is on the
    list, whichever event it belongs to. Every name on the list must also
    be a field some event carries, or the list is tagging nothing.
    """
    page = _read(_METRICS_PAGE)
    tags = _phalanx_tags(page)
    events = await _registered_events()
    assert tags, "the page quotes an empty tag list"
    assert len(tags) == len(set(tags)), "the tag list repeats a name"
    carried = {
        field for payload in events.values() for field in payload.model_fields
    }
    assert not sorted(set(tags) - carried)
    wrong: list[str] = []
    for name, payload in events.items():
        section = _event_section(page, name)
        for field in payload.model_fields:
            cells = _field_cells(section, field)
            expected = "tag" if field in tags else "field"
            if cells is None or cells[2] != expected:
                wrong.append(f"{name}.{field}: {expected}")
    assert not wrong


@pytest.mark.asyncio
async def test_metrics_page_says_which_events_each_tag_applies_to() -> None:
    """The Tags section's table names every event each tag lands on.

    A tag name applies to every event carrying a field of that name,
    which is easy to forget when adding one: this table is where the
    page spells that out, so it is checked against the payloads here.
    """
    page = _read(_METRICS_PAGE)
    section = _section(page, "Tags")
    events = await _registered_events()
    wrong: list[str] = []
    for tag in _phalanx_tags(page):
        cells = _field_cells(section, tag)
        carriers = {
            name
            for name, payload in events.items()
            if tag in payload.model_fields
        }
        if cells is None:
            wrong.append(tag)
        elif cells[1] == "every event":
            if carriers != set(events):
                wrong.append(tag)
        elif set(re.findall(r"`([^`]+)`", cells[1])) != carriers:
            wrong.append(tag)
    assert not wrong


@pytest.mark.asyncio
async def test_metrics_page_names_every_enum_value() -> None:
    """Every value an enum-typed field can carry is named in its section.

    Those values are what a query filters on and what a tag's series are,
    so each has to be quotable from the page rather than guessed at.
    """
    page = _read(_METRICS_PAGE)
    missing: list[str] = []
    for name, payload in (await _registered_events()).items():
        section = _event_section(page, name)
        for field, info in payload.model_fields.items():
            enum = _enum_of(info.annotation)
            if enum is None:
                continue
            values = {member.value for member in enum}
            missing.extend(
                f"{name}.{field}={value}"
                for value in _uncoded(values, section)
            )
    assert not missing


def test_metrics_page_names_every_status_class() -> None:
    """The ``api_request`` section names every ``status_class`` value.

    ``status_class`` is a string in the Avro schema, because enum symbols
    may not begin with a digit, so the enum check above cannot see it;
    its vocabulary is :class:`HttpStatusClass` all the same.
    """
    section = _event_section(_read(_METRICS_PAGE), "api_request")
    assert not _uncoded({value.value for value in HttpStatusClass}, section)


def test_metrics_page_has_an_example_query_per_capability() -> None:
    """The page carries one InfluxQL query for each PRD #713 question.

    Sync lag, request volume, request latency, and webhook deliveries:
    each query has to read its measurement and group by the tags that
    answer its question.
    """
    queries = _section(_read(_METRICS_PAGE), "Example queries")
    blocks = [
        block.split("```", 1)[0] for block in queries.split("```sql\n")[1:]
    ]
    wanted = [
        (
            "edition_published",
            (
                'PERCENTILE("ltd_lag", 50)',
                'PERCENTILE("ltd_lag", 95)',
                '"organization" = ',
                "now() - 24h",
            ),
        ),
        ("api_request", ('GROUP BY time(1m), "route", "status_class"',)),
        ("api_request", ('PERCENTILE("duration", 95)', 'GROUP BY "route"')),
        (
            "github_webhook_received",
            ('GROUP BY time(1h), "event_type", "outcome"',),
        ),
    ]
    unanswered = [
        f"{event}: {snippets}"
        for event, snippets in wanted
        if not any(
            f'"{_METRICS_TOPIC}.{event}"' in block
            and all(snippet in block for snippet in snippets)
            for block in blocks
        )
    ]
    assert not unanswered


def test_docs_index_links_the_metrics_page() -> None:
    """The index points at the metrics catalog."""
    assert _METRICS_PAGE in _read("index.md")


def test_transport_page_metrics_event_links_the_metrics_page() -> None:
    """The transport page's metrics-event section points at the catalog."""
    reading = _section(_read(_TRANSPORT_PAGE), "Reading a copy")
    metrics_event = reading.split("\n### Logs\n", 1)[0]
    assert _METRICS_PAGE in metrics_event


def test_conditional_get_section_links_the_metrics_page() -> None:
    """The conventions page's conditional GET section points at the catalog."""
    section = _section(
        _read(_API_PAGE), "Conditional GET: the `ETag` validator"
    )
    assert _METRICS_PAGE in section


def test_docs_index_links_the_github_integration_page() -> None:
    """The index points at the GitHub integration page."""
    assert _GITHUB_PAGE in _read("index.md")


def test_github_webhook_table_matches_the_router() -> None:
    """The webhook table has one row per event the endpoint dispatches.

    Checked both ways: a callback registered for an event or action the
    table does not name is behaviour an operator would have to read the
    handler to discover, and a row for one the router no longer
    registers describes a delivery that is now ``ignored``.
    """
    section = _section(_read(_GITHUB_PAGE), "Webhook events")
    documented = {cells[0].strip("`") for cells in _code_rows(section)}
    subscriptions = _webhook_subscriptions()
    assert subscriptions, "the webhook router registers no events"
    assert documented == subscriptions


def test_github_page_names_every_default_branch_trigger() -> None:
    """Every ``trigger`` the default-branch rule logs is documented.

    The trigger is how an operator tells a webhook-driven rewrite from
    one the audit made after a missed delivery, so every value has to be
    quotable from the page.
    """
    page = _read(_GITHUB_PAGE)
    triggers = {trigger.value for trigger in DefaultBranchTrigger}
    assert triggers, "the rule reports no triggers"
    assert not _uncoded(triggers, page)


def test_github_page_documents_the_default_branch_field() -> None:
    """The page names the read-only ``github.default_branch`` field.

    The field is on the binding the API returns and absent from the one
    it accepts, which is what "GitHub is the source of truth" means on
    the wire.
    """
    assert "default_branch" in ProjectGitHubBinding.model_fields
    assert "default_branch" not in ProjectGitHubBindingCreate.model_fields
    assert not _uncoded({"github.default_branch"}, _read(_GITHUB_PAGE))


def test_github_knobs_documented_with_env_var_and_default() -> None:
    """Every GitHub setting is a row naming its env var and default.

    Checked both ways, off :class:`Configuration`: a GitHub or audit
    setting the table lacks is one an operator cannot find, and a row
    naming a setting that no longer exists is a knob that does nothing.
    """
    section = _section(_read(_GITHUB_PAGE), "Configuration")
    env_prefix = Configuration.model_config.get("env_prefix", "")
    knobs = {
        name
        for name in Configuration.model_fields
        if name.startswith(_GITHUB_KNOB_PREFIXES)
    }
    assert knobs, "configuration exposes no GitHub knobs"
    documented = {cells[0].strip("`") for cells in _code_rows(section)}
    assert documented == knobs
    for name in sorted(knobs):
        cells = _cells(_table_row(section, name))
        env_var = f"{env_prefix}{name}".upper()
        default = Configuration.model_fields[name].default
        assert cells[1] == f"`{env_var}`", name
        assert cells[2] == _documented_default(default), name


def test_github_audit_backfill_names_the_feature_flag() -> None:
    """The backfill section names the flag the audit only runs behind.

    The audit is how an upgraded environment's ``NULL`` columns are
    filled, and it ships disabled, so an operator reading how the
    backfill works has to learn there that it needs turning on.
    """
    section = _section(_read(_GITHUB_PAGE), "The audit is the backfill")
    assert "git_ref_audit_enabled" in Configuration.model_fields
    assert not _uncoded({"git_ref_audit_enabled"}, section)


def test_github_log_lines_exist_in_the_code() -> None:
    """Every row of the log table is a line the code writes, exactly.

    The message has to be one a GitHub module emits, at the level the
    row says, adding exactly the fields the row lists, so a renamed
    message or a dropped field cannot leave the page describing a line
    nobody can grep for.
    """
    documented = _documented_log_lines(_read(_GITHUB_PAGE))
    assert documented, "the page documents no log lines"
    emitted: dict[str, list[_LogCall]] = {}
    for module_name in _GITHUB_LOG_MODULES:
        for message, calls in _log_calls(module_name).items():
            emitted.setdefault(message, []).extend(calls)
    wrong = sorted(
        message
        for message, row in documented.items()
        if row not in emitted.get(message, [])
    )
    assert not wrong


def test_github_page_documents_every_default_branch_log_line() -> None:
    """Every line the default-branch rule and processor write has a row.

    Read off the two modules' syntax trees, so a log line added to
    either has to be carried into the table.
    """
    documented = _documented_log_lines(_read(_GITHUB_PAGE))
    missing = sorted(
        message
        for module_name in _DEFAULT_BRANCH_LOG_MODULES
        for message in _log_calls(module_name)
        if message not in documented
    )
    assert not missing


def test_github_log_fields_table_matches_the_bound_context() -> None:
    """The log-fields table lists exactly what the two modules bind.

    Those fields ride on every line the table below it lists, so they
    are documented once rather than on each row.
    """
    section = _section(_read(_GITHUB_PAGE), "Reading an outcome")
    table = _subsection(section, "Log fields")
    documented = {cells[0].strip("`") for cells in _code_rows(table)}
    bound = {
        field
        for module_name in _DEFAULT_BRANCH_LOG_MODULES
        for field in _bound_log_fields(module_name)
    }
    assert bound, "the default-branch modules bind no fields"
    assert documented == bound


def test_github_keeper_sync_section_names_the_tracking_log() -> None:
    """The keeper-sync section quotes the line reporting ``tracking_source``.

    The line is how an operator tells a synced ``__main`` that follows
    the default branch from one mirroring LTD, so its message, the
    fields that say so, and every ``tracking_source`` value have to be
    on the page and in the code.
    """
    section = _section(_read(_GITHUB_PAGE), "Keeper-synced projects")
    fields = {"tracking_source", "ltd_tracked_refs", "git_ref"}
    calls = _log_calls(_KEEPER_SYNC_SERVICE_MODULE).get(
        _KEEPER_SYNC_TRACKING_MESSAGE, []
    )
    assert any(fields <= call.fields for call in calls)
    assert f"`{_KEEPER_SYNC_TRACKING_MESSAGE}`" in section
    sources = {source.value for source in TrackingDerivationSource}
    assert not _uncoded(fields | sources, section)


def test_github_manual_fallback_documented() -> None:
    """The operators' section quotes the edition endpoints it relies on.

    Read off the editions router and the ``PATCH`` body model, so the
    recipe cannot keep naming a path or a field that has moved.
    """
    section = _section(
        _read(_GITHUB_PAGE), "Pinned `__main` editions are left for operators"
    )
    patch = _route_path("patch_edition", router=editions_router)
    delete = _route_path("delete_edition", router=editions_router)
    assert f"PATCH {patch.replace('{edition}', '__main')}" in section
    assert f"DELETE {delete}" in section
    fields = {"tracking_mode", "tracking_params", "build"}
    assert fields <= set(EditionUpdate.model_fields)
    assert not _uncoded(fields, section)
