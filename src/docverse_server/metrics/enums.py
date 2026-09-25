"""Dedicated string enums for Sasquatch metrics event payloads.

These enums are intentionally separate from the API client enums in
``docverse.models`` (SQR-112 D4/D7): the metrics schema is a
published Avro contract consumed by Sasquatch and must be able to
evolve independently of the HTTP API. Each enum is mapped from its
API-side counterpart at the emission site, so a rename on either side
is an explicit, reviewable change rather than a silent schema break.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from docverse.models import EditionKind, OrgRole, PrincipalType
    from docverse_server.domain.conditional_get import PreconditionKind

__all__ = [
    "ConditionalGetEndpoint",
    "ConditionalGetOutcome",
    "ConditionalGetPrecondition",
    "EditionPublishTrigger",
    "HttpStatusClass",
    "LifecycleAction",
    "LifecycleActionTrigger",
    "LifecycleReapAction",
    "MembershipChangeAction",
    "MetricsEditionKind",
    "MetricsOrgRole",
    "MetricsPrincipalType",
    "WebhookOutcome",
]


class MetricsEditionKind(StrEnum):
    """Kind of edition, as recorded on edition metrics events.

    Mirrors :class:`docverse.models.EditionKind` value-for-value;
    the emission site maps the API enum to this one so the metrics Avro
    schema does not depend on the API model.
    """

    main = "main"
    release = "release"
    draft = "draft"
    major = "major"
    minor = "minor"
    alternate = "alternate"

    @classmethod
    def from_api(cls, kind: EditionKind) -> MetricsEditionKind:
        """Map the API :class:`~docverse.models.EditionKind`.

        Values are identical, so this is a straight value lookup; keeping
        the mapping explicit lets the metrics schema evolve independently
        of the API model (SQR-112 D4).
        """
        return cls(kind.value)


class LifecycleAction(StrEnum):
    """The management operation recorded on a ``*_lifecycle`` event.

    A single consolidated enum (SQR-112 D4) backs both
    :class:`~docverse_server.metrics.payloads.ProjectLifecycleEvent` and
    :class:`~docverse_server.metrics.payloads.EditionLifecycleEvent`: rather
    than a distinct event type per CRUD verb, one ``project_lifecycle`` /
    ``edition_lifecycle`` event carries the verb in this field. The
    emission site selects the action statically (each handler knows its
    own operation). ``rollback`` applies only to editions; projects emit
    just ``create``/``update``/``delete``.
    """

    create = "create"
    update = "update"
    delete = "delete"
    rollback = "rollback"


class MembershipChangeAction(StrEnum):
    """The membership operation recorded on a ``membership_changed`` event.

    Unlike the CRUD-shaped :class:`LifecycleAction`, an org membership is
    only ever added or removed (an in-place role change is modelled as a
    remove + add by the API), so this event carries a dedicated
    add/remove verb. The emission site selects the action statically:
    ``post_member`` emits ``add``, ``delete_member`` emits ``remove``, and
    ``patch_member`` emits a ``remove`` of the old role followed by an
    ``add`` of the new one when a role actually changes.
    """

    add = "add"
    remove = "remove"


class MetricsOrgRole(StrEnum):
    """Org role recorded on a ``membership_changed`` event.

    Mirrors :class:`docverse.models.OrgRole` value-for-value; the
    emission site maps the API enum to this one so the metrics Avro
    schema does not depend on the API model (SQR-112 D4).
    """

    reader = "reader"
    uploader = "uploader"
    admin = "admin"

    @classmethod
    def from_api(cls, role: OrgRole) -> MetricsOrgRole:
        """Map the API :class:`~docverse.models.OrgRole`.

        Values are identical, so this is a straight value lookup; keeping
        the mapping explicit lets the metrics schema evolve independently
        of the API model.
        """
        return cls(role.value)


class MetricsPrincipalType(StrEnum):
    """Principal type recorded on a ``membership_changed`` event.

    Mirrors :class:`docverse.models.PrincipalType` value-for-value;
    the emission site maps the API enum to this one so the metrics Avro
    schema does not depend on the API model (SQR-112 D4).
    """

    user = "user"
    group = "group"

    @classmethod
    def from_api(cls, principal_type: PrincipalType) -> MetricsPrincipalType:
        """Map the API :class:`~docverse.models.PrincipalType`.

        Values are identical, so this is a straight value lookup; keeping
        the mapping explicit lets the metrics schema evolve independently
        of the API model.
        """
        return cls(principal_type.value)


class LifecycleActionTrigger(StrEnum):
    """Which worker drove a ``lifecycle_action`` reap (SQR-112 D7).

    ``lifecycle_action`` is emitted by every maintenance worker that
    retires a resource; this enum records which one performed a given
    reap. Each worker selects its trigger statically at the emission
    site.

    ``lifecycle_eval`` and ``git_ref_audit`` soft-delete rows.
    ``purgatory_cleanup`` is the far end of that same road: it reaps a
    build that was soft-deleted long enough ago by permanently
    reclaiming its object-store content. Carrying both steps on one
    event type is what lets a consumer follow a resource from the rule
    that retired it through to the sweep that freed its bytes.
    """

    lifecycle_eval = "lifecycle_eval"
    git_ref_audit = "git_ref_audit"
    purgatory_cleanup = "purgatory_cleanup"


class LifecycleReapAction(StrEnum):
    """What drove a reap on a ``lifecycle_action`` event.

    The first three members mirror the lifecycle-rule ``type``
    discriminators
    (:class:`docverse_server.domain.lifecycle.LifecycleRule`)
    value-for-value; the emission site maps the matched rule's ``type``
    to this enum so the metrics Avro schema evolves independently of the
    rule schema. The ``lifecycle_eval`` worker emits
    ``draft_inactivity`` (editions) and ``build_history_orphan``
    (builds); ``git_ref_audit`` emits only ``ref_deleted``.

    ``retention_expired`` is the exception, and deliberately not a rule
    ``type``: no lifecycle rule reclaims storage. What retires a build's
    content is the organization's ``purgatory_retention`` elapsing after
    the row was soft-deleted, so the ``purgatory_cleanup`` sweep names
    this member directly rather than reaching it through
    :meth:`from_rule_type`.
    """

    draft_inactivity = "draft_inactivity"
    build_history_orphan = "build_history_orphan"
    ref_deleted = "ref_deleted"
    retention_expired = "retention_expired"

    @classmethod
    def from_rule_type(cls, rule_type: str) -> LifecycleReapAction:
        """Map a lifecycle-rule ``type`` discriminator to this enum.

        Values are identical to the rule ``type`` strings, so this is a
        straight value lookup; keeping the mapping explicit makes a rename
        on either side a reviewable change rather than a silent schema
        break. The reaper workers filter their rule sets to the kinds they
        own before evaluation, so every ``rule_type`` reaching here is one
        of the three rule-mirroring members.
        """
        return cls(rule_type)


class EditionPublishTrigger(StrEnum):
    """What caused a ``publish_edition`` job to run.

    ``publish_edition`` is a shared worker reached from more than one
    flow (SQR-112 D7), so the event records which one drove this
    publish:

    - ``build`` — a client-uploaded build's edition-tracking fan-out.
    - ``keeper_sync`` — the LTD Keeper backfill (the publish job's
      ``queue_jobs`` row carries a ``keeper_sync_run_id``).
    - ``rollback`` — a user-initiated edition rollback (the rollback
      handler enqueues a ``publish_edition`` job with no
      ``keeper_sync_run_id``, tagging its payload ``trigger=rollback``
      so it is not conflated with build fan-out).
    - ``reconcile`` — the periodic reconciliation loop (PRD #612)
      re-driving a publish some other flow lost. These are repairs
      rather than new work, so charting them next to ``build`` is how
      an operator sees how much of an environment's publish traffic is
      the system healing itself.

    ``build`` is the default: a publish with neither a
    ``keeper_sync_run_id`` nor an explicit payload ``trigger`` is the
    ordinary build-driven fan-out.
    """

    build = "build"
    keeper_sync = "keeper_sync"
    rollback = "rollback"
    reconcile = "reconcile"


class ConditionalGetEndpoint(StrEnum):
    """Which read endpoint evaluated a conditional GET.

    Every conditional endpoint shares one event type rather than
    getting its own, so a single query answers "how much traffic are
    our validators actually saving?" across the API. The value is an
    endpoint identity, not the route template, so a path change does
    not break the Avro contract.
    """

    projects_list = "projects_list"
    """``GET /orgs/{org}/projects``."""

    project = "project"
    """``GET /orgs/{org}/projects/{project}``."""

    organization = "organization"
    """``GET /orgs/{org}``."""


class ConditionalGetOutcome(StrEnum):
    """Whether a conditional GET was answered 304 or with a body."""

    not_modified = "not_modified"
    """The caller already held the current representation (304)."""

    modified = "modified"
    """The representation had changed, so it was sent in full (200)."""

    @classmethod
    def from_not_modified(cls, *, not_modified: bool) -> ConditionalGetOutcome:
        """Map the domain evaluation's boolean answer."""
        return cls.not_modified if not_modified else cls.modified


class ConditionalGetPrecondition(StrEnum):
    """Which request header decided a conditional GET.

    Mirrors
    :class:`~docverse_server.domain.conditional_get.PreconditionKind`
    value-for-value; the emission site maps the domain enum to this one
    so the published Avro schema does not move whenever the RFC
    evaluator's internals are refactored (SQR-112 D4).
    """

    etag = "etag"
    """``If-None-Match`` was evaluated.

    The only member: ``ETag`` is the sole validator Docverse publishes,
    so no other header can decide a conditional GET. Kept as an enum
    rather than collapsed away so a future validator is an added member
    rather than a changed field type in the Avro schema.
    """

    @classmethod
    def from_domain(cls, kind: PreconditionKind) -> ConditionalGetPrecondition:
        """Map the domain :class:`PreconditionKind`.

        Values are identical, so this is a straight value lookup;
        keeping it explicit makes a divergence a compile-time edit
        rather than a silent schema break.
        """
        return cls(kind.value)


class HttpStatusClass(StrEnum):
    """The class of an HTTP response status, as recorded on ``api_request``.

    The five classes of RFC 9110 §15, valued by the ``Nxx`` shorthand an
    operator already writes for them. ``status_class`` is an InfluxDB tag
    so a dashboard can group latency and volume by outcome without the
    cardinality of every distinct ``status_code``, which the event keeps
    as a field for drill-down.
    """

    informational = "1xx"
    """``1xx``: an interim response."""

    successful = "2xx"
    """``2xx``: the request succeeded."""

    redirection = "3xx"
    """``3xx``: a redirect, or a ``304 Not Modified``."""

    client_error = "4xx"
    """``4xx``: the request was refused, not found, or malformed."""

    server_error = "5xx"
    """``5xx``: the server failed to answer the request."""

    @classmethod
    def from_status_code(cls, status_code: int) -> HttpStatusClass:
        """Map an HTTP status code to its class by its hundreds digit.

        Raises
        ------
        ValueError
            If ``status_code`` is outside RFC 9110's ``100``-``599``
            range, where no class applies.
        """
        if not 100 <= status_code <= 599:
            msg = f"HTTP status code {status_code} has no status class"
            raise ValueError(msg)
        return cls(f"{status_code // 100}xx")


class WebhookOutcome(StrEnum):
    """What became of one GitHub webhook delivery.

    Recorded on ``github_webhook_received``, one per delivery, so the
    stream answers both "how many deliveries arrive?" and "how many of
    them did anything?". The members follow the order a delivery is
    decided in: the app must be configured, the signature must verify,
    and a subscribed event type is then dispatched to its callbacks.
    """

    dispatched = "dispatched"
    """The event type is subscribed and every callback ran (``200``)."""

    ignored = "ignored"
    """Signed, but no callback subscribes to the event type (``200``).

    GitHub sends every event type the app is subscribed to on its side,
    such as ``ping`` when a webhook is first set up, whether or not
    Docverse acts on it.
    """

    invalid_signature = "invalid_signature"
    """The request was unsigned or its HMAC did not verify (``401``)."""

    not_configured = "not_configured"
    """This deployment has no GitHub App configured (``404``)."""

    error = "error"
    """The delivery raised while being parsed or dispatched (``500``)."""
