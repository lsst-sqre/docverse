"""Dedicated string enums for Sasquatch metrics event payloads.

These enums are intentionally separate from the API client enums in
``docverse.client.models`` (SQR-112 D4/D7): the metrics schema is a
published Avro contract consumed by Sasquatch and must be able to
evolve independently of the HTTP API. Each enum is mapped from its
API-side counterpart at the emission site, so a rename on either side
is an explicit, reviewable change rather than a silent schema break.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from docverse.client.models import EditionKind, OrgRole, PrincipalType

__all__ = [
    "EditionPublishTrigger",
    "LifecycleAction",
    "LifecycleActionTrigger",
    "LifecycleReapAction",
    "MembershipChangeAction",
    "MetricsEditionKind",
    "MetricsOrgRole",
    "MetricsPrincipalType",
]


class MetricsEditionKind(StrEnum):
    """Kind of edition, as recorded on edition metrics events.

    Mirrors :class:`docverse.client.models.EditionKind` value-for-value;
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
        """Map the API :class:`~docverse.client.models.EditionKind`.

        Values are identical, so this is a straight value lookup; keeping
        the mapping explicit lets the metrics schema evolve independently
        of the API model (SQR-112 D4).
        """
        return cls(kind.value)


class LifecycleAction(StrEnum):
    """The management operation recorded on a ``*_lifecycle`` event.

    A single consolidated enum (SQR-112 D4) backs both
    :class:`~docverse.metrics.payloads.ProjectLifecycleEvent` and
    :class:`~docverse.metrics.payloads.EditionLifecycleEvent`: rather
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
    ``post_member`` emits ``add`` and ``delete_member`` emits ``remove``.
    """

    add = "add"
    remove = "remove"


class MetricsOrgRole(StrEnum):
    """Org role recorded on a ``membership_changed`` event.

    Mirrors :class:`docverse.client.models.OrgRole` value-for-value; the
    emission site maps the API enum to this one so the metrics Avro
    schema does not depend on the API model (SQR-112 D4).
    """

    reader = "reader"
    uploader = "uploader"
    admin = "admin"

    @classmethod
    def from_api(cls, role: OrgRole) -> MetricsOrgRole:
        """Map the API :class:`~docverse.client.models.OrgRole`.

        Values are identical, so this is a straight value lookup; keeping
        the mapping explicit lets the metrics schema evolve independently
        of the API model.
        """
        return cls(role.value)


class MetricsPrincipalType(StrEnum):
    """Principal type recorded on a ``membership_changed`` event.

    Mirrors :class:`docverse.client.models.PrincipalType` value-for-value;
    the emission site maps the API enum to this one so the metrics Avro
    schema does not depend on the API model (SQR-112 D4).
    """

    user = "user"
    group = "group"

    @classmethod
    def from_api(cls, principal_type: PrincipalType) -> MetricsPrincipalType:
        """Map the API :class:`~docverse.client.models.PrincipalType`.

        Values are identical, so this is a straight value lookup; keeping
        the mapping explicit lets the metrics schema evolve independently
        of the API model.
        """
        return cls(principal_type.value)


class LifecycleActionTrigger(StrEnum):
    """Which worker drove a ``lifecycle_action`` reap (SQR-112 D7).

    ``lifecycle_action`` is emitted by two shared reaper workers that
    both soft-delete resources; this enum records which one performed a
    given reap. Each worker selects its trigger statically at the
    emission site (``lifecycle_eval`` vs. ``git_ref_audit``).
    """

    lifecycle_eval = "lifecycle_eval"
    git_ref_audit = "git_ref_audit"


class LifecycleReapAction(StrEnum):
    """The lifecycle rule that drove a reap on a ``lifecycle_action`` event.

    Mirrors the lifecycle-rule ``type`` discriminators
    (:class:`docverse.domain.lifecycle.LifecycleRule`) value-for-value;
    the emission site maps the matched rule's ``type`` to this enum so the
    metrics Avro schema evolves independently of the rule schema. The
    ``lifecycle_eval`` worker emits ``draft_inactivity`` (editions) and
    ``build_history_orphan`` (builds); ``git_ref_audit`` emits only
    ``ref_deleted``.
    """

    draft_inactivity = "draft_inactivity"
    build_history_orphan = "build_history_orphan"
    ref_deleted = "ref_deleted"

    @classmethod
    def from_rule_type(cls, rule_type: str) -> LifecycleReapAction:
        """Map a lifecycle-rule ``type`` discriminator to this enum.

        Values are identical to the rule ``type`` strings, so this is a
        straight value lookup; keeping the mapping explicit makes a rename
        on either side a reviewable change rather than a silent schema
        break. The reaper workers filter their rule sets to the kinds they
        own before evaluation, so every ``rule_type`` reaching here is one
        of this enum's members.
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
    """

    build = "build"
    keeper_sync = "keeper_sync"
