"""Pydantic models for queue job resources."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    ValidationInfo,
    field_validator,
    model_serializer,
)

from ._examples import (
    EXAMPLE_BUILD_ID,
    EXAMPLE_BUILD_URL,
    EXAMPLE_EDITION_URL,
    EXAMPLE_JOB_ID,
    EXAMPLE_JOB_URL,
    EXAMPLE_PUBLISH_JOB_ID,
    EXAMPLE_PUBLISH_JOB_URL,
    EXAMPLE_RUN_ID,
)
from .queue_enums import JobKind, JobStatus, RetiredBuildStatus

if TYPE_CHECKING:
    from pydantic import GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema

__all__ = [
    "BuildProcessingProgress",
    "EditionUpdateRef",
    "ExcludedPathScrubBuild",
    "ExcludedPathScrubEntry",
    "ExcludedPathScrubProgress",
    "PublishJobRef",
    "QueueJob",
]


class EditionUpdateRef(BaseModel):
    """An entry in a build_processing job's ``editions_updated``/``skipped``.

    All fields are optional and unknown keys are preserved
    (``extra="allow"``) so the payload can grow without breaking older
    clients.
    """

    model_config = ConfigDict(extra="allow")

    slug: str | None = Field(
        default=None,
        description="Slug of the edition that was updated or skipped.",
    )

    action: str | None = Field(
        default=None,
        description=(
            "Tracking action applied to the edition (e.g. ``updated`` or"
            " ``created``); omitted for skipped editions."
        ),
    )

    edition_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the edition resource (a HATEOAS link clients"
            " follow instead of reconstructing the path). Omitted when the"
            " Docverse API base URL could not be resolved."
        ),
        examples=[EXAMPLE_EDITION_URL],
    )


class PublishJobRef(BaseModel):
    """An entry in a build_processing job's ``publish_jobs``.

    Identifies a child ``publish_edition`` queue job enqueued for an
    updated edition. All fields are optional and unknown keys are
    preserved (``extra="allow"``).
    """

    model_config = ConfigDict(extra="allow")

    edition_slug: str | None = Field(
        default=None,
        description="Slug of the edition this publish job targets.",
    )

    publish_queue_job_public_id: str | None = Field(
        default=None,
        description=(
            "Public Crockford Base32 identifier of the publish_edition job."
        ),
        examples=[EXAMPLE_PUBLISH_JOB_ID],
    )

    job_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the publish_edition job resource (a"
            " HATEOAS link clients follow instead of reconstructing the"
            " path). Omitted when the Docverse API base URL could not be"
            " resolved."
        ),
        examples=[EXAMPLE_PUBLISH_JOB_URL],
    )


class BuildProcessingProgress(BaseModel):
    """Typed ``progress`` payload for a ``build_processing`` queue job.

    All fields are optional and unknown keys are preserved
    (``extra="allow"``) so other job kinds — whose progress shapes are
    not modelled here — round-trip unchanged through this model.
    """

    model_config = ConfigDict(extra="allow")

    message: str | None = Field(
        default=None,
        description="Human-readable progress message.",
    )

    object_count: int | None = Field(
        default=None,
        description="Number of objects uploaded to the object store.",
    )

    total_size_bytes: int | None = Field(
        default=None,
        description="Total size in bytes of the uploaded objects.",
    )

    editions_updated: list[EditionUpdateRef] | None = Field(
        default=None,
        description="Editions whose pointer was moved to this build.",
    )

    editions_skipped: list[EditionUpdateRef] | None = Field(
        default=None,
        description="Editions the stale-build guard left unchanged.",
    )

    publish_jobs: list[PublishJobRef] | None = Field(
        default=None,
        description=(
            "Child publish_edition jobs enqueued for updated editions."
        ),
    )

    edition_tracking_error: bool | None = Field(
        default=None,
        description="``True`` when edition tracking failed for this build.",
    )

    deleted_skipped: bool | None = Field(
        default=None,
        description=(
            "``True`` when the build was deleted before it could be"
            " published, so it was cancelled instead. Set whether the"
            " delete landed before the worker picked the job up or while"
            " the worker was still uploading."
        ),
    )

    retired_status: RetiredBuildStatus | None = Field(
        default=None,
        description=(
            "Status the build was found in when the worker stood down,"
            " on a job that completed without publishing because"
            " something else retired the build first. ``missing`` means"
            " the row was gone by the time the worker looked. ``null``"
            " (the key omitted) on jobs that were never retired."
        ),
        examples=[RetiredBuildStatus.superseded],
    )

    @model_serializer(mode="wrap")
    def _drop_none_keys(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        """Serialize to only keys whose value is not ``None``.

        Every field is *declared* (and ``__get_pydantic_json_schema__``
        keeps them in the serialization-mode schema), but a non-build job
        validated into this model leaves the eight build-specific typed
        fields ``None``. Dropping them at serialization time keeps a
        non-build job's ``progress`` to its real keys (e.g. ``message`` plus
        its ``extra='allow'`` extras) instead of leaking eight ``null`` keys,
        while a build job still emits every field it actually set.
        """
        return {k: v for k, v in handler(self).items() if v is not None}

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Keep the typed field schema in serialization mode.

        Without this, the ``_drop_none_keys`` model serializer collapses
        the serialization-mode JSON schema to a property-less
        ``{type: object, additionalProperties: true}``. FastAPI builds
        response-model schemas in serialization mode, so the generated
        OpenAPI / api-types would regress ``progress`` back to a free-form
        object with none of the nine typed fields. Dropping the
        model-level serializer from the core schema before generating
        restores the declared fields; runtime drop-``None`` serialization
        is unaffected (it runs off the compiled serializer, not this).
        """
        if handler.mode == "serialization":
            core_schema = cast(
                "CoreSchema",
                {k: v for k, v in core_schema.items() if k != "serialization"},
            )
        return handler(core_schema)


class ExcludedPathScrubEntry(BaseModel):
    """One excluded path found in a build, in a scrub report row.

    Unknown keys are preserved (``extra="allow"``) so the report can grow
    without breaking older clients.
    """

    model_config = ConfigDict(extra="allow")

    path: str = Field(
        description=(
            "Build-relative path of the excluded entry, up to and"
            " including its first excluded segment. A directory ends"
            " with ``/`` and stands for every object under it (for"
            " example ``.doctrees/``); a file has no trailing slash (for"
            " example ``.buildinfo``). An entry inside a nested sub-site"
            " keeps its parent segments, as in ``v1/.doctrees/``."
        ),
        examples=[".doctrees/"],
    )

    object_count: int = Field(
        description="Number of objects stored under the entry.",
        examples=[80],
    )

    total_size_bytes: int = Field(
        description="Total size in bytes of the entry's objects.",
        examples=[64_000_000],
    )


class ExcludedPathScrubBuild(BaseModel):
    """One build's row in an ``excluded_path_scrub`` report.

    A row is written for every build the scrub found excluded paths in,
    and for every build it could not examine or finish (with ``error``
    set). A build a real scrub left alone because it was deleted or
    scrubbed by another run in the meantime has ``skip_reason`` set. A
    build that holds no excluded paths is counted in ``builds_examined``
    but gets no row. Unknown keys are preserved (``extra="allow"``).
    """

    model_config = ConfigDict(extra="allow")

    build_id: str = Field(
        description="Public Crockford Base32 identifier of the build.",
        examples=[EXAMPLE_BUILD_ID],
    )

    build_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the build resource (a HATEOAS link clients"
            " follow instead of reconstructing the path). ``null`` when"
            " the Docverse API base URL could not be resolved."
        ),
        examples=[EXAMPLE_BUILD_URL],
    )

    edition_slugs: list[str] = Field(
        default_factory=list,
        description=(
            "Slugs of the live editions whose current build this is, so"
            " an operator can see what serves the build being scrubbed."
        ),
        examples=[["__main", "v1"]],
    )

    entries: list[ExcludedPathScrubEntry] = Field(
        default_factory=list,
        description=(
            "Excluded entries found in the build, in path order. Empty"
            " when the build could not be examined."
        ),
    )

    error: str | None = Field(
        default=None,
        description=(
            "Why the build could not be examined or its excluded paths"
            " could not all be removed and recorded, or ``null`` when"
            " nothing failed. A failed build is counted in"
            " ``builds_failed``, is left unstamped so a later scrub"
            " retries it, and the scrub moves on to the next build."
        ),
    )

    skip_reason: str | None = Field(
        default=None,
        description=(
            "Why a real scrub left the build alone although it held"
            " excluded paths: the build was deleted or retired, or"
            " another scrub finished it, after this one listed it."
            " ``null`` for a build the scrub went on to scrub, and always"
            " ``null`` in a dry run. A skipped build is counted in"
            " ``builds_skipped``."
        ),
        examples=["the build was deleted during the scrub"],
    )


class ExcludedPathScrubProgress(BaseModel):
    """Typed ``progress`` payload for an ``excluded_path_scrub`` queue job.

    The scrub's report: summary counters plus one row per build it found
    excluded paths in or failed to examine. The worker rewrites it as it
    goes, so a long scrub can be read before it finishes. In a dry run
    the ``*_removed`` counters and the rows describe what the scrub
    *would* remove; nothing is deleted. In a real run the ``*_removed``
    counters are what was actually deleted, so a build skipped before
    its deletes contributes nothing to them. Unknown keys are preserved
    (``extra="allow"``).
    """

    model_config = ConfigDict(extra="allow")

    dry_run: bool = Field(
        description=(
            "``true`` when the scrub only reports what it would remove."
        ),
    )

    builds_examined: int = Field(
        default=0,
        description=(
            "Builds the scrub has listed so far, whether or not they"
            " held excluded paths."
        ),
    )

    builds_affected: int = Field(
        default=0,
        description="Builds found holding at least one excluded path.",
    )

    objects_removed: int = Field(
        default=0,
        description=(
            "Excluded objects removed across the affected builds (in a"
            " dry run, the objects that would be removed)."
        ),
    )

    bytes_removed: int = Field(
        default=0,
        description=(
            "Total size in bytes of ``objects_removed`` (in a dry run,"
            " the bytes that would be removed)."
        ),
    )

    builds_failed: int = Field(
        default=0,
        description=(
            "Builds the scrub could not examine or finish; each has a"
            " row with ``error`` set."
        ),
    )

    builds_skipped: int = Field(
        default=0,
        description=(
            "Builds a real scrub found excluded paths in but left alone,"
            " because they were retired or scrubbed by another run"
            " after this one listed them; each has a row with"
            " ``skip_reason`` set."
        ),
    )

    builds: list[ExcludedPathScrubBuild] = Field(
        default_factory=list,
        description=(
            "One row per affected, skipped or failed build, in scrub order."
        ),
    )


class QueueJob(BaseModel):
    """Response model for a queue job."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(
        description="URL to this queue job resource.",
        examples=[EXAMPLE_JOB_URL],
    )

    id: str = Field(
        description="Public Crockford Base32 identifier for the job.",
        examples=[EXAMPLE_JOB_ID],
    )

    kind: JobKind = Field(description="Kind of background job.")

    status: JobStatus = Field(description="Current status of the job.")

    keeper_sync_run_id: str | None = Field(
        default=None,
        description=(
            "Public Crockford Base32 identifier of the keeper-sync run this"
            " job is attributed to, or ``null`` for jobs not part of a run."
        ),
        examples=[EXAMPLE_RUN_ID],
    )

    subject_label: str | None = Field(
        default=None,
        description=(
            "Human-readable identifier for the resource this job targets"
            " (e.g. an LTD slug for keeper-sync project jobs)."
        ),
        examples=["pipelines"],
    )

    subject_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the primary API resource this job processes (a"
            " HATEOAS link clients follow instead of reconstructing the"
            " path): the build for build_processing jobs, the edition for"
            " publish jobs. ``null`` when no API resource exists or it could"
            " not be resolved."
        ),
        examples=[EXAMPLE_BUILD_URL],
    )

    build_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the build this job processes, or ``null`` when"
            " the job targets no build or it could not be resolved."
        ),
        examples=[EXAMPLE_BUILD_URL],
    )

    edition_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the edition this job targets, or ``null`` when"
            " the job targets no edition or it could not be resolved."
        ),
        examples=[EXAMPLE_EDITION_URL],
    )

    phase: str | None = Field(
        default=None,
        description=(
            "Current processing phase (e.g., inventory, tracking,"
            " editions, dashboard)."
        ),
        examples=["editions"],
    )

    progress: BuildProcessingProgress | ExcludedPathScrubProgress | None = (
        Field(
            default=None,
            description=(
                "Structured progress data. An ``excluded_path_scrub`` job"
                " carries an ``ExcludedPathScrubProgress`` report; every"
                " other kind is validated as ``BuildProcessingProgress``,"
                " which types ``build_processing`` jobs and round-trips"
                " other kinds' fields via ``extra='allow'``."
            ),
        )
    )

    errors: dict[str, Any] | None = Field(
        default=None,
        description="Collected error details.",
    )

    date_created: datetime = Field(
        description="Timestamp when the job was enqueued."
    )

    date_started: datetime | None = Field(
        default=None,
        description="Timestamp when a worker picked up the job.",
    )

    date_completed: datetime | None = Field(
        default=None,
        description="Timestamp when the job finished.",
    )

    @field_validator("progress", mode="before")
    @classmethod
    def _validate_progress_for_kind(
        cls, value: Any, info: ValidationInfo
    ) -> Any:
        """Validate a raw ``progress`` mapping into its kind's model.

        Both progress models accept unknown keys, so letting the union
        choose by shape alone could type one kind's payload as the
        other's. The job's ``kind`` (declared, and so validated, before
        ``progress``) decides instead: ``excluded_path_scrub`` gets
        `ExcludedPathScrubProgress`, every other kind
        `BuildProcessingProgress`. A model instance passes through.
        """
        if not isinstance(value, Mapping):
            return value
        if info.data.get("kind") == JobKind.excluded_path_scrub:
            return ExcludedPathScrubProgress.model_validate(value)
        return BuildProcessingProgress.model_validate(value)
