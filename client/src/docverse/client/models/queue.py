"""Pydantic models for queue job resources."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
)

from .queue_enums import JobKind, JobStatus

if TYPE_CHECKING:
    from pydantic import GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema

__all__ = [
    "BuildProcessingProgress",
    "EditionUpdateRef",
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
    )

    queue_job_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the publish_edition queue-job resource (a"
            " HATEOAS link clients follow instead of reconstructing the"
            " path). Omitted when the Docverse API base URL could not be"
            " resolved."
        ),
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

    @model_serializer(mode="wrap")
    def _drop_none_keys(
        self, handler: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        """Serialize to only keys whose value is not ``None``.

        Every field is *declared* (and ``__get_pydantic_json_schema__``
        keeps them in the serialization-mode schema), but a non-build job
        validated into this model leaves the six build-specific typed
        fields ``None``. Dropping them at serialization time keeps a
        non-build job's ``progress`` to its real keys (e.g. ``message`` plus
        its ``extra='allow'`` extras) instead of leaking six ``null`` keys,
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
        object with none of the seven typed fields. Dropping the
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


class QueueJob(BaseModel):
    """Response model for a queue job."""

    model_config = ConfigDict(from_attributes=True)

    self_url: str = Field(description="URL to this queue job resource.")

    id: str = Field(
        description="Public Crockford Base32 identifier for the job."
    )

    kind: JobKind = Field(description="Kind of background job.")

    status: JobStatus = Field(description="Current status of the job.")

    keeper_sync_run_id: int | None = Field(
        default=None,
        description=(
            "Identifier of the keeper-sync run this job is attributed to,"
            " or ``null`` for jobs not part of a run."
        ),
    )

    subject_label: str | None = Field(
        default=None,
        description=(
            "Human-readable identifier for the resource this job targets"
            " (e.g. an LTD slug for keeper-sync project jobs)."
        ),
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
    )

    build_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the build this job processes, or ``null`` when"
            " the job targets no build or it could not be resolved."
        ),
    )

    edition_url: str | None = Field(
        default=None,
        description=(
            "Absolute URL of the edition this job targets, or ``null`` when"
            " the job targets no edition or it could not be resolved."
        ),
    )

    phase: str | None = Field(
        default=None,
        description=(
            "Current processing phase (e.g., inventory, tracking,"
            " editions, dashboard)."
        ),
    )

    progress: BuildProcessingProgress | None = Field(
        default=None,
        description=(
            "Structured progress data. Typed for ``build_processing`` jobs;"
            " other kinds round-trip their fields via ``extra='allow'``."
        ),
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
