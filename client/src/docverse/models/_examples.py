"""Shared example values for OpenAPI schema documentation.

These constants feed the ``examples`` argument of model fields so the
generated OpenAPI docs show realistic values instead of ``"string"``.
They share one consistent vocabulary — the ``lsst`` organization's
``pipelines`` project — so cross-references between examples line up
(e.g. a build's ``self_url`` uses the same id as the build ``id``
field). The URLs are anchored on ``https://example.org`` because the
schema is rendered statically; at runtime every ``*_url`` field carries
an absolute URL built from the incoming request.

The Base32 identifiers are genuine Crockford Base32 values minted with
:func:`docverse.domain.base32id.mint_resource_id_for_timestamp` (12
characters plus a 2-character checksum, hyphenated every 4), so they
round-trip through the real validators.
"""

from __future__ import annotations

__all__ = [
    "EXAMPLE_API_URL",
    "EXAMPLE_BUILD_ID",
    "EXAMPLE_BUILD_URL",
    "EXAMPLE_EDITION_URL",
    "EXAMPLE_JOB_ID",
    "EXAMPLE_JOB_URL",
    "EXAMPLE_ORG_URL",
    "EXAMPLE_PROJECT_URL",
    "EXAMPLE_PUBLISH_JOB_ID",
    "EXAMPLE_PUBLISH_JOB_URL",
    "EXAMPLE_RUN_ID",
    "EXAMPLE_TOMBSTONE_ID",
]

EXAMPLE_API_URL = "https://example.org/docverse/api"
"""Base URL of the example Docverse API deployment."""

EXAMPLE_ORG_URL = f"{EXAMPLE_API_URL}/orgs/lsst"
"""Example organization resource URL."""

EXAMPLE_PROJECT_URL = f"{EXAMPLE_ORG_URL}/projects/pipelines"
"""Example project resource URL."""

EXAMPLE_EDITION_URL = f"{EXAMPLE_PROJECT_URL}/editions/v1"
"""Example edition resource URL."""

EXAMPLE_BUILD_ID = "1txq-55pj-1x5m-16"
"""Example public Base32 identifier for a build."""

EXAMPLE_BUILD_URL = f"{EXAMPLE_PROJECT_URL}/builds/{EXAMPLE_BUILD_ID}"
"""Example build resource URL."""

EXAMPLE_JOB_ID = "1vg8-zbgy-3rhq-52"
"""Example public Base32 identifier for a queue job."""

EXAMPLE_JOB_URL = f"{EXAMPLE_ORG_URL}/jobs/{EXAMPLE_JOB_ID}"
"""Example queue-job resource URL."""

EXAMPLE_PUBLISH_JOB_ID = "1vp1-yvk8-39he-47"
"""Example public Base32 identifier for a child publish_edition job."""

EXAMPLE_PUBLISH_JOB_URL = f"{EXAMPLE_ORG_URL}/jobs/{EXAMPLE_PUBLISH_JOB_ID}"
"""Example child publish_edition queue-job resource URL."""

EXAMPLE_RUN_ID = "1vg8-z8f9-18w0-84"
"""Example public Base32 identifier for a keeper-sync run."""

EXAMPLE_TOMBSTONE_ID = "1v3e-2qhh-39ej-55"
"""Example public Base32 identifier for a keeper-sync tombstone."""
