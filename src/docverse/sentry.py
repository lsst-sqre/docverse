"""Docverse-specific Sentry initialization."""

from __future__ import annotations

from importlib.metadata import version
from typing import Literal

import sentry_sdk
from safir.sentry import initialize_sentry as _safir_initialize_sentry
from safir.sentry import should_enable_sentry

__all__ = ["DocverseSentryComponent", "initialize_sentry"]


DocverseSentryComponent = Literal[
    "api", "worker", "worker-keeper-sync", "worker-lifecycle-eval", "cli"
]
"""Tag values for the ``component`` Sentry global tag.

One label per Docverse entry point so events from the FastAPI app, the two
arq worker pools, and ``docverse-admin`` can be filtered apart on Sentry.
"""


def initialize_sentry(component: DocverseSentryComponent) -> None:
    """Initialize Sentry for one Docverse process.

    A no-op when ``SENTRY_DSN`` is unset, so local development, CI, and
    ``nox -s test`` runs never report. When the env var is set, delegates
    to :func:`safir.sentry.initialize_sentry` with the
    ``setuptools_scm``-derived ``docverse`` package version as the Sentry
    ``release``, then attaches ``service`` and ``component`` global tags so
    every event from this process carries them.
    """
    if not should_enable_sentry():
        return
    _safir_initialize_sentry(release=version("docverse"))
    scope = sentry_sdk.get_global_scope()
    scope.set_tag("service", "docverse")
    scope.set_tag("component", component)
