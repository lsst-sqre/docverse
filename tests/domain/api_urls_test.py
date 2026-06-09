"""Tests for the Docverse API URL builders.

The drift-guard tests pin each builder's path template to the live
FastAPI route of the same resource (``get_edition`` / ``get_build`` /
``get_queue_job``). Each builder composes a resource URL as
``base + <route path minus the application path prefix>``; the base that
Repertoire hands back already carries that prefix. A route rename — of
either a path segment or the route's name — therefore fails these tests.
"""

from __future__ import annotations

from docverse.config import config
from docverse.domain.api_urls import build_url, edition_url, queue_job_url
from docverse.main import app

_BASE = "https://docverse.example/api"


def _route_suffix(name: str, **params: str) -> str:
    """Path of the named route relative to the application path prefix."""
    full = str(app.url_path_for(name, **params))
    return full.removeprefix(config.path_prefix)


def test_edition_url_pins_to_get_edition_route() -> None:
    url = edition_url(
        _BASE, org="org-slug", project="proj-slug", edition="ed-slug"
    )
    suffix = _route_suffix(
        "get_edition",
        org="org-slug",
        project="proj-slug",
        edition="ed-slug",
    )
    assert url == f"{_BASE}{suffix}"


def test_build_url_pins_to_get_build_route() -> None:
    url = build_url(_BASE, org="org-slug", project="proj-slug", build="BLDID")
    suffix = _route_suffix(
        "get_build", org="org-slug", project="proj-slug", build="BLDID"
    )
    assert url == f"{_BASE}{suffix}"


def test_queue_job_url_pins_to_get_queue_job_route() -> None:
    url = queue_job_url(_BASE, job="JOBID")
    suffix = _route_suffix("get_queue_job", job="JOBID")
    assert url == f"{_BASE}{suffix}"


def test_builders_strip_trailing_slash_from_base() -> None:
    """A trailing slash on the base does not double up in the path."""
    assert edition_url(
        _BASE + "/", org="o", project="p", edition="e"
    ) == edition_url(_BASE, org="o", project="p", edition="e")
