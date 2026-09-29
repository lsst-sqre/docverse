"""Readers for the fields GitHub webhook payloads share.

The webhook processors read the same few fields out of a delivery —
chiefly the ``repository`` block's owner, name, and numeric id — and
each used to carry its own copy of that parsing, with drifting fallback
rules. The readers here are the one copy, so every event resolves a
given ``repository`` block to the same repository.

Parsing is defensive, as a webhook payload is untrusted JSON: a missing
or wrong-shape field reads as ``None`` rather than raising, and the
caller decides whether what is left is enough to act on. Nothing here
logs; the processors log with their own event context.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

__all__ = ["RepositoryCoordinates", "coerce_int", "repository_coordinates"]


@dataclass(frozen=True, slots=True)
class RepositoryCoordinates:
    """Where a webhook's ``repository`` block says the repository is.

    Each field is ``None`` when the payload gave nothing usable for it.
    The processors look projects and bindings up by :attr:`repo_id`
    when present, with ``(owner, name)`` as the fallback for rows whose
    numeric id is still unresolved.
    """

    owner: str | None
    """The owner's login: ``owner.login``, ``owner.name``, or the
    ``full_name`` owner segment."""

    name: str | None
    """The repository name: ``name``, or the ``full_name`` name
    segment."""

    repo_id: int | None
    """The stable numeric id, ``repository.id``."""


def repository_coordinates(repo: object) -> RepositoryCoordinates:
    """Read the owner, name, and id from a payload's ``repository`` block.

    Parameters
    ----------
    repo
        The payload's ``repository`` value. Usually a mapping, but any
        value is tolerated: anything else reads as all-``None``
        coordinates.

    Returns
    -------
    RepositoryCoordinates
        The block's coordinates. The owner is the first non-empty
        string of ``owner.login`` and ``owner.name`` (the older
        payload shape), and the name is ``name`` when it is a
        non-empty string. Otherwise — a missing, empty-string, or
        non-string value — each falls back to its segment of
        ``full_name`` split at the first ``/``. The id is kept only
        when it is a non-bool ``int`` (see :func:`coerce_int`).
    """
    if not isinstance(repo, Mapping):
        return RepositoryCoordinates(owner=None, name=None, repo_id=None)
    fallback_owner, fallback_name = _full_name_segments(repo.get("full_name"))
    owner_block = repo.get("owner")
    if not isinstance(owner_block, Mapping):
        owner_block = {}
    owner = (
        _non_empty_str(owner_block.get("login"))
        or _non_empty_str(owner_block.get("name"))
        or fallback_owner
    )
    name = _non_empty_str(repo.get("name")) or fallback_name
    return RepositoryCoordinates(
        owner=owner, name=name, repo_id=coerce_int(repo.get("id"))
    )


def coerce_int(value: object) -> int | None:
    """Return ``value`` when it is a non-bool ``int``, else ``None``.

    GitHub sends numeric ids as JSON integers. Anything else — a
    string, a float, ``null`` — reads as no id rather than being
    converted. The bool exclusion matters because ``isinstance(True,
    int)`` is true in Python: a malformed ``"id": true`` would
    otherwise leak through as the id ``1``.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _full_name_segments(full_name: object) -> tuple[str | None, str | None]:
    """Split ``"owner/name"`` at its first ``/``, empty segments as None."""
    if not isinstance(full_name, str) or "/" not in full_name:
        return None, None
    owner, name = full_name.split("/", 1)
    return owner or None, name or None


def _non_empty_str(value: object) -> str | None:
    """Return ``value`` when it is a non-empty string, else ``None``."""
    return value if isinstance(value, str) and value else None
