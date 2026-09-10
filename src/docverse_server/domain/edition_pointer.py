"""The edition pointer as the CDN edge stores it.

An edition's presence at the edge is exactly one key/value pair in the
Cloudflare Workers KV namespace: the key names the edition
(``{project_slug}/{edition_slug}``) and the value names the build whose
objects the Worker should serve for it. This module is the server-side
model of that pair — the shape a publisher writes, and the shape a
reconciler reads back to compare against the database.

Both halves live in the domain layer rather than in
`docverse_server.storage.editionpublisher` because the reconciliation
planner (`docverse_server.domain.edition_reconcile`) has to build the
same key the publisher writes in order to look a pointer up, and the
domain layer cannot import from storage.
"""

from __future__ import annotations

from dataclasses import dataclass

from .cache_profile import CacheProfile

__all__ = ["EditionPointer", "edition_pointer_key"]


def edition_pointer_key(project_slug: str, edition_slug: str) -> str:
    """Build the CDN key that identifies one edition's pointer.

    The single source of truth for the key format, shared by every
    operation that touches a pointer — publish, unpublish, and the
    reconciler's read-back. The Cloudflare Worker resolver derives the
    same key from the request hostname and path prefix
    (``cloudflare-worker/src/resolver.ts``), so a divergence here would
    not fail loudly: it would write pointers nothing ever reads.

    Parameters
    ----------
    project_slug
        Slug of the project the edition belongs to.
    edition_slug
        Slug of the edition.

    Returns
    -------
    str
        The KV key, ``{project_slug}/{edition_slug}``.
    """
    return f"{project_slug}/{edition_slug}"


@dataclass(frozen=True, slots=True)
class EditionPointer:
    """What the edge currently serves for one edition.

    A decoded KV value. Frozen because it is a read of somebody else's
    state: the only way to change what the edge serves is to publish
    again, never to mutate the record of what was read.
    """

    build_public_id: str
    """Base32 public id of the build the edge serves for this edition.

    Compared against the edition's current build to detect a pointer
    that was never moved. Stored in the KV value under ``build_id``.
    """

    r2_prefix: str
    """Object-store key prefix the Worker serves objects from.

    Compared against ``builds.storage_prefix``. A pointer can carry the
    right build id and the wrong prefix — a content-hash migration
    re-homed the objects — and the edge would keep serving the old
    location, so the prefix is checked independently of the build id.
    """

    cache_profile: CacheProfile | None
    """Edge cache profile recorded with the pointer, if any.

    ``None`` for a pointer written before the field existed, or one
    carrying a profile name this server does not know. The Worker falls
    back to the short profile in that case, so an unknown profile is a
    degraded pointer rather than a broken one.
    """
