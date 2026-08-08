"""Edge cache-profile derivation for published editions.

This module is the single Python-side home for the CDN cache policy:
the profile names written into the Cloudflare Workers KV edition
pointer, the ``Cache-Control`` header values the Worker emits for each
profile, and the pure function mapping an edition's kind onto a
profile.

The ``CACHE_CONTROL_*`` values mirror the constants of the same name in
``cloudflare-worker/src/resolver.ts``. The Worker — not the server — is
what actually emits these headers; they live here so logs, metrics, and
docs can name the policy without re-deriving it.
"""

from __future__ import annotations

from typing import Literal

from docverse.models import EditionKind

__all__ = [
    "CACHE_CONTROL_LONG",
    "CACHE_CONTROL_SHORT",
    "CACHE_PROFILE_LONG",
    "CACHE_PROFILE_SHORT",
    "CacheProfile",
    "compute_cache_profile",
]

CacheProfile = Literal["long", "short"]
"""Name of an edge cache profile.

Threaded from `compute_cache_profile` through the
`~docverse_server.storage.editionpublisher.EditionPublisher` protocol
into the Cloudflare Workers KV pointer payload. Typing it as a literal
keeps a misspelled profile from silently degrading to the Worker's
``short`` fallback.
"""

CACHE_PROFILE_LONG: CacheProfile = "long"
"""Profile for read-heavy editions whose content rarely changes."""

CACHE_PROFILE_SHORT: CacheProfile = "short"
"""Profile for frequently-rebuilt editions (the safe default)."""

CACHE_CONTROL_SHORT = "public, max-age=60"
"""``Cache-Control`` the Worker emits for the ``short`` profile."""

CACHE_CONTROL_LONG = "public, max-age=300, s-maxage=86400"
"""``Cache-Control`` the Worker emits for the ``long`` profile.

Five minutes in the browser, one day at the edge. Publishes of
long-profile editions trigger a hostname-wide Cloudflare purge so the
edge copy is replaced rather than waited out.
"""

_LONG_PROFILE_KINDS = frozenset(
    {
        EditionKind.main,
        EditionKind.release,
        EditionKind.major,
        EditionKind.minor,
    }
)
"""Edition kinds that cache long at the edge.

``main`` and ``release`` are read-heavy and stable. The ``major`` /
``minor`` rollups are aliases for a release line: they only change when
a release publish updates them, and that publish already purges the
hostname.
"""


def compute_cache_profile(edition_kind: EditionKind) -> CacheProfile:
    """Derive the edge cache profile for an edition kind.

    Parameters
    ----------
    edition_kind
        Kind of the edition being published.

    Returns
    -------
    CacheProfile
        ``"long"`` for ``main``, ``release``, ``major``, and ``minor``
        editions; ``"short"`` for ``draft`` and ``alternate`` editions,
        which are rebuilt often enough that maintainers should see
        changes without waiting on a purge.
    """
    if edition_kind in _LONG_PROFILE_KINDS:
        return CACHE_PROFILE_LONG
    return CACHE_PROFILE_SHORT
