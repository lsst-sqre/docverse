"""The promote-only policy for automatically re-derived edition kinds.

Two code paths re-derive an edition's :class:`~docverse.models.EditionKind`
long after the row was created: keeper-sync's per-sync kind refresh and
the native build-upload tracking path. Both face the same question —
*"the rule chain now says this ref is a release; may I rewrite the row?"*
— and both must answer it identically, so the decision lives here rather
than in either service.
"""

from __future__ import annotations

from docverse.models import EditionKind

__all__ = ["KIND_PROMOTIONS", "is_kind_promotion"]


KIND_PROMOTIONS: frozenset[tuple[EditionKind, EditionKind]] = frozenset(
    {(EditionKind.draft, EditionKind.release)}
)
"""``(current kind, derived kind)`` pairs an automated refresh may write.

Promote-only by construction (PRD #498): editions created before the
built-in version rules landed are all ``draft``, so promoting them to
``release`` is the whole healing story. Encoding the policy as an
allow-list of transitions — rather than "derived != current" — is what
makes every other case safe without a special case: demotions never
appear here, ``main`` / ``major`` / ``minor`` / ``alternate`` are never a
source kind, and a ``release`` an operator set by hand through the
editions PATCH API is never a source kind either, so manual decisions
survive every subsequent refresh.
"""


def is_kind_promotion(current: EditionKind, derived: EditionKind) -> bool:
    """Report whether an automated refresh may write *derived*.

    Parameters
    ----------
    current
        The kind currently stored on the edition row.
    derived
        The kind the rule chain (or LTD tracking mode) just derived.

    Returns
    -------
    bool
        `True` only for the transitions in :data:`KIND_PROMOTIONS`; every
        other pair — including ``(kind, kind)`` — is a no-op.
    """
    return (current, derived) in KIND_PROMOTIONS
