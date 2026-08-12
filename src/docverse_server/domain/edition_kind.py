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
appear here, and ``main`` / ``major`` / ``minor`` / ``alternate`` are
never a source kind.

The allow-list alone cannot protect an operator's *decision*, though,
only the kinds it happens to exclude. An edition PATCHed from
``release`` back to ``draft`` lands on ``(draft, release)`` — precisely
the pair listed here — so the next sync or upload would re-derive
``release`` and silently undo the demotion. Hand-set kinds are therefore
pinned out of band, by the edition's ``kind_manually_set`` flag rather
than by this table; see :func:`is_kind_promotion`.
"""


def is_kind_promotion(
    current: EditionKind,
    derived: EditionKind,
    *,
    kind_manually_set: bool,
) -> bool:
    """Report whether an automated refresh may write *derived*.

    Parameters
    ----------
    current
        The kind currently stored on the edition row.
    derived
        The kind the rule chain (or LTD tracking mode) just derived.
    kind_manually_set
        Whether an operator set this edition's kind by hand through the
        editions PATCH API. A hand-set kind is never rewritten, whatever
        the transition — passing it explicitly (keyword-only, no
        default) keeps a caller from silently reverting an operator.

    Returns
    -------
    bool
        `True` only for an edition with no manual override whose
        transition is in :data:`KIND_PROMOTIONS`; every other pair —
        including ``(kind, kind)`` — is a no-op.
    """
    if kind_manually_set:
        return False
    return (current, derived) in KIND_PROMOTIONS
