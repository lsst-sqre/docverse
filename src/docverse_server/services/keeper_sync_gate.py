"""The eligibility gate every keeper-sync endpoint runs before anything else.

Five operator-facing keeper-sync entry points answer the same question
before they do any work: does this organization exist, is LTD Keeper sync
turned on for it, and — for the three that address one LTD project — is
that project inside the organization's sync scope? The answers are
identical everywhere, and so are the messages: ``docs/keeper-sync-scope.md``
quotes the 404 an operator gets once a project falls out of the sync
scope, which makes that wording a documented contract — and a contract is
worth exactly one copy. This module is it, so a grep for the message finds
the single line below that composes it.

The gate comes in three widths, so a caller asks for the checks it
actually needs and nothing more:

- :func:`require_org` — the lookup alone, for the run reads that address
  a run by id and care nothing about the scope or the ``enabled`` flag.
- :func:`require_sync_enabled` — lookup plus ``enabled``, for the callers
  that address the whole organization rather than one LTD slug.
- :func:`require_sync_eligible` — all three checks, for the per-project
  endpoints.

Only the *disabled* verdict varies between callers, which is why it is
the one thing :func:`require_sync_enabled` parameterises: reading a
project's sync status on an org with sync off is a missing resource
(404), while launching a backfill on one is a conflict with the org's
current state (409). Everything else — the lookup, the scope rule, the
messages — is fixed here.
"""

from __future__ import annotations

from safir.fastapi import ClientRequestError

from docverse.models import KeeperSyncConfig
from docverse_server.domain.organization import Organization
from docverse_server.exceptions import NotFoundError
from docverse_server.storage.organization_store import OrganizationStore

__all__ = [
    "require_org",
    "require_sync_eligible",
    "require_sync_enabled",
]


async def require_org(
    org_store: OrganizationStore, *, org_slug: str
) -> Organization:
    """Load an organization by slug, or raise a 404.

    Parameters
    ----------
    org_store
        Store the organization is read through.
    org_slug
        Slug of the organization to load.

    Returns
    -------
    Organization
        The organization row.

    Raises
    ------
    NotFoundError
        If no organization has this slug.
    """
    org = await org_store.get_by_slug(org_slug)
    if org is None:
        msg = f"Organization {org_slug!r} not found"
        raise NotFoundError(msg)
    return org


async def require_sync_enabled(
    org_store: OrganizationStore,
    *,
    org_slug: str,
    disabled_error: type[ClientRequestError] = NotFoundError,
) -> tuple[Organization, KeeperSyncConfig]:
    """Load an organization whose LTD Keeper sync is turned on.

    The two-clause gate, for the endpoints that address the whole
    organization rather than one LTD project: an empty or narrow scope
    is not their concern, only whether sync is configured at all.

    An organization that has never been configured for keeper-sync has
    no stored config at all, which counts as disabled — a missing config
    and ``enabled: false`` are the same answer to the caller.

    Parameters
    ----------
    org_store
        Store the organization is read through.
    org_slug
        Slug of the organization to load.
    disabled_error
        Exception raised when sync is off. Defaults to
        :class:`~docverse_server.exceptions.NotFoundError`; ``POST
        /orgs/{org}/keeper-sync/runs`` passes
        :class:`~docverse_server.exceptions.ConflictError` instead,
        because asking a sync-disabled org for a backfill conflicts with
        its state rather than addressing a resource that is not there.

    Returns
    -------
    tuple of (Organization, KeeperSyncConfig)
        The organization row and its keeper-sync config, which is
        guaranteed non-``None`` and enabled on return.

    Raises
    ------
    NotFoundError
        If no organization has this slug.
    ClientRequestError
        Of type ``disabled_error``, if sync is not enabled on it.
    """
    org = await require_org(org_store, org_slug=org_slug)
    config = org.keeper_sync_config
    if config is None or not config.enabled:
        msg = f"LTD Keeper sync is not enabled for organization {org_slug!r}"
        raise disabled_error(msg)
    return org, config


async def require_sync_eligible(
    org_store: OrganizationStore, *, org_slug: str, ltd_slug: str
) -> tuple[Organization, KeeperSyncConfig]:
    """Load an organization that syncs this LTD project.

    The full three-clause gate behind ``GET/POST
    /orgs/{org}/keeper-sync/projects/{ltd_slug}...``. All three failures
    are 404: from the caller's point of view the resource these
    endpoints address is "a sync-eligible project on this org", and it
    does not exist when the org is missing, when sync is off, or when
    the slug is outside the scope.

    Scope is resolved through
    :meth:`~docverse.models.KeeperSyncConfig.is_in_scope` rather than
    re-derived here, so these endpoints agree with the worker's run
    discovery and tier crons by construction — including on the rules an
    inline ``project_slugs`` check would miss, such as excludes winning
    over every include.

    Parameters
    ----------
    org_store
        Store the organization is read through.
    org_slug
        Slug of the organization to load.
    ltd_slug
        LTD project slug the caller is addressing.

    Returns
    -------
    tuple of (Organization, KeeperSyncConfig)
        The organization row and its keeper-sync config.

    Raises
    ------
    NotFoundError
        If the org does not exist, if LTD Keeper sync is not enabled on
        it, or if ``ltd_slug`` is not in its keeper-sync scope.
    """
    org, config = await require_sync_enabled(org_store, org_slug=org_slug)
    if not config.is_in_scope(ltd_slug):
        msg = (
            f"LTD slug {ltd_slug!r} is not in the keeper-sync scope"
            f" for organization {org_slug!r}"
        )
        raise NotFoundError(msg)
    return org, config
