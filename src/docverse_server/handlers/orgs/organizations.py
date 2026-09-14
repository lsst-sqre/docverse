"""Organization endpoint."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status

from docverse.models import OrganizationSummary, OrganizationUpdate, OrgRole
from docverse_server.dependencies.auth import (
    AuthenticatedUser,
    require_admin,
    require_reader,
)
from docverse_server.dependencies.context import (
    RequestContext,
    context_dependency,
)
from docverse_server.domain.base32id import serialize_base32_id
from docverse_server.domain.conditional_get import (
    datetime_to_microseconds,
    make_weak_etag,
)
from docverse_server.domain.organization import (
    Organization as OrganizationDomain,
)
from docverse_server.exceptions import NotFoundError, PermissionDeniedError
from docverse_server.handlers.conditional import evaluate_conditional_get
from docverse_server.handlers.params import OrgSlugParam
from docverse_server.metrics import ConditionalGetEndpoint

from .models import Organization

router = APIRouter()


def _organization_summary(
    org: OrganizationDomain, request: Request, *, role: OrgRole
) -> OrganizationSummary:
    """Build an ``OrganizationSummary`` for a listing entry."""
    return OrganizationSummary(
        self_url=str(request.url_for("get_organization", org=org.slug)),
        id=serialize_base32_id(org.public_id),
        slug=org.slug,
        title=org.title,
        role=role,
    )


@router.get(
    "/orgs",
    response_model=list[OrganizationSummary],
    summary="List organizations the caller can access",
    name="get_organizations",
    # 403 is already declared at the orgs router level (see main.py), so no
    # route-level responses= is needed here — matching the sibling routes.
)
async def get_organizations(
    *,
    request: Request,
    context: Annotated[RequestContext, Depends(context_dependency)],
) -> list[OrganizationSummary]:
    """List organizations in which the caller holds an effective role.

    Any authenticated user may call this. A caller sees each organization
    where they have a direct or group membership, along with their
    effective role; a superadmin sees every organization (role reported as
    ``admin``). An empty list is a valid response.
    """
    username = request.headers.get("X-Auth-Request-User")
    if not username:
        msg = "Authentication required"
        raise PermissionDeniedError(msg)
    context.rebind_logger(username=username)

    async with context.session.begin():
        org_service = context.factory.create_organization_service()
        auth_service = context.factory.create_authorization_service()
        if auth_service.is_superadmin(username):
            orgs = await org_service.list_all()
            summaries = [
                _organization_summary(org, request, role=OrgRole.admin)
                for org in orgs
            ]
        else:
            token = request.headers.get("X-Auth-Request-Token", "")
            user_info_store = context.factory.get_user_info_store()
            groups = await user_info_store.get_groups(token)
            membership_store = context.factory.create_membership_store()
            role_map = await membership_store.list_effective_roles(
                username=username, groups=groups
            )
            org_store = context.factory.create_org_store()
            summaries = []
            for org_id, role in role_map.items():
                org = await org_store.get_by_id(org_id)
                if org is None:
                    continue
                summaries.append(
                    _organization_summary(org, request, role=role)
                )
    summaries.sort(key=lambda summary: summary.slug)
    return summaries


def _organization_etag(org: OrganizationDomain) -> str:
    """Build the entity-tag for one organization's representation.

    The material is the endpoint's identity, the org's public id, and
    its ``date_updated``. The endpoint takes no query parameters, so
    unlike the project listing there is no request state to fold in:
    one organization has exactly one representation per clock tick.
    """
    return make_weak_etag(
        (
            ConditionalGetEndpoint.organization.value,
            org.public_id,
            datetime_to_microseconds(org.date_updated),
        )
    )


@router.get(
    "/orgs/{org}",
    response_model=Organization,
    summary="Get an organization",
    name="get_organization",
    responses={
        status.HTTP_304_NOT_MODIFIED: {
            "description": (
                "The caller's ``If-None-Match`` or ``If-Modified-Since``"
                " already matched this organization, so no body is sent."
                " The ``ETag`` and ``Last-Modified`` validators are"
                " repeated so a poller can carry them into its next"
                " request."
            )
        }
    },
)
async def get_organization(
    *,
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_reader)],
) -> Organization | Response:
    # The authorization dependency has already loaded the org row, so
    # the watermark is in hand before this body runs and the validators
    # cost nothing: a caller that is up to date is answered without the
    # services query below ever being issued.
    #
    # The watermark is the org's own clock. The embedded service
    # summaries are the one part of this body it does not cover — an
    # edit to a service row that is merely *assigned* to a slot changes
    # a summary without touching the org. Slot assignments themselves
    # are org columns, so a re-slot does move it (PRD #634 §5).
    async with context.session.begin():
        not_modified = await evaluate_conditional_get(
            context,
            endpoint=ConditionalGetEndpoint.organization,
            organization=org_slug,
            etag=_organization_etag(user.org),
            last_modified=user.org.date_updated,
        )
        if not_modified is not None:
            return not_modified
        # Load services to build embedded summaries for slot assignments
        infra_service = context.factory.create_infrastructure_service()
        services = await infra_service.list_by_org_id(org_id=user.org.id)
    return Organization.from_domain(
        user.org, context.request, services=services
    )


@router.patch(
    "/orgs/{org}",
    response_model=Organization,
    summary="Update an organization",
    name="patch_organization",
)
async def patch_organization(
    *,
    org_slug: OrgSlugParam,
    data: OrganizationUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> Organization:
    async with context.session.begin():
        service = context.factory.create_organization_service()
        org = await service.update(user.org.slug, data)
        if org is None:
            msg = f"Organization {user.org.slug!r} not found"
            raise NotFoundError(msg)
        infra_service = context.factory.create_infrastructure_service()
        services = await infra_service.list_by_org_id(org_id=org.id)
        await context.session.commit()
    return Organization.from_domain(org, context.request, services=services)
