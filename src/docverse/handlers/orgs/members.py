"""Organization membership endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import (
    OrgMembershipCreate,
    OrgMembershipUpdate,
    PrincipalType,
)
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import ConflictError, NotFoundError
from docverse.handlers.params import MemberIdParam, OrgSlugParam
from docverse.handlers.responses import error_responses
from docverse.metrics import (
    MembershipChangeAction,
    MembershipChangedEvent,
    MetricsOrgRole,
    MetricsPrincipalType,
)

from .models import OrgMembership

router = APIRouter()


def _parse_member_id(member_id: str) -> tuple[PrincipalType, str]:
    """Parse a member ID string into (principal_type, principal).

    Format: ``{principal_type}:{principal}``
    """
    if ":" not in member_id:
        msg = f"Invalid member ID format: {member_id!r}"
        raise NotFoundError(msg)
    type_str, principal = member_id.split(":", 1)
    try:
        principal_type = PrincipalType(type_str)
    except ValueError as exc:
        msg = f"Invalid principal type in member ID: {type_str!r}"
        raise NotFoundError(msg) from exc
    return principal_type, principal


@router.get(
    "/orgs/{org}/members",
    response_model=list[OrgMembership],
    summary="List organization members",
    name="get_members",
)
async def get_members(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> list[OrgMembership]:
    async with context.session.begin():
        store = context.factory.create_membership_store()
        members = await store.list_by_org(user.org.id)
    return [
        OrgMembership.from_domain(m, context.request, org_slug)
        for m in members
    ]


@router.post(
    "/orgs/{org}/members",
    response_model=OrgMembership,
    status_code=status.HTTP_201_CREATED,
    summary="Add an organization member",
    name="post_member",
    responses=error_responses(status.HTTP_409_CONFLICT),
)
async def post_member(
    org_slug: OrgSlugParam,
    data: OrgMembershipCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> OrgMembership:
    async with context.session.begin():
        store = context.factory.create_membership_store()
        existing = await store.get_by_principal(
            org_id=user.org.id,
            principal_type=data.principal_type,
            principal=data.principal,
        )
        if existing is not None:
            msg = (
                f"Membership for {data.principal_type.value}:"
                f"{data.principal} already exists"
            )
            raise ConflictError(msg)
        member = await store.create(org_id=user.org.id, data=data)
        await context.session.commit()
    # Publish after the commit so the event reflects durably persisted
    # state. Production runs raise_on_error=False, so a metrics-backend
    # outage cannot fail this request (no defensive try/except). Mapped
    # from the API enums at the emission site (SQR-112 D4); membership is
    # org-scoped, so project is None.
    await context.events.membership_changed.publish(
        MembershipChangedEvent(
            organization=org_slug,
            project=None,
            action=MembershipChangeAction.add,
            role=MetricsOrgRole.from_api(member.role),
            principal_type=MetricsPrincipalType.from_api(
                member.principal_type
            ),
            principal=member.principal,
        )
    )
    response_model = OrgMembership.from_domain(
        member, context.request, org_slug
    )
    context.response.headers["Location"] = response_model.self_url
    return response_model


@router.get(
    "/orgs/{org}/members/{member}",
    response_model=OrgMembership,
    summary="Get an organization member",
    name="get_member",
)
async def get_member(
    org_slug: OrgSlugParam,
    member_id: MemberIdParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> OrgMembership:
    principal_type, principal = _parse_member_id(member_id)
    async with context.session.begin():
        store = context.factory.create_membership_store()
        member = await store.get_by_principal(
            org_id=user.org.id,
            principal_type=principal_type,
            principal=principal,
        )
        if member is None:
            msg = f"Member {member_id!r} not found"
            raise NotFoundError(msg)
    return OrgMembership.from_domain(member, context.request, org_slug)


@router.patch(
    "/orgs/{org}/members/{member}",
    response_model=OrgMembership,
    summary="Update an organization member",
    name="patch_member",
)
async def patch_member(
    org_slug: OrgSlugParam,
    member_id: MemberIdParam,
    data: OrgMembershipUpdate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> OrgMembership:
    """Update a member's role.

    Only ``role`` is mutable; ``principal`` and ``principal_type`` are
    immutable and rejected by the request model's ``extra="forbid"``.
    Follows the merge-patch house style: unset fields leave the member
    unchanged.
    """
    principal_type, principal = _parse_member_id(member_id)
    updates = data.model_dump(exclude_unset=True)
    previous_role = None
    async with context.session.begin():
        store = context.factory.create_membership_store()
        # Read the current membership first so an in-place role change can
        # carry the prior role on the remove event, and so not-found is
        # handled uniformly for both the update and empty-patch paths.
        current = await store.get_by_principal(
            org_id=user.org.id,
            principal_type=principal_type,
            principal=principal,
        )
        if current is None:
            msg = f"Member {member_id!r} not found"
            raise NotFoundError(msg)
        if "role" in updates:
            previous_role = current.role
            member = await store.update_role(
                org_id=user.org.id,
                principal_type=principal_type,
                principal=principal,
                role=updates["role"],
            )
        else:
            # Empty merge patch: return the current membership unchanged.
            member = current
        if member is None:
            msg = f"Member {member_id!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
    # An in-place role change is modelled as a remove of the old role plus
    # an add of the new one (MembershipChangeAction carries no update verb).
    # Publish after the commit (best-effort; raise_on_error=False) and only
    # when the role actually changed, so a no-op patch stays silent.
    if previous_role is not None and previous_role != member.role:
        metrics_principal_type = MetricsPrincipalType.from_api(
            member.principal_type
        )
        await context.events.membership_changed.publish(
            MembershipChangedEvent(
                organization=org_slug,
                project=None,
                action=MembershipChangeAction.remove,
                role=MetricsOrgRole.from_api(previous_role),
                principal_type=metrics_principal_type,
                principal=member.principal,
            )
        )
        await context.events.membership_changed.publish(
            MembershipChangedEvent(
                organization=org_slug,
                project=None,
                action=MembershipChangeAction.add,
                role=MetricsOrgRole.from_api(member.role),
                principal_type=metrics_principal_type,
                principal=member.principal,
            )
        )
    return OrgMembership.from_domain(member, context.request, org_slug)


@router.delete(
    "/orgs/{org}/members/{member}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove an organization member",
    name="delete_member",
)
async def delete_member(
    org_slug: OrgSlugParam,
    member_id: MemberIdParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> None:
    principal_type, principal = _parse_member_id(member_id)
    async with context.session.begin():
        store = context.factory.create_membership_store()
        # Capture the membership before deleting it so the removal event
        # can carry the gone principal's role and identity.
        member = await store.get_by_principal(
            org_id=user.org.id,
            principal_type=principal_type,
            principal=principal,
        )
        if member is None:
            msg = f"Member {member_id!r} not found"
            raise NotFoundError(msg)
        await store.delete(
            org_id=user.org.id,
            principal_type=principal_type,
            principal=principal,
        )
        await context.session.commit()
    # Publish after the commit (best-effort; raise_on_error=False). Mapped
    # from the API enums at the emission site; membership is org-scoped,
    # so project is None.
    await context.events.membership_changed.publish(
        MembershipChangedEvent(
            organization=org_slug,
            project=None,
            action=MembershipChangeAction.remove,
            role=MetricsOrgRole.from_api(member.role),
            principal_type=MetricsPrincipalType.from_api(
                member.principal_type
            ),
            principal=member.principal,
        )
    )
