"""Organization membership endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import OrgMembershipCreate, PrincipalType
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import ConflictError, NotFoundError

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
    "/orgs/{org_slug}/members",
    response_model=list[OrgMembership],
    summary="List organization members",
    name="get_members",
)
async def get_members(
    org_slug: str,
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
    "/orgs/{org_slug}/members",
    response_model=OrgMembership,
    status_code=status.HTTP_201_CREATED,
    summary="Add an organization member",
    name="post_member",
)
async def post_member(
    org_slug: str,
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
    return OrgMembership.from_domain(member, context.request, org_slug)


@router.get(
    "/orgs/{org_slug}/members/{member_id}",
    response_model=OrgMembership,
    summary="Get an organization member",
    name="get_member",
)
async def get_member(
    org_slug: str,
    member_id: str,
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


@router.delete(
    "/orgs/{org_slug}/members/{member_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove an organization member",
    name="delete_member",
)
async def delete_member(
    org_slug: str,  # noqa: ARG001
    member_id: str,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> None:
    principal_type, principal = _parse_member_id(member_id)
    async with context.session.begin():
        store = context.factory.create_membership_store()
        deleted = await store.delete(
            org_id=user.org.id,
            principal_type=principal_type,
            principal=principal,
        )
        if not deleted:
            msg = f"Member {member_id!r} not found"
            raise NotFoundError(msg)
        await context.session.commit()
