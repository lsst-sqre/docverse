"""Organization credential endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import OrganizationCredentialCreate
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.exceptions import NotFoundError
from docverse.handlers.params import CredentialLabelParam, OrgSlugParam

from .models import OrganizationCredentialResponse

router = APIRouter()


@router.get(
    "/orgs/{org}/credentials",
    response_model=list[OrganizationCredentialResponse],
    summary="List organization credentials",
    name="get_credentials",
)
async def get_credentials(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> list[OrganizationCredentialResponse]:
    async with context.session.begin():
        service = context.factory.create_credential_service()
        credentials = await service.list_by_org(org_slug=org_slug)
    return [
        OrganizationCredentialResponse.from_domain(
            c, context.request, org_slug
        )
        for c in credentials
    ]


@router.post(
    "/orgs/{org}/credentials",
    response_model=OrganizationCredentialResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an organization credential",
    name="post_credential",
)
async def post_credential(
    org_slug: OrgSlugParam,
    data: OrganizationCredentialCreate,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> OrganizationCredentialResponse:
    async with context.session.begin():
        service = context.factory.create_credential_service()
        credential = await service.create(
            org_slug=org_slug,
            label=data.label,
            service_type=data.service_type,
            credential=data.credential,
        )
        await context.session.commit()
    return OrganizationCredentialResponse.from_domain(
        credential, context.request, org_slug
    )


@router.get(
    "/orgs/{org}/credentials/{credential}",
    response_model=OrganizationCredentialResponse,
    summary="Get an organization credential",
    name="get_credential",
)
async def get_credential(
    org_slug: OrgSlugParam,
    credential_label: CredentialLabelParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],
) -> OrganizationCredentialResponse:
    async with context.session.begin():
        store = context.factory.create_credential_store()
        result = await store.get_by_label(
            organization_id=user.org.id, label=credential_label
        )
    if result is None:
        msg = f"Credential {credential_label!r} not found"
        raise NotFoundError(msg)
    cred, _encrypted = result
    return OrganizationCredentialResponse.from_domain(
        cred, context.request, org_slug
    )


@router.delete(
    "/orgs/{org}/credentials/{credential}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an organization credential",
    name="delete_credential",
)
async def delete_credential(
    org_slug: OrgSlugParam,
    credential_label: CredentialLabelParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> None:
    async with context.session.begin():
        service = context.factory.create_credential_service()
        await service.delete(org_slug=org_slug, label=credential_label)
        await context.session.commit()
