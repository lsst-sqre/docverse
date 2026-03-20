"""Organization credential endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from docverse.client.models import OrganizationCredentialCreate
from docverse.client.models.infrastructure import CredentialProvider
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
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
            provider=CredentialProvider(data.credentials.provider),
            credentials=data.credentials.model_dump(exclude={"provider"}),
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
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> OrganizationCredentialResponse:
    async with context.session.begin():
        service = context.factory.create_credential_service()
        cred = await service.get_by_label(
            org_slug=org_slug, label=credential_label
        )
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
