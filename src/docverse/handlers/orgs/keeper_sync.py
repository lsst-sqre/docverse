"""LTD Keeper sync configuration endpoints within an organization."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from docverse.client.models import KeeperSyncConfig
from docverse.dependencies.auth import AuthenticatedUser, require_admin
from docverse.dependencies.context import RequestContext, context_dependency
from docverse.handlers.params import OrgSlugParam

router = APIRouter()


@router.get(
    "/orgs/{org}/keeper-sync",
    response_model=KeeperSyncConfig,
    summary="Get the organization's LTD Keeper sync configuration",
    name="get_org_keeper_sync_config",
)
async def get_org_keeper_sync_config(
    org_slug: OrgSlugParam,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> KeeperSyncConfig:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_config_service()
        return await service.get(org_slug=org_slug)


@router.put(
    "/orgs/{org}/keeper-sync",
    response_model=KeeperSyncConfig,
    summary="Replace the organization's LTD Keeper sync configuration",
    name="put_org_keeper_sync_config",
)
async def put_org_keeper_sync_config(
    org_slug: OrgSlugParam,
    data: KeeperSyncConfig,
    context: Annotated[RequestContext, Depends(context_dependency)],
    user: Annotated[AuthenticatedUser, Depends(require_admin)],  # noqa: ARG001
) -> KeeperSyncConfig:
    async with context.session.begin():
        service = context.factory.create_keeper_sync_config_service()
        result = await service.put(org_slug=org_slug, config=data)
        await context.session.commit()
    return result
