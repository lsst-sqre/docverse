"""Organization-scoped HTTP handlers for Docverse."""

from fastapi import APIRouter

from .builds import router as builds_router
from .editions import router as editions_router
from .members import router as members_router
from .organizations import router as organizations_router
from .projects import router as projects_router

orgs_router = APIRouter(tags=["orgs"])
orgs_router.include_router(organizations_router)
orgs_router.include_router(projects_router)
orgs_router.include_router(builds_router)
orgs_router.include_router(editions_router)
orgs_router.include_router(members_router)

__all__ = ["orgs_router"]
