"""Organization-scoped HTTP handlers for Docverse."""

from fastapi import APIRouter

from .builds import router as builds_router
from .credentials import router as credentials_router
from .dashboard import org_router as dashboard_org_router
from .dashboard import project_router as dashboard_project_router
from .dashboard_template import (
    org_default_router as dashboard_template_org_default_router,
)
from .dashboard_template import (
    project_override_router as dashboard_template_project_override_router,
)
from .editions import router as editions_router
from .jobs import router as jobs_router
from .keeper_sync import router as keeper_sync_router
from .members import router as members_router
from .organizations import router as organizations_router
from .projects import router as projects_router
from .services import router as services_router

orgs_router = APIRouter()
orgs_router.include_router(organizations_router, tags=["orgs"])
orgs_router.include_router(projects_router, tags=["projects"])
orgs_router.include_router(builds_router, tags=["projects"])
orgs_router.include_router(editions_router, tags=["projects"])
orgs_router.include_router(members_router, tags=["orgs"])
orgs_router.include_router(credentials_router, tags=["orgs"])
orgs_router.include_router(services_router, tags=["orgs"])
orgs_router.include_router(dashboard_project_router, tags=["projects"])
orgs_router.include_router(dashboard_org_router, tags=["orgs"])
orgs_router.include_router(
    dashboard_template_org_default_router, tags=["orgs"]
)
orgs_router.include_router(
    dashboard_template_project_override_router, tags=["projects"]
)
orgs_router.include_router(keeper_sync_router, tags=["orgs"])
orgs_router.include_router(jobs_router, tags=["jobs"])

__all__ = ["orgs_router"]
