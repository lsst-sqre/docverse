"""Worker task functions for Docverse."""

from .build_processing import build_processing
from .build_processing_reaper import build_processing_reaper
from .dashboard_build import dashboard_build
from .dashboard_build_reaper import dashboard_build_reaper
from .dashboard_sync import dashboard_sync
from .dashboard_sync_reaper import dashboard_sync_reaper
from .git_ref_audit import git_ref_audit
from .git_ref_audit_discovery import git_ref_audit_discovery
from .keeper_sync import (
    keeper_sync_project,
    keeper_sync_reaper,
    keeper_sync_run_discovery,
    keeper_sync_tier_discovery,
    keeper_sync_tier_main,
    keeper_sync_tier_other,
)
from .lifecycle_eval import lifecycle_eval
from .lifecycle_eval_dispatcher import lifecycle_eval_dispatcher
from .lifecycle_reaper import lifecycle_reaper
from .ping import ping
from .project_github_resolve import project_github_resolve
from .publish_edition import publish_edition
from .publish_edition_reaper import publish_edition_reaper

__all__ = [
    "build_processing",
    "build_processing_reaper",
    "dashboard_build",
    "dashboard_build_reaper",
    "dashboard_sync",
    "dashboard_sync_reaper",
    "git_ref_audit",
    "git_ref_audit_discovery",
    "keeper_sync_project",
    "keeper_sync_reaper",
    "keeper_sync_run_discovery",
    "keeper_sync_tier_discovery",
    "keeper_sync_tier_main",
    "keeper_sync_tier_other",
    "lifecycle_eval",
    "lifecycle_eval_dispatcher",
    "lifecycle_reaper",
    "ping",
    "project_github_resolve",
    "publish_edition",
    "publish_edition_reaper",
]
