"""GitHub-backed dashboard template storage."""

from __future__ import annotations

from .binding_store import (
    DashboardGitHubTemplateBindingCreate,
    DashboardGitHubTemplateBindingStore,
)
from .source import GitHubTemplateSource
from .template_store import (
    DashboardGitHubTemplateStore,
    GitHubTemplateFileInput,
    GitHubTemplateKey,
    UpsertResult,
)

__all__ = [
    "DashboardGitHubTemplateBindingCreate",
    "DashboardGitHubTemplateBindingStore",
    "DashboardGitHubTemplateStore",
    "GitHubTemplateFileInput",
    "GitHubTemplateKey",
    "GitHubTemplateSource",
    "UpsertResult",
]
