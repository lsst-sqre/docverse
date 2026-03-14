"""Handler-level response models for admin endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import Organization as _OrganizationBase
from docverse.domain.organization import Organization as OrganizationDomain


class Organization(_OrganizationBase):
    """Organization response model with HATEOAS self_url."""

    @classmethod
    def from_domain(cls, domain: OrganizationDomain, request: Request) -> Self:
        """Create from a domain object, adding the self_url."""
        return cls(
            self_url=str(
                request.url_for("admin_get_organization", org_slug=domain.slug)
            ),
            slug=domain.slug,
            title=domain.title,
            base_domain=domain.base_domain,
            url_scheme=domain.url_scheme,
            root_path_prefix=domain.root_path_prefix,
            slug_rewrite_rules=domain.slug_rewrite_rules,
            lifecycle_rules=domain.lifecycle_rules,
            purgatory_retention=int(
                domain.purgatory_retention.total_seconds()
            ),
            date_created=domain.date_created,
            date_updated=domain.date_updated,
        )
