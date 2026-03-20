"""Handler-level response models for admin endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import Organization as _OrganizationBase
from docverse.client.models import OrganizationServiceSummary
from docverse.domain.organization import Organization as OrganizationDomain
from docverse.domain.organization_service import (
    OrganizationService as OrganizationServiceDomain,
)


class Organization(_OrganizationBase):
    """Organization response model with HATEOAS self_url."""

    @classmethod
    def from_domain(
        cls,
        domain: OrganizationDomain,
        request: Request,
        *,
        services: list[OrganizationServiceDomain] | None = None,
    ) -> Self:
        """Create from a domain object, adding the self_url.

        Parameters
        ----------
        domain
            The organization domain model.
        request
            The current request (for URL generation).
        services
            All services for this org, used to build embedded
            service summaries for the slot assignments.
        """
        # Build a lookup of services by label
        svc_by_label: dict[str, OrganizationServiceDomain] = {}
        if services:
            svc_by_label = {s.label: s for s in services}

        def _make_summary(
            label: str | None,
        ) -> OrganizationServiceSummary | None:
            if label is None or label not in svc_by_label:
                return None
            svc = svc_by_label[label]
            return OrganizationServiceSummary(
                self_url=str(
                    request.url_for("admin_get_organization", org=domain.slug)
                ),
                label=svc.label,
                category=svc.category,
                provider=svc.provider,
            )

        return cls(
            self_url=str(
                request.url_for("admin_get_organization", org=domain.slug)
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
            publishing_store=_make_summary(domain.publishing_store_label),
            staging_store=_make_summary(domain.staging_store_label),
            cdn_service=_make_summary(domain.cdn_service_label),
            dns_service=_make_summary(domain.dns_service_label),
            date_created=domain.date_created,
            date_updated=domain.date_updated,
        )
