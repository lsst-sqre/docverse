"""Handler-level response models for org-scoped endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import Build as _BuildBase
from docverse.client.models import (
    DefaultEditionConfig,
    OrganizationServiceSummary,
)
from docverse.client.models import Edition as _EditionBase
from docverse.client.models import (
    EditionBuildHistoryEntry as _EditionBuildHistoryEntryBase,
)
from docverse.client.models import Organization as _OrganizationBase
from docverse.client.models import (
    OrganizationCredential as _OrganizationCredentialBase,
)
from docverse.client.models import (
    OrganizationService as _OrganizationServiceBase,
)
from docverse.client.models import OrgMembership as _OrgMembershipBase
from docverse.client.models import Project as _ProjectBase
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build as BuildDomain
from docverse.domain.edition import Edition as EditionDomain
from docverse.domain.edition_build_history import (
    EditionBuildHistoryWithBuild as EditionBuildHistoryWithBuildDomain,
)
from docverse.domain.membership import OrgMembership as OrgMembershipDomain
from docverse.domain.organization import Organization as OrganizationDomain
from docverse.domain.organization_credential import (
    OrganizationCredential as OrganizationCredentialDomain,
)
from docverse.domain.organization_service import (
    OrganizationService as OrganizationServiceDomain,
)
from docverse.domain.project import Project as ProjectDomain


class Organization(_OrganizationBase):
    """Organization response model with HATEOAS URLs."""

    services_url: str
    credentials_url: str
    projects_url: str
    members_url: str

    @classmethod
    def from_domain(
        cls,
        domain: OrganizationDomain,
        request: Request,
        *,
        services: list[OrganizationServiceDomain] | None = None,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs.

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
                    request.url_for(
                        "get_service", org=domain.slug, service=svc.label
                    )
                ),
                label=svc.label,
                category=svc.category,
                provider=svc.provider,
            )

        return cls(
            self_url=str(request.url_for("get_organization", org=domain.slug)),
            services_url=str(request.url_for("get_services", org=domain.slug)),
            credentials_url=str(
                request.url_for("get_credentials", org=domain.slug)
            ),
            projects_url=str(request.url_for("get_projects", org=domain.slug)),
            members_url=str(request.url_for("get_members", org=domain.slug)),
            slug=domain.slug,
            title=domain.title,
            base_domain=domain.base_domain,
            url_scheme=domain.url_scheme,
            root_path_prefix=domain.root_path_prefix,
            slug_rewrite_rules=domain.slug_rewrite_rules,
            lifecycle_rules=domain.lifecycle_rules,
            default_edition_config=(
                DefaultEditionConfig.model_validate(
                    domain.default_edition_config
                )
                if domain.default_edition_config is not None
                else None
            ),
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


class Project(_ProjectBase):
    """Project response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: ProjectDomain,
        request: Request,
        org_slug: str,
        *,
        default_edition: EditionDomain | None = None,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        edition_response = None
        if default_edition is not None:
            edition_response = Edition.from_domain(
                default_edition, request, org_slug, domain.slug
            )
        return cls(
            self_url=str(
                request.url_for(
                    "get_project",
                    org=org_slug,
                    project=domain.slug,
                )
            ),
            org_url=str(request.url_for("get_organization", org=org_slug)),
            editions_url=str(
                request.url_for(
                    "get_editions",
                    org=org_slug,
                    project=domain.slug,
                )
            ),
            builds_url=str(
                request.url_for(
                    "get_builds",
                    org=org_slug,
                    project=domain.slug,
                )
            ),
            slug=domain.slug,
            title=domain.title,
            doc_repo=domain.doc_repo,
            slug_rewrite_rules=domain.slug_rewrite_rules,
            lifecycle_rules=domain.lifecycle_rules,
            default_edition=edition_response,
            date_created=domain.date_created,
            date_updated=domain.date_updated,
        )


class Build(_BuildBase):
    """Build response model with HATEOAS URLs."""

    @classmethod
    def from_domain(  # noqa: PLR0913
        cls,
        domain: BuildDomain,
        request: Request,
        org_slug: str,
        project_slug: str,
        *,
        upload_url: str | None = None,
        queue_url: str | None = None,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        build_id_str = serialize_base32_id(domain.public_id)
        return cls(
            self_url=str(
                request.url_for(
                    "get_build",
                    org=org_slug,
                    project=project_slug,
                    build=build_id_str,
                )
            ),
            project_url=str(
                request.url_for(
                    "get_project",
                    org=org_slug,
                    project=project_slug,
                )
            ),
            id=build_id_str,
            git_ref=domain.git_ref,
            alternate_name=domain.alternate_name,
            content_hash=domain.content_hash,
            status=domain.status,
            upload_url=upload_url,
            queue_url=queue_url,
            object_count=domain.object_count,
            total_size_bytes=domain.total_size_bytes,
            uploader=domain.uploader,
            annotations=domain.annotations,
            date_created=domain.date_created,
            date_uploaded=domain.date_uploaded,
            date_completed=domain.date_completed,
        )


class Edition(_EditionBase):
    """Edition response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: EditionDomain,
        request: Request,
        org_slug: str,
        project_slug: str,
        *,
        published_url: str | None = None,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        build_url: str | None = None
        if domain.current_build_public_id is not None:
            build_id_str = serialize_base32_id(domain.current_build_public_id)
            build_url = str(
                request.url_for(
                    "get_build",
                    org=org_slug,
                    project=project_slug,
                    build=build_id_str,
                )
            )
        return cls(
            self_url=str(
                request.url_for(
                    "get_edition",
                    org=org_slug,
                    project=project_slug,
                    edition=domain.slug,
                )
            ),
            project_url=str(
                request.url_for(
                    "get_project",
                    org=org_slug,
                    project=project_slug,
                )
            ),
            build_url=build_url,
            published_url=published_url,
            slug=domain.slug,
            title=domain.title,
            kind=domain.kind,
            tracking_mode=domain.tracking_mode,
            tracking_params=domain.tracking_params,
            lifecycle_exempt=domain.lifecycle_exempt,
            date_created=domain.date_created,
            date_updated=domain.date_updated,
        )


class EditionBuildHistoryResponse(_EditionBuildHistoryEntryBase):
    """Edition build history response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: EditionBuildHistoryWithBuildDomain,
        request: Request,
        org_slug: str,
        project_slug: str,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        build_id_str = serialize_base32_id(domain.build_public_id)
        return cls(
            build_id=build_id_str,
            build_url=str(
                request.url_for(
                    "get_build",
                    org=org_slug,
                    project=project_slug,
                    build=build_id_str,
                )
            ),
            git_ref=domain.build_git_ref,
            build_status=domain.build_status,
            build_deleted=domain.build_date_deleted is not None,
            position=domain.position,
            date_created=domain.date_created,
        )


class OrgMembership(_OrgMembershipBase):
    """Membership response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: OrgMembershipDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        member_id = f"{domain.principal_type.value}:{domain.principal}"
        return cls(
            self_url=str(
                request.url_for(
                    "get_member",
                    org=org_slug,
                    member=member_id,
                )
            ),
            org_url=str(request.url_for("get_organization", org=org_slug)),
            id=member_id,
            principal=domain.principal,
            principal_type=domain.principal_type,
            role=domain.role,
        )


class OrganizationCredentialResponse(_OrganizationCredentialBase):
    """Organization credential response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: OrganizationCredentialDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        return cls(
            self_url=str(
                request.url_for(
                    "get_credential",
                    org=org_slug,
                    credential=domain.label,
                )
            ),
            org_url=str(request.url_for("get_organization", org=org_slug)),
            label=domain.label,
            provider=domain.provider,
            date_created=domain.date_created,
            date_updated=domain.date_updated,
        )


class OrganizationServiceResponse(_OrganizationServiceBase):
    """Organization service response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: OrganizationServiceDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        return cls(
            self_url=str(
                request.url_for(
                    "get_service",
                    org=org_slug,
                    service=domain.label,
                )
            ),
            org_url=str(request.url_for("get_organization", org=org_slug)),
            credential_url=str(
                request.url_for(
                    "get_credential",
                    org=org_slug,
                    credential=domain.credential_label,
                )
            ),
            label=domain.label,
            category=domain.category,
            provider=domain.provider,
            config=domain.config,
            credential_label=domain.credential_label,
            date_created=domain.date_created,
            date_updated=domain.date_updated,
        )
