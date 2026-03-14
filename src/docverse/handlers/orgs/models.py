"""Handler-level response models for org-scoped endpoints."""

from __future__ import annotations

from typing import Self

from starlette.requests import Request

from docverse.client.models import Build as _BuildBase
from docverse.client.models import Edition as _EditionBase
from docverse.client.models import OrgMembership as _OrgMembershipBase
from docverse.client.models import Project as _ProjectBase
from docverse.domain.base32id import serialize_base32_id
from docverse.domain.build import Build as BuildDomain
from docverse.domain.edition import Edition as EditionDomain
from docverse.domain.membership import OrgMembership as OrgMembershipDomain
from docverse.domain.project import Project as ProjectDomain


class Project(_ProjectBase):
    """Project response model with HATEOAS URLs."""

    @classmethod
    def from_domain(
        cls,
        domain: ProjectDomain,
        request: Request,
        org_slug: str,
    ) -> Self:
        """Create from a domain object, adding HATEOAS URLs."""
        return cls(
            self_url=str(
                request.url_for(
                    "get_project",
                    org_slug=org_slug,
                    project_slug=domain.slug,
                )
            ),
            org_url=str(
                request.url_for("admin_get_organization", org_slug=org_slug)
            ),
            editions_url=str(
                request.url_for(
                    "get_editions",
                    org_slug=org_slug,
                    project_slug=domain.slug,
                )
            ),
            builds_url=str(
                request.url_for(
                    "get_builds",
                    org_slug=org_slug,
                    project_slug=domain.slug,
                )
            ),
            slug=domain.slug,
            title=domain.title,
            doc_repo=domain.doc_repo,
            slug_rewrite_rules=domain.slug_rewrite_rules,
            lifecycle_rules=domain.lifecycle_rules,
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
                    org_slug=org_slug,
                    project_slug=project_slug,
                    build_id=build_id_str,
                )
            ),
            project_url=str(
                request.url_for(
                    "get_project",
                    org_slug=org_slug,
                    project_slug=project_slug,
                )
            ),
            id=domain.public_id,
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
                    org_slug=org_slug,
                    project_slug=project_slug,
                    build_id=build_id_str,
                )
            )
        return cls(
            self_url=str(
                request.url_for(
                    "get_edition",
                    org_slug=org_slug,
                    project_slug=project_slug,
                    edition_slug=domain.slug,
                )
            ),
            project_url=str(
                request.url_for(
                    "get_project",
                    org_slug=org_slug,
                    project_slug=project_slug,
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
                    org_slug=org_slug,
                    member_id=member_id,
                )
            ),
            org_url=str(
                request.url_for("admin_get_organization", org_slug=org_slug)
            ),
            id=member_id,
            principal=domain.principal,
            principal_type=domain.principal_type,
            role=domain.role,
        )
