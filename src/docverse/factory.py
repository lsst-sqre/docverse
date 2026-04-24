"""Factory for creating Docverse service objects."""

from __future__ import annotations

import httpx
import structlog
from rubin.repertoire import DiscoveryClient
from safir.arq import ArqQueue
from sqlalchemy.ext.asyncio import AsyncSession

from .services.authorization import AuthorizationService
from .services.build import BuildService
from .services.credential import CredentialService
from .services.credential_encryptor import CredentialEncryptor
from .services.dashboard.enqueue import DashboardBuildEnqueuer
from .services.dashboard.publisher import DashboardPublisher
from .services.dashboard_templates import (
    DashboardTemplateBindingService,
    TemplateResolver,
)
from .services.edition import EditionService
from .services.edition_publishing import EditionPublishingService
from .services.edition_tracking import (
    EditionTrackingDeps,
    EditionTrackingService,
)
from .services.infrastructure import InfrastructureService
from .services.lock_service import LockService
from .services.organization import OrganizationService
from .services.project import ProjectService
from .storage.build_store import BuildStore
from .storage.dashboard_templates.github import (
    DashboardGitHubTemplateBindingStore,
    DashboardGitHubTemplateStore,
)
from .storage.edition_build_history_store import EditionBuildHistoryStore
from .storage.edition_store import EditionStore
from .storage.editionpublisher import (
    EditionPublisher,
    create_edition_publisher,
)
from .storage.membership_store import OrgMembershipStore
from .storage.objectstore import ObjectStore, create_objectstore
from .storage.organization_credential_store import OrganizationCredentialStore
from .storage.organization_service_store import OrganizationServiceStore
from .storage.organization_store import OrganizationStore
from .storage.project_store import ProjectStore
from .storage.queue_backend import (
    ArqQueueBackend,
    NullQueueBackend,
    QueueBackend,
)
from .storage.queue_job_store import QueueJobStore
from .storage.user_info_store import UserInfoStore


class Factory:
    """Build Docverse service objects."""

    def __init__(  # noqa: PLR0913
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
        credential_encryptor: CredentialEncryptor | None = None,
        superadmin_usernames: list[str] | None = None,
        http_client: httpx.AsyncClient | None = None,
        arq_queue: ArqQueue | None = None,
        discovery: DiscoveryClient | None = None,
        *,
        default_queue_name: str,
    ) -> None:
        self._session = session
        self._logger = logger
        self._credential_encryptor = credential_encryptor
        self._superadmin_usernames = superadmin_usernames or []
        self._http_client = http_client
        self._arq_queue = arq_queue
        self._discovery = discovery
        self._default_queue_name = default_queue_name

    def set_logger(self, logger: structlog.stdlib.BoundLogger) -> None:
        """Set the logger for the factory."""
        self._logger = logger

    def create_queue_backend(self) -> QueueBackend:
        """Create a :class:`QueueBackend` for enqueuing jobs."""
        if self._arq_queue is None:
            return NullQueueBackend()
        return ArqQueueBackend(
            arq_queue=self._arq_queue,
            default_queue_name=self._default_queue_name,
        )

    def _create_org_store(self) -> OrganizationStore:
        return OrganizationStore(session=self._session, logger=self._logger)

    def _create_project_store(self) -> ProjectStore:
        return ProjectStore(session=self._session, logger=self._logger)

    def create_organization_service(self) -> OrganizationService:
        """Create an OrganizationService."""
        store = self._create_org_store()
        return OrganizationService(
            store=store,
            service_store=self.create_service_store(),
            logger=self._logger,
        )

    def create_project_service(self) -> ProjectService:
        """Create a ProjectService."""
        store = self._create_project_store()
        org_store = self._create_org_store()
        edition_store = EditionStore(
            session=self._session, logger=self._logger
        )
        return ProjectService(
            store=store,
            org_store=org_store,
            edition_store=edition_store,
            logger=self._logger,
        )

    def create_build_service(self) -> BuildService:
        """Create a BuildService."""
        store = BuildStore(session=self._session, logger=self._logger)
        org_store = self._create_org_store()
        project_store = self._create_project_store()
        queue_backend = self.create_queue_backend()
        queue_job_store = QueueJobStore(
            session=self._session, logger=self._logger
        )
        return BuildService(
            store=store,
            org_store=org_store,
            project_store=project_store,
            queue_backend=queue_backend,
            queue_job_store=queue_job_store,
            logger=self._logger,
        )

    def create_edition_build_history_store(
        self,
    ) -> EditionBuildHistoryStore:
        """Create an EditionBuildHistoryStore."""
        return EditionBuildHistoryStore(
            session=self._session, logger=self._logger
        )

    def create_edition_tracking_service(self) -> EditionTrackingService:
        """Create an EditionTrackingService.

        The factory always wires in a :class:`LockService` so worker
        call paths (``build_processing``) get the EDITION_UPDATE
        advisory lock around each ``set_current_build`` call. Direct
        unit-test constructions of the service may omit ``lock_service``
        on the :class:`EditionTrackingDeps` dataclass.
        """
        deps = EditionTrackingDeps(
            edition_store=EditionStore(
                session=self._session, logger=self._logger
            ),
            history_store=EditionBuildHistoryStore(
                session=self._session, logger=self._logger
            ),
            project_store=self._create_project_store(),
            org_store=self._create_org_store(),
            logger=self._logger,
            lock_service=self.create_lock_service(),
        )
        return EditionTrackingService(deps)

    def create_edition_service(self) -> EditionService:
        """Create an EditionService."""
        store = EditionStore(session=self._session, logger=self._logger)
        org_store = self._create_org_store()
        project_store = self._create_project_store()
        history_store = EditionBuildHistoryStore(
            session=self._session, logger=self._logger
        )
        build_store = BuildStore(session=self._session, logger=self._logger)
        queue_backend = self.create_queue_backend()
        queue_job_store = QueueJobStore(
            session=self._session, logger=self._logger
        )
        return EditionService(
            store=store,
            org_store=org_store,
            project_store=project_store,
            logger=self._logger,
            history_store=history_store,
            build_store=build_store,
            queue_backend=queue_backend,
            queue_job_store=queue_job_store,
        )

    def create_authorization_service(self) -> AuthorizationService:
        """Create an AuthorizationService."""
        membership_store = OrgMembershipStore(
            session=self._session, logger=self._logger
        )
        return AuthorizationService(
            membership_store=membership_store,
            logger=self._logger,
            superadmin_usernames=self._superadmin_usernames,
        )

    def create_membership_store(self) -> OrgMembershipStore:
        """Create an OrgMembershipStore."""
        return OrgMembershipStore(session=self._session, logger=self._logger)

    def create_queue_job_store(self) -> QueueJobStore:
        """Create a QueueJobStore."""
        return QueueJobStore(session=self._session, logger=self._logger)

    def create_credential_store(self) -> OrganizationCredentialStore:
        """Create an OrganizationCredentialStore."""
        return OrganizationCredentialStore(
            session=self._session, logger=self._logger
        )

    def create_service_store(self) -> OrganizationServiceStore:
        """Create an OrganizationServiceStore."""
        return OrganizationServiceStore(
            session=self._session, logger=self._logger
        )

    def create_credential_service(self) -> CredentialService:
        """Create a CredentialService.

        Raises
        ------
        RuntimeError
            If the credential encryptor is not configured.
        """
        if self._credential_encryptor is None:
            msg = "Credential encryption is not configured"
            raise RuntimeError(msg)
        return CredentialService(
            store=self.create_credential_store(),
            org_store=self._create_org_store(),
            service_store=self.create_service_store(),
            encryptor=self._credential_encryptor,
            logger=self._logger,
        )

    def create_infrastructure_service(self) -> InfrastructureService:
        """Create an InfrastructureService."""
        return InfrastructureService(
            store=self.create_service_store(),
            credential_store=self.create_credential_store(),
            org_store=self._create_org_store(),
            logger=self._logger,
        )

    def create_lock_service(self) -> LockService:
        """Create a LockService bound to this factory's session."""
        return LockService(session=self._session, logger=self._logger)

    def create_edition_publishing_service(self) -> EditionPublishingService:
        """Create an EditionPublishingService."""
        return EditionPublishingService(
            org_store=self._create_org_store(),
            edition_store=EditionStore(
                session=self._session, logger=self._logger
            ),
            history_store=EditionBuildHistoryStore(
                session=self._session, logger=self._logger
            ),
            publisher_provider=self.create_edition_publisher_for_org,
            logger=self._logger,
        )

    def _create_dashboard_github_template_binding_store(
        self,
    ) -> DashboardGitHubTemplateBindingStore:
        return DashboardGitHubTemplateBindingStore(
            session=self._session, logger=self._logger
        )

    def create_dashboard_template_binding_service(
        self,
    ) -> DashboardTemplateBindingService:
        """Create a :class:`DashboardTemplateBindingService`."""
        return DashboardTemplateBindingService(
            binding_store=self._create_dashboard_github_template_binding_store(),
            org_store=self._create_org_store(),
            project_store=self._create_project_store(),
            logger=self._logger,
        )

    def create_dashboard_build_enqueuer(
        self,
    ) -> DashboardBuildEnqueuer:
        """Create a DashboardBuildEnqueuer."""
        return DashboardBuildEnqueuer(
            org_store=self._create_org_store(),
            project_store=self._create_project_store(),
            queue_backend=self.create_queue_backend(),
            queue_job_store=self.create_queue_job_store(),
            logger=self._logger,
        )

    def create_template_resolver(self) -> TemplateResolver:
        """Create a TemplateResolver for render-time template lookup."""
        binding_store = DashboardGitHubTemplateBindingStore(
            session=self._session, logger=self._logger
        )
        template_store = DashboardGitHubTemplateStore(
            session=self._session, logger=self._logger
        )
        return TemplateResolver(
            binding_store=binding_store,
            template_store=template_store,
            logger=self._logger,
        )

    def create_dashboard_publisher(self) -> DashboardPublisher:
        """Create a DashboardPublisher for one render.

        Raises
        ------
        RuntimeError
            If the Repertoire discovery client is not configured.
        """
        if self._discovery is None:
            msg = "DiscoveryClient is required to build a DashboardPublisher"
            raise RuntimeError(msg)
        return DashboardPublisher(
            org_store=self._create_org_store(),
            project_store=self._create_project_store(),
            edition_store=EditionStore(
                session=self._session, logger=self._logger
            ),
            build_store=BuildStore(session=self._session, logger=self._logger),
            discovery=self._discovery,
            logger=self._logger,
            template_resolver=self.create_template_resolver(),
        )

    async def create_edition_publisher_for_org(
        self, *, org_id: int, service_label: str
    ) -> EditionPublisher:
        """Resolve an org's EditionPublisher from its service configuration.

        Uses the two-step resolution: service label -> config +
        credential_label -> decrypt credential -> build EditionPublisher.

        Parameters
        ----------
        org_id
            Organization ID.
        service_label
            Service label to use (typically the org's
            ``cdn_service_label``).

        Returns
        -------
        EditionPublisher
            An unopened EditionPublisher. Caller must use as async
            context manager.
        """
        if self._http_client is None:
            msg = "HTTP client is required to build an EditionPublisher"
            raise RuntimeError(msg)

        # Step 1: Load the service config
        service_store = self.create_service_store()
        svc = await service_store.get_by_label(
            organization_id=org_id, label=service_label
        )
        if svc is None:
            msg = f"Service {service_label!r} not found"
            raise RuntimeError(msg)

        # Step 2: Decrypt the credential
        credential_service = self.create_credential_service()
        _cred, cred_payload = await credential_service.get_decrypted(
            org_id=org_id, label=svc.credential_label
        )

        # Step 3: Build the EditionPublisher from config + credentials
        return create_edition_publisher(
            provider=svc.provider,
            config=svc.config,
            credentials=cred_payload,
            logger=self._logger,
            http_client=self._http_client,
        )

    async def create_objectstore_for_org(
        self, *, org_id: int, service_label: str
    ) -> ObjectStore:
        """Resolve an org's ObjectStore from its service configuration.

        Uses the two-step resolution: service label -> config +
        credential_label -> decrypt credential -> build ObjectStore.

        Parameters
        ----------
        org_id
            Organization ID.
        service_label
            Service label to use (e.g., the org's
            ``publishing_store_label``).

        Returns
        -------
        ObjectStore
            An unopened ObjectStore. Caller must use as async context
            manager.
        """
        # Step 1: Load the service config
        service_store = self.create_service_store()
        svc = await service_store.get_by_label(
            organization_id=org_id, label=service_label
        )
        if svc is None:
            msg = f"Service {service_label!r} not found"
            raise RuntimeError(msg)

        # Step 2: Decrypt the credential
        credential_service = self.create_credential_service()
        _cred, cred_payload = await credential_service.get_decrypted(
            org_id=org_id, label=svc.credential_label
        )

        # Step 3: Build the ObjectStore from config + credentials
        return create_objectstore(
            provider=svc.provider,
            config=svc.config,
            credentials=cred_payload,
            logger=self._logger,
            http_client=self._http_client,
        )


class HandlerFactory(Factory):
    """Factory for request handlers with arq queue and user info."""

    def __init__(  # noqa: PLR0913
        self,
        session: AsyncSession,
        logger: structlog.stdlib.BoundLogger,
        arq_queue: ArqQueue,
        user_info_store: UserInfoStore,
        credential_encryptor: CredentialEncryptor | None = None,
        superadmin_usernames: list[str] | None = None,
        discovery: DiscoveryClient | None = None,
        *,
        default_queue_name: str,
    ) -> None:
        super().__init__(
            session=session,
            logger=logger,
            credential_encryptor=credential_encryptor,
            superadmin_usernames=superadmin_usernames,
            arq_queue=arq_queue,
            discovery=discovery,
            default_queue_name=default_queue_name,
        )
        self._user_info_store = user_info_store

    def get_user_info_store(self) -> UserInfoStore:
        """Get the UserInfoStore instance."""
        return self._user_info_store
