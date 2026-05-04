"""Factory for creating Docverse service objects."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
import structlog
from pydantic import SecretStr
from rubin.repertoire import DiscoveryClient
from safir.arq import ArqQueue
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from .services.authorization import AuthorizationService
from .services.build import BuildService
from .services.credential import CredentialService
from .services.credential_encryptor import CredentialEncryptor
from .services.dashboard.enqueue import DashboardBuildEnqueuer
from .services.dashboard.publisher import DashboardPublisher
from .services.dashboard_templates import (
    DashboardRebuildFanout,
    DashboardSyncEnqueuer,
    DashboardTemplateBindingService,
    DashboardTemplateSyncer,
    InstallationEventProcessor,
    PushEventProcessor,
    RenameEventProcessor,
    TemplateResolver,
)
from .services.edition import EditionService
from .services.edition_publishing import EditionPublishingService
from .services.edition_tracking import (
    EditionTrackingDeps,
    EditionTrackingService,
)
from .services.infrastructure import InfrastructureService
from .services.keeper_sync_config import KeeperSyncConfigService
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
from .storage.github import GitHubAppClient, GitHubAppNotConfiguredError
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


@dataclass(frozen=True)
class WebhookDispatch:
    """Bundle of objects the GitHub webhook handler needs per delivery.

    The HMAC secret verifies ``x-hub-signature-256``; the three
    processors handle the event types the dashboard-template feature
    subscribes to. Created fresh per request inside
    :meth:`Factory.create_webhook_dispatch` so each delivery binds to
    the request's own DB session and logger.
    """

    webhook_secret: str
    push: PushEventProcessor
    rename: RenameEventProcessor
    installation: InstallationEventProcessor


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
        github_app_id: int | None = None,
        github_app_private_key: SecretStr | None = None,
        github_webhook_secret: SecretStr | None = None,
        github_app_name: str = "lsst-sqre/docverse",
        *,
        github_app_validated: bool = True,
        default_queue_name: str,
    ) -> None:
        self._session = session
        self._logger = logger
        self._credential_encryptor = credential_encryptor
        self._superadmin_usernames = superadmin_usernames or []
        self._http_client = http_client
        self._arq_queue = arq_queue
        self._discovery = discovery
        self._github_app_id = github_app_id
        self._github_app_private_key = github_app_private_key
        self._github_webhook_secret = github_webhook_secret
        self._github_app_name = github_app_name
        self._github_app_validated = github_app_validated
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

    def create_org_store(self) -> OrganizationStore:
        """Create an :class:`OrganizationStore`."""
        return OrganizationStore(session=self._session, logger=self._logger)

    def create_project_store(self) -> ProjectStore:
        """Create a :class:`ProjectStore`."""
        return ProjectStore(session=self._session, logger=self._logger)

    def create_build_store(self) -> BuildStore:
        """Create a :class:`BuildStore`."""
        return BuildStore(session=self._session, logger=self._logger)

    def create_edition_store(self) -> EditionStore:
        """Create an :class:`EditionStore`."""
        return EditionStore(session=self._session, logger=self._logger)

    def create_organization_service(self) -> OrganizationService:
        """Create an OrganizationService."""
        store = self.create_org_store()
        return OrganizationService(
            store=store,
            service_store=self.create_service_store(),
            logger=self._logger,
        )

    def create_keeper_sync_config_service(self) -> KeeperSyncConfigService:
        """Create a KeeperSyncConfigService."""
        return KeeperSyncConfigService(
            org_store=self.create_org_store(),
            logger=self._logger,
        )

    def create_project_service(self) -> ProjectService:
        """Create a ProjectService."""
        store = self.create_project_store()
        org_store = self.create_org_store()
        edition_store = self.create_edition_store()
        return ProjectService(
            store=store,
            org_store=org_store,
            edition_store=edition_store,
            logger=self._logger,
        )

    def create_build_service(self) -> BuildService:
        """Create a BuildService."""
        store = self.create_build_store()
        org_store = self.create_org_store()
        project_store = self.create_project_store()
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
            edition_store=self.create_edition_store(),
            history_store=EditionBuildHistoryStore(
                session=self._session, logger=self._logger
            ),
            project_store=self.create_project_store(),
            org_store=self.create_org_store(),
            logger=self._logger,
            lock_service=self.create_lock_service(),
        )
        return EditionTrackingService(deps)

    def create_edition_service(self) -> EditionService:
        """Create an EditionService."""
        store = self.create_edition_store()
        org_store = self.create_org_store()
        project_store = self.create_project_store()
        history_store = EditionBuildHistoryStore(
            session=self._session, logger=self._logger
        )
        build_store = self.create_build_store()
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
            org_store=self.create_org_store(),
            service_store=self.create_service_store(),
            encryptor=self._credential_encryptor,
            logger=self._logger,
        )

    def create_infrastructure_service(self) -> InfrastructureService:
        """Create an InfrastructureService."""
        return InfrastructureService(
            store=self.create_service_store(),
            credential_store=self.create_credential_store(),
            org_store=self.create_org_store(),
            logger=self._logger,
        )

    def create_lock_service(self) -> LockService:
        """Create a LockService bound to this factory's session."""
        return LockService(session=self._session, logger=self._logger)

    def _require_github_app_config(
        self,
    ) -> tuple[int, SecretStr, SecretStr]:
        """Return the three GitHub App secrets, or raise if any is unset.

        The GitHub App feature is all-or-nothing: callers that touch
        any of the three secrets must treat them as a single bundle so
        a partial configuration cannot silently degrade behaviour. The
        gate also rejects when the startup-time credential validation
        has been recorded as failed — keeping the binding endpoints +
        webhook in lockstep with the startup hook's
        ``set_github_app_validated(False)`` decision.

        Raises
        ------
        GitHubAppNotConfiguredError
            If any of ``github_app_id``, ``github_app_private_key``, or
            ``github_webhook_secret`` is unset, or the startup-time
            validation marked the credentials as invalid.
        """
        if (
            self._github_app_id is None
            or self._github_app_private_key is None
            or self._github_webhook_secret is None
        ):
            msg = "GitHub App is not configured"
            raise GitHubAppNotConfiguredError(msg)
        if not self._github_app_validated:
            msg = "GitHub App credentials failed startup validation"
            raise GitHubAppNotConfiguredError(msg)
        return (
            self._github_app_id,
            self._github_app_private_key,
            self._github_webhook_secret,
        )

    def create_github_app_client(self) -> GitHubAppClient:
        """Create a GitHubAppClient from the configured GitHub App secrets.

        The returned :class:`GitHubAppClient` exposes installation-token
        exchange and a :class:`InstallationAuth` factory; downstream
        helpers (tree fetcher, compare API helper) attach that auth to
        the shared ``httpx.AsyncClient`` per request rather than
        receiving a pre-authenticated client of their own.

        Raises
        ------
        GitHubAppNotConfiguredError
            If any of ``github_app_id``, ``github_app_private_key``, or
            ``github_webhook_secret`` is unset. Callers at HTTP
            boundaries translate this to a feature-disabled response
            (503 for admin endpoints, 404 for the webhook endpoint).
        RuntimeError
            If no shared ``httpx.AsyncClient`` is configured on the
            factory — the GitHub REST calls need one.
        """
        app_id, private_key, _ = self._require_github_app_config()
        if self._http_client is None:
            msg = "HTTP client is required to build a GitHubAppClient"
            raise RuntimeError(msg)
        factory = GitHubAppClientFactory(
            id=app_id,
            key=private_key.get_secret_value(),
            name=self._github_app_name,
            http_client=self._http_client,
        )
        return GitHubAppClient(
            factory=factory,
            http_client=self._http_client,
            logger=self._logger,
        )

    def create_edition_publishing_service(self) -> EditionPublishingService:
        """Create an EditionPublishingService."""
        return EditionPublishingService(
            org_store=self.create_org_store(),
            edition_store=self.create_edition_store(),
            history_store=EditionBuildHistoryStore(
                session=self._session, logger=self._logger
            ),
            publisher_provider=self.create_edition_publisher_for_org,
            logger=self._logger,
        )

    def create_dashboard_github_template_binding_store(
        self,
    ) -> DashboardGitHubTemplateBindingStore:
        """Create a :class:`DashboardGitHubTemplateBindingStore`."""
        return DashboardGitHubTemplateBindingStore(
            session=self._session, logger=self._logger
        )

    def create_dashboard_template_binding_service(
        self,
    ) -> DashboardTemplateBindingService:
        """Create a :class:`DashboardTemplateBindingService`."""
        return DashboardTemplateBindingService(
            binding_store=self.create_dashboard_github_template_binding_store(),
            org_store=self.create_org_store(),
            project_store=self.create_project_store(),
            logger=self._logger,
        )

    def create_dashboard_build_enqueuer(
        self,
    ) -> DashboardBuildEnqueuer:
        """Create a DashboardBuildEnqueuer."""
        return DashboardBuildEnqueuer(
            org_store=self.create_org_store(),
            project_store=self.create_project_store(),
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

    def create_dashboard_sync_enqueuer(self) -> DashboardSyncEnqueuer:
        """Create a :class:`DashboardSyncEnqueuer`."""
        return DashboardSyncEnqueuer(
            binding_store=self.create_dashboard_github_template_binding_store(),
            queue_backend=self.create_queue_backend(),
            queue_job_store=self.create_queue_job_store(),
            logger=self._logger,
        )

    def create_dashboard_rebuild_fanout(self) -> DashboardRebuildFanout:
        """Create a :class:`DashboardRebuildFanout`."""
        return DashboardRebuildFanout(
            binding_store=self.create_dashboard_github_template_binding_store(),
            project_store=self.create_project_store(),
            enqueuer=self.create_dashboard_build_enqueuer(),
            logger=self._logger,
        )

    def create_dashboard_template_syncer(self) -> DashboardTemplateSyncer:
        """Create a :class:`DashboardTemplateSyncer`.

        Raises
        ------
        GitHubAppNotConfiguredError
            If the GitHub App feature is not configured.
        RuntimeError
            If the shared HTTP client is not configured.
        """
        if self._http_client is None:
            msg = "HTTP client is required to build a DashboardTemplateSyncer"
            raise RuntimeError(msg)
        return DashboardTemplateSyncer(
            binding_store=self.create_dashboard_github_template_binding_store(),
            template_store=DashboardGitHubTemplateStore(
                session=self._session, logger=self._logger
            ),
            app_client=self.create_github_app_client(),
            http_client=self._http_client,
            logger=self._logger,
        )

    def create_webhook_dispatch(self) -> WebhookDispatch:
        """Return the webhook secret + every event-type processor.

        The webhook handler needs the HMAC secret (to verify
        ``x-hub-signature-256``) and one processor per registered
        event type. Bundling them into one accessor gives the handler
        a single ``GitHubAppNotConfiguredError`` raise site to
        translate into its 404 feature-disabled response, and the
        gidgethub router dispatches the right processor by event +
        action without per-handler factory plumbing.

        Raises
        ------
        GitHubAppNotConfiguredError
            If any of the three GitHub App secrets is unset.
        RuntimeError
            If the shared HTTP client is not configured.
        """
        _, _, webhook_secret = self._require_github_app_config()
        if self._http_client is None:
            msg = "HTTP client is required to build a PushEventProcessor"
            raise RuntimeError(msg)
        binding_store = self.create_dashboard_github_template_binding_store()
        template_store = DashboardGitHubTemplateStore(
            session=self._session, logger=self._logger
        )
        push = PushEventProcessor(
            binding_store=binding_store,
            enqueuer=self.create_dashboard_sync_enqueuer(),
            app_client=self.create_github_app_client(),
            http_client=self._http_client,
            logger=self._logger,
        )
        rename = RenameEventProcessor(
            binding_store=binding_store,
            template_store=template_store,
            logger=self._logger,
        )
        installation = InstallationEventProcessor(
            binding_store=binding_store,
            logger=self._logger,
        )
        return WebhookDispatch(
            webhook_secret=webhook_secret.get_secret_value(),
            push=push,
            rename=rename,
            installation=installation,
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
            org_store=self.create_org_store(),
            project_store=self.create_project_store(),
            edition_store=self.create_edition_store(),
            build_store=self.create_build_store(),
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
        http_client: httpx.AsyncClient | None = None,
        github_app_id: int | None = None,
        github_app_private_key: SecretStr | None = None,
        github_webhook_secret: SecretStr | None = None,
        *,
        github_app_validated: bool = True,
        default_queue_name: str,
    ) -> None:
        super().__init__(
            session=session,
            logger=logger,
            credential_encryptor=credential_encryptor,
            superadmin_usernames=superadmin_usernames,
            arq_queue=arq_queue,
            discovery=discovery,
            http_client=http_client,
            github_app_id=github_app_id,
            github_app_private_key=github_app_private_key,
            github_webhook_secret=github_webhook_secret,
            github_app_validated=github_app_validated,
            default_queue_name=default_queue_name,
        )
        self._user_info_store = user_info_store

    def get_user_info_store(self) -> UserInfoStore:
        """Get the UserInfoStore instance."""
        return self._user_info_store
