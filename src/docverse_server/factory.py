"""Factory for creating Docverse service objects."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx
import structlog
from pydantic import SecretStr
from rubin.repertoire import DiscoveryClient
from safir.arq import ArqQueue
from safir.github import GitHubAppClientFactory
from sqlalchemy.ext.asyncio import AsyncSession

from .services.authorization import AuthorizationService
from .services.build import BuildService
from .services.cdn_purge_coalescer import CdnPurgeCoalescer
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
from .services.inventory_census import InventoryCensusService
from .services.keeper_sync import (
    DEFAULT_COPY_CONCURRENCY,
    BuildContentCopier,
    CopyResult,
    KeeperSyncContext,
    KeeperSyncService,
)
from .services.keeper_sync_config import KeeperSyncConfigService
from .services.keeper_sync_project import KeeperSyncProjectService
from .services.keeper_sync_run import KeeperSyncRunService
from .services.keeper_sync_tombstone import KeeperSyncTombstoneService
from .services.lock_service import LockService
from .services.organization import OrganizationService
from .services.project import ProjectService
from .services.project_github_binding import ProjectGitHubBindingResolver
from .services.ref_deleted_processor import RefDeletedWebhookProcessor
from .storage.build_store import BuildStore
from .storage.cdncachepurger import CdnCachePurger, create_cdn_cache_purger
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
from .storage.git_ref_audit_run_store import GitRefAuditRunStore
from .storage.github import (
    GitHubAppClient,
    GitHubAppNotConfiguredError,
    GitHubRefSetFetcher,
)
from .storage.inventory_census_store import InventoryCensusStore
from .storage.keeper_sync import KeeperSyncStateStore
from .storage.keeper_sync_run_store import KeeperSyncRunStore
from .storage.lifecycle_eval_run_store import LifecycleEvalRunStore
from .storage.ltd import LtdClient, LtdProductsClient, LtdS3Source
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

    The HMAC secret verifies ``x-hub-signature-256``; one processor per
    registered event type handles the work. Created fresh per request
    inside :meth:`Factory.create_webhook_dispatch` so each delivery
    binds to the request's own DB session and logger.
    """

    webhook_secret: str
    push: PushEventProcessor
    rename: RenameEventProcessor
    installation: InstallationEventProcessor
    ref_deleted: RefDeletedWebhookProcessor


class Factory:
    """Build Docverse service objects."""

    def __init__(
        self,
        *,
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
        github_app_validated: bool = True,
        purge_coalescer: CdnPurgeCoalescer | None = None,
        default_queue_name: str,
        keeper_sync_copy_concurrency: int = DEFAULT_COPY_CONCURRENCY,
    ) -> None:
        # A Factory is per-job / per-request, so an instance created here
        # coalesces nothing beyond the single publish this Factory drives
        # — i.e. the pre-coalescing behaviour. Production wiring injects
        # the process-lifetime instance held by
        # ``WorkerFactoryBuilder``; the local fallback keeps directly
        # constructed factories (tests, one-off scripts) working without
        # sharing coalescing state between them.
        self._purge_coalescer = purge_coalescer or CdnPurgeCoalescer()
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
        # Defaults to the copier's own fallback so directly constructed
        # factories (tests, scripts) behave exactly as before; the arq
        # worker — the only process that actually copies build content —
        # threads ``Config.keeper_sync_copy_concurrency`` through
        # ``WorkerFactoryBuilder``.
        self._keeper_sync_copy_concurrency = keeper_sync_copy_concurrency

    def set_logger(self, logger: structlog.stdlib.BoundLogger) -> None:
        """Set the logger for the factory."""
        self._logger = logger

    @property
    def discovery(self) -> DiscoveryClient | None:
        """Repertoire discovery client, or ``None`` when not configured."""
        return self._discovery

    @property
    def purge_coalescer(self) -> CdnPurgeCoalescer:
        """CDN purge coalescer backing this factory's publishing service."""
        return self._purge_coalescer

    @property
    def keeper_sync_copy_concurrency(self) -> int:
        """Fan-out bound handed to every copier this factory builds."""
        return self._keeper_sync_copy_concurrency

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

    def create_keeper_sync_run_store(self) -> KeeperSyncRunStore:
        """Create a :class:`KeeperSyncRunStore`."""
        return KeeperSyncRunStore(session=self._session, logger=self._logger)

    def create_lifecycle_eval_run_store(self) -> LifecycleEvalRunStore:
        """Create a :class:`LifecycleEvalRunStore`."""
        return LifecycleEvalRunStore(
            session=self._session, logger=self._logger
        )

    def create_git_ref_audit_run_store(self) -> GitRefAuditRunStore:
        """Create a :class:`GitRefAuditRunStore`."""
        return GitRefAuditRunStore(session=self._session, logger=self._logger)

    def create_inventory_census_store(self) -> InventoryCensusStore:
        """Create an :class:`InventoryCensusStore`."""
        return InventoryCensusStore(session=self._session, logger=self._logger)

    def create_inventory_census_service(self) -> InventoryCensusService:
        """Create an :class:`InventoryCensusService`."""
        return InventoryCensusService(
            store=self.create_inventory_census_store(),
            logger=self._logger,
        )

    def create_github_ref_set_fetcher(self) -> GitHubRefSetFetcher:
        """Create a :class:`GitHubRefSetFetcher`.

        Used by the daily ``git_ref_audit`` worker (PRD #346) and by
        the proactive ``sync_project`` pre-fetch (PRD #332). Both
        callers paginate ``git/matching-refs/{heads,tags}`` against
        the shared ``httpx.AsyncClient`` and attach installation
        auth per request, so the fetcher is built once per worker
        tick and shared across per-project fan-out.

        Raises
        ------
        RuntimeError
            If no shared ``httpx.AsyncClient`` is configured.
        """
        if self._http_client is None:
            msg = "HTTP client is required to build a GitHubRefSetFetcher"
            raise RuntimeError(msg)
        return GitHubRefSetFetcher(http_client=self._http_client)

    def create_keeper_sync_run_service(self) -> KeeperSyncRunService:
        """Create a :class:`KeeperSyncRunService`."""
        return KeeperSyncRunService(
            org_store=self.create_org_store(),
            run_store=self.create_keeper_sync_run_store(),
            queue_backend=self.create_queue_backend(),
            queue_job_store=self.create_queue_job_store(),
            logger=self._logger,
        )

    def create_keeper_sync_project_service(
        self,
    ) -> KeeperSyncProjectService:
        """Create a :class:`KeeperSyncProjectService`."""
        return KeeperSyncProjectService(
            org_store=self.create_org_store(),
            project_store=self.create_project_store(),
            edition_store=self.create_edition_store(),
            state_store=self.create_keeper_sync_state_store(),
            ltd_client_factory=self.create_ltd_client,
            logger=self._logger,
        )

    def create_ltd_products_client(
        self, *, base_url: str
    ) -> LtdProductsClient:
        """Create a :class:`LtdProductsClient`.

        Raises
        ------
        RuntimeError
            If the shared HTTP client is not configured.
        """
        if self._http_client is None:
            msg = "HTTP client is required to build an LtdProductsClient"
            raise RuntimeError(msg)
        return LtdProductsClient(
            http_client=self._http_client,
            base_url=base_url,
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
        if self._github_app_id is None:
            raise GitHubAppNotConfiguredError(missing_secret="app_id")
        if self._github_app_private_key is None:
            raise GitHubAppNotConfiguredError(missing_secret="private_key")
        if self._github_webhook_secret is None:
            raise GitHubAppNotConfiguredError(missing_secret="webhook_secret")
        if not self._github_app_validated:
            # The startup validator failed but does not record which
            # specific credential is to blame; the canonical failure
            # mode is a malformed PEM or a key that no longer matches
            # the registered app, both of which surface through the
            # private key. Keep the explicit "failed startup
            # validation" wording in pod logs and on the
            # ``SlackException`` rendering while tagging the Sentry
            # event with ``missing_secret="private_key"`` so the event
            # routes to the same operator persona as the unset-key
            # case.
            raise GitHubAppNotConfiguredError(
                missing_secret="private_key",
                message="GitHub App credentials failed startup validation",
            )
        return (
            self._github_app_id,
            self._github_app_private_key,
            self._github_webhook_secret,
        )

    def create_project_github_binding_resolver(
        self,
    ) -> ProjectGitHubBindingResolver:
        """Create a :class:`ProjectGitHubBindingResolver`.

        Used by the future ``git_ref_audit`` worker (PRD #346) and by
        the proactive ``sync_project`` pre-fetch (PRD #332). Both
        callers need the same "installation > anonymous > skip" decision
        before calling :class:`GitHubRefSetFetcher`, so the resolver is
        the one place that ladder lives.

        Raises
        ------
        GitHubAppNotConfiguredError
            If any of the three GitHub App secrets is unset. The audit
            cannot mint installation tokens without them; routing this
            via the same Sentry pipeline as other GitHub failures keeps
            misconfiguration loud.
        RuntimeError
            If no shared ``httpx.AsyncClient`` is configured.
        """
        return ProjectGitHubBindingResolver(
            session=self._session,
            project_store=self.create_project_store(),
            app_client=self.create_github_app_client(),
            logger=self._logger,
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
            purger_provider=self.create_cdn_cache_purger_for_org,
            purge_coalescer=self._purge_coalescer,
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
            project_store=self.create_project_store(),
            logger=self._logger,
        )
        installation = InstallationEventProcessor(
            binding_store=binding_store,
            project_store=self.create_project_store(),
            logger=self._logger,
        )
        ref_deleted = RefDeletedWebhookProcessor(
            project_store=self.create_project_store(),
            edition_store=self.create_edition_store(),
            edition_service=self.create_edition_service(),
            org_store=self.create_org_store(),
            publishing_service=self.create_edition_publishing_service(),
            logger=self._logger,
        )
        return WebhookDispatch(
            webhook_secret=webhook_secret.get_secret_value(),
            push=push,
            rename=rename,
            installation=installation,
            ref_deleted=ref_deleted,
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

        provider, config, credentials = await self._resolve_cdn_service(
            org_id=org_id, service_label=service_label
        )
        return create_edition_publisher(
            provider=provider,
            config=config,
            credentials=credentials,
            logger=self._logger,
            http_client=self._http_client,
        )

    async def create_cdn_cache_purger_for_org(
        self, *, org_id: int, service_label: str
    ) -> CdnCachePurger:
        """Resolve an org's CdnCachePurger from its service configuration.

        Shares the org's CDN service row (and therefore its API token)
        with `create_edition_publisher_for_org`, so operators manage one
        Cloudflare service rather than two.

        Parameters
        ----------
        org_id
            Organization ID.
        service_label
            Service label to use (typically the org's
            ``cdn_service_label``).

        Returns
        -------
        CdnCachePurger
            An unopened CdnCachePurger. Caller must use as async context
            manager. Organizations whose service has no ``zone_id`` get a
            no-op purger rather than an error.
        """
        if self._http_client is None:
            msg = "HTTP client is required to build a CdnCachePurger"
            raise RuntimeError(msg)

        provider, config, credentials = await self._resolve_cdn_service(
            org_id=org_id, service_label=service_label
        )
        return create_cdn_cache_purger(
            provider=provider,
            config=config,
            credentials=credentials,
            logger=self._logger,
            http_client=self._http_client,
        )

    async def _resolve_cdn_service(
        self, *, org_id: int, service_label: str
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        """Load a service row and decrypt its credential.

        Returns the service's provider, its non-secret config, and the
        decrypted credential payload — the three inputs every CDN
        storage factory takes.

        Raises
        ------
        RuntimeError
            If no service with ``service_label`` exists for the org.
        """
        service_store = self.create_service_store()
        svc = await service_store.get_by_label(
            organization_id=org_id, label=service_label
        )
        if svc is None:
            msg = f"Service {service_label!r} not found"
            raise RuntimeError(msg)

        credential_service = self.create_credential_service()
        _cred, cred_payload = await credential_service.get_decrypted(
            org_id=org_id, label=svc.credential_label
        )
        return svc.provider, svc.config, cred_payload

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

    def create_ltd_client(
        self, *, base_url: str = "https://keeper.lsst.codes"
    ) -> LtdClient:
        """Create an :class:`LtdClient` over the shared HTTP client."""
        if self._http_client is None:
            msg = "HTTP client is required to build an LtdClient"
            raise RuntimeError(msg)
        return LtdClient(
            http_client=self._http_client,
            base_url=base_url,
            logger=self._logger,
        )

    def create_ltd_s3_source(
        self, *, bucket: str = "lsst-the-docs"
    ) -> LtdS3Source:
        """Create an unopened anonymous S3 source for ``bucket``."""
        return LtdS3Source(bucket=bucket, logger=self._logger)

    def create_build_content_copier_for_org(
        self,
        *,
        org_id: int,
        service_label: str,
    ) -> AbstractAsyncContextManager[BuildContentCopier]:
        """Return an async-CM that yields a wired-up copier for ``org``.

        Used as ``async with factory.create_build_content_copier_for_org(
        org_id=..., service_label=...) as copier:``. Both the LTD source
        and the per-org destination are opened on entry and closed on
        exit so a sync slot's resource lifetime is tightly bounded.

        The copier's fan-out bound comes from this factory's
        ``keeper_sync_copy_concurrency``, so an operator can move the
        sync worker's memory ceiling without a code change. Peak
        resident size scales with the pool's ``max_jobs`` times that
        bound times the largest object under a build prefix.
        """

        @asynccontextmanager
        async def _open() -> AsyncGenerator[BuildContentCopier]:
            async with self._session.begin():
                destination = await self.create_objectstore_for_org(
                    org_id=org_id, service_label=service_label
                )
            source = self.create_ltd_s3_source()
            async with source, destination:
                yield BuildContentCopier(
                    source=source,
                    destination=destination,
                    logger=self._logger,
                    max_concurrent=self._keeper_sync_copy_concurrency,
                )

        return _open()

    def create_keeper_sync_state_store(self) -> KeeperSyncStateStore:
        """Create a :class:`KeeperSyncStateStore`."""
        return KeeperSyncStateStore(session=self._session, logger=self._logger)

    def create_keeper_sync_tombstone_service(
        self,
    ) -> KeeperSyncTombstoneService:
        """Create a :class:`KeeperSyncTombstoneService`."""
        return KeeperSyncTombstoneService(
            session=self._session,
            state_store=self.create_keeper_sync_state_store(),
            logger=self._logger,
        )

    def create_keeper_sync_service(
        self,
        *,
        org_id: int,
        service_label: str,
        ltd_base_url: str = "https://keeper.lsst.codes",
    ) -> KeeperSyncService:
        """Create a :class:`KeeperSyncService` for one org's sync run.

        Always wires a :class:`LockService`, so the sync worker takes the
        EDITION_UPDATE advisory lock around the aggregate pointer
        updates it shares with the native ``build_processing`` path.
        Direct unit-test constructions of the service may omit it and
        run unwrapped, exactly as ``EditionTrackingService`` does.
        """
        ltd_client = self.create_ltd_client(base_url=ltd_base_url)

        async def copy_callable(
            source_prefix: str, dest_prefix: str
        ) -> CopyResult:
            async with self.create_build_content_copier_for_org(
                org_id=org_id, service_label=service_label
            ) as copier:
                return await copier.copy_build(
                    source_prefix=source_prefix, dest_prefix=dest_prefix
                )

        async def manifest_callable(source_prefix: str) -> str:
            async with self.create_build_content_copier_for_org(
                org_id=org_id, service_label=service_label
            ) as copier:
                return await copier.compute_manifest_hash(
                    source_prefix=source_prefix
                )

        context = KeeperSyncContext(
            org_store=self.create_org_store(),
            project_store=self.create_project_store(),
            project_service=self.create_project_service(),
            edition_store=self.create_edition_store(),
            build_store=self.create_build_store(),
            state_store=self.create_keeper_sync_state_store(),
        )
        # The proactive lifecycle evaluator needs all three GitHub-aware
        # deps. Missing GitHub-App secrets or an unconfigured HTTP client
        # disables the proactive pass — sync_project then falls through
        # to the existing per-edition path; the regular lifecycle_eval
        # / git_ref_audit crons still catch any deletable editions on
        # their own schedule.
        tombstone_service = self.create_keeper_sync_tombstone_service()
        binding_resolver: ProjectGitHubBindingResolver | None
        ref_set_fetcher: GitHubRefSetFetcher | None
        try:
            binding_resolver = self.create_project_github_binding_resolver()
        except (GitHubAppNotConfiguredError, RuntimeError):
            binding_resolver = None
        try:
            ref_set_fetcher = self.create_github_ref_set_fetcher()
        except RuntimeError:
            ref_set_fetcher = None
        return KeeperSyncService(
            session=self._session,
            context=context,
            ltd_client=ltd_client,
            copy_callable=copy_callable,
            manifest_callable=manifest_callable,
            logger=self._logger,
            tombstone_service=tombstone_service,
            binding_resolver=binding_resolver,
            ref_set_fetcher=ref_set_fetcher,
            lock_service=self.create_lock_service(),
        )


class HandlerFactory(Factory):
    """Factory for request handlers with arq queue and user info."""

    def __init__(
        self,
        *,
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
        github_app_html_url: str | None = None,
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
        self._github_app_html_url = github_app_html_url

    def get_user_info_store(self) -> UserInfoStore:
        """Get the UserInfoStore instance."""
        return self._user_info_store

    @property
    def github_app_html_url(self) -> str | None:
        """The GitHub App's public install-page URL, or ``None``.

        Captured from the startup ``GET /app`` validation and threaded
        through
        :class:`docverse_server.dependencies.context.ContextDependency`.
        Handlers read it as ``context.factory.github_app_html_url`` to
        populate ``github.app_url`` on project responses. ``None`` when
        the GitHub App feature is unconfigured or its credentials failed
        startup validation.
        """
        return self._github_app_html_url
