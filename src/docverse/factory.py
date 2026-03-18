"""Factory for creating Docverse service objects."""

from __future__ import annotations

from abc import ABC, abstractmethod

import structlog
from safir.arq import ArqQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session

from .services.authorization import AuthorizationService
from .services.build import BuildService
from .services.credential import CredentialService
from .services.credential_encryptor import CredentialEncryptor
from .services.edition import EditionService
from .services.organization import OrganizationService
from .services.project import ProjectService
from .storage.build_store import BuildStore
from .storage.edition_store import EditionStore
from .storage.membership_store import OrgMembershipStore
from .storage.objectstore_factory import create_objectstore
from .storage.organization_credential_store import OrganizationCredentialStore
from .storage.organization_store import OrganizationStore
from .storage.project_store import ProjectStore
from .storage.queue_backend import (
    ArqQueueBackend,
    NullQueueBackend,
    QueueBackend,
)
from .storage.queue_job_store import QueueJobStore
from .storage.s3_objectstore import S3ObjectStore
from .storage.user_info_store import UserInfoStore


class Factory(ABC):
    """Build Docverse service objects."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
        credential_encryptor: CredentialEncryptor | None = None,
    ) -> None:
        self._session = session
        self._logger = logger
        self._credential_encryptor = credential_encryptor

    def set_logger(self, logger: structlog.stdlib.BoundLogger) -> None:
        """Set the logger for the factory."""
        self._logger = logger

    @abstractmethod
    def _create_queue_backend(self) -> QueueBackend: ...

    def _create_org_store(self) -> OrganizationStore:
        return OrganizationStore(session=self._session, logger=self._logger)

    def _create_project_store(self) -> ProjectStore:
        return ProjectStore(session=self._session, logger=self._logger)

    def create_organization_service(self) -> OrganizationService:
        """Create an OrganizationService."""
        store = self._create_org_store()
        return OrganizationService(store=store, logger=self._logger)

    def create_project_service(self) -> ProjectService:
        """Create a ProjectService."""
        store = self._create_project_store()
        org_store = self._create_org_store()
        return ProjectService(
            store=store, org_store=org_store, logger=self._logger
        )

    def create_build_service(self) -> BuildService:
        """Create a BuildService."""
        store = BuildStore(session=self._session, logger=self._logger)
        org_store = self._create_org_store()
        project_store = self._create_project_store()
        queue_backend = self._create_queue_backend()
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

    def create_edition_service(self) -> EditionService:
        """Create an EditionService."""
        store = EditionStore(session=self._session, logger=self._logger)
        org_store = self._create_org_store()
        project_store = self._create_project_store()
        return EditionService(
            store=store,
            org_store=org_store,
            project_store=project_store,
            logger=self._logger,
        )

    def create_authorization_service(self) -> AuthorizationService:
        """Create an AuthorizationService."""
        membership_store = OrgMembershipStore(
            session=self._session, logger=self._logger
        )
        return AuthorizationService(
            membership_store=membership_store, logger=self._logger
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
            encryptor=self._credential_encryptor,
            logger=self._logger,
        )

    async def create_objectstore_for_org(
        self, *, org_id: int, credential_label: str
    ) -> S3ObjectStore:
        """Resolve an org's ObjectStore from its credential.

        Parameters
        ----------
        org_id
            Organization ID.
        credential_label
            Credential label to use.

        Returns
        -------
        S3ObjectStore
            An unopened S3ObjectStore. Caller must use as async context
            manager.
        """
        credential_service = self.create_credential_service()
        cred, payload = await credential_service.get_decrypted(
            org_id=org_id, label=credential_label
        )
        return create_objectstore(
            service_type=cred.service_type, credential=payload
        )


class HandlerFactory(Factory):
    """Factory for request handlers with arq queue and user info."""

    def __init__(
        self,
        session: async_scoped_session[AsyncSession],
        logger: structlog.stdlib.BoundLogger,
        arq_queue: ArqQueue,
        user_info_store: UserInfoStore,
        credential_encryptor: CredentialEncryptor | None = None,
    ) -> None:
        super().__init__(
            session=session,
            logger=logger,
            credential_encryptor=credential_encryptor,
        )
        self._arq_queue = arq_queue
        self._user_info_store = user_info_store

    def _create_queue_backend(self) -> ArqQueueBackend:
        return ArqQueueBackend(arq_queue=self._arq_queue)

    def create_queue_backend(self) -> ArqQueueBackend:
        """Create an ArqQueueBackend."""
        return ArqQueueBackend(arq_queue=self._arq_queue)

    def get_user_info_store(self) -> UserInfoStore:
        """Get the UserInfoStore instance."""
        return self._user_info_store


class WorkerFactory(Factory):
    """Factory for worker functions using a null queue backend."""

    def _create_queue_backend(self) -> NullQueueBackend:
        return NullQueueBackend()
