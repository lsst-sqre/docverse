"""User info store for resolving user group memberships."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from rubin.gafaelfawr import GafaelfawrClient

__all__ = [
    "GafaelfawrUserInfoStore",
    "StubUserInfoStore",
    "UserInfoStore",
]


@runtime_checkable
class UserInfoStore(Protocol):
    """Protocol for resolving user group memberships.

    The production implementation will use the Gafaelfawr client
    (https://gafaelfawr.lsst.io/user-guide/client.html).
    """

    async def get_groups(self, token: str) -> list[str]:
        """Get the group memberships for a user.

        Parameters
        ----------
        token
            Authentication token for the user.

        Returns
        -------
        list of str
            Group names the user belongs to.
        """
        ...


class GafaelfawrUserInfoStore:
    """Resolve user groups via the Gafaelfawr API."""

    def __init__(self, gafaelfawr_client: GafaelfawrClient) -> None:
        self._client = gafaelfawr_client

    async def get_groups(self, token: str) -> list[str]:
        """Get the group memberships for a user.

        Parameters
        ----------
        token
            Authentication token for the user.

        Returns
        -------
        list of str
            Group names the user belongs to.
        """
        user_info = await self._client.get_user_info(token)
        return [g.name for g in user_info.groups]


class StubUserInfoStore:
    """Stub implementation for development and testing."""

    def __init__(
        self,
        groups: list[str] | None = None,
    ) -> None:
        self._groups = groups or []

    async def get_groups(self, token: str) -> list[str]:  # noqa: ARG002
        """Return pre-configured groups."""
        return self._groups
