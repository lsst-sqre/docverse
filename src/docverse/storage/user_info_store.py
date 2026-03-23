"""User info store for resolving user group memberships."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = [
    "StubUserInfoStore",
    "UserInfoStore",
]


@runtime_checkable
class UserInfoStore(Protocol):
    """Protocol for resolving user group memberships and token scopes.

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

    async def get_scopes(self, token: str) -> list[str]:
        """Get the scopes associated with a token.

        Parameters
        ----------
        token
            Authentication token for the user.

        Returns
        -------
        list of str
            Scope names associated with the token.
        """
        ...


class StubUserInfoStore:
    """Stub implementation for development and testing."""

    def __init__(
        self,
        groups: list[str] | None = None,
        scopes: list[str] | None = None,
    ) -> None:
        self._groups = groups or []
        self._scopes = scopes or []

    async def get_groups(self, token: str) -> list[str]:  # noqa: ARG002
        """Return pre-configured groups."""
        return self._groups

    async def get_scopes(self, token: str) -> list[str]:  # noqa: ARG002
        """Return pre-configured scopes."""
        return self._scopes
