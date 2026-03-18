"""Credential encryption using Fernet with key rotation support."""

from __future__ import annotations

from cryptography.fernet import Fernet, MultiFernet

__all__ = ["CredentialEncryptor"]


class CredentialEncryptor:
    """Encrypt and decrypt credential blobs using Fernet.

    Supports key rotation: when a retired key is provided, decryption
    and rotation will accept tokens encrypted with either the current
    or retired key, but new encryptions always use the current key.

    Parameters
    ----------
    current_key
        The active Fernet key (base64url-encoded 32-byte key).
    retired_key
        An optional previous key kept for decryption during rotation.
    """

    def __init__(
        self, *, current_key: str, retired_key: str | None = None
    ) -> None:
        keys = [Fernet(current_key)]
        if retired_key is not None:
            keys.append(Fernet(retired_key))
        self._fernet = MultiFernet(keys)
        self._current = keys[0]

    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt plaintext and return a Fernet token.

        Parameters
        ----------
        plaintext
            The data to encrypt.

        Returns
        -------
        bytes
            The Fernet token (URL-safe base64-encoded).
        """
        return self._current.encrypt(plaintext)

    def decrypt(self, token: bytes) -> bytes:
        """Decrypt a Fernet token.

        Accepts tokens encrypted with either the current or retired key.

        Parameters
        ----------
        token
            The Fernet token to decrypt.

        Returns
        -------
        bytes
            The original plaintext.
        """
        return self._fernet.decrypt(token)

    def rotate(self, token: bytes) -> bytes:
        """Re-encrypt a token under the current key.

        If the token was encrypted with the retired key, it is decrypted
        and re-encrypted with the current key. If already encrypted with
        the current key, a fresh token is returned.

        Parameters
        ----------
        token
            The Fernet token to rotate.

        Returns
        -------
        bytes
            A new Fernet token encrypted with the current key.
        """
        return self._fernet.rotate(token)
