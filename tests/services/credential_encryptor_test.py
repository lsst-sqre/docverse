"""Tests for the CredentialEncryptor."""

from __future__ import annotations

from cryptography.fernet import Fernet

from docverse.services.credential_encryptor import CredentialEncryptor


def test_encrypt_decrypt_roundtrip() -> None:
    key = Fernet.generate_key().decode()
    encryptor = CredentialEncryptor(current_key=key)

    plaintext = b'{"access_key_id": "AKIA...", "secret": "s3cr3t"}'
    token = encryptor.encrypt(plaintext)
    assert encryptor.decrypt(token) == plaintext


def test_decrypt_with_retired_key() -> None:
    old_key = Fernet.generate_key().decode()
    new_key = Fernet.generate_key().decode()

    # Encrypt with old key
    old_encryptor = CredentialEncryptor(current_key=old_key)
    token = old_encryptor.encrypt(b"secret-data")

    # Decrypt with new encryptor that has old key as retired
    new_encryptor = CredentialEncryptor(
        current_key=new_key, retired_key=old_key
    )
    assert new_encryptor.decrypt(token) == b"secret-data"


def test_rotate_re_encrypts_under_current_key() -> None:
    old_key = Fernet.generate_key().decode()
    new_key = Fernet.generate_key().decode()

    # Encrypt with old key
    old_encryptor = CredentialEncryptor(current_key=old_key)
    old_token = old_encryptor.encrypt(b"rotate-me")

    # Rotate with new encryptor
    new_encryptor = CredentialEncryptor(
        current_key=new_key, retired_key=old_key
    )
    rotated_token = new_encryptor.rotate(old_token)

    # The rotated token should be decryptable with just the new key
    current_only = CredentialEncryptor(current_key=new_key)
    assert current_only.decrypt(rotated_token) == b"rotate-me"


def test_encrypt_produces_different_tokens() -> None:
    key = Fernet.generate_key().decode()
    encryptor = CredentialEncryptor(current_key=key)

    token1 = encryptor.encrypt(b"same-data")
    token2 = encryptor.encrypt(b"same-data")
    # Fernet includes a timestamp, so tokens should differ
    assert token1 != token2
