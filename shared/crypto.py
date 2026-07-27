"""Envelope encryption for customer-supplied provider credentials (BYOK).

Fluiq's own API keys are stored as SHA-256 hashes (``postgresql/auth.py``) —
we only ever need to *compare* them. Provider keys are different: to call
OpenAI or Anthropic on a customer's behalf we must recover the plaintext, so
they need reversible encryption with a key that does not live in the database.

Envelope scheme, per credential:

    KMS GenerateDataKey  ->  plaintext DEK + KMS-wrapped DEK
    AES-256-GCM(plaintext, DEK, nonce, aad=org scope)  ->  ciphertext
    persist: ciphertext + nonce + wrapped DEK      (plaintext DEK is discarded)

A fresh DEK per credential means a nonce is never reused across secrets. The
plaintext DEK is never written anywhere; recovering a credential requires both
the database row *and* ``kms:Decrypt`` on the CMK, so a leaked RDS snapshot is
inert on its own. Every unwrap is an authenticated KMS call, which CloudTrail
records — that is the credential-access audit log, for free.

The org scope is bound in as GCM *additional authenticated data*. Ciphertext
lifted from one org's row cannot be decrypted under another org's scope: the
tag check fails. A tenancy bug becomes a hard crypto failure instead of a
silent cross-tenant read.

Backends
--------
``kms`` (default, production) resolves credentials through boto3's default
chain — the ECS task role, no static keys. ``local`` exists so the feature can
be developed and unit-tested without AWS; it wraps DEKs under a static key from
the environment and refuses to run unless explicitly selected.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import secrets
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import config

logger = logging.getLogger(__name__)

# Bumped only if the scheme itself changes (cipher, AAD shape, wrapping). Stored
# per row so a future re-wrap job can find rows written under an older scheme.
KEY_VERSION = 1

_NONCE_BYTES = 12   # GCM standard; 96-bit nonces are the only size AESGCM accepts natively
_DEK_BYTES = 32     # AES-256

# Unwrapped DEKs are cached in process to keep KMS off the hot path — an eval
# batch would otherwise make one kms:Decrypt per judge call. Bounded and
# short-lived: this is plaintext key material in memory, so it should survive a
# burst, not a deploy.
_DEK_CACHE_TTL_SECONDS = 300.0
_DEK_CACHE_MAX = 512


class CredentialEncryptionUnavailable(RuntimeError):
    """Raised when the encryption backend is not configured.

    Callers must treat this as "the BYOK feature is off", never as a reason to
    fall back to storing the secret unencrypted.
    """


@dataclass(frozen=True)
class SealedSecret:
    """An encrypted credential. Safe to persist; safe to log (no key material)."""
    ciphertext: bytes
    nonce: bytes
    wrapped_dek: bytes
    key_version: int = KEY_VERSION


# ── helpers ──────────────────────────────────────────────────────────────────

def fingerprint(plaintext: str) -> str:
    """Stable non-reversible id for a secret.

    Used to dedupe (an org pasting the same key twice) and to scope caches that
    must not be shared across credentials. Truncated to 16 hex chars — enough to
    make collisions irrelevant at per-org scale, short enough to index.
    """
    return hashlib.sha256(plaintext.encode()).hexdigest()[:16]


def last4(plaintext: str) -> str:
    """Display suffix, e.g. ``…a1B2``. The only part of a key we ever return."""
    return plaintext[-4:] if len(plaintext) >= 4 else ""


def _aad(org_id: str, key_version: int) -> bytes:
    """Additional authenticated data: binds ciphertext to one org and scheme."""
    return f"fluiq:credential:v{key_version}:{org_id}".encode()


def _aesgcm(key: bytes):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    return AESGCM(key)


# ── DEK cache ────────────────────────────────────────────────────────────────

_dek_cache: dict[str, tuple[bytes, float]] = {}
_dek_lock = threading.Lock()


def _dek_cache_get(wrapped: bytes) -> Optional[bytes]:
    key = hashlib.sha256(wrapped).hexdigest()
    with _dek_lock:
        hit = _dek_cache.get(key)
        if hit is None:
            return None
        dek, expires_at = hit
        if time.monotonic() > expires_at:
            _dek_cache.pop(key, None)
            return None
        return dek


def _dek_cache_put(wrapped: bytes, dek: bytes) -> None:
    key = hashlib.sha256(wrapped).hexdigest()
    with _dek_lock:
        if len(_dek_cache) >= _DEK_CACHE_MAX:
            # Cheap eviction: drop whatever is oldest by expiry.
            oldest = min(_dek_cache, key=lambda k: _dek_cache[k][1])
            _dek_cache.pop(oldest, None)
        _dek_cache[key] = (dek, time.monotonic() + _DEK_CACHE_TTL_SECONDS)


def clear_dek_cache() -> None:
    """Drop cached key material (tests, credential revocation, key rotation)."""
    with _dek_lock:
        _dek_cache.clear()


# ── backends ─────────────────────────────────────────────────────────────────

def _backend() -> str:
    return (getattr(config, "CREDENTIAL_ENCRYPTION_BACKEND", "kms") or "kms").lower()


@lru_cache(maxsize=1)
def _kms_client():
    import boto3
    return boto3.client("kms", region_name=config.AWS_REGION)


@lru_cache(maxsize=1)
def _local_key() -> bytes:
    raw = getattr(config, "CREDENTIAL_ENCRYPTION_LOCAL_KEY", None)
    if not raw:
        raise CredentialEncryptionUnavailable(
            "CREDENTIAL_ENCRYPTION_BACKEND=local requires CREDENTIAL_ENCRYPTION_LOCAL_KEY "
            "(base64-encoded 32 bytes)"
        )
    try:
        key = base64.b64decode(raw, validate=True)
    except Exception as exc:
        raise CredentialEncryptionUnavailable(
            "CREDENTIAL_ENCRYPTION_LOCAL_KEY is not valid base64"
        ) from exc
    if len(key) != _DEK_BYTES:
        raise CredentialEncryptionUnavailable(
            f"CREDENTIAL_ENCRYPTION_LOCAL_KEY must decode to {_DEK_BYTES} bytes, got {len(key)}"
        )
    return key


def is_configured() -> bool:
    """Whether credentials can be sealed right now.

    Routes use this to disable the BYOK surface cleanly instead of failing
    mid-write and leaving a half-created credential.
    """
    try:
        if _backend() == "local":
            _local_key()
            return True
        return bool(getattr(config, "CREDENTIAL_KMS_KEY_ID", None))
    except CredentialEncryptionUnavailable:
        return False


def _generate_dek() -> tuple[bytes, bytes]:
    """Return ``(plaintext_dek, wrapped_dek)``."""
    if _backend() == "local":
        dek = secrets.token_bytes(_DEK_BYTES)
        nonce = secrets.token_bytes(_NONCE_BYTES)
        wrapped = nonce + _aesgcm(_local_key()).encrypt(nonce, dek, b"fluiq:dek")
        return dek, wrapped

    key_id = getattr(config, "CREDENTIAL_KMS_KEY_ID", None)
    if not key_id:
        raise CredentialEncryptionUnavailable("CREDENTIAL_KMS_KEY_ID is not configured")
    resp = _kms_client().generate_data_key(KeyId=key_id, KeySpec="AES_256")
    return resp["Plaintext"], resp["CiphertextBlob"]


def _unwrap_dek(wrapped: bytes) -> bytes:
    cached = _dek_cache_get(wrapped)
    if cached is not None:
        return cached

    if _backend() == "local":
        nonce, body = wrapped[:_NONCE_BYTES], wrapped[_NONCE_BYTES:]
        dek = _aesgcm(_local_key()).decrypt(nonce, body, b"fluiq:dek")
    else:
        key_id = getattr(config, "CREDENTIAL_KMS_KEY_ID", None)
        if not key_id:
            raise CredentialEncryptionUnavailable("CREDENTIAL_KMS_KEY_ID is not configured")
        # KeyId is passed explicitly so KMS verifies the blob was wrapped under
        # the CMK we expect, rather than trusting the key id embedded in it.
        dek = _kms_client().decrypt(CiphertextBlob=wrapped, KeyId=key_id)["Plaintext"]

    _dek_cache_put(wrapped, dek)
    return dek


# ── public API ───────────────────────────────────────────────────────────────

def seal(plaintext: str, *, org_id: str) -> SealedSecret:
    """Encrypt ``plaintext`` under a fresh DEK bound to ``org_id``."""
    if not plaintext:
        raise ValueError("refusing to seal an empty secret")
    dek, wrapped = _generate_dek()
    nonce = secrets.token_bytes(_NONCE_BYTES)
    ciphertext = _aesgcm(dek).encrypt(
        nonce, plaintext.encode(), _aad(str(org_id), KEY_VERSION)
    )
    return SealedSecret(
        ciphertext=ciphertext,
        nonce=nonce,
        wrapped_dek=wrapped,
        key_version=KEY_VERSION,
    )


def unseal(sealed: SealedSecret, *, org_id: str) -> str:
    """Recover the plaintext. Raises ``InvalidTag`` if ``org_id`` doesn't match.

    Callers must use the return value immediately and never persist, log, or
    attach it to a trace, an exception message, or a Kafka payload.
    """
    dek = _unwrap_dek(sealed.wrapped_dek)
    plaintext = _aesgcm(dek).decrypt(
        sealed.nonce, sealed.ciphertext, _aad(str(org_id), sealed.key_version)
    )
    return plaintext.decode()
