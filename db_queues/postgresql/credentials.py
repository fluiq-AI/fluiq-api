"""Customer provider credentials (BYOK) — encrypted at rest, write-only in the API.

Rules this module exists to enforce:

* **Nothing here ever returns plaintext to a caller that renders it.**
  ``list_credentials`` returns :class:`CredentialSummary` (provider, label,
  last4, fingerprint, status) and there is no "reveal" query. A compromised
  admin session cannot harvest customer provider keys, because the read path
  to do so does not exist.
* **Plaintext is sealed before it reaches SQL.** The pool only ever sees
  ciphertext, so a query log or a statement-level trace cannot leak a key.
* **Deleting means deleting.** Revocation is a hard row delete, not a status
  flag — for a credential, a soft delete leaves recoverable key material.

``unseal_active`` is the one function that yields plaintext. It is for building
a provider client and nothing else: use the value immediately, never persist,
log, attach to an exception message, or put it on a Kafka payload (the eval
topic in particular is PLAINTEXT on the wire).
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Optional

import config
from shared import crypto
from . import postgres_client

logger = logging.getLogger(__name__)

_TABLE = config.POSTGRES_CREDENTIALS_TABLE

# Derived from the single provider registry (shared/providers.py) rather than
# restated here — the two drifting is how a key gets stored for a provider
# the judge then refuses to route.
from shared.providers import VALID_PROVIDERS  # noqa: F401


@dataclass
class CredentialSummary:
    """Everything the API is willing to say about a stored credential."""
    credential_id:    str
    provider:         str
    label:            Optional[str]
    last4:            str
    fingerprint:      str
    status:           str
    last_verified_at: Optional[Any] = None
    last_error:       Optional[str] = None
    created_at:       Optional[Any] = None

    def to_dict(self) -> dict:
        return {
            "credential_id":    self.credential_id,
            "provider":         self.provider,
            "label":            self.label,
            # Display only — enough to tell two keys apart, useless to an attacker.
            "key_preview":      f"…{self.last4}" if self.last4 else "",
            "fingerprint":      self.fingerprint,
            "status":           self.status,
            "last_verified_at": self.last_verified_at.isoformat() if self.last_verified_at else None,
            "last_error":       self.last_error,
            "created_at":       self.created_at.isoformat() if self.created_at else None,
        }


def _row_to_summary(row) -> CredentialSummary:
    return CredentialSummary(
        credential_id=str(row["credential_id"]),
        provider=row["provider"],
        label=row["label"],
        last4=row["last4"],
        fingerprint=row["fingerprint"],
        status=row["status"],
        last_verified_at=row["last_verified_at"],
        last_error=row["last_error"],
        created_at=row["created_at"],
    )


async def store_credential(
    org_id: uuid.UUID,
    provider: str,
    plaintext: str,
    label: Optional[str] = None,
) -> CredentialSummary:
    """Seal and persist a provider key. Re-pasting the same key updates in place.

    Raises ``ValueError`` for an unknown provider or empty secret, and
    ``crypto.CredentialEncryptionUnavailable`` when no encryption backend is
    configured — never falls back to storing the secret in the clear.
    """
    provider = (provider or "").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise ValueError(f"unsupported provider: {provider!r}")

    plaintext = (plaintext or "").strip()
    if not plaintext:
        raise ValueError("credential is empty")

    sealed = crypto.seal(plaintext, org_id=str(org_id))
    fp = crypto.fingerprint(plaintext)
    tail = crypto.last4(plaintext)
    # Drop the local reference promptly; the row below only carries ciphertext.
    del plaintext

    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {_TABLE}
                (org_id, provider, label, ciphertext, nonce, wrapped_dek,
                 key_version, last4, fingerprint, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'active')
            ON CONFLICT (org_id, provider, fingerprint) DO UPDATE SET
                label       = EXCLUDED.label,
                ciphertext  = EXCLUDED.ciphertext,
                nonce       = EXCLUDED.nonce,
                wrapped_dek = EXCLUDED.wrapped_dek,
                key_version = EXCLUDED.key_version,
                status      = 'active',
                last_error  = NULL,
                updated_at  = NOW()
            RETURNING credential_id, provider, label, last4, fingerprint,
                      status, last_verified_at, last_error, created_at
            """,
            org_id, provider, label, sealed.ciphertext, sealed.nonce,
            sealed.wrapped_dek, sealed.key_version, tail, fp,
        )
    logger.info(
        "[CREDENTIALS] stored org=%s provider=%s fingerprint=%s", org_id, provider, fp,
    )
    return _row_to_summary(row)


async def list_credentials(org_id: uuid.UUID) -> list[CredentialSummary]:
    """All credentials for an org. Never includes key material."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT credential_id, provider, label, last4, fingerprint,
                   status, last_verified_at, last_error, created_at
            FROM {_TABLE}
            WHERE org_id = $1
            ORDER BY provider, created_at DESC
            """,
            org_id,
        )
    return [_row_to_summary(r) for r in rows]


async def delete_credential(org_id: uuid.UUID, credential_id: uuid.UUID) -> bool:
    """Hard-delete a credential. ``org_id`` is in the predicate, not just checked."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"DELETE FROM {_TABLE} WHERE org_id = $1 AND credential_id = $2 "
            f"RETURNING credential_id",
            org_id, credential_id,
        )
    if row is not None:
        # The wrapped DEK is gone from the DB, but its unwrapped form may still
        # be sitting in the process cache. Revocation has to be immediate.
        crypto.clear_dek_cache()
        logger.info("[CREDENTIALS] deleted org=%s credential=%s", org_id, credential_id)
    return row is not None


async def mark_invalid(
    org_id: uuid.UUID, credential_id: uuid.UUID, error: str,
) -> None:
    """Flag a credential the provider rejected.

    Deliberately does not delete: the customer needs to see *which* key broke in
    order to rotate it. Callers must not silently fall back to a Fluiq-managed
    key when this fires — that bills us for usage sold as BYOK and hides the
    breakage from the customer.
    """
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"UPDATE {_TABLE} SET status = 'invalid', last_error = $3, updated_at = NOW() "
            f"WHERE org_id = $1 AND credential_id = $2",
            org_id, credential_id, (error or "")[:500],
        )
    logger.warning(
        "[CREDENTIALS] marked invalid org=%s credential=%s", org_id, credential_id,
    )


async def mark_verified(org_id: uuid.UUID, credential_id: uuid.UUID) -> None:
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"UPDATE {_TABLE} SET status = 'active', last_error = NULL, "
            f"last_verified_at = NOW(), updated_at = NOW() "
            f"WHERE org_id = $1 AND credential_id = $2",
            org_id, credential_id,
        )


async def unseal_by_id(
    org_id: uuid.UUID, credential_id: uuid.UUID,
) -> Optional[str]:
    """Plaintext for one specific credential, whatever its status.

    Separate from :func:`unseal_active` because re-verifying a key must address
    it by id: an org can hold several keys for one provider, and "the active
    one" would resolve the wrong row. Accepts non-active rows so a credential
    marked invalid can be re-checked after the customer fixes it upstream.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT ciphertext, nonce, wrapped_dek, key_version
            FROM {_TABLE}
            WHERE org_id = $1 AND credential_id = $2
            """,
            org_id, credential_id,
        )
    if row is None:
        return None

    sealed = crypto.SealedSecret(
        ciphertext=bytes(row["ciphertext"]),
        nonce=bytes(row["nonce"]),
        wrapped_dek=bytes(row["wrapped_dek"]),
        key_version=row["key_version"],
    )
    try:
        return crypto.unseal(sealed, org_id=str(org_id))
    except Exception:
        logger.exception(
            "[CREDENTIALS] unseal failed org=%s credential=%s", org_id, credential_id,
        )
        return None


async def unseal_active(
    org_id: uuid.UUID, provider: str,
) -> Optional[tuple[uuid.UUID, str]]:
    """Return ``(credential_id, plaintext)`` for an org's active key, or None.

    The only plaintext-yielding path in this module. Use the secret to build a
    provider client and let it go — do not store it, log it, or forward it.
    ``None`` means "this org has no usable key for this provider", which callers
    should treat as a hard stop for BYOK work, not as a cue to fall back.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT credential_id, ciphertext, nonce, wrapped_dek, key_version
            FROM {_TABLE}
            WHERE org_id = $1 AND provider = $2 AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            org_id, provider,
        )
    if row is None:
        return None

    sealed = crypto.SealedSecret(
        ciphertext=bytes(row["ciphertext"]),
        nonce=bytes(row["nonce"]),
        wrapped_dek=bytes(row["wrapped_dek"]),
        key_version=row["key_version"],
    )
    try:
        return row["credential_id"], crypto.unseal(sealed, org_id=str(org_id))
    except Exception:
        # Never let the decrypt failure carry key material or ciphertext into
        # the log. A tag failure here means the row was written under a
        # different org scope or a rotated CMK — both are alarming.
        logger.exception(
            "[CREDENTIALS] unseal failed org=%s provider=%s credential=%s",
            org_id, provider, row["credential_id"],
        )
        return None
