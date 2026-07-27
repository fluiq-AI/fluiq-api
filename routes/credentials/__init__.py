"""fluiq-api  —  /api/v1/credentials

Customer-supplied provider keys (BYOK), used to run an org's judge calls on its
own provider account.

GET    /credentials              — list this org's credentials (never the key)
POST   /credentials              — add or replace a key (JWT auth)
POST   /credentials/{id}/verify  — re-check a key against the provider
DELETE /credentials/{id}         — permanently delete a key

The response models here deliberately have no field that could carry key
material. There is no "reveal" endpoint, for any role: once a key is submitted
the only things readable back are the provider, an optional label, and the last
four characters. See ``db_queues/postgresql/credentials.py``.

Keys are verified against the provider before being accepted, using a
list-models call — authenticated, but free and token-less, so a typo is caught
at paste time rather than surfacing as a failed eval hours later. The provider
URLs are module constants, never built from user input, so this cannot be
steered into an SSRF
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from db_queues.postgresql.credentials import (
    VALID_PROVIDERS,
    delete_credential,
    list_credentials,
    mark_invalid,
    mark_verified,
    store_credential,
    unseal_by_id,
)
from routes.auth.helper import get_current_session
from shared import crypto

logger = logging.getLogger(__name__)

credentials_router = APIRouter()

_VERIFY_TIMEOUT_SECONDS = 10.0

# Cheapest authenticated call per provider that proves a key works without
# spending tokens. Hardcoded — never interpolated from request data.
_VERIFY_ENDPOINTS: dict[str, tuple[str, dict]] = {
    "openai":    ("https://api.openai.com/v1/models", {}),
    "anthropic": ("https://api.anthropic.com/v1/models", {"anthropic-version": "2023-06-01"}),
    "gemini":    ("https://generativelanguage.googleapis.com/v1beta/models", {}),
    "moonshot":  ("https://api.moonshot.ai/v1/models", {}),
}


# ── Schemas ──────────────────────────────────────────────────────────────────

class CredentialCreate(BaseModel):
    provider: str
    api_key:  str = Field(min_length=8)
    label:    Optional[str] = Field(default=None, max_length=120)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _require_backend() -> None:
    """Fail loudly when encryption is unavailable rather than storing anything."""
    if not crypto.is_configured():
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Credential storage is not configured on this deployment.",
        )


async def _verify_with_provider(provider: str, api_key: str) -> Optional[str]:
    """Return None if the key works, else a short human-readable reason.

    Providers we can't check generically (Azure needs a deployment endpoint,
    Bedrock uses SigV4 rather than a bearer key) are accepted unverified — the
    alternative is refusing keys that are perfectly valid.
    """
    entry = _VERIFY_ENDPOINTS.get(provider)
    if entry is None:
        return None

    url, extra_headers = entry
    headers = dict(extra_headers)
    params = {}
    if provider == "anthropic":
        headers["x-api-key"] = api_key
    elif provider == "gemini":
        # Gemini takes the key as a query param rather than a header.
        params["key"] = api_key
    else:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        async with httpx.AsyncClient(timeout=_VERIFY_TIMEOUT_SECONDS) as client:
            resp = await client.get(url, headers=headers, params=params)
    except httpx.RequestError:
        # Network trouble on our side is not the customer's key being wrong.
        # Accept it and let the first real judge call settle the question.
        logger.warning("[CREDENTIALS] verification unreachable provider=%s", provider)
        return None

    if resp.status_code in (200, 201):
        return None
    if resp.status_code in (401, 403):
        return "Provider rejected the key (unauthorized)."
    if resp.status_code == 429:
        return None  # rate-limited means the key authenticated fine
    return f"Provider returned HTTP {resp.status_code}."


# ── Routes ───────────────────────────────────────────────────────────────────

# Providers the platform holds a managed judge key for — the env var names the
# evaluator's judge reads (jobs/helper/judge.py). A model on one of these runs
# without the org saving its own key; anything else needs BYOK. Set the same
# managed keys on the API for this hint to be accurate.
_MANAGED_ENV: dict[str, list[str]] = {
    "anthropic": ["ANTHROPIC_API_KEY"],
    "openai":    ["OPENAI_API_KEY"],
    "gemini":    ["GEMINI_API_KEY", "GOOGLE_API_KEY"],
    "moonshot":  ["MOONSHOT_API_KEY"],
}


def _managed_providers() -> List[str]:
    return sorted(
        p for p, envs in _MANAGED_ENV.items() if any(os.getenv(e) for e in envs)
    )


@credentials_router.get("/credentials")
async def read_credentials(session: dict = Depends(get_current_session)) -> dict:
    org_id = uuid.UUID(session["org_id"])
    creds = await list_credentials(org_id)
    return {
        "configured":        crypto.is_configured(),
        "providers":         sorted(VALID_PROVIDERS),
        # Providers usable without the org saving a key (managed platform key).
        "managed_providers": _managed_providers(),
        "credentials":       [c.to_dict() for c in creds],
    }


@credentials_router.post("/credentials", status_code=status.HTTP_201_CREATED)
async def create_credential(
    payload: CredentialCreate,
    session: dict = Depends(get_current_session),
) -> dict:
    _require_backend()
    org_id = uuid.UUID(session["org_id"])

    provider = (payload.provider or "").strip().lower()
    if provider not in VALID_PROVIDERS:
        raise HTTPException(422, f"provider must be one of {sorted(VALID_PROVIDERS)}")

    api_key = (payload.api_key or "").strip()
    if not api_key:
        raise HTTPException(422, "api_key is empty")

    reason = await _verify_with_provider(provider, api_key)
    if reason is not None:
        # 422 not 401 — the caller's session is fine, the key they pasted isn't.
        raise HTTPException(422, reason)

    try:
        summary = await store_credential(org_id, provider, api_key, payload.label)
    except crypto.CredentialEncryptionUnavailable:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Credential storage is not configured on this deployment.",
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc))

    await mark_verified(org_id, uuid.UUID(summary.credential_id))
    summary.status = "active"
    return summary.to_dict()


@credentials_router.post("/credentials/{credential_id}/verify")
async def verify_credential(
    credential_id: uuid.UUID,
    session: dict = Depends(get_current_session),
) -> dict:
    """Re-check a stored key. Flips status to active or invalid."""
    _require_backend()
    org_id = uuid.UUID(session["org_id"])

    creds = {c.credential_id: c for c in await list_credentials(org_id)}
    target = creds.get(str(credential_id))
    if target is None:
        raise HTTPException(404, "Credential not found.")

    # Addressed by id, not by provider: an org can hold several keys for one
    # provider, and "the active one" would verify the wrong row.
    plaintext = await unseal_by_id(org_id, credential_id)
    if plaintext is None:
        raise HTTPException(409, "Credential could not be read for verification.")

    reason = await _verify_with_provider(target.provider, plaintext)
    del plaintext

    if reason is None:
        await mark_verified(org_id, credential_id)
        return {"credential_id": str(credential_id), "status": "active", "error": None}

    await mark_invalid(org_id, credential_id, reason)
    return {"credential_id": str(credential_id), "status": "invalid", "error": reason}


@credentials_router.delete("/credentials/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_credential(
    credential_id: uuid.UUID,
    session: dict = Depends(get_current_session),
) -> None:
    org_id = uuid.UUID(session["org_id"])
    deleted = await delete_credential(org_id, credential_id)
    if not deleted:
        raise HTTPException(404, "Credential not found.")
