import hashlib
import os
import secrets
import uuid
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import config

from shared.model import ApiKeyCreated, OrganizationModel, UserModel, UserType

from . import postgres_client

API_KEY_PREFIX_LENGTH = 11



API_KEY_LIMITS: dict[str, int] = {
    "Free": 1,
    "Team": 5,
    "Growth": 15,
    "Enterprise": 50,
}


async def email_exists(email: str) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {config.config.POSTGRES_USER_TABLE} WHERE email = $1",
            email,
        )
        return row is not None


async def register_user(
    name: str,
    email: str,
    hashed_password: str,
    user_type: UserType = "Free",
) -> Optional[tuple[UserModel, OrganizationModel]]:
    """Create a default organization and a user atomically.

    Returns the new (UserModel, OrganizationModel) pair or None if the
    email is already registered.
    """
    user_id = uuid.uuid4()
    org_id = uuid.uuid4()
    org_name = f"{name}'s Organization"
    api_key_limit = API_KEY_LIMITS.get(user_type, 1)
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            try:
                org_row = await conn.fetchrow(
                    f"INSERT INTO {config.POSTGRES_ORG_TABLE} "
                    f"(org_id, name, user_id, api_key_limit, api_key_usage, created_at) "
                    f"VALUES ($1, $2, $3, $4, 0, NOW()) RETURNING *",
                    org_id, org_name, user_id, api_key_limit,
                )
                user_row = await conn.fetchrow(
                    f"INSERT INTO {config.POSTGRES_USER_TABLE} "
                    f"(user_id, email, hashed_password, name, user_type, org_id, created_at) "
                    f"VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING *",
                    user_id, email, hashed_password, name, user_type, org_id,
                )
            except asyncpg.UniqueViolationError:
                return None
    return UserModel(**dict(user_row)), OrganizationModel(**dict(org_row))


async def get_user_by_email(
    email: str,
) -> Optional[tuple[UserModel, OrganizationModel]]:
    """Look up a user (and their primary org) by email.

    Returns the (UserModel, OrganizationModel) pair or None if the email
    is not registered.
    """
    async with postgres_client.acquire() as conn:
        user_row = await conn.fetchrow(
            f"SELECT * FROM {config.POSTGRES_USER_TABLE} WHERE email = $1",
            email,
        )
        if user_row is None:
            return None
        org_row = await conn.fetchrow(
            f"SELECT * FROM {config.POSTGRES_ORG_TABLE} WHERE org_id = $1",
            user_row["org_id"],
        )
        if org_row is None:
            return None
    return UserModel(**dict(user_row)), OrganizationModel(**dict(org_row))


async def revoke_refresh_token(jti: str, expires_at: datetime) -> None:
    """Insert a refresh token's jti into the revocation list (idempotent)."""
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {config.POSTGRES_REVOKED_TOKEN_TABLE} (jti, expires_at) "
            f"VALUES ($1, $2) ON CONFLICT (jti) DO NOTHING",
            uuid.UUID(str(jti)), expires_at,
        )


async def is_refresh_token_revoked(jti: str) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {config.POSTGRES_REVOKED_TOKEN_TABLE} WHERE jti = $1",
            uuid.UUID(str(jti)),
        )
        return row is not None


async def get_organization(org_id: uuid.UUID) -> Optional[OrganizationModel]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {config.POSTGRES_ORG_TABLE} WHERE org_id = $1",
            org_id,
        )
    if row is None:
        return None
    return OrganizationModel(**dict(row))


async def get_org_tier(org_id: uuid.UUID) -> Optional[str]:
    """Return the tier (`Free` / `Team` / `Growth` / `Enterprise`) for an org.

    The tier is read from the org owner's ``users.user_type`` row. Returns
    ``None`` if the org or its owner cannot be found.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT u.user_type "
            f"FROM {config.POSTGRES_ORG_TABLE} o "
            f"JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = o.user_id "
            f"WHERE o.org_id = $1",
            org_id,
        )
    if row is None:
        return None
    return row["user_type"]


async def resolve_api_key(
    plaintext: str,
) -> Optional[tuple[uuid.UUID, str, uuid.UUID]]:
    """Resolve a plaintext api key to (org_id, prefix, key_id).

    Returns None if the key is unknown. Matching is done against the
    SHA-256 hash stored in `organizations.api_keys[].hashed_key`; the
    plaintext key itself is never compared or persisted.
    """
    if not plaintext:
        return None
    hashed = _hash_api_key(plaintext)
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT org_id, elem->>'prefix' AS prefix, elem->>'key_id' AS key_id "
            f"FROM {config.POSTGRES_ORG_TABLE}, jsonb_array_elements(api_keys) elem "
            f"WHERE elem->>'hashed_key' = $1 LIMIT 1",
            hashed,
        )
    if row is None:
        return None
    return row["org_id"], row["prefix"], uuid.UUID(row["key_id"])


def _generate_api_key() -> str:
    return f"fl_{secrets.token_urlsafe(32)}"


def _hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


async def create_api_key(
    org_id: uuid.UUID, name: str
) -> Optional[ApiKeyCreated]:
    """Append a new API key to the organization and bump usage.

    The plaintext key is returned exactly once; only its SHA-256 hash plus a
    short prefix are persisted. Returns None if the org would exceed its
    limit or does not exist.
    """
    plaintext = _generate_api_key()
    prefix = plaintext[:API_KEY_PREFIX_LENGTH]
    created_at = datetime.now(timezone.utc)
    key_id = uuid.uuid4()
    entry = {
        "key_id": str(key_id),
        "name": name,
        "prefix": prefix,
        "hashed_key": _hash_api_key(plaintext),
        "created_at": created_at.isoformat(),
    }
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"UPDATE {config.POSTGRES_ORG_TABLE} "
                f"SET api_keys = api_keys || $2::jsonb, "
                f"    api_key_usage = api_key_usage + 1, "
                f"    updated_at = NOW() "
                f"WHERE org_id = $1 AND api_key_usage < api_key_limit "
                f"RETURNING org_id",
                org_id, entry,
            )
    if row is None:
        return None
    return ApiKeyCreated(
        key_id=key_id,
        name=name,
        prefix=prefix,
        key=plaintext,
        created_at=created_at,
    )


async def delete_api_key(org_id: uuid.UUID, key_id: uuid.UUID) -> bool:
    """Remove an API key from the organization. Returns True if removed."""
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"UPDATE {config.POSTGRES_ORG_TABLE} "
                f"SET api_keys = COALESCE(("
                f"        SELECT jsonb_agg(elem) "
                f"        FROM jsonb_array_elements(api_keys) elem "
                f"        WHERE elem->>'key_id' <> $2"
                f"    ), '[]'::jsonb), "
                f"    api_key_usage = GREATEST(api_key_usage - 1, 0), "
                f"    updated_at = NOW() "
                f"WHERE org_id = $1 "
                f"  AND EXISTS ("
                f"        SELECT 1 FROM jsonb_array_elements(api_keys) elem "
                f"        WHERE elem->>'key_id' = $2"
                f"    ) "
                f"RETURNING org_id",
                org_id, str(key_id),
            )
    return row is not None


__all__ = [
    "email_exists",
    "register_user",
    "get_user_by_email",
    "revoke_refresh_token",
    "is_refresh_token_revoked",
    "get_organization",
    "get_org_tier",
    "resolve_api_key",
    "create_api_key",
    "delete_api_key",
    "config.POSTGRES_USER_TABLE",
    "config.POSTGRES_ORG_TABLE",
    "config.POSTGRES_REVOKED_TOKEN_TABLE",
    "API_KEY_LIMITS",
]
