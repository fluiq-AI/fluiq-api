import os
import uuid
from datetime import datetime
from typing import Optional

import asyncpg
from dotenv import load_dotenv

from shared.model import OrganizationModel, UserModel, UserType

from . import postgres_client

load_dotenv()

POSTGRES_USER_TABLE = os.getenv("POSTGRES_USER_TABLE", "users")
POSTGRES_ORG_TABLE = os.getenv("POSTGRES_ORG_TABLE", "organizations")
POSTGRES_REVOKED_TOKEN_TABLE = os.getenv(
    "POSTGRES_REVOKED_TOKEN_TABLE", "revoked_refresh_tokens"
)

API_KEY_LIMITS: dict[str, int] = {"Free": 1, "Team": 5, "Enterprise": 50}


async def email_exists(email: str) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {POSTGRES_USER_TABLE} WHERE email = $1",
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
                    f"INSERT INTO {POSTGRES_ORG_TABLE} "
                    f"(org_id, name, user_id, api_key_limit, api_key_usage, created_at) "
                    f"VALUES ($1, $2, $3, $4, 0, NOW()) RETURNING *",
                    org_id, org_name, user_id, api_key_limit,
                )
                user_row = await conn.fetchrow(
                    f"INSERT INTO {POSTGRES_USER_TABLE} "
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
            f"SELECT * FROM {POSTGRES_USER_TABLE} WHERE email = $1",
            email,
        )
        if user_row is None:
            return None
        org_row = await conn.fetchrow(
            f"SELECT * FROM {POSTGRES_ORG_TABLE} WHERE org_id = $1",
            user_row["org_id"],
        )
        if org_row is None:
            return None
    return UserModel(**dict(user_row)), OrganizationModel(**dict(org_row))


async def revoke_refresh_token(jti: str, expires_at: datetime) -> None:
    """Insert a refresh token's jti into the revocation list (idempotent)."""
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {POSTGRES_REVOKED_TOKEN_TABLE} (jti, expires_at) "
            f"VALUES ($1, $2) ON CONFLICT (jti) DO NOTHING",
            uuid.UUID(str(jti)), expires_at,
        )


async def is_refresh_token_revoked(jti: str) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {POSTGRES_REVOKED_TOKEN_TABLE} WHERE jti = $1",
            uuid.UUID(str(jti)),
        )
        return row is not None


__all__ = [
    "email_exists",
    "register_user",
    "get_user_by_email",
    "revoke_refresh_token",
    "is_refresh_token_revoked",
    "POSTGRES_USER_TABLE",
    "POSTGRES_ORG_TABLE",
    "POSTGRES_REVOKED_TOKEN_TABLE",
    "API_KEY_LIMITS",
]
