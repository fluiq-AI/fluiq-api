import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
import bcrypt

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
    "Admin": 0,
}


async def email_exists(email: str) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {config.POSTGRES_USER_TABLE} WHERE email = $1",
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


async def update_user_password(user_id: uuid.UUID, hashed_password: str) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE {config.POSTGRES_USER_TABLE} "
            f"SET hashed_password = $2, updated_at = NOW() "
            f"WHERE user_id = $1 RETURNING user_id",
            user_id, hashed_password,
        )
    return row is not None


async def create_password_reset(
    user_id: uuid.UUID, otp_hash: str, expires_at: datetime
) -> uuid.UUID:
    """Insert a new password reset token and invalidate any prior unused
    tokens for the same user. Returns the new token_id."""
    token_id = uuid.uuid4()
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"UPDATE {config.POSTGRES_PASSWORD_RESET_TABLE} "
                f"SET used_at = NOW() "
                f"WHERE user_id = $1 AND used_at IS NULL",
                user_id,
            )
            await conn.execute(
                f"INSERT INTO {config.POSTGRES_PASSWORD_RESET_TABLE} "
                f"(token_id, user_id, otp_hash, expires_at) "
                f"VALUES ($1, $2, $3, $4)",
                token_id, user_id, otp_hash, expires_at,
            )
    return token_id


async def fetch_active_password_reset(
    user_id: uuid.UUID,
) -> Optional[tuple[uuid.UUID, str, datetime]]:
    """Return (token_id, otp_hash, expires_at) of the latest unused, unexpired
    reset token for the user, or None."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT token_id, otp_hash, expires_at "
            f"FROM {config.POSTGRES_PASSWORD_RESET_TABLE} "
            f"WHERE user_id = $1 AND used_at IS NULL AND expires_at > NOW() "
            f"ORDER BY created_at DESC LIMIT 1",
            user_id,
        )
    if row is None:
        return None
    return row["token_id"], row["otp_hash"], row["expires_at"]


async def consume_password_reset(token_id: uuid.UUID) -> bool:
    """Atomically mark a reset token as used. Returns True if it was still
    unused and unexpired at the moment of consumption."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE {config.POSTGRES_PASSWORD_RESET_TABLE} "
            f"SET used_at = NOW() "
            f"WHERE token_id = $1 AND used_at IS NULL AND expires_at > NOW() "
            f"RETURNING token_id",
            token_id,
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


# Self-serve trial config. Team & Growth can be trialed for 5 days without a
# card; the trial reverts to Free on expiry. Enterprise is sales-led (no trial).
TRIAL_DAYS = 5
TRIALABLE_TIERS = frozenset({"Team", "Growth"})


async def get_org_tier(org_id: uuid.UUID) -> Optional[str]:
    """Return the tier (`Free` / `Team` / `Growth` / `Enterprise`) for an org.

    The tier is read from the org owner's ``users.user_type`` row. Returns
    ``None`` if the org or its owner cannot be found.

    Trials expire lazily here: if an active trial's ``trial_ends_at`` has
    passed, the owner is downgraded to Free in the same round-trip and Free is
    returned. This is the single read path behind ``shared.quotas``, so no cron
    is needed — the plan self-heals the next time usage is checked. The
    ``trial_used`` latch is left set so the trial can't be restarted.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT u.user_id, u.user_type, u.trial_ends_at "
            f"FROM {config.POSTGRES_ORG_TABLE} o "
            f"JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = o.user_id "
            f"WHERE o.org_id = $1",
            org_id,
        )
        if row is None:
            return None
        tier = row["user_type"]
        ends_at = row["trial_ends_at"]
        if (
            ends_at is not None
            and tier != "Free"
            and ends_at <= datetime.now(timezone.utc)
        ):
            await conn.execute(
                f"UPDATE {config.POSTGRES_USER_TABLE} "
                f"SET user_type = 'Free', trial_ends_at = NULL, updated_at = NOW() "
                f"WHERE user_id = $1",
                row["user_id"],
            )
            return "Free"
    return tier


async def get_trial_ends_at(org_id: uuid.UUID) -> Optional[datetime]:
    """Return the owner's active ``trial_ends_at``, or ``None`` if no live trial.

    A timestamp already in the past reads as ``None`` — the caller shouldn't
    surface an expired trial even if ``get_org_tier`` hasn't swept it yet.
    """
    async with postgres_client.acquire() as conn:
        value = await conn.fetchval(
            f"SELECT u.trial_ends_at "
            f"FROM {config.POSTGRES_ORG_TABLE} o "
            f"JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = o.user_id "
            f"WHERE o.org_id = $1",
            org_id,
        )
    if value is None or value <= datetime.now(timezone.utc):
        return None
    return value


async def start_trial(org_id: uuid.UUID, plan: str) -> dict:
    """Start a 5-day trial of ``plan`` for an org, no card required.

    Returns ``{"ok": True, "tier", "trial_ends_at"}`` on success, or
    ``{"ok": False, "error"}`` with a user-facing reason when the trial can't
    start (bad plan, already on a paid plan, or trial already used). Guards run
    inside the same connection as the update so two concurrent requests can't
    both consume the one-shot ``trial_used`` latch.
    """
    if plan not in TRIALABLE_TIERS:
        return {"ok": False, "error": "Trials are only available for the Team and Growth plans."}

    ends_at = datetime.now(timezone.utc) + timedelta(days=TRIAL_DAYS)
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                f"SELECT u.user_id, u.user_type, u.trial_used "
                f"FROM {config.POSTGRES_ORG_TABLE} o "
                f"JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = o.user_id "
                f"WHERE o.org_id = $1 FOR UPDATE OF u",
                org_id,
            )
            if row is None:
                return {"ok": False, "error": "Organization not found."}
            if row["user_type"] != "Free":
                return {"ok": False, "error": "You're already on a paid plan."}
            if row["trial_used"]:
                return {"ok": False, "error": "You've already used your free trial."}
            await conn.execute(
                f"UPDATE {config.POSTGRES_USER_TABLE} "
                f"SET user_type = $2, trial_ends_at = $3, trial_used = TRUE, updated_at = NOW() "
                f"WHERE user_id = $1",
                row["user_id"], plan, ends_at,
            )
    return {"ok": True, "tier": plan, "trial_ends_at": ends_at}


async def get_org_eval_bonus(org_id: uuid.UUID) -> int:
    """Return the admin-granted evaluation adjustment for an org.

    Added on top of the tier's monthly evaluation quota by ``shared.quotas``.
    Returns 0 when the org or column is absent.
    """
    async with postgres_client.acquire() as conn:
        value = await conn.fetchval(
            f"SELECT eval_quota_bonus FROM {config.POSTGRES_ORG_TABLE} WHERE org_id = $1",
            org_id,
        )
    return int(value or 0)


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

async def get_user_by_id(user_id: uuid.UUID) -> Optional[UserModel]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {config.POSTGRES_USER_TABLE} WHERE user_id = $1",
            user_id,
        )
    if row is None:
        return None
    return UserModel(**dict(row))


async def get_platform_stats() -> dict:
    async with postgres_client.acquire() as conn:
        total_users = await conn.fetchval(
            f"SELECT COUNT(*) FROM {config.POSTGRES_USER_TABLE}"
        )
        total_orgs = await conn.fetchval(
            f"SELECT COUNT(*) FROM {config.POSTGRES_ORG_TABLE}"
        )
        type_rows = await conn.fetch(
            f"SELECT user_type, COUNT(*) AS count "
            f"FROM {config.POSTGRES_USER_TABLE} GROUP BY user_type"
        )
    by_type = {row["user_type"]: int(row["count"]) for row in type_rows}
    return {
        "total_users": int(total_users),
        "total_orgs": int(total_orgs),
        "users_by_type": by_type,
    }


async def admin_list_users(
    page: int = 1,
    limit: int = 50,
    search: str = "",
    user_type_filter: Optional[str] = None,
) -> tuple[list[dict], int]:
    offset = (page - 1) * limit
    params: list = []
    idx = 1
    conditions: list[str] = []

    if search:
        conditions.append(f"(u.email ILIKE ${idx} OR u.name ILIKE ${idx})")
        params.append(f"%{search}%")
        idx += 1
    if user_type_filter:
        conditions.append(f"u.user_type = ${idx}")
        params.append(user_type_filter)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with postgres_client.acquire() as conn:
        total = int(await conn.fetchval(
            f"SELECT COUNT(*) FROM {config.POSTGRES_USER_TABLE} u {where}",
            *params,
        ))
        rows = await conn.fetch(
            f"SELECT u.user_id, u.email, u.name, u.user_type, u.org_id, u.created_at, "
            f"o.name AS org_name "
            f"FROM {config.POSTGRES_USER_TABLE} u "
            f"LEFT JOIN {config.POSTGRES_ORG_TABLE} o ON o.org_id = u.org_id "
            f"{where} "
            f"ORDER BY u.created_at DESC "
            f"LIMIT ${idx} OFFSET ${idx + 1}",
            *params, limit, offset,
        )
    return [dict(row) for row in rows], total


async def admin_update_user_type(user_id: uuid.UUID, new_type: str) -> bool:
    new_limit = API_KEY_LIMITS.get(new_type, 1)
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            user_row = await conn.fetchrow(
                f"UPDATE {config.POSTGRES_USER_TABLE} "
                f"SET user_type = $2, updated_at = NOW() "
                f"WHERE user_id = $1 RETURNING org_id",
                user_id, new_type,
            )
            if user_row is None:
                return False
            await conn.execute(
                f"UPDATE {config.POSTGRES_ORG_TABLE} "
                f"SET api_key_limit = $2, updated_at = NOW() "
                f"WHERE org_id = $1",
                user_row["org_id"], new_limit,
            )
    return True


async def admin_get_user_eval_account(user_id: uuid.UUID) -> Optional[dict]:
    """Resolve a user to their org + tier + current eval bonus.

    Used by the admin Evaluations tab so an operator can look up any user and
    see/adjust the evaluation allowance for their organization.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT u.user_id, u.email, u.name, u.user_type, "
            f"o.org_id, o.name AS org_name, o.eval_quota_bonus "
            f"FROM {config.POSTGRES_USER_TABLE} u "
            f"JOIN {config.POSTGRES_ORG_TABLE} o ON o.org_id = u.org_id "
            f"WHERE u.user_id = $1",
            user_id,
        )
    return dict(row) if row is not None else None


async def admin_adjust_eval_bonus(org_id: uuid.UUID, delta: int) -> Optional[int]:
    """Add ``delta`` evaluations to the org's bonus allowance (delta may be < 0).

    The stored bonus is floored at a large negative bound so a deduction can
    cancel a tier quota but never wrap into nonsense. Returns the new bonus,
    or ``None`` if the org does not exist.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE {config.POSTGRES_ORG_TABLE} "
            f"SET eval_quota_bonus = GREATEST(eval_quota_bonus + $2, -1000000000), "
            f"    updated_at = NOW() "
            f"WHERE org_id = $1 RETURNING eval_quota_bonus",
            org_id, delta,
        )
    return int(row["eval_quota_bonus"]) if row is not None else None


async def admin_list_organizations(
    page: int = 1,
    limit: int = 50,
) -> tuple[list[dict], int]:
    offset = (page - 1) * limit
    async with postgres_client.acquire() as conn:
        total = int(await conn.fetchval(
            f"SELECT COUNT(*) FROM {config.POSTGRES_ORG_TABLE}"
        ))
        rows = await conn.fetch(
            f"SELECT o.org_id, o.name AS org_name, o.user_id, "
            f"o.api_key_usage, o.api_key_limit, o.created_at, "
            f"u.email AS owner_email, u.name AS owner_name, u.user_type AS owner_type "
            f"FROM {config.POSTGRES_ORG_TABLE} o "
            f"LEFT JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = o.user_id "
            f"ORDER BY o.created_at DESC "
            f"LIMIT $1 OFFSET $2",
            limit, offset,
        )
    return [dict(row) for row in rows], total


async def find_or_create_oauth_user(
    name: str,
    email: str,
) -> tuple | None:
    """
    Look up a user by email. If found, return them.
    If not, register them with a random secure password (OAuth users never use it).
    Returns (User, Organization) or None on failure.
    """
    from db_queues.postgresql.auth import get_user_by_email, register_user

    result = await get_user_by_email(email)
    if result is not None:
        return result  # existing user — just log them in

    # New user — create account with random password they'll never use
    random_password = secrets.token_urlsafe(32)
    hashed = bcrypt.hashpw(random_password.encode(), bcrypt.gensalt()).decode()

    return await register_user(
        name=name,
        email=email,
        hashed_password=hashed,
        user_type="Free",
    )

async def store_deletion_feedback(
    user_id: uuid.UUID,
    email: str,
    reason: Optional[str],
) -> None:
    """Persist the reason a user deleted their account before their row is removed."""
    async with postgres_client.acquire() as conn:
        await conn.execute(
            "INSERT INTO account_deletion_feedback (user_id, email, reason) "
            "VALUES ($1, $2, $3)",
            user_id, email, reason or None,
        )


async def delete_user_account(user_id: uuid.UUID, org_id: uuid.UUID) -> None:
    """Delete a user and their organization atomically.

    Delete user first (removes the FK reference to org), then org (cascades
    prompts, datasets, etc. via ON DELETE CASCADE).
    """
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"DELETE FROM {config.POSTGRES_USER_TABLE} WHERE user_id = $1",
                user_id,
            )
            await conn.execute(
                f"DELETE FROM {config.POSTGRES_ORG_TABLE} WHERE org_id = $1",
                org_id,
            )


__all__ = [
    "email_exists",
    "register_user",
    "get_user_by_email",
    "get_user_by_id",
    "revoke_refresh_token",
    "is_refresh_token_revoked",
    "update_user_password",
    "create_password_reset",
    "fetch_active_password_reset",
    "consume_password_reset",
    "get_organization",
    "get_org_tier",
    "get_trial_ends_at",
    "start_trial",
    "TRIAL_DAYS",
    "TRIALABLE_TIERS",
    "get_org_eval_bonus",
    "admin_get_user_eval_account",
    "admin_adjust_eval_bonus",
    "resolve_api_key",
    "create_api_key",
    "delete_api_key",
    "get_platform_stats",
    "admin_list_users",
    "admin_update_user_type",
    "admin_list_organizations",
    "store_deletion_feedback",
    "delete_user_account",
    "API_KEY_LIMITS",
]
