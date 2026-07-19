"""Multi-user organization queries: membership, invitations, and multi-org.

The ``organization_members`` table is the source of truth for who may access an
org. ``users.org_id`` stays the user's *current* org (what the JWT is minted
with); ``organizations.user_id`` stays the *owner*. Plan/tier for an org is
``COALESCE(organizations.plan_tier, owner.user_type)`` — see ``auth.get_org_tier``.

Role hierarchy: owner > admin > member.
"""
import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
import config

from shared.model import OrganizationModel

from . import postgres_client
from .auth import API_KEY_LIMITS

ROLE_RANK = {"member": 1, "admin": 2, "owner": 3}

INVITE_EXPIRE_HOURS = 168  # 7 days


# ── Membership reads ───────────────────────────────────────────────────────────

async def list_user_organizations(user_id: uuid.UUID) -> list[dict]:
    """Every org the user belongs to, with their role, effective plan, and size."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT o.org_id, o.name, m.role, "
            f"       COALESCE(o.plan_tier, owner.user_type) AS plan, "
            f"       (SELECT COUNT(*) FROM organization_members mm "
            f"        WHERE mm.org_id = o.org_id) AS member_count, "
            f"       o.created_at "
            f"FROM organization_members m "
            f"JOIN {config.POSTGRES_ORG_TABLE} o ON o.org_id = m.org_id "
            f"JOIN {config.POSTGRES_USER_TABLE} owner ON owner.user_id = o.user_id "
            f"WHERE m.user_id = $1 "
            f"ORDER BY o.created_at",
            user_id,
        )
    return [dict(r) for r in rows]


async def get_membership_role(org_id: uuid.UUID, user_id: uuid.UUID) -> Optional[str]:
    async with postgres_client.acquire() as conn:
        return await conn.fetchval(
            "SELECT role FROM organization_members WHERE org_id = $1 AND user_id = $2",
            org_id, user_id,
        )


async def list_members(org_id: uuid.UUID) -> list[dict]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT m.user_id, u.name, u.email, m.role, m.created_at "
            f"FROM organization_members m "
            f"JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = m.user_id "
            f"WHERE m.org_id = $1 "
            f"ORDER BY CASE m.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END, "
            f"         m.created_at",
            org_id,
        )
    return [dict(r) for r in rows]


async def _count_owners(conn: asyncpg.Connection, org_id: uuid.UUID) -> int:
    return int(await conn.fetchval(
        "SELECT COUNT(*) FROM organization_members WHERE org_id = $1 AND role = 'owner'",
        org_id,
    ))


async def _repoint_current_org(
    conn: asyncpg.Connection, user_id: uuid.UUID, leaving_org: uuid.UUID
) -> None:
    """If the user's active org is ``leaving_org``, move it to any other membership."""
    current = await conn.fetchval(
        f"SELECT org_id FROM {config.POSTGRES_USER_TABLE} WHERE user_id = $1", user_id
    )
    if current != leaving_org:
        return
    fallback = await conn.fetchval(
        "SELECT org_id FROM organization_members "
        "WHERE user_id = $1 AND org_id <> $2 ORDER BY created_at LIMIT 1",
        user_id, leaving_org,
    )
    if fallback is not None:
        await conn.execute(
            f"UPDATE {config.POSTGRES_USER_TABLE} SET org_id = $2, updated_at = NOW() "
            f"WHERE user_id = $1",
            user_id, fallback,
        )


# ── Org lifecycle ──────────────────────────────────────────────────────────────

async def create_organization(user_id: uuid.UUID, name: str) -> OrganizationModel:
    """Create an additional org owned by ``user_id`` (owner membership included).

    A newly created org is stamped ``plan_tier='Free'`` so it can be upgraded
    independently of any paid plan the creator holds on their home org.
    """
    org_id = uuid.uuid4()
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            org_row = await conn.fetchrow(
                f"INSERT INTO {config.POSTGRES_ORG_TABLE} "
                f"(org_id, name, user_id, api_key_limit, api_key_usage, plan_tier, created_at) "
                f"VALUES ($1, $2, $3, $4, 0, 'Free', NOW()) RETURNING *",
                org_id, name.strip(), user_id, API_KEY_LIMITS.get("Free", 1),
            )
            await conn.execute(
                "INSERT INTO organization_members (org_id, user_id, role) "
                "VALUES ($1, $2, 'owner')",
                org_id, user_id,
            )
    return OrganizationModel(**dict(org_row))


async def rename_organization(org_id: uuid.UUID, name: str) -> Optional[OrganizationModel]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE {config.POSTGRES_ORG_TABLE} SET name = $2, updated_at = NOW() "
            f"WHERE org_id = $1 RETURNING *",
            org_id, name.strip(),
        )
    return OrganizationModel(**dict(row)) if row is not None else None


async def user_org_count(user_id: uuid.UUID) -> int:
    async with postgres_client.acquire() as conn:
        return int(await conn.fetchval(
            "SELECT COUNT(*) FROM organization_members WHERE user_id = $1", user_id
        ))


async def delete_organization(org_id: uuid.UUID) -> None:
    """Delete an org and its data. Members whose active org was this one are
    repointed to another of their memberships first. ClickHouse data is purged
    to mirror account deletion. The row delete cascades members/invitations."""
    from db_queues.clickhouse import clickhouse_client

    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            member_ids = [
                r["user_id"] for r in await conn.fetch(
                    "SELECT user_id FROM organization_members WHERE org_id = $1", org_id
                )
            ]
            for uid in member_ids:
                await _repoint_current_org(conn, uid, org_id)
            await conn.execute(
                f"DELETE FROM {config.POSTGRES_ORG_TABLE} WHERE org_id = $1", org_id
            )
    try:
        await clickhouse_client.delete_org_data(org_id)
    except Exception:  # noqa: BLE001 — Postgres delete already committed; CH is best-effort
        pass


# ── Member management ──────────────────────────────────────────────────────────

async def set_member_role(org_id: uuid.UUID, user_id: uuid.UUID, role: str) -> bool:
    """Set a member's role. Promoting to ``owner`` transfers ownership: the prior
    owner(s) are demoted to admin and ``organizations.user_id`` is updated.
    Returns False if the target is not a member."""
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval(
                "SELECT 1 FROM organization_members WHERE org_id = $1 AND user_id = $2",
                org_id, user_id,
            )
            if not exists:
                return False
            if role == "owner":
                await conn.execute(
                    "UPDATE organization_members SET role = 'admin' "
                    "WHERE org_id = $1 AND role = 'owner'",
                    org_id,
                )
                await conn.execute(
                    "UPDATE organization_members SET role = 'owner' "
                    "WHERE org_id = $1 AND user_id = $2",
                    org_id, user_id,
                )
                await conn.execute(
                    f"UPDATE {config.POSTGRES_ORG_TABLE} SET user_id = $2, updated_at = NOW() "
                    f"WHERE org_id = $1",
                    org_id, user_id,
                )
            else:
                await conn.execute(
                    "UPDATE organization_members SET role = $3 "
                    "WHERE org_id = $1 AND user_id = $2",
                    org_id, user_id, role,
                )
    return True


async def remove_member(org_id: uuid.UUID, user_id: uuid.UUID) -> tuple[bool, Optional[str]]:
    """Remove a member (or self-leave). Refuses to remove the last owner.
    Returns (ok, error)."""
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            role = await conn.fetchval(
                "SELECT role FROM organization_members WHERE org_id = $1 AND user_id = $2",
                org_id, user_id,
            )
            if role is None:
                return False, "Not a member of this organization."
            if role == "owner" and await _count_owners(conn, org_id) <= 1:
                return False, "Cannot remove the last owner. Transfer ownership first."
            await conn.execute(
                "DELETE FROM organization_members WHERE org_id = $1 AND user_id = $2",
                org_id, user_id,
            )
            await _repoint_current_org(conn, user_id, org_id)
    return True, None


# ── Invitations ────────────────────────────────────────────────────────────────

def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def create_invitation(
    org_id: uuid.UUID, email: str, role: str, invited_by: uuid.UUID
) -> tuple[Optional[str], Optional[str]]:
    """Create (or refresh) a pending invitation. Returns (raw_token, error).

    Errors when the email is already a member. A prior pending invite for the
    same (org, email) is superseded so the newest link is the only live one.
    """
    async with postgres_client.acquire() as conn:
        already = await conn.fetchval(
            f"SELECT 1 FROM organization_members m "
            f"JOIN {config.POSTGRES_USER_TABLE} u ON u.user_id = m.user_id "
            f"WHERE m.org_id = $1 AND lower(u.email) = lower($2)",
            org_id, email,
        )
        if already:
            return None, "That person is already a member of this organization."
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=INVITE_EXPIRE_HOURS)
        async with conn.transaction():
            await conn.execute(
                "UPDATE organization_invitations SET status = 'revoked' "
                "WHERE org_id = $1 AND lower(email) = lower($2) AND status = 'pending'",
                org_id, email,
            )
            await conn.execute(
                "INSERT INTO organization_invitations "
                "(org_id, email, role, token_hash, invited_by, expires_at) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                org_id, email.strip(), role, _hash_token(token), invited_by, expires_at,
            )
    return token, None


async def list_pending_invitations(org_id: uuid.UUID) -> list[dict]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT invite_id, email, role, created_at, expires_at "
            "FROM organization_invitations "
            "WHERE org_id = $1 AND status = 'pending' AND expires_at > NOW() "
            "ORDER BY created_at DESC",
            org_id,
        )
    return [dict(r) for r in rows]


async def revoke_invitation(org_id: uuid.UUID, invite_id: uuid.UUID) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE organization_invitations SET status = 'revoked' "
            "WHERE invite_id = $1 AND org_id = $2 AND status = 'pending' "
            "RETURNING invite_id",
            invite_id, org_id,
        )
    return row is not None


async def get_invitation_by_token(token: str) -> Optional[dict]:
    """Resolve a raw token to invite details for the accept preview. Returns the
    invite plus org name and inviter name; None if unknown."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT i.invite_id, i.org_id, i.email, i.role, i.status, i.expires_at, "
            f"       o.name AS org_name, inviter.name AS inviter_name "
            f"FROM organization_invitations i "
            f"JOIN {config.POSTGRES_ORG_TABLE} o ON o.org_id = i.org_id "
            f"LEFT JOIN {config.POSTGRES_USER_TABLE} inviter ON inviter.user_id = i.invited_by "
            f"WHERE i.token_hash = $1",
            _hash_token(token),
        )
    return dict(row) if row is not None else None


async def accept_invitation(
    token: str, user_id: uuid.UUID, user_email: str
) -> tuple[Optional[uuid.UUID], Optional[str]]:
    """Accept an invite for the logged-in user. The caller's email must match
    the invite. Adds an org membership and marks the invite accepted. Returns
    (org_id, error)."""
    token_hash = _hash_token(token)
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            inv = await conn.fetchrow(
                "SELECT invite_id, org_id, email, role, status, expires_at "
                "FROM organization_invitations WHERE token_hash = $1 FOR UPDATE",
                token_hash,
            )
            if inv is None:
                return None, "This invitation link is invalid."
            if inv["status"] != "pending":
                return None, "This invitation has already been used or revoked."
            if inv["expires_at"] <= datetime.now(timezone.utc):
                return None, "This invitation has expired."
            if inv["email"].lower() != user_email.lower():
                return None, "This invitation was sent to a different email address."
            await conn.execute(
                "INSERT INTO organization_members (org_id, user_id, role) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role",
                inv["org_id"], user_id, inv["role"],
            )
            await conn.execute(
                "UPDATE organization_invitations "
                "SET status = 'accepted', accepted_at = NOW() WHERE invite_id = $1",
                inv["invite_id"],
            )
    return inv["org_id"], None


async def set_user_current_org(user_id: uuid.UUID, org_id: uuid.UUID) -> None:
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"UPDATE {config.POSTGRES_USER_TABLE} SET org_id = $2, updated_at = NOW() "
            f"WHERE user_id = $1",
            user_id, org_id,
        )


# ── Admin helpers (platform operators; no membership requirement) ───────────────

async def admin_add_member(
    org_id: uuid.UUID, email: str, role: str
) -> tuple[bool, Optional[str]]:
    """Add an existing user (by email) to any org. Returns (ok, error)."""
    async with postgres_client.acquire() as conn:
        user = await conn.fetchrow(
            f"SELECT user_id FROM {config.POSTGRES_USER_TABLE} WHERE lower(email) = lower($1)",
            email,
        )
        if user is None:
            return False, "No user with that email exists."
        org = await conn.fetchval(
            f"SELECT 1 FROM {config.POSTGRES_ORG_TABLE} WHERE org_id = $1", org_id
        )
        if not org:
            return False, "Organization not found."
        await conn.execute(
            "INSERT INTO organization_members (org_id, user_id, role) VALUES ($1, $2, $3) "
            "ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role",
            org_id, user["user_id"], role,
        )
    return True, None


async def admin_set_org_plan(org_id: uuid.UUID, plan_tier: str) -> bool:
    """Set an org's plan override + matching API-key limit."""
    new_limit = API_KEY_LIMITS.get(plan_tier, 1)
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE {config.POSTGRES_ORG_TABLE} "
            f"SET plan_tier = $2, api_key_limit = $3, updated_at = NOW() "
            f"WHERE org_id = $1 RETURNING org_id",
            org_id, plan_tier, new_limit,
        )
    return row is not None


async def admin_list_user_organizations(user_id: uuid.UUID) -> list[dict]:
    return await list_user_organizations(user_id)
