"""Per-organization overrides of the built-in LLM-as-Judge prompts.

Customers can fork any platform judge prompt (``eval_judge_prompts``) for their
own org. The evaluator worker resolves org override → platform template → code
default, so deleting an override reverts the org to the platform prompt.

Every save is recorded in ``eval_judge_prompt_org_versions``; history survives
a reset (the live row is deleted, versions are kept) so "restore" still works
afterwards. Version numbers continue from the org's history high-water mark.
"""
import uuid
from typing import Any, Optional

from . import postgres_client


async def list_org_judge_prompts(org_id: uuid.UUID) -> list[dict[str, Any]]:
    """All platform judge prompts with this org's override state merged in.

    ``template`` is what the org's evaluations effectively use; ``platform_template``
    is what a reset reverts to.
    """
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.name,
                   p.description,
                   p.required_vars,
                   p.template                          AS platform_template,
                   COALESCE(o.template, p.template)    AS template,
                   (o.org_id IS NOT NULL)              AS is_overridden,
                   COALESCE(o.version, 0)              AS version,
                   COALESCE(o.updated_at, p.updated_at) AS updated_at
            FROM eval_judge_prompts p
            LEFT JOIN eval_judge_prompt_org_overrides o
                   ON o.name = p.name AND o.org_id = $1
            ORDER BY p.name
            """,
            org_id,
        )
    return [dict(r) for r in rows]


async def get_org_judge_prompt(org_id: uuid.UUID, name: str) -> Optional[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT p.name,
                   p.description,
                   p.required_vars,
                   p.template                          AS platform_template,
                   COALESCE(o.template, p.template)    AS template,
                   (o.org_id IS NOT NULL)              AS is_overridden,
                   COALESCE(o.version, 0)              AS version,
                   COALESCE(o.updated_at, p.updated_at) AS updated_at
            FROM eval_judge_prompts p
            LEFT JOIN eval_judge_prompt_org_overrides o
                   ON o.name = p.name AND o.org_id = $1
            WHERE p.name = $2
            """,
            org_id, name,
        )
    return dict(row) if row is not None else None


async def _save_override(
    conn,
    org_id: uuid.UUID,
    name: str,
    template: str,
    updated_by: Optional[uuid.UUID],
) -> None:
    """Snapshot into history and upsert the live override row."""
    prev = await conn.fetchval(
        """
        SELECT COALESCE(MAX(version), 0) FROM eval_judge_prompt_org_versions
        WHERE org_id = $1 AND name = $2
        """,
        org_id, name,
    )
    new_version = int(prev) + 1
    await conn.execute(
        """
        INSERT INTO eval_judge_prompt_org_versions (org_id, name, version, template, updated_by)
        VALUES ($1, $2, $3, $4, $5)
        """,
        org_id, name, new_version, template, updated_by,
    )
    await conn.execute(
        """
        INSERT INTO eval_judge_prompt_org_overrides
            (org_id, name, template, version, updated_by)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (org_id, name) DO UPDATE SET
            template = EXCLUDED.template,
            version = EXCLUDED.version,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW()
        """,
        org_id, name, template, new_version, updated_by,
    )


async def upsert_org_judge_prompt(
    org_id: uuid.UUID,
    name: str,
    template: str,
    updated_by: Optional[uuid.UUID] = None,
) -> Optional[dict[str, Any]]:
    """Create/update this org's override. Returns None if the prompt name is unknown."""
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval(
                "SELECT 1 FROM eval_judge_prompts WHERE name = $1", name
            )
            if not exists:
                return None
            await _save_override(conn, org_id, name, template, updated_by)
    return await get_org_judge_prompt(org_id, name)


async def reset_org_judge_prompt(org_id: uuid.UUID, name: str) -> Optional[dict[str, Any]]:
    """Delete the org override so the org reverts to the platform prompt.

    History rows are kept so a later restore still works. Returns the merged
    view (now un-overridden), or None if the prompt name is unknown.
    """
    async with postgres_client.acquire() as conn:
        await conn.execute(
            "DELETE FROM eval_judge_prompt_org_overrides WHERE org_id = $1 AND name = $2",
            org_id, name,
        )
    return await get_org_judge_prompt(org_id, name)


async def list_org_judge_prompt_versions(
    org_id: uuid.UUID, name: str
) -> list[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT version_id, name, version, template, updated_by, created_at
            FROM eval_judge_prompt_org_versions
            WHERE org_id = $1 AND name = $2
            ORDER BY version DESC
            """,
            org_id, name,
        )
    return [dict(r) for r in rows]


async def restore_org_judge_prompt_version(
    org_id: uuid.UUID,
    name: str,
    version: int,
    updated_by: Optional[uuid.UUID] = None,
) -> Optional[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            snap = await conn.fetchval(
                """
                SELECT template FROM eval_judge_prompt_org_versions
                WHERE org_id = $1 AND name = $2 AND version = $3
                """,
                org_id, name, version,
            )
            if snap is None:
                return None
            await _save_override(conn, org_id, name, snap, updated_by)
    return await get_org_judge_prompt(org_id, name)


__all__ = [
    "list_org_judge_prompts",
    "get_org_judge_prompt",
    "upsert_org_judge_prompt",
    "reset_org_judge_prompt",
    "list_org_judge_prompt_versions",
    "restore_org_judge_prompt_version",
]
