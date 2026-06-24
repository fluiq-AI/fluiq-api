"""Admin CRUD for the platform-global LLM-as-Judge prompts.

These rows are seeded by the evaluator worker (``eval_judge_prompts``). The
admin console reads them, edits the ``template``, and can roll back to any saved
version or reset to the pristine ``default_template``. Every save is recorded in
``eval_judge_prompt_versions`` for history.
"""
import uuid
from typing import Any, Optional

from . import postgres_client


async def list_judge_prompts() -> list[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, template, default_template, description, required_vars,
                   is_overridden, version, updated_at
            FROM eval_judge_prompts
            ORDER BY name
            """
        )
    return [dict(r) for r in rows]


async def get_judge_prompt(name: str) -> Optional[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT name, template, default_template, description, required_vars,
                   is_overridden, version, updated_at
            FROM eval_judge_prompts WHERE name = $1
            """,
            name,
        )
    return dict(row) if row is not None else None


async def _save(
    conn,
    name: str,
    template: str,
    *,
    is_overridden: bool,
    updated_by: Optional[uuid.UUID],
) -> dict[str, Any]:
    """Bump version, snapshot into history, and update the live row."""
    current = await conn.fetchrow(
        "SELECT version FROM eval_judge_prompts WHERE name = $1 FOR UPDATE", name
    )
    new_version = int(current["version"]) + 1
    await conn.execute(
        """
        INSERT INTO eval_judge_prompt_versions (name, version, template, updated_by)
        VALUES ($1, $2, $3, $4)
        """,
        name, new_version, template, updated_by,
    )
    row = await conn.fetchrow(
        """
        UPDATE eval_judge_prompts
        SET template = $2, is_overridden = $3, version = $4,
            updated_by = $5, updated_at = NOW()
        WHERE name = $1
        RETURNING name, template, default_template, description, required_vars,
                  is_overridden, version, updated_at
        """,
        name, template, is_overridden, new_version, updated_by,
    )
    return dict(row)


async def update_judge_prompt(
    name: str, template: str, updated_by: Optional[uuid.UUID] = None
) -> Optional[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval(
                "SELECT 1 FROM eval_judge_prompts WHERE name = $1", name
            )
            if not exists:
                return None
            return await _save(conn, name, template, is_overridden=True, updated_by=updated_by)


async def reset_judge_prompt(
    name: str, updated_by: Optional[uuid.UUID] = None
) -> Optional[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT default_template FROM eval_judge_prompts WHERE name = $1", name
            )
            if row is None:
                return None
            return await _save(
                conn, name, row["default_template"], is_overridden=False, updated_by=updated_by
            )


async def list_judge_prompt_versions(name: str) -> list[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT version_id, name, version, template, updated_by, created_at
            FROM eval_judge_prompt_versions
            WHERE name = $1
            ORDER BY version DESC
            """,
            name,
        )
    return [dict(r) for r in rows]


async def restore_judge_prompt_version(
    name: str, version: int, updated_by: Optional[uuid.UUID] = None
) -> Optional[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            snap = await conn.fetchrow(
                """
                SELECT template FROM eval_judge_prompt_versions
                WHERE name = $1 AND version = $2
                """,
                name, version,
            )
            if snap is None:
                return None
            return await _save(
                conn, name, snap["template"], is_overridden=True, updated_by=updated_by
            )


__all__ = [
    "list_judge_prompts",
    "get_judge_prompt",
    "update_judge_prompt",
    "reset_judge_prompt",
    "list_judge_prompt_versions",
    "restore_judge_prompt_version",
]
