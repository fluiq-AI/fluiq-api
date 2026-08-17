"""PostgreSQL CRUD helpers for saved prompts."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

from . import postgres_client

VALID_ENVIRONMENTS = {"development", "staging", "production"}

_ENV_SUBQUERY = """
    COALESCE(
        (SELECT json_object_agg(
            pe.environment,
            json_build_object(
                'env_id',      pe.env_id::text,
                'version',     pe.version,
                'deployed_at', pe.deployed_at::text
            )
         )
         FROM prompt_environments pe
         WHERE pe.prompt_id = p.prompt_id
        ),
        '{}'::json
    ) AS environments
"""


async def list_prompts(org_id: uuid.UUID) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT p.prompt_id, p.org_id, p.name, p.slug, p.template, p.model,
                   p.variables, p.kind, p.is_deployed, p.deployed_at, p.version,
                   p.created_at, p.updated_at,
                   {_ENV_SUBQUERY}
            FROM prompts p
            WHERE p.org_id = $1
            ORDER BY p.created_at DESC
            """,
            org_id,
        )
        return [dict(r) for r in rows]


async def get_prompt_full(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
) -> Optional[Dict[str, Any]]:
    """Fetch a single prompt with its environments embedded."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT p.*,
                   {_ENV_SUBQUERY}
            FROM prompts p
            WHERE p.prompt_id = $1 AND p.org_id = $2
            """,
            prompt_id, org_id,
        )
        return dict(row) if row else None


async def get_prompt_by_slug(
    org_id: uuid.UUID, slug: str
) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM prompts WHERE org_id = $1 AND slug = $2",
            org_id, slug,
        )
        return dict(row) if row else None


async def get_deployed_prompt_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """Fetch a deployed prompt by slug across all orgs (for API-key-auth SDK calls)."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM prompts WHERE slug = $1 AND is_deployed = TRUE LIMIT 1",
            slug,
        )
        return dict(row) if row else None


async def get_deployed_prompt_for_org(
    org_id: uuid.UUID, slug: str
) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM prompts WHERE org_id = $1 AND slug = $2 AND is_deployed = TRUE",
            org_id, slug,
        )
        return dict(row) if row else None


async def create_prompt(
    org_id: uuid.UUID,
    name: str,
    slug: str,
    template: str,
    model: Optional[str],
    variables: List[str],
    kind: str = "completion",
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create a prompt. ``config`` carries scorer settings that aren't the body
    itself — today a judge's choice set."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO prompts
                (org_id, name, slug, template, model, variables, kind, config, created_at)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, NOW())
            RETURNING *
            """,
            org_id, name, slug, template, model,
            [{"name": v} for v in variables], kind,
            json.dumps(config) if config else None,
        )
        return dict(row)


async def get_custom_judge_template(
    org_id: uuid.UUID, slug: str
) -> Optional[str]:
    """Return the live template for a client-defined judge prompt, or None.

    Used by the block-mode /evaluate path to resolve custom judges referenced by
    slug in ``fluiq.eval(custom_judges={...})``. Only ``kind = 'judge'`` rows are
    eligible so a completion prompt can never be run as a judge by accident.
    """
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT template FROM prompts WHERE org_id = $1 AND slug = $2 AND kind = 'judge'",
            org_id, slug,
        )
        return row["template"] if row else None


async def update_prompt(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
    *,
    name: Optional[str] = None,
    template: Optional[str] = None,
    model: Optional[str] = None,
    variables: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
    clear_config: bool = False,
) -> Optional[Dict[str, Any]]:
    """Update a prompt, snapshotting the previous version first.

    ``config`` is three-valued on purpose: omitted leaves it untouched, a dict
    replaces it, and ``clear_config`` removes it. Without the third case there
    would be no way to turn a choice-scored judge back into a free-scoring one.
    """
    sets = ["updated_at = NOW()", "version = version + 1"]
    args: list[Any] = []
    idx = 1

    if name is not None:
        sets.append(f"name = ${idx}")
        args.append(name)
        idx += 1
    if template is not None:
        sets.append(f"template = ${idx}")
        args.append(template)
        idx += 1
    if model is not None:
        sets.append(f"model = ${idx}")
        args.append(model)
        idx += 1
    if variables is not None:
        sets.append(f"variables = ${idx}::jsonb")
        args.append([{"name": v} for v in variables])
        idx += 1
    if clear_config:
        sets.append("config = NULL")
    elif config is not None:
        sets.append(f"config = ${idx}::jsonb")
        args.append(json.dumps(config))
        idx += 1

    args += [prompt_id, org_id]
    async with postgres_client.acquire() as conn:
        # Snapshot the current state before overwriting
        current = await conn.fetchrow(
            "SELECT * FROM prompts WHERE prompt_id = $1 AND org_id = $2",
            prompt_id, org_id,
        )
        if current is None:
            return None
        await conn.execute(
            """
            INSERT INTO prompt_versions (prompt_id, org_id, version, name, template, model, variables)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            current["prompt_id"], current["org_id"], current["version"],
            current["name"], current["template"], current["model"], current["variables"],
        )
        row = await conn.fetchrow(
            f"""
            UPDATE prompts SET {', '.join(sets)}
            WHERE prompt_id = ${idx} AND org_id = ${idx + 1}
            RETURNING *
            """,
            *args,
        )
        return dict(row) if row else None


async def list_versions(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT version_id, prompt_id, org_id, version, name, template, model, variables, created_at
            FROM prompt_versions
            WHERE prompt_id = $1 AND org_id = $2
            ORDER BY version DESC
            """,
            prompt_id, org_id,
        )
        return [dict(r) for r in rows]


async def restore_version(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
    version: int,
) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        snap = await conn.fetchrow(
            """
            SELECT * FROM prompt_versions
            WHERE prompt_id = $1 AND org_id = $2 AND version = $3
            """,
            prompt_id, org_id, version,
        )
        if snap is None:
            return None
        # Snapshot current state before restore
        current = await conn.fetchrow(
            "SELECT * FROM prompts WHERE prompt_id = $1 AND org_id = $2",
            prompt_id, org_id,
        )
        if current is None:
            return None
        await conn.execute(
            """
            INSERT INTO prompt_versions (prompt_id, org_id, version, name, template, model, variables)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            current["prompt_id"], current["org_id"], current["version"],
            current["name"], current["template"], current["model"], current["variables"],
        )
        row = await conn.fetchrow(
            """
            UPDATE prompts
            SET name = $1, template = $2, model = $3, variables = $4,
                version = version + 1, updated_at = NOW()
            WHERE prompt_id = $5 AND org_id = $6
            RETURNING *
            """,
            snap["name"], snap["template"], snap["model"], snap["variables"],
            prompt_id, org_id,
        )
        return dict(row) if row else None


async def set_deployed(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
    deploy: bool,
) -> Optional[Dict[str, Any]]:
    deployed_at = datetime.now(timezone.utc) if deploy else None
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE prompts
            SET is_deployed = $1, deployed_at = $2, updated_at = NOW()
            WHERE prompt_id = $3 AND org_id = $4
            RETURNING *
            """,
            deploy, deployed_at, prompt_id, org_id,
        )
        return dict(row) if row else None


async def delete_prompt(prompt_id: uuid.UUID, org_id: uuid.UUID) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM prompts WHERE prompt_id = $1 AND org_id = $2",
            prompt_id, org_id,
        )
        return result == "DELETE 1"


# ── Environment promotions ─────────────────────────────────────────────────────

async def deploy_to_environment(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
    environment: str,
) -> Optional[Dict[str, Any]]:
    """Snapshot the current prompt head into prompt_environments for this env.

    Returns the updated prompt (with environments) on success, None if the
    prompt doesn't exist.
    """
    async with postgres_client.acquire() as conn:
        current = await conn.fetchrow(
            "SELECT * FROM prompts WHERE prompt_id = $1 AND org_id = $2",
            prompt_id, org_id,
        )
        if current is None:
            return None
        await conn.execute(
            """
            INSERT INTO prompt_environments
                (prompt_id, org_id, environment, version, template, model, variables)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (prompt_id, environment) DO UPDATE
            SET version     = EXCLUDED.version,
                template    = EXCLUDED.template,
                model       = EXCLUDED.model,
                variables   = EXCLUDED.variables,
                deployed_at = NOW()
            """,
            prompt_id, org_id, environment,
            current["version"], current["template"], current["model"], current["variables"],
        )
    return await get_prompt_full(prompt_id, org_id)


async def undeploy_from_environment(
    prompt_id: uuid.UUID,
    org_id: uuid.UUID,
    environment: str,
) -> Optional[Dict[str, Any]]:
    """Remove a prompt from an environment. Returns updated prompt or None if not found."""
    async with postgres_client.acquire() as conn:
        exists = await conn.fetchrow(
            "SELECT 1 FROM prompts WHERE prompt_id = $1 AND org_id = $2",
            prompt_id, org_id,
        )
        if exists is None:
            return None
        await conn.execute(
            "DELETE FROM prompt_environments WHERE prompt_id = $1 AND org_id = $2 AND environment = $3",
            prompt_id, org_id, environment,
        )
    return await get_prompt_full(prompt_id, org_id)


async def get_prompt_for_environment(
    org_id: uuid.UUID,
    slug: str,
    environment: str,
) -> Optional[Dict[str, Any]]:
    """Fetch the deployed snapshot for a specific environment (used by SDK fetch)."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT pe.env_id, pe.environment, pe.version, pe.template, pe.model,
                   pe.variables, pe.deployed_at,
                   p.prompt_id, p.org_id, p.name, p.slug,
                   p.created_at, p.updated_at
            FROM prompt_environments pe
            JOIN prompts p ON p.prompt_id = pe.prompt_id
            WHERE p.org_id = $1 AND p.slug = $2 AND pe.environment = $3
            """,
            org_id, slug, environment,
        )
        return dict(row) if row else None
