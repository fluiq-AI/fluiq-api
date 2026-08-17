"""PostgreSQL helpers for dataset-scoped custom scorers.

A custom scorer is a client-defined check that a dataset remembers for its
metrics runs — either an LLM-as-judge prompt (``prompts`` row with
``kind='judge'``) or a deterministic expression (``kind='code'``). Both are
org-wide and reusable; this table links a slug + threshold to a dataset, and the
evaluator routes on the prompt row's kind.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List

from . import postgres_client


async def dataset_owned(dataset_id: uuid.UUID, org_id: uuid.UUID) -> bool:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM datasets WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        return row is not None


async def list_dataset_scorers(dataset_id: uuid.UUID, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """The dataset's saved scorers, joined to their current judge prompt so the
    UI can show (and edit) the template. A scorer whose prompt was deleted keeps
    its link but reports a null template."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.slug, s.threshold, s.created_at,
                   p.name AS name, p.template AS template, p.kind AS kind,
                   p.config AS config
            FROM dataset_scorers s
            LEFT JOIN prompts p
              ON p.org_id = s.org_id AND p.slug = s.slug
             AND p.kind IN ('judge', 'code')
            WHERE s.dataset_id = $1 AND s.org_id = $2
            ORDER BY s.created_at
            """,
            dataset_id, org_id,
        )
        return [dict(r) for r in rows]


async def link_dataset_scorer(
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
    slug: str,
    threshold: float,
) -> None:
    """Attach a scorer slug to the dataset (idempotent; updates the threshold)."""
    async with postgres_client.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dataset_scorers (dataset_id, org_id, slug, threshold)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (dataset_id, slug) DO UPDATE SET threshold = EXCLUDED.threshold
            """,
            dataset_id, org_id, slug, threshold,
        )


async def unlink_dataset_scorer(dataset_id: uuid.UUID, org_id: uuid.UUID, slug: str) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM dataset_scorers WHERE dataset_id = $1 AND org_id = $2 AND slug = $3",
            dataset_id, org_id, slug,
        )
        return result == "DELETE 1"


# ── Per-dataset judge-prompt overrides ──────────────────────────────────────

async def list_dataset_judge_prompts(
    dataset_id: uuid.UUID, org_id: uuid.UUID
) -> Dict[str, str]:
    """{prompt_name: template} overridden for this dataset."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT name, template FROM dataset_judge_prompts "
            "WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        return {r["name"]: r["template"] for r in rows}


async def upsert_dataset_judge_prompt(
    dataset_id: uuid.UUID, org_id: uuid.UUID, name: str, template: str,
) -> None:
    async with postgres_client.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dataset_judge_prompts (dataset_id, org_id, name, template)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (dataset_id, name)
            DO UPDATE SET template = EXCLUDED.template, updated_at = NOW()
            """,
            dataset_id, org_id, name, template,
        )


async def delete_dataset_judge_prompt(
    dataset_id: uuid.UUID, org_id: uuid.UUID, name: str,
) -> bool:
    """Drop a dataset override so the metric falls back to org → platform."""
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM dataset_judge_prompts "
            "WHERE dataset_id = $1 AND org_id = $2 AND name = $3",
            dataset_id, org_id, name,
        )
        return result == "DELETE 1"


async def dataset_scorer_map(dataset_id: uuid.UUID, org_id: uuid.UUID) -> Dict[str, float]:
    """{slug: threshold} for the dataset's scorers — merged into a metrics run's
    ``custom_judges`` so every run on the dataset applies its saved scorers."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT slug, threshold FROM dataset_scorers WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        return {r["slug"]: float(r["threshold"]) for r in rows}
