"""PostgreSQL CRUD helpers for datasets."""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from . import postgres_client


async def list_datasets(org_id: uuid.UUID) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT d.dataset_id, d.org_id, d.name, d.description,
                   d.created_at, d.updated_at,
                   COUNT(e.example_id)::int AS example_count
            FROM datasets d
            LEFT JOIN dataset_examples e ON e.dataset_id = d.dataset_id
            WHERE d.org_id = $1
            GROUP BY d.dataset_id
            ORDER BY d.created_at DESC
            """,
            org_id,
        )
        return [dict(r) for r in rows]


async def create_dataset(
    org_id: uuid.UUID,
    name: str,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO datasets (org_id, name, description)
            VALUES ($1, $2, $3)
            RETURNING *
            """,
            org_id, name, description,
        )
        return dict(row)


async def delete_dataset(dataset_id: uuid.UUID, org_id: uuid.UUID) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM datasets WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        return result == "DELETE 1"


async def list_examples(
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM dataset_examples
            WHERE dataset_id = $1 AND org_id = $2
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            """,
            dataset_id, org_id, limit, offset,
        )
        return [dict(r) for r in rows]


async def add_example(
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
    input_text: str,
    expected_output: Optional[str],
    metadata: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        exists = await conn.fetchrow(
            "SELECT 1 FROM datasets WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        if exists is None:
            return None
        row = await conn.fetchrow(
            """
            INSERT INTO dataset_examples (dataset_id, org_id, input, expected_output, metadata)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            RETURNING *
            """,
            dataset_id, org_id, input_text, expected_output, json.dumps(metadata),
        )
        return dict(row) if row else None


async def delete_example(
    example_id: uuid.UUID,
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM dataset_examples
            WHERE example_id = $1 AND dataset_id = $2 AND org_id = $3
            """,
            example_id, dataset_id, org_id,
        )
        return result == "DELETE 1"
