"""PostgreSQL CRUD helpers for dataset batch runs and agent links."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from . import postgres_client


# ── Runs ────────────────────────────────────────────────────────────────────────

async def create_run(
    org_id: uuid.UUID,
    dataset_id: uuid.UUID,
    kind: str,
    depth: Optional[str] = None,
    model: Optional[str] = None,
    batch_id: Optional[uuid.UUID] = None,
) -> Optional[Dict[str, Any]]:
    """Create a dataset run. Returns None if the dataset is not the org's."""
    async with postgres_client.acquire() as conn:
        owns = await conn.fetchrow(
            "SELECT 1 FROM datasets WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        if owns is None:
            return None
        row = await conn.fetchrow(
            """
            INSERT INTO dataset_runs (dataset_id, org_id, kind, depth, model, batch_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            """,
            dataset_id, org_id, kind, depth, model, batch_id,
        )
        return dict(row)


async def add_run_items(
    run_id: uuid.UUID,
    org_id: uuid.UUID,
    items: List[Tuple[uuid.UUID, str, str]],
) -> None:
    """Bulk-insert (example_id, trace_id, source) rows and set the run total."""
    if not items:
        return
    async with postgres_client.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO dataset_run_items (run_id, org_id, example_id, trace_id, source)
            VALUES ($1, $2, $3, $4, $5)
            """,
            [(run_id, org_id, ex_id, trace_id, source) for ex_id, trace_id, source in items],
        )
        await conn.execute(
            "UPDATE dataset_runs SET total = $2 WHERE run_id = $1",
            run_id, len(items),
        )


async def finalize_run(
    run_id: uuid.UUID,
    org_id: uuid.UUID,
    status: str,
    summary: Dict[str, Any],
) -> None:
    async with postgres_client.acquire() as conn:
        await conn.execute(
            """
            UPDATE dataset_runs
            SET status = $3, summary = $4::jsonb, finished_at = NOW()
            WHERE run_id = $1 AND org_id = $2
            """,
            run_id, org_id, status, json.dumps(summary),
        )


async def get_run(run_id: uuid.UUID, org_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM dataset_runs WHERE run_id = $1 AND org_id = $2",
            run_id, org_id,
        )
        return dict(row) if row else None


async def get_run_items(run_id: uuid.UUID, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT i.item_id, i.example_id, i.trace_id, i.source, i.created_at,
                   e.input, e.expected_output
            FROM dataset_run_items i
            LEFT JOIN dataset_examples e ON e.example_id = i.example_id
            WHERE i.run_id = $1 AND i.org_id = $2
            ORDER BY i.created_at
            """,
            run_id, org_id,
        )
        return [dict(r) for r in rows]


async def list_runs(
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT r.*, COUNT(i.item_id)::int AS item_count
            FROM dataset_runs r
            LEFT JOIN dataset_run_items i ON i.run_id = r.run_id
            WHERE r.dataset_id = $1 AND r.org_id = $2
            GROUP BY r.run_id
            ORDER BY r.created_at DESC
            LIMIT $3 OFFSET $4
            """,
            dataset_id, org_id, limit, offset,
        )
        return [dict(r) for r in rows]


# ── Agent links ─────────────────────────────────────────────────────────────────

async def link_agent(
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
    agent_key: str,
    agent_kind: str,
) -> bool:
    """Link an agent to a dataset (idempotent). False if dataset not the org's."""
    async with postgres_client.acquire() as conn:
        owns = await conn.fetchrow(
            "SELECT 1 FROM datasets WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        if owns is None:
            return False
        await conn.execute(
            """
            INSERT INTO dataset_agent_links (dataset_id, org_id, agent_key, agent_kind)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (dataset_id, agent_key, agent_kind) DO NOTHING
            """,
            dataset_id, org_id, agent_key, agent_kind,
        )
        return True


async def unlink_agent(
    dataset_id: uuid.UUID,
    org_id: uuid.UUID,
    agent_key: str,
    agent_kind: str,
) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM dataset_agent_links
            WHERE dataset_id = $1 AND org_id = $2 AND agent_key = $3 AND agent_kind = $4
            """,
            dataset_id, org_id, agent_key, agent_kind,
        )
        return result == "DELETE 1"


async def list_agent_links(dataset_id: uuid.UUID, org_id: uuid.UUID) -> List[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT agent_key, agent_kind, created_at
            FROM dataset_agent_links
            WHERE dataset_id = $1 AND org_id = $2
            ORDER BY created_at DESC
            """,
            dataset_id, org_id,
        )
        return [dict(r) for r in rows]


async def datasets_for_agent(
    org_id: uuid.UUID,
    agent_key: str,
    agent_kind: str,
) -> List[uuid.UUID]:
    """Dataset ids that auto-append runs of this agent. Used by the tracer hook."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT dataset_id FROM dataset_agent_links
            WHERE org_id = $1 AND agent_key = $2 AND agent_kind = $3
            """,
            org_id, agent_key, agent_kind,
        )
        return [r["dataset_id"] for r in rows]
