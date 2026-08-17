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
    task: Optional[Dict[str, Any]] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Create a dataset run. Returns None if the dataset is not the org's.

    ``task`` present means the run EXECUTES a prompt+model against every example
    and grades the fresh output; absent means it grades output that already
    exists (the legacy behaviour).
    """
    async with postgres_client.acquire() as conn:
        owns = await conn.fetchrow(
            "SELECT 1 FROM datasets WHERE dataset_id = $1 AND org_id = $2",
            dataset_id, org_id,
        )
        if owns is None:
            return None
        row = await conn.fetchrow(
            """
            INSERT INTO dataset_runs
                (dataset_id, org_id, kind, depth, model, batch_id, task, name,
                 description, tags)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10::jsonb)
            RETURNING *
            """,
            dataset_id, org_id, kind, depth, model, batch_id,
            json.dumps(task) if task else None, name, description,
            json.dumps(tags or []),
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


async def record_generation(
    run_id: uuid.UUID,
    org_id: uuid.UUID,
    trace_id: str,
    *,
    output: Optional[str] = None,
    error: Optional[str] = None,
    latency_ms: Optional[int] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    cost_usd: Optional[float] = None,
) -> None:
    """Store one task execution's output and technical metrics on its run item,
    and advance the run's generation counters.

    Counters are bumped in the same transaction as the item write so a report
    read never sees an item that has generated but a run that says it hasn't.
    """
    async with postgres_client.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE dataset_run_items
                SET output = $3, gen_error = $4, latency_ms = $5,
                    input_tokens = $6, output_tokens = $7, cost_usd = $8
                WHERE run_id = $1 AND trace_id = $2
                """,
                run_id, trace_id, output, error, latency_ms,
                input_tokens, output_tokens, cost_usd,
            )
            await conn.execute(
                """
                UPDATE dataset_runs
                SET generated  = generated + 1,
                    gen_failed = gen_failed + $3
                WHERE run_id = $1 AND org_id = $2
                """,
                run_id, org_id, 1 if error else 0,
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
                   i.output, i.gen_error, i.latency_ms,
                   i.input_tokens, i.output_tokens, i.cost_usd,
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
