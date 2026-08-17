"""fluiq-api — Aggregate score definitions.

  GET    /api/v1/aggregates       list (session)
  POST   /api/v1/aggregates       create (session)
  PATCH  /api/v1/aggregates/{id}  edit (session)
  DELETE /api/v1/aggregates/{id}  remove (session)

See ``shared.aggregates`` for what an aggregate is and why the weighting lives
in one shared definition rather than in each reader's head.
"""
from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator

from db_queues.postgresql import postgres_client
from routes.auth.helper import get_current_session
from shared.aggregates import AggregateError, parse_components

aggregates_router = APIRouter()

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$")
MAX_PER_ORG = 20


class AggregatePayload(BaseModel):
    name:        str
    slug:        Optional[str] = None
    description: Optional[str] = None
    components:  List[Dict[str, Any]] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")[:63]


def _serialize(row: Any) -> Dict[str, Any]:
    components = row["components"]
    if isinstance(components, str):
        try:
            components = json.loads(components)
        except ValueError:
            components = []
    return {
        "aggregate_id": str(row["aggregate_id"]),
        "slug":         row["slug"],
        "name":         row["name"],
        "description":  row.get("description"),
        "components":   components or [],
    }


@aggregates_router.get("/aggregates")
async def list_aggregates(session: dict = Depends(get_current_session)):
    org_id = uuid.UUID(session["org_id"])
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM aggregate_scores WHERE org_id = $1 ORDER BY name", org_id,
        )
    return {"aggregates": [_serialize(r) for r in rows]}


@aggregates_router.post("/aggregates", status_code=status.HTTP_201_CREATED)
async def create_aggregate(
    payload: AggregatePayload,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    slug = (payload.slug or _slugify(payload.name)).strip().lower()
    if not _SLUG_RE.match(slug):
        raise HTTPException(
            422, "Could not derive a valid slug; use letters, numbers and hyphens.",
        )
    try:
        components = parse_components(payload.components)
    except AggregateError as exc:
        raise HTTPException(422, str(exc)) from exc

    async with postgres_client.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM aggregate_scores WHERE org_id = $1", org_id,
        )
        if count >= MAX_PER_ORG:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"An organization can hold {MAX_PER_ORG} aggregates. More than "
                f"that and they stop being a summary.",
            )
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO aggregate_scores (org_id, slug, name, description, components)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                RETURNING *
                """,
                org_id, slug, payload.name, payload.description,
                json.dumps(components),
            )
        except Exception as exc:  # noqa: BLE001
            if "unique" in str(exc).lower():
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    f"An aggregate with slug '{slug}' already exists.",
                ) from exc
            raise
    return _serialize(row)


class AggregatePatch(BaseModel):
    name:        Optional[str] = None
    description: Optional[str] = None
    components:  Optional[List[Dict[str, Any]]] = None


@aggregates_router.patch("/aggregates/{aggregate_id}")
async def update_aggregate(
    aggregate_id: uuid.UUID,
    payload: AggregatePatch,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(422, "Nothing to update.")

    sets, args, idx = ["updated_at = NOW()"], [], 1
    for column in ("name", "description"):
        if column in fields:
            sets.append(f"{column} = ${idx}")
            args.append(fields[column])
            idx += 1
    if "components" in fields:
        try:
            components = parse_components(fields["components"])
        except AggregateError as exc:
            raise HTTPException(422, str(exc)) from exc
        sets.append(f"components = ${idx}::jsonb")
        args.append(json.dumps(components))
        idx += 1

    args += [aggregate_id, org_id]
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE aggregate_scores SET {', '.join(sets)} "
            f"WHERE aggregate_id = ${idx} AND org_id = ${idx + 1} RETURNING *",
            *args,
        )
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Aggregate not found")
    return _serialize(row)


@aggregates_router.delete(
    "/aggregates/{aggregate_id}", status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_aggregate(
    aggregate_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM aggregate_scores WHERE aggregate_id = $1 AND org_id = $2",
            aggregate_id, org_id,
        )
    if result != "DELETE 1":
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Aggregate not found")


async def for_org(org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """An org's aggregates, for reports that apply them. Best-effort: a report
    is still a report without its composite."""
    try:
        async with postgres_client.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM aggregate_scores WHERE org_id = $1", org_id,
            )
        return [_serialize(r) for r in rows]
    except Exception:  # noqa: BLE001
        return []


__all__ = ["aggregates_router", "for_org"]
