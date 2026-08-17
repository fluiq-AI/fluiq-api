"""fluiq-api — Saved views.

  GET    /api/v1/views?surface=traces   list views (session)
  POST   /api/v1/views                  create (session)
  PATCH  /api/v1/views/{id}             rename / re-filter / share (session)
  DELETE /api/v1/views/{id}             delete (session)

A view is a named set of filters over a dashboard list. Filters are otherwise
ephemeral component state — you narrow the trace list to the thing you care
about and it's gone on reload, and unreachable by anyone else. A view is what
turns a filter into a review queue the team shares.
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, field_validator

from db_queues.postgresql import postgres_client
from routes.auth.helper import get_current_session

views_router = APIRouter()

#: Surfaces that can hold a view. A view is meaningless on a list whose filters
#: it doesn't share, so this is an allowlist rather than free text.
VALID_SURFACES = frozenset({"traces"})

MAX_VIEWS_PER_SURFACE = 50
#: Filters are opaque, so this bounds them by size rather than by shape.
MAX_FILTER_BYTES = 8_000


class ViewPayload(BaseModel):
    name:        str
    surface:     str = "traces"
    description: Optional[str] = None
    filters:     Dict[str, Any] = {}
    shared:      bool = True
    project_id:  Optional[uuid.UUID] = None

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 80:
            raise ValueError("name must be 80 characters or fewer")
        return v

    @field_validator("surface")
    @classmethod
    def _surface(cls, v: str) -> str:
        v = (v or "traces").strip().lower()
        if v not in VALID_SURFACES:
            raise ValueError(f"surface must be one of: {', '.join(sorted(VALID_SURFACES))}")
        return v

    @field_validator("filters")
    @classmethod
    def _filters(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if len(json.dumps(v)) > MAX_FILTER_BYTES:
            raise ValueError("filters are too large")
        return v


class ViewPatch(BaseModel):
    name:        Optional[str] = None
    description: Optional[str] = None
    filters:     Optional[Dict[str, Any]] = None
    shared:      Optional[bool] = None


def _serialize(row: Any) -> Dict[str, Any]:
    filters = row["filters"]
    if isinstance(filters, str):
        try:
            filters = json.loads(filters)
        except ValueError:
            filters = {}
    return {
        "view_id":     str(row["view_id"]),
        "surface":     row["surface"],
        "name":        row["name"],
        "description": row.get("description"),
        "filters":     filters or {},
        "shared":      bool(row["shared"]),
        "created_by":  str(row["created_by"]) if row.get("created_by") else None,
        "project_id":  str(row["project_id"]) if row.get("project_id") else None,
        "created_at":  row["created_at"].isoformat() if row.get("created_at") else None,
    }


@views_router.get("/views")
async def list_views(
    surface: str = Query(default="traces"),
    session: dict = Depends(get_current_session),
):
    """Views for a surface: the org's shared ones, plus the caller's private ones.

    A private view belonging to a teammate is deliberately invisible — that is
    what "private" has to mean for the shared/private distinction to be worth
    offering at all.
    """
    org_id = uuid.UUID(session["org_id"])
    user_id = session.get("sub")
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM saved_views
            WHERE org_id = $1 AND surface = $2
              AND (shared OR created_by = $3::uuid)
            ORDER BY name
            """,
            org_id, surface, str(user_id) if user_id else None,
        )
    return {"views": [_serialize(r) for r in rows]}


@views_router.post("/views", status_code=status.HTTP_201_CREATED)
async def create_view(
    payload: ViewPayload,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    user_id = session.get("sub")
    async with postgres_client.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT count(*) FROM saved_views WHERE org_id = $1 AND surface = $2",
            org_id, payload.surface,
        )
        if existing >= MAX_VIEWS_PER_SURFACE:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"This surface already has {MAX_VIEWS_PER_SURFACE} views. "
                    "Delete one that has stopped being useful."
                ),
            )
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO saved_views
                    (org_id, project_id, surface, name, description, filters, shared, created_by)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::uuid)
                RETURNING *
                """,
                org_id, payload.project_id, payload.surface, payload.name,
                payload.description, json.dumps(payload.filters), payload.shared,
                str(user_id) if user_id else None,
            )
        except Exception as exc:  # noqa: BLE001
            if "unique" in str(exc).lower():
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"A view named '{payload.name}' already exists here.",
                ) from exc
            raise
    return _serialize(row)


@views_router.patch("/views/{view_id}")
async def update_view(
    view_id: uuid.UUID,
    payload: ViewPatch,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update.")
    if "name" in fields and not (fields["name"] or "").strip():
        raise HTTPException(status_code=422, detail="name cannot be empty")
    if "filters" in fields and len(json.dumps(fields["filters"])) > MAX_FILTER_BYTES:
        raise HTTPException(status_code=422, detail="filters are too large")

    sets = ["updated_at = NOW()"]
    args: list[Any] = []
    idx = 1
    for column in ("name", "description", "shared"):
        if column in fields:
            sets.append(f"{column} = ${idx}")
            args.append(fields[column])
            idx += 1
    if "filters" in fields:
        sets.append(f"filters = ${idx}::jsonb")
        args.append(json.dumps(fields["filters"]))
        idx += 1

    args += [view_id, org_id]
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE saved_views SET {', '.join(sets)}
            WHERE view_id = ${idx} AND org_id = ${idx + 1}
            RETURNING *
            """,
            *args,
        )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="View not found")
    return _serialize(row)


@views_router.delete("/views/{view_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_view(
    view_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM saved_views WHERE view_id = $1 AND org_id = $2",
            view_id, org_id,
        )
    if result != "DELETE 1":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="View not found")


__all__ = ["views_router"]
