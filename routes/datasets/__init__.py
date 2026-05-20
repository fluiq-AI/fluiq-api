"""fluiq-api — Dataset management routes.

  GET    /api/v1/datasets                                   list datasets (session)
  POST   /api/v1/datasets                                   create dataset (session)
  DELETE /api/v1/datasets/{id}                              delete dataset (session)
  GET    /api/v1/datasets/{id}/examples                     list examples (session)
  POST   /api/v1/datasets/{id}/examples                     add example (session)
  DELETE /api/v1/datasets/{id}/examples/{example_id}        delete example (session)
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_validator

from db_queues.postgresql.datasets import (
    add_example,
    create_dataset,
    delete_dataset,
    delete_example,
    list_datasets,
    list_examples,
)
from routes.auth.helper import get_current_session

datasets_router = APIRouter()


# ── Pydantic models ───────────────────────────────────────────────────────────

class CreateDatasetRequest(BaseModel):
    name:        str
    description: Optional[str] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 200:
            raise ValueError("name must be 200 characters or fewer")
        return v


class AddExampleRequest(BaseModel):
    input:           str
    expected_output: Optional[str]       = None
    metadata:        Dict[str, Any]      = {}

    @field_validator("input")
    @classmethod
    def _validate_input(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("input is required")
        return v


# ── Endpoints ─────────────────────────────────────────────────────────────────

@datasets_router.get("/datasets")
async def get_datasets(session: dict = Depends(get_current_session)):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_datasets(org_id)
    return {"datasets": [_serialize_dataset(r) for r in rows]}


@datasets_router.post("/datasets", status_code=status.HTTP_201_CREATED)
async def create_new_dataset(
    payload: CreateDatasetRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    try:
        row = await create_dataset(org_id, payload.name, payload.description)
    except Exception as exc:
        if "unique" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A dataset named '{payload.name}' already exists.",
            )
        raise
    return _serialize_dataset({**row, "example_count": 0})


@datasets_router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_dataset(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    deleted = await delete_dataset(dataset_id, org_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")


@datasets_router.get("/datasets/{dataset_id}/examples")
async def get_examples(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_examples(dataset_id, org_id)
    return {"examples": [_serialize_example(r) for r in rows]}


@datasets_router.post("/datasets/{dataset_id}/examples", status_code=status.HTTP_201_CREATED)
async def add_dataset_example(
    dataset_id: uuid.UUID,
    payload: AddExampleRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    row = await add_example(
        dataset_id, org_id,
        input_text=payload.input,
        expected_output=payload.expected_output,
        metadata=payload.metadata,
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    return _serialize_example(row)


@datasets_router.delete(
    "/datasets/{dataset_id}/examples/{example_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_example(
    dataset_id: uuid.UUID,
    example_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    deleted = await delete_example(example_id, dataset_id, org_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Example not found")


# ── Serializers ───────────────────────────────────────────────────────────────

def _serialize_dataset(row: dict) -> dict:
    return {
        "dataset_id":    str(row["dataset_id"]),
        "org_id":        str(row["org_id"]),
        "name":          row["name"],
        "description":   row.get("description"),
        "example_count": row.get("example_count", 0),
        "created_at":    row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at":    row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def _serialize_example(row: dict) -> dict:
    meta = row.get("metadata") or {}
    if isinstance(meta, str):
        import json as _json
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    return {
        "example_id":      str(row["example_id"]),
        "dataset_id":      str(row["dataset_id"]),
        "org_id":          str(row["org_id"]),
        "input":           row["input"],
        "expected_output": row.get("expected_output"),
        "metadata":        meta,
        "created_at":      row["created_at"].isoformat() if row.get("created_at") else None,
    }


__all__ = ["datasets_router"]
