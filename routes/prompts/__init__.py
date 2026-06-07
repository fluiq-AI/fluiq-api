"""fluiq-api — Prompt management routes.

  GET    /api/v1/prompts                              list saved prompts (session)
  POST   /api/v1/prompts                              create prompt (session)
  PATCH  /api/v1/prompts/{id}                         update template/name (session)
  DELETE /api/v1/prompts/{id}                         delete (session)
  POST   /api/v1/prompts/{id}/deploy                  legacy deploy toggle (session)
  GET    /api/v1/prompts/{id}/versions                version history (session)
  POST   /api/v1/prompts/{id}/versions/{v}/restore    restore version (session)
  POST   /api/v1/prompts/{id}/environments/{env}      promote/unpromote env (session)
  GET    /api/v1/prompts/fetch/{slug}                 fetch by slug + env (API key)
"""
from __future__ import annotations

import re
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, field_validator

from db_queues.postgresql.auth import resolve_api_key
from db_queues.postgresql.prompts import (
    VALID_ENVIRONMENTS,
    create_prompt,
    delete_prompt,
    deploy_to_environment,
    get_deployed_prompt_for_org,
    get_prompt_for_environment,
    get_prompt_full,
    list_prompts,
    list_versions,
    restore_version,
    set_deployed,
    undeploy_from_environment,
    update_prompt,
)
from routes.auth.helper import extract_api_key, get_current_session

prompts_router = APIRouter()

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$")


# ── Pydantic models ───────────────────────────────────────────────────────────

class SavePromptRequest(BaseModel):
    name:      str
    slug:      str
    template:  str
    model:     Optional[str] = None
    variables: List[str]     = []

    @field_validator("slug")
    @classmethod
    def _validate_slug(cls, v: str) -> str:
        v = v.strip().lower()
        if not _SLUG_RE.match(v):
            raise ValueError(
                "slug must be 2–63 lowercase alphanumeric characters or hyphens, "
                "and cannot start or end with a hyphen"
            )
        return v

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        return v

    @field_validator("template")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("template is required")
        return v


class UpdatePromptRequest(BaseModel):
    name:      Optional[str]       = None
    template:  Optional[str]       = None
    model:     Optional[str]       = None
    variables: Optional[List[str]] = None


class DeployRequest(BaseModel):
    deploy: bool = True


# ── Session-authenticated CRUD ────────────────────────────────────────────────

@prompts_router.get("/prompts")
async def get_prompts(session: dict = Depends(get_current_session)):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_prompts(org_id)
    return {"prompts": [_serialize(r) for r in rows]}


@prompts_router.post("/prompts", status_code=status.HTTP_201_CREATED)
async def save_prompt(
    payload: SavePromptRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    try:
        row = await create_prompt(
            org_id=org_id,
            name=payload.name,
            slug=payload.slug,
            template=payload.template,
            model=payload.model,
            variables=payload.variables,
        )
    except Exception as exc:
        if "unique" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A prompt with slug '{payload.slug}' already exists.",
            )
        raise
    full = await get_prompt_full(row["prompt_id"], org_id)
    return _serialize(full or row)


@prompts_router.patch("/prompts/{prompt_id}")
async def edit_prompt(
    prompt_id: uuid.UUID,
    payload: UpdatePromptRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    row = await update_prompt(
        prompt_id, org_id,
        name=payload.name,
        template=payload.template,
        model=payload.model,
        variables=payload.variables,
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    full = await get_prompt_full(prompt_id, org_id)
    return _serialize(full or row)


@prompts_router.delete("/prompts/{prompt_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_prompt(
    prompt_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    deleted = await delete_prompt(prompt_id, org_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")


@prompts_router.post("/prompts/{prompt_id}/deploy")
async def deploy_prompt(
    prompt_id: uuid.UUID,
    payload: DeployRequest,
    session: dict = Depends(get_current_session),
):
    """Legacy single-flag deploy — kept for backward compat. Prefer /environments/{env}."""
    org_id = uuid.UUID(session["org_id"])
    row = await set_deployed(prompt_id, org_id, deploy=payload.deploy)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    full = await get_prompt_full(prompt_id, org_id)
    return _serialize(full or row)


# ── Named environment promotions ──────────────────────────────────────────────

@prompts_router.post("/prompts/{prompt_id}/environments/{environment}")
async def set_environment(
    prompt_id: uuid.UUID,
    environment: str,
    payload: DeployRequest,
    session: dict = Depends(get_current_session),
):
    """Promote the current prompt head to a named environment, or remove it."""
    if environment not in VALID_ENVIRONMENTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"environment must be one of: {', '.join(sorted(VALID_ENVIRONMENTS))}",
        )
    org_id = uuid.UUID(session["org_id"])
    if payload.deploy:
        full = await deploy_to_environment(prompt_id, org_id, environment)
    else:
        full = await undeploy_from_environment(prompt_id, org_id, environment)
    if full is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    return _serialize(full)


# ── Version history ───────────────────────────────────────────────────────────

@prompts_router.get("/prompts/{prompt_id}/versions")
async def get_versions(
    prompt_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_versions(prompt_id, org_id)
    return {"versions": [_serialize_version(r) for r in rows]}


@prompts_router.post("/prompts/{prompt_id}/versions/{version}/restore")
async def restore_prompt_version(
    prompt_id: uuid.UUID,
    version: int,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    row = await restore_version(prompt_id, org_id, version)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Version not found")
    full = await get_prompt_full(prompt_id, org_id)
    return _serialize(full or row)


# ── API-key-authenticated fetch (for SDK) ─────────────────────────────────────

@prompts_router.get("/prompts/fetch/{slug}")
async def fetch_prompt(
    slug: str,
    env: str = "production",
    api_key: Optional[str] = Depends(extract_api_key),
    api_key_query: Optional[str] = Query(default=None, alias="api_key"),
):
    """Fetch a deployed prompt by slug + environment. Used by the SDK.

    ``env`` defaults to ``production``. Falls back to the legacy ``is_deployed``
    flag when no environment row exists and env is ``production``.

    The API key arrives as an ``Authorization: Bearer`` header; the ``api_key``
    query parameter is still honoured for older SDK builds.
    """
    if env not in VALID_ENVIRONMENTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"env must be one of: {', '.join(sorted(VALID_ENVIRONMENTS))}",
        )
    api_key = api_key or api_key_query
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
        )
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, _prefix, _key_id = resolved

    row = await get_prompt_for_environment(org_id, slug, env)
    if row is None and env == "production":
        # Backward compat: honour the old is_deployed flag.
        row = await get_deployed_prompt_for_org(org_id, slug)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No prompt '{slug}' deployed to {env}.",
        )
    return _serialize_fetch(row)


# ── Serializers ───────────────────────────────────────────────────────────────

def _parse_environments(raw: Any) -> Dict[str, Any]:
    """Convert the json_object_agg result into a typed dict."""
    if not raw:
        return {"development": None, "staging": None, "production": None}
    if isinstance(raw, str):
        import json as _json
        raw = _json.loads(raw)
    out: Dict[str, Any] = {"development": None, "staging": None, "production": None}
    for env_name, env_data in (raw or {}).items():
        if env_data:
            out[env_name] = {
                "env_id":      env_data.get("env_id"),
                "version":     env_data.get("version"),
                "deployed_at": env_data.get("deployed_at"),
            }
    return out


def _serialize_version(row: dict) -> dict:
    vars_raw = row.get("variables") or []
    variables = [v["name"] if isinstance(v, dict) else v for v in vars_raw]
    return {
        "version_id": str(row["version_id"]),
        "prompt_id":  str(row["prompt_id"]),
        "version":    row["version"],
        "name":       row["name"],
        "template":   row["template"],
        "model":      row.get("model"),
        "variables":  variables,
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
    }


def _serialize(row: dict) -> dict:
    return {
        "prompt_id":    str(row["prompt_id"]),
        "org_id":       str(row["org_id"]),
        "name":         row["name"],
        "slug":         row["slug"],
        "template":     row["template"],
        "model":        row.get("model"),
        "variables":    row.get("variables") or [],
        "is_deployed":  bool(row.get("is_deployed", False)),
        "deployed_at":  row["deployed_at"].isoformat() if row.get("deployed_at") else None,
        "version":      row.get("version", 1),
        "environments": _parse_environments(row.get("environments")),
        "created_at":   row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at":   row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def _serialize_fetch(row: dict) -> dict:
    """Slimmer response for the SDK fetch endpoint (template + metadata only)."""
    vars_raw = row.get("variables") or []
    variables = [v["name"] if isinstance(v, dict) else v for v in vars_raw]
    return {
        "prompt_id":  str(row["prompt_id"]),
        "slug":       row["slug"],
        "name":       row.get("name", ""),
        "template":   row["template"],
        "model":      row.get("model"),
        "variables":  variables,
        "version":    row.get("version", 1),
        "environment": row.get("environment", "production"),
        "deployed_at": row.get("deployed_at").isoformat()
                       if hasattr(row.get("deployed_at"), "isoformat") else row.get("deployed_at"),
    }


__all__ = ["prompts_router"]
