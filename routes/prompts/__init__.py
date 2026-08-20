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
from pydantic import BaseModel, Field, field_validator

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
from shared.code_scorer import (
    BUILTIN_NAMES,
    SCOPE_NAMES,
    ScorerError,
    compile_scorer,
    run_scorer,
)
from shared.placeholders import ANSWER_PLACEHOLDER_RE

prompts_router = APIRouter()

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$")

#: What a saved prompt can be. 'code' is a deterministic scorer rather than a
#: prompt, but it lives here to inherit slugs, versioning, and env deployment —
#: a scorer needs all three for exactly the reasons a prompt does.
VALID_PROMPT_KINDS = frozenset({"completion", "judge", "code"})


# ── Pydantic models ───────────────────────────────────────────────────────────

MAX_PROMPT_TOOLS = 32
MAX_MCP_SERVERS = 8

#: What a judge prompt grades. An agent fails in more than one place — the wrong
#: tool, the wrong documents, the wrong order, the wrong path, the wrong final
#: answer — and each is a different prompt with different evidence in front of
#: it. 'output' is the default and the only one that needs no trace.
VALID_SCORE_TARGETS = frozenset({
    "output", "retrieval", "tools", "trajectory", "coordination",
})


class ToolDef(BaseModel):
    """A tool the prompt's model may call.

    Carried on the prompt rather than only on a run, because for an agentic
    prompt the toolset *is* part of the prompt: the same template with a
    different set of tools is a different thing to evaluate, and tool selection
    is the layer the agentic evaluator grades.
    """
    name:        str
    description: str = ""
    parameters:  Dict[str, Any] = Field(default_factory=lambda: {"type": "object", "properties": {}})


class McpServerDef(BaseModel):
    """An MCP server whose tools the prompt may call.

    ``tools`` is optional: naming the server alone records the dependency, and
    listing its tools lets the evaluator score whether the right one was picked
    without having to reach the server at eval time.
    """
    label:       str
    url:         Optional[str] = None
    description: str = ""
    tools:       List[ToolDef] = Field(default_factory=list)


def _validate_tools(tools: Optional[List[ToolDef]]) -> List[Dict[str, Any]]:
    """Reject duplicate or unnamed tools at author time.

    Providers reject duplicates too, but the error they return names the JSON
    schema rather than the tool, which is no help when it surfaces mid-run.
    """
    if not tools:
        return []
    if len(tools) > MAX_PROMPT_TOOLS:
        raise ValueError(f"At most {MAX_PROMPT_TOOLS} tools per prompt.")
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for tool in tools:
        name = (tool.name or "").strip()
        if not name:
            raise ValueError("Every tool needs a name.")
        if name in seen:
            raise ValueError(f"Duplicate tool: {name!r}")
        seen.add(name)
        out.append({
            "name":        name,
            "description": (tool.description or "").strip(),
            "parameters":  tool.parameters or {"type": "object", "properties": {}},
            "kind":        "tool",
        })
    return out


def _validate_mcp(servers: Optional[List[McpServerDef]]) -> List[Dict[str, Any]]:
    """Same contract as tools, one level down.

    A tool name must be unique across the whole offered set, not just within
    its server — the model picks by name and never sees which server a name
    came from, so two servers exporting ``search`` is genuinely ambiguous.
    """
    if not servers:
        return []
    if len(servers) > MAX_MCP_SERVERS:
        raise ValueError(f"At most {MAX_MCP_SERVERS} MCP servers per prompt.")
    seen_labels: set[str] = set()
    seen_tools: set[str] = set()
    out: List[Dict[str, Any]] = []
    for server in servers:
        label = (server.label or "").strip()
        if not label:
            raise ValueError("Every MCP server needs a label.")
        if label in seen_labels:
            raise ValueError(f"Duplicate MCP server: {label!r}")
        seen_labels.add(label)
        tools: List[Dict[str, Any]] = []
        for tool in server.tools or []:
            name = (tool.name or "").strip()
            if not name:
                raise ValueError(f"MCP server {label!r}: every tool needs a name.")
            if name in seen_tools:
                raise ValueError(
                    f"Tool {name!r} is offered by more than one MCP server. "
                    f"The model selects by name, so the duplicate is ambiguous."
                )
            seen_tools.add(name)
            tools.append({
                "name":        name,
                "description": (tool.description or "").strip(),
                "parameters":  tool.parameters or {"type": "object", "properties": {}},
                "kind":        "mcp",
                "server":      label,
            })
        out.append({
            "label":       label,
            "url":         (server.url or "").strip() or None,
            "description": (server.description or "").strip(),
            "tools":       tools,
        })
    return out


class SavePromptRequest(BaseModel):
    name:      str
    slug:      str
    template:  str
    model:     Optional[str] = None
    variables: List[str]     = []
    kind:      str           = "completion"
    # Agentic prompts carry what the model may call. Stored under the prompt's
    # existing ``config`` jsonb, so this needs no migration.
    tools:       List[ToolDef]      = Field(default_factory=list)
    mcp_servers: List[McpServerDef] = Field(default_factory=list)
    #: Which part of a run a judge prompt grades. Ignored for non-judge kinds.
    target:      str                = "output"

    @field_validator("target")
    @classmethod
    def _validate_target(cls, v: str) -> str:
        v = (v or "output").strip().lower()
        if v not in VALID_SCORE_TARGETS:
            raise ValueError(
                f"target must be one of: {', '.join(sorted(VALID_SCORE_TARGETS))}"
            )
        return v

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, v: str) -> str:
        v = (v or "completion").strip().lower()
        if v not in VALID_PROMPT_KINDS:
            raise ValueError(f"kind must be one of: {', '.join(sorted(VALID_PROMPT_KINDS))}")
        return v

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
    # Omitted leaves the prompt's toolset untouched; an empty list clears it.
    # Without that distinction there would be no way to remove the last tool.
    tools:       Optional[List[ToolDef]]      = None
    mcp_servers: Optional[List[McpServerDef]] = None
    target:      Optional[str]                = None


class DeployRequest(BaseModel):
    deploy: bool = True


# ── Code scorers ──────────────────────────────────────────────────────────────

def _validate_code_scorer(source: str) -> None:
    """Reject an invalid code scorer at author time.

    A scorer that fails to compile would otherwise be skipped silently on every
    example of every run — the run would complete, the metric would simply be
    absent, and nothing would say why.
    """
    try:
        compile_scorer(source)
    except ScorerError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc),
        ) from exc


class TestScorerRequest(BaseModel):
    """A scorer plus one example to try it on."""
    source:   str
    output:   str = ""
    expected: str = ""
    input:    str = ""
    metadata: Dict[str, Any] = {}


@prompts_router.get("/prompts/code-scorer/reference")
async def code_scorer_reference(_session: dict = Depends(get_current_session)):
    """What a code scorer may reference, for the editor's help panel.

    Served rather than duplicated in the frontend so the two can't disagree
    about which helpers exist.
    """
    return {
        "scope":    list(SCOPE_NAMES),
        "builtins": list(BUILTIN_NAMES),
    }


@prompts_router.post("/prompts/code-scorer/test")
async def test_code_scorer(
    payload: TestScorerRequest,
    _session: dict = Depends(get_current_session),
):
    """Run a code scorer against one example and return what it scored.

    Authoring a scorer blind and finding out across a whole run is the slow way
    to get it wrong, so the editor can try one here first. Errors come back 200
    with ``ok: false`` — a scorer that legitimately rejects its input is a normal
    result to render, not a failed request.
    """
    try:
        score, reason = run_scorer(
            payload.source,
            output=payload.output,
            expected=payload.expected,
            input=payload.input,
            metadata=payload.metadata,
        )
    except ScorerError as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "score": score, "reason": reason}


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
    if payload.kind == "judge" and not ANSWER_PLACEHOLDER_RE.search(payload.template):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "A judge prompt must reference the {{answer}} placeholder. "
                "Use {{question}}, {{answer}}, and {{context}} to inject the data "
                "under evaluation, and ask the model to return a JSON object with "
                "a numeric \"score\" (0-1) and a \"reason\"."
            ),
        )
    if payload.kind == "code":
        _validate_code_scorer(payload.template)
    try:
        tools = _validate_tools(payload.tools)
        mcp = _validate_mcp(payload.mcp_servers)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc),
        ) from exc
    # Only write a config when there is something in it, so an ordinary
    # completion prompt keeps a NULL config rather than an empty envelope.
    config = {k: v for k, v in {"tools": tools, "mcp_servers": mcp}.items() if v}
    # Only recorded for judges, and only when it isn't the default — an
    # 'output' target stored explicitly would imply a choice nobody made.
    if payload.kind == "judge" and payload.target != "output":
        config["target"] = payload.target
    try:
        row = await create_prompt(
            org_id=org_id,
            name=payload.name,
            slug=payload.slug,
            template=payload.template,
            model=payload.model,
            variables=payload.variables,
            kind=payload.kind,
            config=config or None,
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
    # An edit that breaks a code scorer must be rejected here, not discovered
    # once it has silently skipped every example of the next run.
    if payload.template is not None:
        existing = await get_prompt_full(prompt_id, org_id)
        if existing and existing.get("kind") == "code":
            _validate_code_scorer(payload.template)

    # Toolset edits merge into the existing config rather than replacing it, so
    # editing tools can't silently drop the MCP servers (or a code scorer's
    # settings) that share the same column.
    config_kwargs: Dict[str, Any] = {}
    if (
        payload.tools is not None
        or payload.mcp_servers is not None
        or payload.target is not None
    ):
        current = await get_prompt_full(prompt_id, org_id)
        # _prompt_config, not dict(): asyncpg hands jsonb back as a string, and
        # dict() on a string raises rather than parsing it.
        config = dict(_prompt_config(current or {}))
        try:
            if payload.tools is not None:
                config["tools"] = _validate_tools(payload.tools)
            if payload.mcp_servers is not None:
                config["mcp_servers"] = _validate_mcp(payload.mcp_servers)
            if payload.target is not None:
                target = payload.target.strip().lower()
                if target not in VALID_SCORE_TARGETS:
                    raise ValueError(
                        f"target must be one of: {', '.join(sorted(VALID_SCORE_TARGETS))}"
                    )
                # Dropped rather than stored when it returns to the default, so
                # the config doesn't accumulate no-op keys.
                if target == "output":
                    config.pop("target", None)
                else:
                    config["target"] = target
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc),
            ) from exc
        config = {k: v for k, v in config.items() if v}
        # An emptied config becomes NULL rather than {}, matching create.
        config_kwargs = {"config": config} if config else {"clear_config": True}

    row = await update_prompt(
        prompt_id, org_id,
        name=payload.name,
        template=payload.template,
        model=payload.model,
        variables=payload.variables,
        **config_kwargs,
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


def _prompt_config(row: dict) -> dict:
    """The prompt's config as a dict, whatever the driver handed back.

    asyncpg returns a jsonb column as a string unless a codec is registered,
    and the two shapes reaching the serializer would otherwise produce a
    toolset that is sometimes a list and sometimes a character.
    """
    raw = row.get("config")
    if isinstance(raw, str):
        import json as _json
        try:
            raw = _json.loads(raw)
        except ValueError:
            return {}
    return raw if isinstance(raw, dict) else {}


def _serialize(row: dict) -> dict:
    config = _prompt_config(row)
    return {
        "prompt_id":    str(row["prompt_id"]),
        "org_id":       str(row["org_id"]),
        "name":         row["name"],
        "slug":         row["slug"],
        "template":     row["template"],
        "model":        row.get("model"),
        "variables":    row.get("variables") or [],
        "kind":         row.get("kind") or "completion",
        "tools":        config.get("tools") or [],
        "mcp_servers":  config.get("mcp_servers") or [],
        "target":       config.get("target") or "output",
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
    config = _prompt_config(row)
    return {
        "prompt_id":  str(row["prompt_id"]),
        "slug":       row["slug"],
        "name":       row.get("name", ""),
        "template":   row["template"],
        "model":      row.get("model"),
        "variables":  variables,
        # The SDK needs these to hand the same toolset to the model that the
        # prompt was authored and evaluated with.
        "tools":       config.get("tools") or [],
        "mcp_servers": config.get("mcp_servers") or [],
        "target":      config.get("target") or "output",
        "version":    row.get("version", 1),
        "environment": row.get("environment", "production"),
        "deployed_at": row.get("deployed_at").isoformat()
                       if hasattr(row.get("deployed_at"), "isoformat") else row.get("deployed_at"),
    }


__all__ = ["prompts_router"]
