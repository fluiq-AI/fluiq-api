"""fluiq-api — Org-facing judge-prompt customization.

Lets a customer read the LLM-as-Judge prompt templates their evaluations use
and fork any of them for their own org. The evaluator worker resolves
org override → platform template → code default, so a reset simply deletes the
override row.

  GET    /api/v1/eval/judge-prompts                        list, with org override state
  PUT    /api/v1/eval/judge-prompts/{name}                 save org override (validated)
  POST   /api/v1/eval/judge-prompts/{name}/reset           revert to the platform prompt
  GET    /api/v1/eval/judge-prompts/{name}/versions        org version history
  POST   /api/v1/eval/judge-prompts/{name}/restore/{v}     restore an org version
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from db_queues.postgresql.eval_prompts_org import (
    get_org_judge_prompt,
    list_org_judge_prompt_versions,
    list_org_judge_prompts,
    reset_org_judge_prompt,
    restore_org_judge_prompt_version,
    upsert_org_judge_prompt,
)
from routes.auth.helper import get_current_session
from shared.placeholders import identifiers

judge_prompts_router = APIRouter()

_identifiers = identifiers


class OrgJudgePromptView(BaseModel):
    name: str
    description: Optional[str]
    required_vars: List[str]
    template: str            # what this org's evaluations effectively use
    platform_template: str   # what a reset reverts to
    is_overridden: bool      # True when this org has forked the prompt
    version: int             # 0 until the org saves its first override
    updated_at: datetime


class OrgJudgePromptListResponse(BaseModel):
    prompts: list[OrgJudgePromptView]


class UpdateOrgJudgePromptRequest(BaseModel):
    template: str


class OrgJudgePromptVersionView(BaseModel):
    version_id: uuid.UUID
    name: str
    version: int
    template: str
    updated_by: Optional[uuid.UUID]
    created_at: datetime


class OrgJudgePromptVersionsResponse(BaseModel):
    versions: list[OrgJudgePromptVersionView]


def _validate_template(prompt_row: dict, template: str) -> None:
    """Reject an edit that is empty or drops a required placeholder — a saved
    override missing one would silently fall back to the platform prompt in the
    worker, which is worse than failing loudly here."""
    if not template or not template.strip():
        raise HTTPException(status_code=400, detail="Template cannot be empty.")
    required = set(prompt_row.get("required_vars") or [])
    missing = sorted(required - _identifiers(template))
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                "Template is missing required placeholder(s): "
                + ", ".join("{{" + m + "}}" for m in missing)
            ),
        )


@judge_prompts_router.get("/eval/judge-prompts", response_model=OrgJudgePromptListResponse)
async def org_judge_prompts_list(
    session: dict = Depends(get_current_session),
) -> OrgJudgePromptListResponse:
    org_id = uuid.UUID(session["org_id"])
    rows = await list_org_judge_prompts(org_id)
    return OrgJudgePromptListResponse(prompts=[OrgJudgePromptView(**r) for r in rows])


@judge_prompts_router.put("/eval/judge-prompts/{name}", response_model=OrgJudgePromptView)
async def org_judge_prompt_update(
    name: str,
    body: UpdateOrgJudgePromptRequest,
    session: dict = Depends(get_current_session),
) -> OrgJudgePromptView:
    org_id = uuid.UUID(session["org_id"])
    row = await get_org_judge_prompt(org_id, name)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    _validate_template(row, body.template)
    updated = await upsert_org_judge_prompt(
        org_id, name, body.template, updated_by=uuid.UUID(session["sub"])
    )
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    return OrgJudgePromptView(**updated)


@judge_prompts_router.post("/eval/judge-prompts/{name}/reset", response_model=OrgJudgePromptView)
async def org_judge_prompt_reset(
    name: str,
    session: dict = Depends(get_current_session),
) -> OrgJudgePromptView:
    org_id = uuid.UUID(session["org_id"])
    row = await reset_org_judge_prompt(org_id, name)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    return OrgJudgePromptView(**row)


@judge_prompts_router.get(
    "/eval/judge-prompts/{name}/versions", response_model=OrgJudgePromptVersionsResponse
)
async def org_judge_prompt_versions(
    name: str,
    session: dict = Depends(get_current_session),
) -> OrgJudgePromptVersionsResponse:
    org_id = uuid.UUID(session["org_id"])
    rows = await list_org_judge_prompt_versions(org_id, name)
    return OrgJudgePromptVersionsResponse(
        versions=[OrgJudgePromptVersionView(**r) for r in rows]
    )


@judge_prompts_router.post(
    "/eval/judge-prompts/{name}/restore/{version}", response_model=OrgJudgePromptView
)
async def org_judge_prompt_restore(
    name: str,
    version: int,
    session: dict = Depends(get_current_session),
) -> OrgJudgePromptView:
    org_id = uuid.UUID(session["org_id"])
    row = await restore_org_judge_prompt_version(
        org_id, name, version, updated_by=uuid.UUID(session["sub"])
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Prompt or version not found"
        )
    return OrgJudgePromptView(**row)


__all__ = ["judge_prompts_router"]
