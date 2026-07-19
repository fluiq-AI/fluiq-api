"""fluiq-api — Human feedback and annotations on traces.

Two human signal channels, stored in the ClickHouse ``evaluations`` table so
they ride the same read paths as judge scores (trace drawer, exports) while the
quality rollup excludes them (evaluator LIKE 'human.%'):

  POST /api/v1/feedback                        end-user feedback, API-key auth
                                               (SDK: ``fluiq.feedback(...)``)
  POST /api/v1/traces/{trace_id}/annotations   team annotation, session auth

evaluator = 'human.feedback' | 'human.annotation'; score is normalized to 0..1
(booleans map to 1/0), free-text lives in details.comment.
"""
from __future__ import annotations

import re
import uuid
from typing import Any, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_validator

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import resolve_api_key
from routes.auth.helper import extract_api_key, get_current_session

feedback_router = APIRouter()

_NAME_RE = re.compile(r"^[a-z][a-z0-9_\-]{0,63}$")
_COMMENT_CAP = 2000


def _coerce_score(value: Union[bool, int, float]) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="value must be a number or boolean")


def _parse_trace_id(raw: str) -> str:
    try:
        return str(uuid.UUID(str(raw)))
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=422, detail="trace_id must be a UUID")


class FeedbackRequest(BaseModel):
    api_key:       Optional[str] = None       # also accepted via header
    trace_id:      str
    root_trace_id: Optional[str] = None
    name:          str = "user_feedback"      # e.g. "thumbs", "csat"
    value:         Union[bool, float]
    comment:       Optional[str] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        v = (v or "user_feedback").strip().lower()
        if not _NAME_RE.match(v):
            raise ValueError(
                "name must be lowercase letters/digits/underscores/hyphens, "
                "starting with a letter"
            )
        return v


class AnnotationRequest(BaseModel):
    value:         Union[bool, float]
    root_trace_id: Optional[str] = None
    metric:        Optional[str] = None       # judge metric this annotation targets
    comment:       Optional[str] = None


@feedback_router.post("/feedback", status_code=status.HTTP_202_ACCEPTED)
async def submit_feedback(
    payload: FeedbackRequest,
    api_key: Optional[str] = Depends(extract_api_key),
) -> dict[str, Any]:
    """Record end-user feedback (thumbs, rating) against a trace."""
    key = api_key or payload.api_key
    if not key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, prefix, _key_id = resolved

    trace_id = _parse_trace_id(payload.trace_id)
    root_trace_id = (
        _parse_trace_id(payload.root_trace_id) if payload.root_trace_id else trace_id
    )
    details: dict[str, Any] = {"source": "end_user"}
    if payload.comment and payload.comment.strip():
        details["comment"] = payload.comment.strip()[:_COMMENT_CAP]

    await clickhouse_client.insert_human_score(
        organization_id=org_id,
        trace_id=trace_id,
        root_trace_id=root_trace_id,
        evaluator="human.feedback",
        metric=payload.name,
        score=_coerce_score(payload.value),
        details=details,
        api_key_prefix=prefix or "",
    )
    return {"ok": True}


@feedback_router.post(
    "/traces/{trace_id}/annotations", status_code=status.HTTP_201_CREATED
)
async def annotate_trace(
    trace_id: str,
    payload: AnnotationRequest,
    session: dict = Depends(get_current_session),
) -> dict[str, Any]:
    """Record a team member's verdict on a trace (or on one judge metric of it),
    e.g. agreeing/disagreeing with an automated score."""
    org_id = uuid.UUID(session["org_id"])
    tid = _parse_trace_id(trace_id)
    root_tid = _parse_trace_id(payload.root_trace_id) if payload.root_trace_id else tid

    details: dict[str, Any] = {
        "source":       "dashboard",
        "annotated_by": session["sub"],
    }
    if payload.metric and payload.metric.strip():
        details["target_metric"] = payload.metric.strip()[:120]
    if payload.comment and payload.comment.strip():
        details["comment"] = payload.comment.strip()[:_COMMENT_CAP]

    await clickhouse_client.insert_human_score(
        organization_id=org_id,
        trace_id=tid,
        root_trace_id=root_tid,
        evaluator="human.annotation",
        metric="annotation",
        score=_coerce_score(payload.value),
        details=details,
    )
    return {"ok": True}


__all__ = ["feedback_router"]
