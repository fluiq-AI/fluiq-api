"""fluiq-api — Review: the human half of the quality loop.

  GET    /api/v1/review/queue           traces wanting a human (session)
  GET    /api/v1/review/matrix          human verdict × judge score (session)
  POST   /api/v1/traces/{id}/flag       raise a review flag (session)
  DELETE /api/v1/traces/{id}/flag       resolve it (session)
  POST   /api/v1/review/to-dataset      send reviewed traces to a dataset (session)
  GET    /api/v1/review/judge-agreement scored the judge against humans (session)
  GET    /api/v1/review/labelled-metrics which pairings can be compared (session)

Feedback was already being collected — ``fluiq.feedback()`` has written to
ClickHouse for a while — and doing nothing with it is the expensive kind of
waste, because it is the only signal in the system that a real person produced.

This surface closes that loop: find the traces a human or a judge thinks are
bad, look at them, and turn the ones worth keeping into dataset examples so the
next eval run catches the same failure.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from db_queues.clickhouse import clickhouse_client
from shared import judge_agreement
from db_queues.postgresql.datasets import add_examples_bulk
from db_queues.postgresql.dataset_scorers import dataset_owned
from routes.auth.helper import get_current_session

review_router = APIRouter()

#: Below this a score counts as "bad" for the queue and the matrix. The same
#: number as the compare view's pass mark, so a trace called failing in one place
#: is not called passing in another.
REVIEW_THRESHOLD = 0.5

VALID_SOURCES = frozenset({"all", "flagged", "feedback", "low_score"})


class FlagRequest(BaseModel):
    note:   str = Field("", max_length=1000)
    reason: str = "manual"


class ToDatasetRequest(BaseModel):
    dataset_id: uuid.UUID
    trace_ids:  List[uuid.UUID]


@review_router.get("/review/queue")
async def get_queue(
    hours: int = Query(168, ge=1, le=8760),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    source: str = Query("all"),
    session: dict = Depends(get_current_session),
):
    """Traces wanting a human, worst first."""
    if source not in VALID_SOURCES:
        raise HTTPException(
            status_code=422,
            detail=f"source must be one of: {', '.join(sorted(VALID_SOURCES))}",
        )
    org_id = uuid.UUID(session["org_id"])
    items = await clickhouse_client.fetch_review_queue(
        org_id, hours=hours, limit=limit, offset=offset, source=source,
    )
    return {"items": items, "limit": limit, "offset": offset, "source": source}


@review_router.get("/review/matrix")
async def get_matrix(
    hours: int = Query(168, ge=1, le=8760),
    session: dict = Depends(get_current_session),
):
    """Human verdict crossed with judge score, plus what each cell means.

    The verdicts are served with the counts rather than left to the frontend: the
    whole value of this view is knowing *which* of the eval and the app to fix,
    and that reading should not be re-invented per client.
    """
    org_id = uuid.UUID(session["org_id"])
    matrix = await clickhouse_client.fetch_review_matrix(
        org_id, hours=hours, threshold=REVIEW_THRESHOLD,
    )
    return {
        "window_hours": hours,
        "threshold":    REVIEW_THRESHOLD,
        **matrix,
        "verdicts": {
            "agreed_good": {
                "label":  "Working",
                "detail": "People and judges both say these are good. Nothing to do.",
                "action": None,
            },
            "judge_harsh": {
                "label":  "Judge too harsh",
                "detail": (
                    "People liked these; the judge marked them down. The score is "
                    "wrong, not the app."
                ),
                "action": "fix_eval",
            },
            "judge_lenient": {
                "label":  "Judge too lenient",
                "detail": (
                    "The judge passed these; people didn't. This is the dangerous "
                    "cell — every one of these shipped unnoticed."
                ),
                "action": "fix_eval",
            },
            "agreed_bad": {
                "label":  "Genuinely bad",
                "detail": (
                    "People and judges agree these are bad. The eval is working; "
                    "the app needs the work."
                ),
                "action": "fix_app",
            },
        },
    }


@review_router.post("/traces/{trace_id}/flag", status_code=status.HTTP_201_CREATED)
async def flag_trace(
    trace_id: uuid.UUID,
    payload: FlagRequest,
    session: dict = Depends(get_current_session),
):
    """Mark a trace as needing a human."""
    await clickhouse_client.set_review_flag(
        organization_id=uuid.UUID(session["org_id"]),
        trace_id=trace_id,
        status="open",
        reason=payload.reason if payload.reason in ("manual", "low_score", "feedback") else "manual",
        note=payload.note.strip(),
        actor=str(session.get("sub") or ""),
    )
    return {"status": "open"}


@review_router.delete("/traces/{trace_id}/flag", status_code=status.HTTP_204_NO_CONTENT)
async def resolve_flag(
    trace_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """Resolve a flag.

    Recorded as resolved rather than deleted, so the queue can exclude it while
    the fact that someone dealt with it survives — a flag that vanishes leaves no
    evidence the review happened.
    """
    await clickhouse_client.set_review_flag(
        organization_id=uuid.UUID(session["org_id"]),
        trace_id=trace_id,
        status="resolved",
        actor=str(session.get("sub") or ""),
    )


@review_router.post("/review/to-dataset", status_code=status.HTTP_201_CREATED)
async def send_to_dataset(
    payload: ToDatasetRequest,
    session: dict = Depends(get_current_session),
):
    """Turn reviewed traces into dataset examples — the flywheel.

    A bad response you looked at is worth more as a test case than as a
    memory. Each trace becomes an example whose ``expected_output`` is left
    empty on purpose: what the model *did* say is wrong by construction here, so
    filling it in would enshrine the failure as the target.
    """
    org_id = uuid.UUID(session["org_id"])
    if not await dataset_owned(payload.dataset_id, org_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    if not payload.trace_ids:
        raise HTTPException(status_code=422, detail="No traces selected")
    if len(payload.trace_ids) > 100:
        raise HTTPException(status_code=422, detail="Select at most 100 traces at a time")

    rows: List[tuple] = []
    for trace_id in payload.trace_ids:
        traces = await clickhouse_client.fetch_traces(
            org_id, root_trace_id=trace_id, limit=1,
        )
        if not traces:
            continue
        event = traces[0].get("event") or {}
        if not isinstance(event, dict):
            continue
        question = _question_of(event)
        if not question:
            continue
        rows.append((
            question,
            None,   # expected_output — see the docstring
            {
                "source_trace_id": str(trace_id),
                "from_review":     True,
                **({"model": event["model"]} if isinstance(event.get("model"), str) else {}),
            },
        ))

    if not rows:
        raise HTTPException(
            status_code=422,
            detail="None of the selected traces had an input to capture.",
        )
    added = await add_examples_bulk(payload.dataset_id, org_id, rows)
    return {"added": added, "requested": len(payload.trace_ids)}


@review_router.get("/review/labelled-metrics")
async def get_labelled_metrics(
    hours: int = Query(720, ge=1, le=8760),
    session: dict = Depends(get_current_session),
):
    """The judge metrics and rubric fields available to compare."""
    org_id = uuid.UUID(session["org_id"])
    return {
        "window_hours": hours,
        **await clickhouse_client.fetch_labelled_metrics(org_id, hours=hours),
    }


@review_router.get("/review/judge-agreement")
async def get_judge_agreement(
    judge_metric: str = Query(..., min_length=1, max_length=120),
    human_field:  Optional[str] = Query(None, max_length=120),
    hours:        int = Query(720, ge=1, le=8760),
    threshold:    float = Query(REVIEW_THRESHOLD, ge=0.0, le=1.0),
    session: dict = Depends(get_current_session),
):
    """Score the judge itself against the humans who labelled the same traces.

    The workshop's closing move (74:43), and the one thing that turns a judge
    from an opinion into a measurement: run it over examples people already
    graded and check that it agrees.

    The disagreeing examples come back with the aggregate, because the number
    tells you a judge is wrong and only the rows tell you how to fix it.
    """
    org_id = uuid.UUID(session["org_id"])
    pairs = await clickhouse_client.fetch_judge_agreement_pairs(
        org_id, judge_metric=judge_metric, human_field=human_field, hours=hours,
    )
    return {
        "judge_metric": judge_metric,
        "human_field":  human_field,
        "window_hours": hours,
        "threshold":    threshold,
        **judge_agreement.compare(pairs, threshold=threshold),
        "disagreements": judge_agreement.disagreements(pairs, threshold=threshold),
    }


def _question_of(event: Dict[str, Any]) -> str:
    """The user-side text of an event, however the integration recorded it."""
    messages = event.get("messages")
    if isinstance(messages, list):
        for message in reversed(messages):
            if isinstance(message, dict) and message.get("role") == "user":
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content
    for key in ("input", "prompt", "question"):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


__all__ = ["review_router", "REVIEW_THRESHOLD"]
