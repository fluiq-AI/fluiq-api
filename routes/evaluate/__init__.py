"""fluiq-api — POST /api/v1/evaluate

Runs LLM-as-judge evaluation for the metrics requested by fluiq.eval().
Stores results in the ClickHouse evaluations table and returns scores
synchronously so the SDK can apply thresholds in real-time.

Auth: API key in request body (same pattern as /secure).
No tier gate — all instrumented accounts can use evaluations.

Supported metrics: hallucination, faithfulness, relevance,
                   toxicity, coherence, completeness.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

import config
from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, wait_for_playground_reply
from db_queues.postgresql.auth import resolve_api_key
from realtime import trace_broker
from routes.auth.helper import get_current_session
from .judge import run_metrics, SUPPORTED_METRICS

evaluate_router = APIRouter()


# ── Auth helper ───────────────────────────────────────────────────────────────

async def _resolve_org(api_key: str) -> uuid.UUID:
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    return resolved[0]


# ── Request / response models ─────────────────────────────────────────────────

class EvaluateRequest(BaseModel):
    api_key:     str
    trace_id:    Optional[str]       = None
    model:       str                 = ""
    prompt:      str                 = ""
    response:    str                 = ""
    context:     str                 = ""
    metrics:     List[str]           = ["hallucination", "relevance"]
    judge_model: str                 = "claude-haiku-4-5-20251001"
    thresholds:  Dict[str, float]    = {}


class MetricResult(BaseModel):
    metric:  str
    score:   float
    reason:  str
    passed:  bool


class EvaluateResponse(BaseModel):
    trace_id: Optional[str]
    scores:   Dict[str, float]
    results:  List[MetricResult]
    passed:   bool
    failures: List[str]


# ── POST /evaluate ────────────────────────────────────────────────────────────

@evaluate_router.post("/evaluate", response_model=EvaluateResponse)
async def evaluate(payload: EvaluateRequest) -> EvaluateResponse:
    """Run LLM-as-judge evaluation for a prompt/response pair.

    Called by the SDK after each LLM call when ``fluiq.eval()`` is active.
    Each requested metric is scored 0–1 by a judge model running server-side.
    Results are stored in ClickHouse and returned synchronously so the SDK
    can enforce thresholds.
    """
    org_id = await _resolve_org(payload.api_key)

    valid_metrics = [m for m in payload.metrics if m in SUPPORTED_METRICS]
    if not valid_metrics:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"No valid metrics in request. Supported: {sorted(SUPPORTED_METRICS)}",
        )

    raw_results = run_metrics(
        valid_metrics,
        response=payload.response,
        prompt=payload.prompt,
        context=payload.context,
        judge_model=payload.judge_model,
    )

    # Persist to ClickHouse (best-effort)
    trace_id = payload.trace_id or str(uuid.uuid4())
    try:
        await clickhouse_client.insert_evaluations(
            organization_id=org_id,
            trace_id=trace_id,
            results=raw_results,
            judge_model=payload.judge_model,
        )
    except Exception:
        pass

    # Fan-out to SSE clients so the frontend merges each metric immediately
    # without waiting for a page refresh. One enriched message per metric
    # mirrors the shape produced by the evaluator worker.
    for metric, data in raw_results.items():
        try:
            await trace_broker.publish(
                str(org_id),
                {
                    "kind": "enriched",
                    "enrichment": "evaluation",
                    "organization_id": str(org_id),
                    "api_key_prefix": None,
                    "trace_id": trace_id,
                    "root_trace_id": trace_id,
                    "evaluation": {
                        "metric": metric,
                        "score": float(data.get("score", 0.0)),
                        "evaluator": "fluiq.eval",
                        "judge_model": payload.judge_model,
                        "details": {"reason": data.get("reason", "")},
                    },
                },
            )
        except Exception:
            pass

    # Build response
    thresholds = payload.thresholds
    failures:       list[str]          = []
    metric_results: list[MetricResult] = []

    for metric, data in raw_results.items():
        score     = data["score"]
        threshold = thresholds.get(metric, 0.0)
        passed    = score >= threshold
        if not passed and threshold > 0:
            failures.append(metric)
        metric_results.append(MetricResult(
            metric=metric,
            score=score,
            reason=data.get("reason", ""),
            passed=passed,
        ))

    return EvaluateResponse(
        trace_id=trace_id,
        scores={m: d["score"] for m, d in raw_results.items()},
        results=metric_results,
        passed=len(failures) == 0,
        failures=failures,
    )


# ── POST /evaluate/playground ─────────────────────────────────────────────────

class PlaygroundRequest(BaseModel):
    prompt:      str              = ""
    response:    str              = ""
    context:     str              = ""
    metrics:     List[str]        = ["hallucination", "relevance"]
    judge_model: str              = "claude-haiku-4-5-20251001"
    thresholds:  Dict[str, float] = {}


@evaluate_router.post("/evaluate/playground", response_model=EvaluateResponse)
async def evaluate_playground(
    payload: PlaygroundRequest,
    session: dict = Depends(get_current_session),
) -> EvaluateResponse:
    """Session-authenticated playground endpoint that dispatches to the eval worker.

    Publishes a ``playground_eval`` job to the Kafka eval topic and awaits the
    worker's reply via the playground reply consumer. Evaluation logic lives
    exclusively in the worker — the API process never calls the judge directly.
    """
    org_id = uuid.UUID(session["org_id"])

    valid_metrics = [m for m in payload.metrics if m in SUPPORTED_METRICS]
    if not valid_metrics:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"No valid metrics. Supported: {sorted(SUPPORTED_METRICS)}",
        )

    correlation_id = str(uuid.uuid4())
    trace_id       = str(uuid.uuid4())

    job = {
        "operation":      "playground_eval",
        "correlation_id": correlation_id,
        "organization_id": str(org_id),
        "trace_id":       trace_id,
        "prompt":         payload.prompt,
        "response":       payload.response,
        "context":        payload.context,
        "metrics":        valid_metrics,
        "judge_model":    payload.judge_model,
        "thresholds":     payload.thresholds,
    }
    await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))

    timeout = config.KAFKA_PLAYGROUND_CHECK_TIMEOUT or 30.0
    result = await wait_for_playground_reply(correlation_id, timeout=float(timeout))

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Evaluation worker did not respond in time. Try again.",
        )

    metric_results = [MetricResult(**r) for r in (result.get("results") or [])]
    return EvaluateResponse(
        trace_id=trace_id,
        scores=result.get("scores") or {},
        results=metric_results,
        passed=result.get("passed", True),
        failures=result.get("failures") or [],
    )


__all__ = ["evaluate_router", "EvaluateRequest", "EvaluateResponse"]