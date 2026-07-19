"""fluiq-api — Dataset management routes.

  GET    /api/v1/datasets                                   list datasets (session)
  POST   /api/v1/datasets                                   create dataset (session)
  DELETE /api/v1/datasets/{id}                              delete dataset (session)
  GET    /api/v1/datasets/{id}/examples                     list examples (session)
  POST   /api/v1/datasets/{id}/examples                     add example (session)
  DELETE /api/v1/datasets/{id}/examples/{example_id}        delete example (session)
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

import config
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, field_validator, model_validator

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue
from db_queues.postgresql.datasets import (
    add_example,
    add_examples_bulk,
    create_dataset,
    delete_dataset,
    delete_example,
    existing_source_trace_ids,
    get_example,
    list_datasets,
    list_examples,
)
from db_queues.postgresql.dataset_runs import (
    add_run_items,
    create_run,
    finalize_run,
    get_run,
    get_run_items,
    link_agent,
    list_agent_links,
    list_runs,
    unlink_agent,
)
from db_queues.postgresql.auth import resolve_api_key
from routes.auth.helper import extract_api_key, get_current_session
from shared.quotas import bump_eval_count, get_quota_status
from shared.dataset_media import store_trajectory_media, hydrate_trajectory_media

datasets_router = APIRouter()
logger = logging.getLogger(__name__)


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

    @model_validator(mode="after")
    def _require_content(self) -> "AddExampleRequest":
        if self.input.strip():
            return self
        # Trace-backed examples (a run added from the drawer, e.g. CrewAI's crew
        # root) may have no direct input — the input is derived from the pinned
        # trajectory server-side — so accept them when a source_trace_id or a
        # final output is present. Only truly empty drafts are rejected.
        meta = self.metadata if isinstance(self.metadata, dict) else {}
        if meta.get("source_trace_id"):
            return self
        if self.expected_output and self.expected_output.strip():
            return self
        raise ValueError("input is required")


# Metrics a 'metrics' dataset run may request — mirrors the evaluator worker's
# _build_evaluator names (plus per-org custom judges, referenced by slug).
RUN_METRICS = frozenset({
    "hallucination", "faithfulness", "relevance",
    "toxicity", "coherence", "completeness",
})


class CreateRunRequest(BaseModel):
    kind:  str                      # 'agentic' | 'security' | 'metrics'
    depth: Optional[str] = None     # agentic depth: fast | standard | deep
    # kind='metrics' only: which metrics to grade each example on, plus
    # optional per-org custom judges {slug: threshold}.
    metrics:       Optional[List[str]]      = None
    custom_judges: Optional[Dict[str, float]] = None

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, v: str) -> str:
        if v not in ("agentic", "security", "metrics"):
            raise ValueError("kind must be 'agentic', 'security', or 'metrics'")
        return v

    @field_validator("metrics")
    @classmethod
    def _validate_metrics(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return None
        cleaned = [m.strip().lower() for m in v if m and m.strip()]
        unknown = sorted(set(cleaned) - RUN_METRICS)
        if unknown:
            raise ValueError(
                f"unknown metrics: {', '.join(unknown)}. "
                f"Supported: {', '.join(sorted(RUN_METRICS))}"
            )
        return cleaned or None


class LinkAgentRequest(BaseModel):
    agent_key:  str
    agent_kind: str


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
    limit:  int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_examples(dataset_id, org_id, limit=limit, offset=offset)
    examples = [_serialize_example(r) for r in rows]
    # Auto-enrich each example with its run's eval / security / cost signals,
    # looked up live from ClickHouse by source_trace_id. This "backfills" as the
    # eval/security workers finish (a refresh picks up new scores). Best-effort:
    # a ClickHouse hiccup must never break the examples list.
    try:
        await _attach_enrichment(org_id, examples)
    except Exception:
        pass
    return {"examples": examples}


@datasets_router.post("/datasets/{dataset_id}/examples", status_code=status.HTTP_201_CREATED)
async def add_dataset_example(
    dataset_id: uuid.UUID,
    payload: AddExampleRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    input_text = payload.input
    expected = payload.expected_output
    # Trace-backed example (a run added from the drawer): pin the whole trajectory
    # (media → S3) into the no-TTL store so the example is self-contained for
    # agentic eval, and derive a representative IO summary from it for agentic
    # wrapper roots (e.g. CrewAI's crew) that have no direct input of their own.
    src = payload.metadata.get("source_trace_id") if isinstance(payload.metadata, dict) else None
    if src:
        try:
            events = await _snapshot_run(org_id, uuid.UUID(str(src)))
            if events and not input_text.strip():
                d_inp, d_out = _trajectory_input_output(events)
                if d_inp.strip():
                    input_text = d_inp
                if not expected and d_out:
                    expected = d_out
        except Exception:
            pass
    row = await add_example(
        dataset_id, org_id,
        input_text=input_text,
        expected_output=expected,
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


@datasets_router.get("/datasets/{dataset_id}/examples/{example_id}/trajectory")
async def get_example_trajectory(
    dataset_id: uuid.UUID,
    example_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """The pinned agent trajectory behind a trace-backed example.

    A dataset example imported from an agent run carries the whole run
    (all spans: LLM calls, tasks/agents, tool calls, MCP calls, media) in the
    no-TTL ``dataset_trajectory_spans`` store, keyed by its ``source_trace_id``.
    The examples list only shows input/expected_output; this returns a compact,
    display-ready summary of the trajectory so the UI can show *how* the run got
    there — the steps, agents, tools and MCP calls — not just its IO.
    """
    org_id = uuid.UUID(session["org_id"])
    # Resolve the example (dataset + org scoped) and its source trace.
    example = await get_example(example_id, dataset_id, org_id)
    if example is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Example not found")
    meta = example.get("metadata") or {}
    if isinstance(meta, str):  # asyncpg returns jsonb as a string
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    src = meta.get("source_trace_id") if isinstance(meta, dict) else None
    if not src:
        # Text-only example (no run behind it) — nothing to show.
        return {"trace_id": None, "has_trajectory": False, "stats": {}, "steps": []}

    try:
        src_uuid = uuid.UUID(str(src))
    except (ValueError, TypeError):
        return {"trace_id": str(src), "has_trajectory": False, "stats": {}, "steps": []}

    # Prefer the pinned snapshot; fall back to a live snapshot (older examples /
    # a run pinned before this feature). Best-effort — never 500 the drawer.
    events: List[dict] = []
    try:
        events = await clickhouse_client.get_dataset_trajectory(org_id, src_uuid)
        if not events:
            events = await _snapshot_run(org_id, src_uuid)
        events = hydrate_trajectory_media(events)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[DATASET-TRAJECTORY] load failed trace=%s: %s", src, exc)
        events = []

    summary = _trajectory_summary(events)
    summary["trace_id"] = str(src)
    summary["has_trajectory"] = bool(summary["steps"])
    return summary


# ── Dataset batch runs (agentic eval / security) ──────────────────────────────

def _synthetic_event(example: dict, trace_id: str) -> dict:
    """Build a single LLM-shaped event from a text-only example so the eval /
    security workers (which read messages/input + response) can score it."""
    raw = example.get("input") or ""
    event: dict = {
        "trace_id":      trace_id,
        "root_trace_id": trace_id,
        "type":          "llm",
        "integration":   "DATASET",
        "input":         raw,
        "response":      example.get("expected_output") or "",
    }
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            event["messages"] = parsed
    except (ValueError, TypeError):
        pass
    return event


def _root_event(events: List[dict], trace_id: str) -> dict:
    """Pick the run's root span (matching trace_id) or fall back to the first."""
    for e in events:
        if e.get("trace_id") == trace_id and (e.get("root_trace_id") in (None, trace_id)):
            return e
    return events[0]


@datasets_router.post("/datasets/{dataset_id}/runs", status_code=status.HTTP_201_CREATED)
async def create_dataset_run(
    dataset_id: uuid.UUID,
    payload: CreateRunRequest,
    session: dict = Depends(get_current_session),
):
    """Launch a batch agentic-eval, security, or metrics run over every example.

    For each example we re-run the *real* source trace when one is recorded
    (``metadata.source_trace_id``) — fetching its full span tree so agentic /
    tool context is preserved — and fall back to a synthetic single-turn event
    for text-only examples. Results land in ClickHouse (keyed by trace_id) via
    the normal workers; ``GET /datasets/runs/{run_id}`` aggregates the report.
    """
    return await _launch_run(uuid.UUID(session["org_id"]), dataset_id, payload)


async def _launch_run(org_id: uuid.UUID, dataset_id: uuid.UUID, payload: CreateRunRequest):
    """Shared run-launch body for the session route and the CI (API-key) route."""
    if payload.kind in ("agentic", "metrics"):
        quota = await get_quota_status(org_id)
        if quota.eval_over:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=f"Evaluation quota exceeded for {quota.tier} tier.",
            )

    run_metrics = payload.metrics or ["hallucination", "relevance"]
    # For metrics runs the kind-specific config column records which metrics
    # ran (the way `depth` records the agentic depth).
    depth = ",".join(run_metrics) if payload.kind == "metrics" else payload.depth
    run = await create_run(org_id, dataset_id, payload.kind, depth)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    run_id = run["run_id"]

    examples = await list_examples(dataset_id, org_id, limit=500)
    if not examples:
        await finalize_run(run_id, org_id, "complete", {"total": 0, "completed": 0})
        return _serialize_run({**run, "item_count": 0})

    items: List[tuple] = []
    for ex in examples:
        meta = ex.get("metadata") or {}
        src = meta.get("source_trace_id") if isinstance(meta, dict) else None
        events: List[dict] = []
        trace_id = ""
        source = "text"

        if src:
            try:
                src_uuid = uuid.UUID(str(src))
                # Prefer the pinned, retention-independent snapshot; if this run
                # was never snapshotted (e.g. auto-appended), pin it now (lazily)
                # while the source trace is still alive.
                events = await clickhouse_client.get_dataset_trajectory(org_id, src_uuid)
                if not events:
                    events = await _snapshot_run(org_id, src_uuid)
                # Rehydrate S3-stored media into fresh presigned URLs so the
                # evaluator's vision/OCR paths can fetch it.
                events = hydrate_trajectory_media(events)
            except Exception:
                events = []
            if events:
                trace_id = str(src)
                source = "trace"

        if source == "text":
            trace_id = str(uuid.uuid4())
            events = [_synthetic_event(ex, trace_id)]

        # ── publish the job the workers already know how to run ──
        if payload.kind == "metrics":
            # Grade the example's recorded answer with the chosen LLM metrics
            # against its expected output. Always under a FRESH trace_id so the
            # eval rows are uniquely attributable to this run (source traces may
            # already carry production/agentic eval rows).
            root = _root_event(events, trace_id)
            answer = str(
                (meta.get("output") if isinstance(meta, dict) else None)
                or root.get("response") or root.get("output") or ""
            )
            event = {**root, "response": answer}
            trace_id = str(uuid.uuid4())
            event["trace_id"] = event["root_trace_id"] = trace_id
            expected = str(ex.get("expected_output") or "").strip()
            job = {
                "operation":       "sdk_llm",
                "organization_id": str(org_id),
                "trace_id":        trace_id,
                "event":           event,
                "eval_config": {
                    "metrics":       run_metrics,
                    "custom_judges": payload.custom_judges or {},
                },
            }
            # The reference is the expected output — unless that IS the answer
            # being graded (text-only example with no recorded output), where a
            # self-comparison would trivially score 1.0.
            if expected and expected != answer.strip():
                job["reference"] = expected
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
        elif payload.kind == "agentic":
            if source == "trace":
                job = {
                    "operation":       "agent_eval",
                    "organization_id": str(org_id),
                    "trace_id":        trace_id,
                    "root_trace_id":   trace_id,
                    "events":          events,
                }
                if payload.depth:
                    job["depth"] = payload.depth
            else:
                # A single synthetic turn has no agentic trajectory; score it with
                # the standard LLM metrics so the example still gets a number.
                job = {
                    "operation":       "sdk_llm",
                    "organization_id": str(org_id),
                    "trace_id":        trace_id,
                    "event":           events[0],
                    "eval_config":     {"metrics": ["hallucination", "relevance"]},
                }
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
        else:  # security
            job = {
                "operation":       "sdk_security",
                "organization_id": str(org_id),
                "trace_id":        trace_id,
                "event":           _root_event(events, trace_id),
                "security_config": {"guardrail": "default"},
            }
            await kafka_queue.add_job(job, topic=config.KAFKA_SECURITY_TOPIC, key=str(org_id))

        items.append((ex["example_id"], trace_id, source))

    await add_run_items(run_id, org_id, items)
    return _serialize_run({**run, "total": len(items), "item_count": len(items)})


@datasets_router.get("/datasets/{dataset_id}/runs")
async def get_dataset_runs(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_runs(dataset_id, org_id)
    return {"runs": [_serialize_run(r) for r in rows]}


@datasets_router.get("/datasets/runs/{run_id}")
async def get_dataset_run_report(
    run_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """Aggregate a run's per-example worker results (from ClickHouse) into a
    report, and finalize the run once every example has a result."""
    return await _build_report(uuid.UUID(session["org_id"]), run_id)


async def _build_report(org_id: uuid.UUID, run_id: uuid.UUID):
    """Shared report body for the session route and the CI (API-key) route."""
    run = await get_run(run_id, org_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    items = await get_run_items(run_id, org_id)
    trace_ids = [it["trace_id"] for it in items]

    if run["kind"] == "agentic":
        rows = await clickhouse_client.fetch_dataset_eval_results(org_id, trace_ids)
        by_trace: Dict[str, List[dict]] = {}
        for r in rows:
            by_trace.setdefault(r["trace_id"], []).append(r)
        summary = _agentic_summary(by_trace, len(items))
    elif run["kind"] == "metrics":
        rows = await clickhouse_client.fetch_dataset_eval_results(org_id, trace_ids)
        by_trace = {}
        for r in rows:
            if r["evaluator"] == "fluiq.eval" and r["score"] is not None:
                by_trace.setdefault(r["trace_id"], []).append(r)
        summary = _metrics_summary(by_trace, len(items))
    else:
        rows = await clickhouse_client.fetch_dataset_security_results(org_id, trace_ids)
        by_trace = {}
        for r in rows:
            by_trace[r["trace_id"]] = r  # dedupe: keep one row per trace
        summary = _security_summary(by_trace, len(items))

    completed = summary["completed"]
    if completed >= len(items) and run["status"] == "running":
        await finalize_run(run_id, org_id, "complete", summary)
        run = {**run, "status": "complete", "summary": summary}

    item_out = [
        {
            "example_id":      str(it["example_id"]),
            "trace_id":        it["trace_id"],
            "source":          it["source"],
            "input":           it.get("input"),
            "expected_output": it.get("expected_output"),
            "done":            it["trace_id"] in by_trace,
            "result": (
                by_trace.get(it["trace_id"])
                if run["kind"] == "security"
                else [
                    {"metric": r["metric"], "score": r["score"]}
                    for r in by_trace.get(it["trace_id"], [])
                ]
                if run["kind"] == "metrics"
                else None
            ),
        }
        for it in items
    ]
    return {"run": _serialize_run(run), "summary": summary, "items": item_out}


def _agentic_summary(by_trace: Dict[str, List[dict]], total: int) -> dict:
    """Blend per-example eval rows into run-level scores + per-layer averages."""
    completed = len(by_trace)
    run_scores: List[float] = []
    passes: List[bool] = []
    layer_scores: Dict[str, List[float]] = {}
    metric_scores: Dict[str, List[float]] = {}
    for rows in by_trace.values():
        for r in rows:
            if r["layer"] == "deterministic" and r["run_score"] is not None:
                run_scores.append(r["run_score"])
                passes.append(bool(r["run_passed"]))
            if r["layer"] and r["score"] is not None:
                layer_scores.setdefault(r["layer"], []).append(r["score"])
            if not r["layer"] and r["score"] is not None and r["evaluator"] == "fluiq.eval":
                metric_scores.setdefault(r["metric"], []).append(r["score"])

    def _avg(xs: List[float]) -> Optional[float]:
        return round(sum(xs) / len(xs), 4) if xs else None

    return {
        "kind":          "agentic",
        "total":         total,
        "completed":     completed,
        "avg_run_score": _avg(run_scores),
        "pass_rate":     round(sum(passes) / len(passes), 4) if passes else None,
        "layers":        {k: _avg(v) for k, v in layer_scores.items()},
        "metrics":       {k: _avg(v) for k, v in metric_scores.items()},
    }


def _metrics_summary(by_trace: Dict[str, List[dict]], total: int) -> dict:
    """Blend per-example metric rows into run-level per-metric averages."""
    completed = len(by_trace)
    metric_scores: Dict[str, List[float]] = {}
    example_avgs: List[float] = []
    for rows in by_trace.values():
        scores = [r["score"] for r in rows]
        for r in rows:
            metric_scores.setdefault(r["metric"], []).append(r["score"])
        if scores:
            example_avgs.append(sum(scores) / len(scores))

    def _avg(xs: List[float]) -> Optional[float]:
        return round(sum(xs) / len(xs), 4) if xs else None

    return {
        "kind":      "metrics",
        "total":     total,
        "completed": completed,
        "avg_score": _avg(example_avgs),
        "metrics":   {k: _avg(v) for k, v in metric_scores.items()},
    }


# Score movements smaller than this are noise (LLM judges are not perfectly
# deterministic), so the comparison classifies them as unchanged.
_COMPARE_EPSILON = 0.05


def _example_scores(kind: str, rows: List[dict]) -> tuple[Optional[float], Dict[str, float]]:
    """One example's (overall score, per-metric/layer scores) for comparison."""
    per: Dict[str, float] = {}
    overall: Optional[float] = None
    if kind == "agentic":
        for r in rows:
            if r["layer"] and r["score"] is not None:
                per[r["layer"]] = r["score"]
            if r["layer"] == "deterministic" and r["run_score"] is not None:
                overall = r["run_score"]
        if overall is None and per:
            overall = min(per.values())
    else:  # metrics
        scored = [r for r in rows if r["evaluator"] == "fluiq.eval" and r["score"] is not None]
        for r in scored:
            per[r["metric"]] = r["score"]
        if scored:
            overall = sum(r["score"] for r in scored) / len(scored)
    return overall, per


@datasets_router.get("/datasets/runs/{run_id}/compare")
async def compare_dataset_runs(
    run_id: uuid.UUID,
    against: uuid.UUID = Query(..., description="Baseline run to compare against"),
    session: dict = Depends(get_current_session),
):
    """Run-vs-run regression report: per-metric deltas plus every example that
    regressed, improved, or stayed flat versus the baseline run. Examples are
    joined by example_id (trace ids differ between runs)."""
    org_id = uuid.UUID(session["org_id"])
    run = await get_run(run_id, org_id)
    baseline = await get_run(against, org_id)
    if run is None or baseline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    if run["dataset_id"] != baseline["dataset_id"]:
        raise HTTPException(status_code=400, detail="Runs belong to different datasets")
    if run["kind"] != baseline["kind"]:
        raise HTTPException(status_code=400, detail="Runs are of different kinds")
    if run["kind"] not in ("agentic", "metrics"):
        raise HTTPException(
            status_code=400, detail="Comparison supports agentic and metrics runs"
        )

    items_a = await get_run_items(run_id, org_id)
    items_b = await get_run_items(against, org_id)
    rows_a = await clickhouse_client.fetch_dataset_eval_results(
        org_id, [it["trace_id"] for it in items_a]
    )
    rows_b = await clickhouse_client.fetch_dataset_eval_results(
        org_id, [it["trace_id"] for it in items_b]
    )
    by_trace_a: Dict[str, List[dict]] = {}
    for r in rows_a:
        by_trace_a.setdefault(r["trace_id"], []).append(r)
    by_trace_b: Dict[str, List[dict]] = {}
    for r in rows_b:
        by_trace_b.setdefault(r["trace_id"], []).append(r)

    a_by_example = {str(it["example_id"]): it for it in items_a}
    b_by_example = {str(it["example_id"]): it for it in items_b}

    examples: List[dict] = []
    counts = {"regressed": 0, "improved": 0, "unchanged": 0, "added": 0, "missing": 0}
    metric_pairs: Dict[str, List[tuple[float, float]]] = {}

    for ex_id, it in a_by_example.items():
        score_a, per_a = _example_scores(run["kind"], by_trace_a.get(it["trace_id"], []))
        base_it = b_by_example.get(ex_id)
        if base_it is None:
            counts["added"] += 1
            status_label = "added"
            score_b, per_b = None, {}
        else:
            score_b, per_b = _example_scores(
                run["kind"], by_trace_b.get(base_it["trace_id"], [])
            )
            if score_a is None or score_b is None:
                status_label = "pending"
            elif score_a < score_b - _COMPARE_EPSILON:
                counts["regressed"] += 1
                status_label = "regressed"
            elif score_a > score_b + _COMPARE_EPSILON:
                counts["improved"] += 1
                status_label = "improved"
            else:
                counts["unchanged"] += 1
                status_label = "unchanged"
            for name in set(per_a) & set(per_b):
                metric_pairs.setdefault(name, []).append((per_a[name], per_b[name]))
        examples.append({
            "example_id":     ex_id,
            "input":          (str(it.get("input") or ""))[:300],
            "score":          score_a,
            "baseline_score": score_b,
            "delta": round(score_a - score_b, 4) if score_a is not None and score_b is not None else None,
            "scores":          per_a,
            "baseline_scores": per_b,
            "status":          status_label,
        })
    counts["missing"] = len(set(b_by_example) - set(a_by_example))

    # Per-metric deltas over examples present in BOTH runs, so a changed
    # example mix can't masquerade as a score movement.
    metrics_out = [
        {
            "metric":       name,
            "avg":          round(sum(a for a, _ in pairs) / len(pairs), 4),
            "baseline_avg": round(sum(b for _, b in pairs) / len(pairs), 4),
            "delta":        round(sum(a - b for a, b in pairs) / len(pairs), 4),
        }
        for name, pairs in sorted(metric_pairs.items())
    ]

    # Regressions first, biggest drop on top.
    order = {"regressed": 0, "improved": 1, "unchanged": 2, "added": 3, "pending": 4}
    examples.sort(key=lambda e: (order.get(e["status"], 5), e["delta"] if e["delta"] is not None else 0))

    return {
        "run":      _serialize_run(run),
        "baseline": _serialize_run(baseline),
        "metrics":  metrics_out,
        "examples": examples,
        "summary":  counts,
    }


_THREAT_KEYS = [
    "injection", "jailbreak", "skeleton_key", "secrets", "indirect_injection",
    "rag_poisoning", "tool_exfiltration", "tool_policy_violation",
    "cross_agent_injection", "image_injection",
]


def _security_summary(by_trace: Dict[str, dict], total: int) -> dict:
    completed = len(by_trace)
    risk_levels: Dict[str, int] = {}
    threats = {k: 0 for k in _THREAT_KEYS}
    blocked = 0
    for r in by_trace.values():
        lvl = (r.get("risk_level") or "clean") or "clean"
        risk_levels[lvl] = risk_levels.get(lvl, 0) + 1
        if r.get("should_block"):
            blocked += 1
        for k in _THREAT_KEYS:
            if r.get(k):
                threats[k] += 1
    flagged = sum(v for k, v in risk_levels.items() if k in ("low", "medium", "high"))
    return {
        "kind":        "security",
        "total":       total,
        "completed":   completed,
        "risk_levels": risk_levels,
        "flagged":     flagged,
        "blocked":     blocked,
        "threats":     {k: v for k, v in threats.items() if v > 0},
    }


# ── CI (API-key) runs — `python -m fluiq.ci` / GitHub Actions ────────────────
# Distinct /ci/eval-runs prefix so these can't collide with the
# /datasets/{dataset_id}/... session routes.

class CIRunRequest(CreateRunRequest):
    api_key:      Optional[str] = None       # also accepted via header
    dataset_id:   Optional[uuid.UUID] = None
    dataset_name: Optional[str] = None       # case-insensitive name lookup


async def _resolve_ci_org(header_key: Optional[str], body_key: Optional[str]) -> uuid.UUID:
    key = header_key or body_key
    if not key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, _prefix, _key_id = resolved
    return org_id


@datasets_router.post("/ci/eval-runs", status_code=status.HTTP_201_CREATED)
async def ci_create_run(
    payload: CIRunRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Launch a dataset eval run from CI, authenticated by API key."""
    org_id = await _resolve_ci_org(api_key, payload.api_key)

    dataset_id = payload.dataset_id
    if dataset_id is None:
        name = (payload.dataset_name or "").strip().lower()
        if not name:
            raise HTTPException(status_code=422, detail="dataset_id or dataset_name required")
        rows = await list_datasets(org_id)
        match = next((r for r in rows if str(r.get("name") or "").strip().lower() == name), None)
        if match is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No dataset named '{payload.dataset_name}'",
            )
        dataset_id = match["dataset_id"]

    return await _launch_run(org_id, dataset_id, payload)


@datasets_router.get("/ci/eval-runs/{run_id}")
async def ci_run_report(
    run_id: uuid.UUID,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Fetch a run's report from CI, authenticated by API key."""
    org_id = await _resolve_ci_org(api_key, None)
    return await _build_report(org_id, run_id)


# ── Agent links (Connect Agents) ──────────────────────────────────────────────

# Import safety bounds: page the agent's root traces and cap a single connect so
# a very high-volume agent can't enqueue an unbounded insert.
_IMPORT_PAGE = 200
_IMPORT_CAP = 2000


def _event_to_example(event: dict, cost: Any) -> tuple:
    """Turn a trace's root event into an (input, expected_output, metadata)
    example — the server-side mirror of the frontend ``traceToDatasetExample``."""
    req = (
        event.get("messages")
        or event.get("input")
        or event.get("contents")
        or event.get("prompts")
    )
    if isinstance(req, str):
        input_text = req
    elif req is not None:
        input_text = json.dumps(req, default=str)
    else:
        input_text = ""

    resp = event.get("output")
    if resp is None:
        resp = event.get("response")
    if resp is None:
        expected: Optional[str] = None
    elif isinstance(resp, str):
        expected = resp
    else:
        expected = json.dumps(resp, default=str)

    metadata: Dict[str, Any] = {}
    model = event.get("model")
    if isinstance(model, str) and model:
        metadata["model"] = model
    if isinstance(cost, (int, float)):
        metadata["cost"] = cost
    tid = event.get("trace_id")
    if isinstance(tid, str) and tid:
        metadata["source_trace_id"] = tid
    integ = event.get("integration")
    if isinstance(integ, str) and integ:
        metadata["integration"] = integ
    return input_text, expected, metadata


def _trajectory_input_output(events: List[dict]) -> tuple:
    """Derive (input, expected) from a run's full trajectory.

    Used for agentic wrapper roots that carry no direct IO of their own (e.g.
    CrewAI's crew span, whose ``kickoff()`` had no inputs): take the first real
    input among the run's spans and the last real output, so the example
    represents the whole multi-agent run instead of an empty root.
    """
    inp = ""
    for ev in events:
        req = ev.get("messages") or ev.get("input") or ev.get("contents") or ev.get("prompts")
        if req:
            inp = req if isinstance(req, str) else json.dumps(req, default=str)
            if inp.strip():
                break
    out = ""
    for ev in reversed(events):
        r = ev.get("output")
        if r is None:
            r = ev.get("response")
        if r:
            out = r if isinstance(r, str) else json.dumps(r, default=str)
            if out.strip():
                break
    return inp, (out or None)


# ── Trajectory display summary ────────────────────────────────────────────────
# Compact, display-ready view of a pinned run. Mirrors (a lean subset of) the
# agentic evaluator's Layer-0 adapter so the Datasets UI shows the same steps /
# tools / MCP calls the evaluator scores, without importing worker code.

def _traj_preview(value: Any, limit: int = 600) -> Optional[str]:
    """A short text preview of an input/output/arg value (JSON-encode non-str)."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value
    else:
        try:
            s = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            s = str(value)
    s = s.strip()
    if not s:
        return None
    return s[:limit] + ("…" if len(s) > limit else "")


def _traj_agent(event: dict) -> Optional[str]:
    """The logical agent a step belongs to (LangGraph node / crew role / fn)."""
    lg = event.get("langgraph")
    if isinstance(lg, dict) and lg.get("langgraph_node"):
        return str(lg["langgraph_node"])
    for k in ("agent", "agent_name", "crew_agent", "role"):
        v = event.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None


def _traj_tool_calls(event: dict) -> List[dict]:
    """Tool + MCP calls on an event, across OpenAI/Anthropic/Gemini/MCP shapes."""
    calls: List[dict] = []

    def add(name, arguments, kind="tool", server=None, output=None):
        if not name:
            return
        calls.append({
            "name": str(name),
            "kind": kind,
            "server": server,
            "arguments": _traj_preview(arguments, 300),
            "output": _traj_preview(output, 300),
        })

    for tc in event.get("tool_calls") or []:
        if isinstance(tc, dict):
            fn = tc.get("function") or {}
            add(fn.get("name") or tc.get("name"),
                fn.get("arguments") if "arguments" in fn else tc.get("arguments"))
    for tu in event.get("tool_uses") or []:
        if isinstance(tu, dict):
            add(tu.get("name"), tu.get("input"))
    for fc in event.get("function_calls") or []:
        if isinstance(fc, dict):
            add(fc.get("name"), fc.get("args"))
    for mc in event.get("mcp_calls") or []:
        if isinstance(mc, dict) and mc.get("type") in (None, "mcp_call") and mc.get("name"):
            add(mc.get("name"), mc.get("arguments"), kind="mcp",
                server=mc.get("server_label"), output=mc.get("output"))
    return calls


def _traj_media(event: dict) -> List[dict]:
    """Distinct media (kind/mime) referenced anywhere in the event."""
    out: List[dict] = []
    seen: set = set()

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            ref = obj.get("_media_ref")
            if isinstance(ref, dict):
                key = (ref.get("kind"), ref.get("mime"), ref.get("sha256"))
                if key not in seen:
                    seen.add(key)
                    out.append({"kind": ref.get("kind"), "mime": ref.get("mime")})
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(event)
    return out


def _trajectory_summary(events: List[dict]) -> Dict[str, Any]:
    """Build a compact, DFS-ordered step list + rollup stats from pinned spans.

    Steps are ordered by a depth-first walk of the ``parent_id`` tree (the events
    carry no absolute timestamp), so the container/root comes first and children
    read in call order; ``depth`` lets the UI indent the hierarchy.
    """
    events = [e for e in events if isinstance(e, dict)]
    if not events:
        return {"stats": {}, "steps": []}

    by_id: Dict[str, dict] = {}
    children: Dict[Optional[str], List[str]] = {}
    order_seen: List[str] = []
    for e in events:
        tid = str(e.get("trace_id") or "")
        if not tid or tid in by_id:
            # No id (or a duplicate) — give it a synthetic id so it still renders
            # once instead of overwriting the original / rendering twice.
            tid = f"_anon_{len(order_seen)}"
        by_id[tid] = e
        order_seen.append(tid)
    for tid in order_seen:
        e = by_id[tid]
        pid = e.get("parent_id")
        pid = str(pid) if pid else None
        if pid == tid:            # self-parent (e.g. CrewAI crew) → treat as root
            pid = None
        if pid not in by_id:      # phantom/absent parent → root
            pid = None
        children.setdefault(pid, []).append(tid)

    steps: List[dict] = []
    agents: set = set()
    models: set = set()
    type_counts: Dict[str, int] = {}
    n_tools = n_mcp = 0

    def visit(tid: str, depth: int) -> None:
        nonlocal n_tools, n_mcp
        e = by_id[tid]
        etype = str(e.get("type") or "llm")
        type_counts[etype] = type_counts.get(etype, 0) + 1
        name = e.get("function") or e.get("name") or e.get("api")
        agent = _traj_agent(e)
        # An explicit agent span (CrewAI/ADK) names the sub-agent in its
        # function/name, not a role field — surface it as the agent too.
        if not agent and etype == "agent" and isinstance(name, str) and name.strip():
            agent = name
        if agent:
            agents.add(agent)
        model = e.get("model")
        if isinstance(model, str) and model:
            models.add(model)
        tcalls = _traj_tool_calls(e)
        for c in tcalls:
            if c["kind"] == "mcp":
                n_mcp += 1
            else:
                n_tools += 1
        steps.append({
            "trace_id":    tid,
            "depth":       depth,
            "type":        etype,
            "name":        name,
            "agent":       agent,
            "model":       model if isinstance(model, str) else None,
            "integration": e.get("integration"),
            "latency":     e.get("latency") if isinstance(e.get("latency"), (int, float)) else None,
            "input":       _traj_preview(
                e.get("messages") or e.get("input") or e.get("contents") or e.get("prompts")),
            "output":      _traj_preview(e.get("output") if e.get("output") is not None else e.get("response")),
            "tool_calls":  tcalls,
            "media":       _traj_media(e),
            "success":     e.get("success"),
            "error":       _traj_preview(e.get("error") or e.get("error_traceback"), 300),
        })
        for child in children.get(tid, []):
            visit(child, depth + 1)

    for tid in children.get(None, []):
        visit(tid, 0)
    # Safety net: any event not reached via the tree (cycle) still gets listed.
    reached = {s["trace_id"] for s in steps}
    for tid in order_seen:
        if tid not in reached:
            visit(tid, 0)

    stats = {
        "spans":  len(steps),
        "tools":  n_tools,
        "mcp":    n_mcp,
        "agents": sorted(agents),
        "models": sorted(models),
        "types":  type_counts,
    }
    return {"stats": stats, "steps": steps}


async def _snapshot_run(org_id: uuid.UUID, root_trace_id: uuid.UUID) -> List[dict]:
    """Pin a run's full trajectory into the no-TTL store, media offloaded to S3.

    Copies every span of the run into ``fluiq.dataset_trajectory_spans`` (which
    has no TTL) after uploading any media to S3, so a dataset example stays fully
    evaluable — trajectory, tools, MCP calls, media — even after the source trace
    hits its retention TTL. Returns the (media-offloaded) span events, or [].
    """
    rows = await clickhouse_client.fetch_traces(
        org_id, root_trace_id=root_trace_id, limit=500,
    )
    events = [r["event"] for r in rows if isinstance(r.get("event"), dict)]
    if not events:
        return []
    events = await store_trajectory_media(org_id, events)
    await clickhouse_client.insert_dataset_trajectory(org_id, root_trace_id, events)
    return events


async def _import_agent_runs(
    org_id: uuid.UUID, dataset_id: uuid.UUID, agent_key: str, agent_kind: str,
) -> int:
    """Import every past root run of an agent as a dataset example (deduped by
    source trace id, capped at ``_IMPORT_CAP``). Each example keeps its
    ``source_trace_id`` so a dataset run re-reads the FULL trajectory (all spans)
    for agentic eval — the example IO is a representative summary, not the whole
    run, which lives in ClickHouse under that trace."""
    seen = await existing_source_trace_ids(dataset_id, org_id)
    to_add: List[tuple] = []
    offset = 0
    while len(to_add) < _IMPORT_CAP:
        rows = await clickhouse_client.fetch_traces(
            org_id, agent_key=agent_key, agent_kind=agent_kind,
            roots_only=True, limit=_IMPORT_PAGE, offset=offset,
        )
        if not rows:
            break
        for r in rows:
            event = r.get("event")
            if not isinstance(event, dict):
                continue
            tid = event.get("trace_id")
            if isinstance(tid, str):
                if tid in seen:
                    continue
                seen.add(tid)
            inp, expected, meta = _event_to_example(event, r.get("cost"))
            # Pin the full trajectory (media → S3) into the no-TTL store so the
            # example is self-contained for agentic eval regardless of the source
            # trace's retention. Also derive a representative IO summary from the
            # whole run for agentic wrapper roots (e.g. CrewAI's crew) that carry
            # no direct input of their own.
            snap_events: List[dict] = []
            if isinstance(tid, str):
                try:
                    snap_events = await _snapshot_run(org_id, uuid.UUID(tid))
                except Exception:
                    snap_events = []
            if not inp.strip() and snap_events:
                d_inp, d_out = _trajectory_input_output(snap_events)
                if d_inp.strip():
                    inp = d_inp
                if not expected and d_out:
                    expected = d_out
            # Keep the run if it has any representable content (input or output);
            # the pinned trajectory preserves the full run for eval either way.
            if not inp.strip() and not (expected and str(expected).strip()):
                continue
            to_add.append((inp, expected, meta))
            if len(to_add) >= _IMPORT_CAP:
                break
        if len(rows) < _IMPORT_PAGE:
            break
        offset += _IMPORT_PAGE
    return await add_examples_bulk(dataset_id, org_id, to_add)


@datasets_router.post("/datasets/{dataset_id}/agents", status_code=status.HTTP_201_CREATED)
async def link_dataset_agent(
    dataset_id: uuid.UUID,
    payload: LinkAgentRequest,
    session: dict = Depends(get_current_session),
):
    """Connect an agent to a dataset: import ALL of its runs to date as examples
    now, and link it so future runs auto-append (the auto-append hook lives in
    the tracer worker). Re-connecting is safe — already-imported runs are skipped
    by their source trace id."""
    org_id = uuid.UUID(session["org_id"])
    ok = await link_agent(dataset_id, org_id, payload.agent_key, payload.agent_kind)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    imported = await _import_agent_runs(org_id, dataset_id, payload.agent_key, payload.agent_kind)
    return {"ok": True, "imported": imported}


@datasets_router.get("/datasets/{dataset_id}/agents")
async def get_dataset_agents(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    links = await list_agent_links(dataset_id, org_id)
    return {
        "agents": [
            {
                "agent_key":  l["agent_key"],
                "agent_kind": l["agent_kind"],
                "created_at": l["created_at"].isoformat() if l.get("created_at") else None,
            }
            for l in links
        ]
    }


@datasets_router.delete(
    "/datasets/{dataset_id}/agents", status_code=status.HTTP_204_NO_CONTENT,
)
async def unlink_dataset_agent(
    dataset_id: uuid.UUID,
    agent_key: str = Query(...),
    agent_kind: str = Query(...),
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    removed = await unlink_agent(dataset_id, org_id, agent_key, agent_kind)
    if not removed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")


# ── Example enrichment (eval / security / cost by source trace) ───────────────

async def _attach_enrichment(org_id: uuid.UUID, examples: List[dict]) -> None:
    """Merge each example's run signals — eval scores, security verdict, cost —
    onto its serialized dict under ``enrichment``, looked up by source_trace_id."""
    trace_ids: List[str] = []
    for ex in examples:
        tid = (ex.get("metadata") or {}).get("source_trace_id")
        if isinstance(tid, str) and tid:
            trace_ids.append(tid)
    trace_ids = list(dict.fromkeys(trace_ids))  # dedupe, preserve order
    if not trace_ids:
        return

    eval_rows = await clickhouse_client.fetch_dataset_eval_results(org_id, trace_ids)
    sec_rows  = await clickhouse_client.fetch_dataset_security_results(org_id, trace_ids)
    cost_map  = await clickhouse_client.fetch_costs_for_traces(org_id, trace_ids)

    evals_by_trace: Dict[str, Dict[str, float]] = {}
    for r in eval_rows:
        if r["score"] is None:
            continue
        key = r["layer"] or r["metric"]          # agentic layer, else metric name
        if key:
            evals_by_trace.setdefault(r["trace_id"], {})[key] = round(r["score"], 4)
    sec_by_trace = {r["trace_id"]: r for r in sec_rows}

    for ex in examples:
        meta = ex.get("metadata") or {}
        tid = meta.get("source_trace_id")
        if not isinstance(tid, str) or not tid:
            continue
        enrichment: Dict[str, Any] = {}

        evals = evals_by_trace.get(tid)
        if evals:
            enrichment["eval"] = evals

        s = sec_by_trace.get(tid)
        if s:
            enrichment["security"] = {
                "risk_level": s.get("risk_level") or "clean",
                "blocked":    bool(s.get("should_block")),
                "threats":    [k for k in _THREAT_KEYS if s.get(k)],
            }

        cost_entry = cost_map.get(tid)
        if cost_entry and cost_entry.get("cost") is not None:
            enrichment["cost"] = cost_entry["cost"]
        elif isinstance(meta.get("cost"), (int, float)):
            enrichment["cost"] = meta["cost"]

        if enrichment:
            ex["enrichment"] = enrichment


# ── Serializers ───────────────────────────────────────────────────────────────

def _serialize_run(row: dict) -> dict:
    summary = row.get("summary") or {}
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except ValueError:
            summary = {}
    return {
        "run_id":      str(row["run_id"]),
        "dataset_id":  str(row["dataset_id"]),
        "kind":        row["kind"],
        "depth":       row.get("depth"),
        "status":      row["status"],
        "total":       row.get("total", 0),
        "item_count":  row.get("item_count", row.get("total", 0)),
        "summary":     summary,
        "created_at":  row["created_at"].isoformat() if row.get("created_at") else None,
        "finished_at": row["finished_at"].isoformat() if row.get("finished_at") else None,
    }


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
