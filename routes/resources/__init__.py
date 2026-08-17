"""fluiq-api — Code-defined resources and locally-run evals.

  POST /api/v1/resources/push     upsert prompts, scorers, datasets (API key)
  POST /api/v1/local-runs         record an eval whose task ran on your machine
  POST /api/v1/local-runs/{id}/results   send scored examples in batches
  POST /api/v1/local-runs/{id}/finish    close the run

Both halves of the code-first workflow live here, and both authenticate by API
key rather than session: they run from a laptop and from CI, neither of which
has a browser to log in with.

Why a *local* run exists
------------------------
A dataset run generates output on our side, which needs the task to be
expressible as a prompt. Real applications are not: they retrieve, call tools,
branch, and post-process. Running the task where the code lives is the only way
to evaluate the thing that actually ships — and it means the eval works against
a service that isn't reachable from the internet, which is most of them during
development.

So the SDK executes the task locally and posts (input, output, expected) here;
the platform scores and records it exactly like any other run, so it lands in
the same history and the same comparison view.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

import config
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field, field_validator

from db_queues.kafka import kafka_queue
from db_queues.postgresql.auth import resolve_api_key
from db_queues.postgresql.dataset_runs import (
    add_run_items,
    create_run,
    finalize_run,
    get_run,
)
from db_queues.postgresql.dataset_scorers import link_dataset_scorer
from db_queues.postgresql.datasets import (
    add_examples_bulk,
    create_dataset,
    list_datasets,
)
from db_queues.postgresql.prompts import (
    create_prompt,
    get_prompt_by_slug,
    update_prompt,
)
from routes.auth.helper import extract_api_key
from shared.choice_scores import ChoiceError, parse_choices
from shared.code_scorer import ScorerError, compile_scorer
from shared.placeholders import ANSWER_PLACEHOLDER_RE
from shared.quotas import bump_eval_count, get_quota_status

resources_router = APIRouter()
logger = logging.getLogger(__name__)

MAX_EXAMPLES_PER_PUSH = 2000
MAX_RESULTS_PER_BATCH = 200


async def _org(api_key: Optional[str], body_key: Optional[str] = None) -> uuid.UUID:
    key = api_key or body_key
    if not key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "API key required")
    resolved = await resolve_api_key(key)
    if resolved is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
    return resolved[0]


# ── Push ──────────────────────────────────────────────────────────────────────

class PromptSpec(BaseModel):
    slug: str
    name: str
    template: str
    model: Optional[str] = None
    kind: str = "completion"
    variables: List[str] = Field(default_factory=list)


class ScorerSpec(BaseModel):
    slug: str
    name: str
    template: str
    kind: str = "judge"
    threshold: float = 0.5
    choices: Optional[List[Dict[str, Any]]] = None
    datasets: List[str] = Field(default_factory=list)


class DatasetSpec(BaseModel):
    name: str
    description: Optional[str] = None
    kind: str = "text"
    examples: List[Dict[str, Any]] = Field(default_factory=list)


class PushRequest(BaseModel):
    api_key:  Optional[str] = None
    prompts:  List[PromptSpec]  = Field(default_factory=list)
    scorers:  List[ScorerSpec]  = Field(default_factory=list)
    datasets: List[DatasetSpec] = Field(default_factory=list)


@resources_router.post("/resources/push")
async def push(
    payload: PushRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Upsert code-declared prompts, scorers, and datasets.

    Upsert, not replace. A push never deletes: the file is one contributor to
    the org's library, not its definition. Someone who trims a fixture locally
    must not wipe the production failures a colleague curated into the same
    dataset from the Review page.
    """
    org_id = await _org(api_key, payload.api_key)
    created: Dict[str, List[str]] = {"prompts": [], "scorers": [], "datasets": []}
    updated: Dict[str, List[str]] = {"prompts": [], "scorers": [], "datasets": []}
    errors: List[str] = []

    # Scorers before datasets: a dataset that names a scorer must find it.
    for spec in payload.prompts + payload.scorers:  # type: ignore[operator]
        is_scorer = isinstance(spec, ScorerSpec)
        bucket = "scorers" if is_scorer else "prompts"
        kind = spec.kind if not is_scorer else spec.kind
        try:
            _validate_body(spec, is_scorer)
        except ValueError as exc:
            errors.append(f"{bucket[:-1]} {spec.slug!r}: {exc}")
            continue

        config_json = (
            {"choices": spec.choices}
            if is_scorer and getattr(spec, "choices", None) else None
        )
        existing = await get_prompt_by_slug(org_id, spec.slug)
        if existing is not None:
            if (existing.get("kind") or "completion") != kind:
                errors.append(
                    f"{bucket[:-1]} {spec.slug!r}: already exists as a "
                    f"{existing.get('kind')} — rename one of them"
                )
                continue
            await update_prompt(
                existing["prompt_id"], org_id,
                name=spec.name, template=spec.template,
                model=getattr(spec, "model", None),
                config=config_json,
                clear_config=config_json is None,
            )
            updated[bucket].append(spec.slug)
        else:
            await create_prompt(
                org_id, spec.name, spec.slug, spec.template,
                model=getattr(spec, "model", None),
                variables=list(getattr(spec, "variables", []) or []),
                kind=kind, config=config_json,
            )
            created[bucket].append(spec.slug)

    # Datasets last, so a scorer link resolves.
    existing_datasets = {d["name"].strip().lower(): d for d in await list_datasets(org_id)}
    for spec in payload.datasets:
        if len(spec.examples) > MAX_EXAMPLES_PER_PUSH:
            errors.append(
                f"dataset {spec.name!r}: {len(spec.examples)} examples exceeds "
                f"the {MAX_EXAMPLES_PER_PUSH} per push"
            )
            continue
        match = existing_datasets.get(spec.name.strip().lower())
        if match is None:
            row = await create_dataset(org_id, spec.name, spec.description, spec.kind)
            dataset_id = row["dataset_id"]
            created["datasets"].append(spec.name)
        else:
            dataset_id = match["dataset_id"]
            updated["datasets"].append(spec.name)

        rows = [
            (
                str(e.get("input") or ""),
                e.get("expected_output"),
                e.get("metadata") or {},
            )
            for e in spec.examples
            if str(e.get("input") or "").strip() or e.get("expected_output")
        ]
        if rows:
            await add_examples_bulk(dataset_id, org_id, rows)

    # Scorer→dataset links, once both ends exist.
    all_datasets = {d["name"].strip().lower(): d for d in await list_datasets(org_id)}
    for spec in payload.scorers:
        for dataset_name in spec.datasets:
            match = all_datasets.get(dataset_name.strip().lower())
            if match is None:
                errors.append(
                    f"scorer {spec.slug!r}: no dataset named {dataset_name!r} to attach to"
                )
                continue
            await link_dataset_scorer(match["dataset_id"], org_id, spec.slug, spec.threshold)

    return {"created": created, "updated": updated, "errors": errors}


def _validate_body(spec: Any, is_scorer: bool) -> None:
    """Reject a resource at push time rather than on the run that needed it."""
    if not (spec.template or "").strip():
        raise ValueError("empty body")
    if not is_scorer:
        if spec.kind not in ("completion", "judge", "code"):
            raise ValueError(f"unknown kind {spec.kind!r}")
        if spec.kind == "judge" and not ANSWER_PLACEHOLDER_RE.search(spec.template):
            raise ValueError("a judge prompt must reference {{answer}}")
        if spec.kind == "code":
            _compile(spec.template)
        return

    if spec.kind == "code":
        _compile(spec.template)
        if spec.choices:
            raise ValueError("choices apply to judges, not code scorers")
        return
    if not ANSWER_PLACEHOLDER_RE.search(spec.template):
        raise ValueError("a judge scorer must reference {{answer}}")
    try:
        parse_choices(spec.choices)
    except ChoiceError as exc:
        raise ValueError(str(exc)) from exc


def _compile(source: str) -> None:
    try:
        compile_scorer(source)
    except ScorerError as exc:
        raise ValueError(str(exc)) from exc


# ── Local runs ────────────────────────────────────────────────────────────────

class LocalRunRequest(BaseModel):
    api_key:     Optional[str] = None
    name:        str
    dataset:     Optional[str] = None
    description: Optional[str] = None
    metrics:     List[str] = Field(default_factory=list)
    custom_judges: Dict[str, float] = Field(default_factory=dict)
    judge:       Optional[str] = None
    #: Recorded on the run so a result is attributable to the code that made it.
    task_label:  Optional[str] = None

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v


class LocalResult(BaseModel):
    """One example the SDK already ran."""
    input:    str
    output:   str
    expected: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    #: Scores computed by local callables, merged with the server's own.
    local_scores: Dict[str, float] = Field(default_factory=dict)


class LocalResultsRequest(BaseModel):
    api_key: Optional[str] = None
    results: List[LocalResult]


@resources_router.post("/local-runs", status_code=status.HTTP_201_CREATED)
async def start_local_run(
    payload: LocalRunRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Open a run whose task executed on the caller's machine.

    A dataset is optional. Without one the run still records and scores, it just
    has no example library behind it — which is the right shape for an eval
    defined entirely by inline data in a repo.
    """
    org_id = await _org(api_key, payload.api_key)
    quota = await get_quota_status(org_id)
    if quota.eval_over:
        raise HTTPException(
            status.HTTP_402_PAYMENT_REQUIRED,
            f"Evaluation quota exceeded for {quota.tier} tier.",
        )

    dataset_id = None
    if payload.dataset:
        wanted = payload.dataset.strip().lower()
        match = next(
            (d for d in await list_datasets(org_id)
             if str(d.get("name") or "").strip().lower() == wanted),
            None,
        )
        if match is None:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, f"No dataset named '{payload.dataset}'",
            )
        dataset_id = match["dataset_id"]
    else:
        # A run row is keyed to a dataset, so an inline-data eval gets a holding
        # dataset named after itself. Created once and reused, so repeated runs
        # of the same eval stay comparable instead of spawning a dataset apiece.
        holder = f"{payload.name} (local)"
        match = next(
            (d for d in await list_datasets(org_id)
             if str(d.get("name") or "").strip().lower() == holder.lower()),
            None,
        )
        if match is None:
            row = await create_dataset(
                org_id, holder,
                "Created automatically for a code-defined eval with inline data.",
                "text",
            )
            dataset_id = row["dataset_id"]
        else:
            dataset_id = match["dataset_id"]

    run = await create_run(
        org_id, dataset_id, "metrics",
        depth=",".join(payload.metrics) if payload.metrics else None,
        model=payload.judge,
        task={
            # The task ran elsewhere, so there is no template to record. What is
            # recorded is *that* it was local and what it was called — enough to
            # tell two runs apart in the history.
            "kind":     "local",
            "template": payload.task_label or payload.name,
            "model":    "local",
        },
        name=payload.name,
        description=payload.description,
    )
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Dataset not found")
    return {"run_id": str(run["run_id"]), "dataset_id": str(dataset_id)}


@resources_router.post("/local-runs/{run_id}/results")
async def post_local_results(
    run_id: uuid.UUID,
    payload: LocalResultsRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Submit a batch of already-executed examples for scoring."""
    org_id = await _org(api_key, payload.api_key)
    run = await get_run(run_id, org_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Run not found")
    if len(payload.results) > MAX_RESULTS_PER_BATCH:
        raise HTTPException(
            422, f"Send at most {MAX_RESULTS_PER_BATCH} results per request",
        )

    metrics = [m for m in (run.get("depth") or "").split(",") if m]
    items: List[tuple] = []
    for result in payload.results:
        trace_id = str(uuid.uuid4())
        event = {
            "trace_id":      trace_id,
            "root_trace_id": trace_id,
            "type":          "llm",
            "integration":   "LOCAL_EVAL",
            "input":         result.input,
            "response":      result.output,
        }
        job: Dict[str, Any] = {
            "operation":       "sdk_llm",
            "organization_id": str(org_id),
            "trace_id":        trace_id,
            "event":           event,
            "eval_config": {
                "metrics":       metrics or ["relevance"],
                "custom_judges": {},
                **({"example_metadata": {
                    k: v for k, v in result.metadata.items()
                    if isinstance(v, (str, int, float, bool))
                }} if result.metadata else {}),
            },
        }
        if result.expected:
            job["reference"] = result.expected
        await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
        bump_eval_count(org_id)
        # example_id is synthetic: an inline-data eval has no dataset row to
        # point at, and the report joins on trace_id anyway.
        items.append((uuid.uuid4(), trace_id, "local"))

    await add_run_items(run_id, org_id, items)
    return {"accepted": len(items)}


@resources_router.post("/local-runs/{run_id}/finish")
async def finish_local_run(
    run_id: uuid.UUID,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Mark a local run as fully submitted.

    Not the same as complete: scoring is still in flight on the workers. This
    only says no more results are coming, so the report stops waiting for a
    batch that will never arrive.
    """
    org_id = await _org(api_key)
    run = await get_run(run_id, org_id)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Run not found")
    return {"ok": True, "total": run.get("total", 0)}


# ── Remote evals ──────────────────────────────────────────────────────────────
#
# A code-defined eval's task lives on a developer's machine, which has no address
# a server can reach. Rather than ask anyone to open a port or run a tunnel, the
# SDK's ``fluiq serve`` registers what it can run and then long-polls for
# requests — the connection is always outbound, so it works from a laptop, a CI
# runner, or inside a VPC with no inbound rules at all.
#
# Registrations and the request queue are held in-process: they describe a live
# process, and a persisted registration would advertise an eval nobody is serving
# any more. The cost is that a multi-replica API can register on one replica and
# queue on another; the queue endpoint reports that honestly rather than hanging.

class RemoteEvalSpec(BaseModel):
    name:        str
    description: Optional[str] = None
    dataset:     Optional[str] = None
    scorers:     List[str] = Field(default_factory=list)


class RegisterRequest(BaseModel):
    api_key: Optional[str] = None
    evals:   List[RemoteEvalSpec]


class TriggerRequest(BaseModel):
    eval: str


#: org_id -> {eval name: spec}
_REGISTERED: Dict[str, Dict[str, Dict[str, Any]]] = {}
#: org_id -> queued run requests, oldest first
_QUEUED: Dict[str, List[Dict[str, Any]]] = {}

#: A registration is dropped if its server hasn't polled within this window —
#: a serve process that was Ctrl-C'd should stop being offered in the UI.
REGISTRATION_TTL_SECONDS = 120.0


def _prune(org_key: str) -> None:
    import time as _time

    now = _time.monotonic()
    live = {
        name: spec
        for name, spec in _REGISTERED.get(org_key, {}).items()
        if now - spec.get("seen_at", 0) < REGISTRATION_TTL_SECONDS
    }
    if live:
        _REGISTERED[org_key] = live
    else:
        _REGISTERED.pop(org_key, None)


@resources_router.post("/remote-evals/register")
async def register_remote_evals(
    payload: RegisterRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Announce which code-defined evals this process can run."""
    import time as _time

    org_id = await _org(api_key, payload.api_key)
    key = str(org_id)
    now = _time.monotonic()
    _REGISTERED[key] = {
        spec.name: {**spec.model_dump(), "seen_at": now} for spec in payload.evals
    }
    return {"registered": [spec.name for spec in payload.evals]}


@resources_router.get("/remote-evals")
async def list_remote_evals(api_key: Optional[str] = Depends(extract_api_key)):
    """Evals a serving process is currently offering, for the dashboard."""
    org_id = await _org(api_key)
    key = str(org_id)
    _prune(key)
    return {
        "evals": [
            {k: v for k, v in spec.items() if k != "seen_at"}
            for spec in _REGISTERED.get(key, {}).values()
        ]
    }


@resources_router.post("/remote-evals/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger_remote_eval(
    payload: TriggerRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Ask the serving process to run an eval."""
    org_id = await _org(api_key)
    key = str(org_id)
    _prune(key)
    if payload.eval not in _REGISTERED.get(key, {}):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No process is currently serving an eval named '{payload.eval}'. "
            f"Run `fluiq serve` where the code lives.",
        )
    _QUEUED.setdefault(key, []).append({"eval": payload.eval})
    return {"queued": payload.eval}


@resources_router.get("/remote-evals/next")
async def next_remote_eval(api_key: Optional[str] = Depends(extract_api_key)):
    """Long-poll for the next queued request.

    Returns 204 when nothing is waiting, which the SDK treats as "ask again" —
    an empty 200 body would be indistinguishable from a malformed one.
    """
    import asyncio as _asyncio
    import time as _time

    org_id = await _org(api_key)
    key = str(org_id)

    # Polling keeps the registration alive; a process that stopped asking has
    # stopped serving.
    for spec in _REGISTERED.get(key, {}).values():
        spec["seen_at"] = _time.monotonic()

    # Held open briefly so a trigger is picked up promptly without the SDK
    # hammering the endpoint. Well inside the client's own timeout.
    for _ in range(30):
        queue = _QUEUED.get(key)
        if queue:
            return queue.pop(0)
        await _asyncio.sleep(1.0)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


__all__ = ["resources_router"]
