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

import asyncio
import re
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

import config
from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, wait_for_playground_reply
from db_queues.postgresql import postgres_client as pg_client
from db_queues.postgresql import credentials as cred_store
from db_queues.postgresql.auth import resolve_api_key
from db_queues.postgresql.prompts import get_custom_judge_template
from realtime import trace_broker
from routes.auth.helper import extract_api_key, get_current_session
from shared.quotas import bump_eval_count, get_quota_status
from shared.providers import BYOK_FOR_PRICE, PROVIDER_DISPLAY
from shared.completions import (
    MILLION,
    MOONSHOT_BASE_URL,
    PRICE_PROVIDER,
    complete_anthropic,
    complete_for,
    complete_gemini,
    complete_openai,
    estimate_cost,
    provider_for_model,
)
from .judge import run_custom_judges, run_metrics, SUPPORTED_METRICS
from .judge import (
    _PROMPTS as _METRIC_PROMPTS,
    _SYSTEM as _JUDGE_SYSTEM,
    _parse as _parse_judge_json,
    _clamp as _clamp_score,
    _ensure_output_contract,
)
from shared.placeholders import substitute

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
    api_key:     Optional[str]       = None
    trace_id:    Optional[str]       = None
    model:       str                 = ""
    prompt:      str                 = ""
    response:    str                 = ""
    context:     str                 = ""
    metrics:     List[str]           = ["hallucination", "relevance"]
    judge_model: str                 = "claude-haiku-4-5-20251001"
    thresholds:  Dict[str, float]    = {}
    # Client-defined judges: {prompt_slug: threshold}. Each slug must resolve to a
    # kind='judge' prompt saved by this org on the Prompts page.
    custom_judges: Dict[str, float]  = {}


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
async def evaluate(
    payload: EvaluateRequest,
    api_key: Optional[str] = Depends(extract_api_key),
) -> EvaluateResponse:
    """Run LLM-as-judge evaluation for a prompt/response pair.

    Called by the SDK after each LLM call when ``fluiq.eval()`` is active.
    Each requested metric is scored 0–1 by a judge model running server-side.
    Results are stored in ClickHouse and returned synchronously so the SDK
    can enforce thresholds.
    """
    org_id = await _resolve_org(api_key or payload.api_key)

    valid_metrics = [m for m in payload.metrics if m in SUPPORTED_METRICS]
    if not valid_metrics and not payload.custom_judges:
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

    # Client-defined custom judges: resolve each slug to its saved judge template
    # (org-scoped, kind='judge') and run it. Unknown slugs are silently skipped.
    if payload.custom_judges:
        templates: Dict[str, str] = {}
        for slug in payload.custom_judges:
            tmpl = await get_custom_judge_template(org_id, slug)
            if tmpl:
                templates[slug] = tmpl
        if templates:
            raw_results.update(run_custom_judges(
                templates,
                response=payload.response,
                prompt=payload.prompt,
                context=payload.context,
                judge_model=payload.judge_model,
            ))

    # Custom-judge thresholds live alongside the built-in metric thresholds.
    thresholds = {**payload.thresholds, **payload.custom_judges}

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

    # Build response (thresholds already merged with custom_judges above)
    failures:       list[str]          = []
    metric_results: list[MetricResult] = []

    for metric, data in raw_results.items():
        score     = data["score"]
        # An absent threshold means the platform default, not "anything goes".
        threshold = thresholds.get(metric, config.EVAL_DEFAULT_THRESHOLD)
        # A judge that errored scored 0 because it never ran, not because the
        # answer was bad. Blocking on that would take production down with the
        # judge provider, so it is reported and not counted as a failure.
        errored   = bool(data.get("error"))
        passed    = errored or score >= threshold
        # An explicit 0 is still an opt-out: "record this metric, never fail on it".
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


# ── POST /evaluate/agentic ────────────────────────────────────────────────────

class AgenticEvalRequest(BaseModel):
    trace_id:      str
    root_trace_id: Optional[str] = None
    # "fast" (L1+L2) | "standard" (+L3 trajectory) | "deep" (+L4 panel).
    depth:         Optional[str] = None
    # Judge selection, as "provider:model" (e.g. "anthropic:claude-sonnet-5").
    # Omitted uses the server default. With BYOK the org pays for whichever
    # provider it picks, so this is a real cost lever, not just a quality one.
    judge:         Optional[str] = None
    # Jury members for depth="deep", same "provider:model" form. Ignored at
    # shallower depths, where no panel is convened.
    jury:          Optional[List[str]] = None
    # The org's own judges to apply, {slug: threshold}. Without this a custom
    # judge could only run inside a dataset run — writing one and pressing "Run
    # Agentic Eval" on a trace did nothing, with no error to explain why.
    custom_judges: Optional[Dict[str, float]] = None


class AgenticEvalResponse(BaseModel):
    ok:       bool
    trace_id: str
    status:   str            # "queued" | "skipped"
    events:   int
    detail:   Optional[str] = None


@evaluate_router.post("/evaluate/agentic", response_model=AgenticEvalResponse)
async def evaluate_agentic(
    payload: AgenticEvalRequest,
    session: dict = Depends(get_current_session),
) -> AgenticEvalResponse:
    """Trigger the agentic (multi-layer) evaluation for a whole trace.

    Fired by the **Run Agentic Eval** button in the trace drawer. Unlike
    ``fluiq.eval()`` (synchronous single-shot per LLM answer), this fetches every
    span of the trace tree, publishes one ``agent_eval`` job to the eval worker,
    and returns immediately. The layered results (deterministic + tool-selection
    + trajectory [+ panel]) stream back to the UI over the existing SSE
    ``trace.enriched`` channel, exactly like single-shot evals.
    """
    org_id  = uuid.UUID(session["org_id"])
    root_id = payload.root_trace_id or payload.trace_id
    try:
        root_uuid = uuid.UUID(root_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=422, detail="Invalid trace id")

    quota = await get_quota_status(org_id)
    if quota.eval_over:
        return AgenticEvalResponse(
            ok=False, trace_id=payload.trace_id, status="skipped", events=0,
            detail=f"Evaluation quota exceeded for {quota.tier} tier.",
        )

    # Pull every span of the trace tree — the agentic evaluator normalizes the
    # whole run (all tool/MCP calls across steps), not just the root span.
    rows = await clickhouse_client.fetch_traces(
        org_id, root_trace_id=root_uuid, limit=500,
    )
    events = [r["event"] for r in rows if isinstance(r.get("event"), dict)]
    if not events:
        raise HTTPException(status_code=404, detail="No trace events found for this id")

    job: Dict[str, Any] = {
        "operation":       "agent_eval",
        "organization_id": str(org_id),
        "trace_id":        payload.trace_id,
        "root_trace_id":   root_id,
        "events":          events,
    }
    if payload.depth:
        job["depth"] = payload.depth
    if payload.judge:
        job["judge"] = payload.judge
    if payload.jury:
        job["jury"] = payload.jury
    if payload.custom_judges:
        # The worker reads custom judges out of eval_config, the same envelope
        # a dataset run sends, so a scorer behaves identically on both paths.
        job["eval_config"] = {"custom_judges": payload.custom_judges}

    await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
    bump_eval_count(org_id)

    return AgenticEvalResponse(
        ok=True, trace_id=payload.trace_id, status="queued", events=len(events),
    )


# ── GET /evaluate/agentic-summary ─────────────────────────────────────────────

class AgenticLayerStat(BaseModel):
    layer: str
    score: Optional[float] = None
    count: int


class AgenticSummaryResponse(BaseModel):
    window_hours:  int
    runs:          int
    pass_rate:     Optional[float] = None
    avg_run_score: Optional[float] = None
    layers:        List[AgenticLayerStat]


@evaluate_router.get("/evaluate/agentic-summary", response_model=AgenticSummaryResponse)
async def agentic_summary(
    window_hours: int = 24,
    session: dict = Depends(get_current_session),
) -> AgenticSummaryResponse:
    """Aggregate agentic-eval health (run pass-rate, avg run score, per-layer
    averages) for the Overview tile. Windowed by ``window_hours`` (1..720)."""
    org_id = uuid.UUID(session["org_id"])
    window_hours = max(1, min(int(window_hours), 720))
    data = await clickhouse_client.fetch_agentic_summary(org_id, window_hours=window_hours)
    return AgenticSummaryResponse(window_hours=window_hours, **data)


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


# ── Model comparison ──────────────────────────────────────────────────────────
#
# The compare/judge model list is NOT hardcoded here — it is derived from the
# model_prices table (see ``fetch_chat_models``) so a newly-priced model shows up
# in the playground the moment its row lands, with no code change. Code only
# holds the mapping between a price-sheet provider and the BYOK provider that
# actually serves the model, plus a filter that keeps non-chat SKUs (audio,
# image, embeddings, dated snapshots…) out of a text-comparison dropdown.

# BYOK provider (what a stored key is filed under) -> the name model_prices uses.
# None means no price sheet, so cost is simply omitted (best-effort).
# Lives in shared.completions so the dataset task runner prices its generations
# exactly the way the judge path prices its calls.
_PRICE_PROVIDER = PRICE_PROVIDER

# Reverse of the above for the providers we can actually serve: price-sheet name
# (lowercased) -> BYOK provider id.
# Both derived from the single provider registry, so a provider added there
# appears in the model dropdown and is labelled correctly without a second edit.
_BYOK_FOR_PRICE   = BYOK_FOR_PRICE
_PROVIDER_DISPLAY = PROVIDER_DISPLAY

# Substrings that mark a model as not a plain text-chat model. Applied to the
# model id so these SKUs never reach a prompt-comparison dropdown.
_NON_CHAT_MARKERS = (
    "audio", "realtime", "transcribe", "tts", "image", "embed", "moderation",
    "whisper", "search", "computer-use", "codex", "deep-research", "instruct",
    "guard", "native-audio", "live", "diarize", "dall", "vision", "video",
    "babbage", "davinci", "tuning", "preview",
)

# Moonshot (Kimi) speaks the OpenAI wire format; only the host differs.
_MOONSHOT_BASE_URL = MOONSHOT_BASE_URL

_MILLION = MILLION


def _is_chat_model(model: str) -> bool:
    """True when a model id looks like a plain text-chat model we can compare.

    Filters out non-chat SKUs and dated snapshots (``gpt-4o-2024-08-06``,
    ``gpt-4-0613``) whose stable alias we prefer, plus legacy gpt-3.x.
    """
    m = model.lower()
    if any(mark in m for mark in _NON_CHAT_MARKERS):
        return False
    if m.startswith("gpt-3"):
        return False
    # Trailing 3+ digit run == a dated/versioned snapshot, prefer the alias.
    if re.search(r"-\d{3,}", m):
        return False
    return True


def _prettify_model(model: str, byok_provider: str) -> str:
    """A human label for a raw model id, tagged with its provider."""
    if byok_provider == "anthropic" and model.startswith("claude-"):
        parts = model.split("-")
        name = parts[1].capitalize() if len(parts) > 1 else "Claude"
        ver = ".".join(parts[2:]) if len(parts) > 2 else ""
        pretty = f"Claude {name} {ver}".strip()
    elif byok_provider == "openai":
        pretty = model.replace("chatgpt", "ChatGPT").replace("gpt", "GPT")
    elif byok_provider == "gemini" and model.startswith("gemini-"):
        pretty = "Gemini " + model[len("gemini-"):].replace("-", " ").title()
    elif byok_provider == "moonshot":
        pretty = model.replace("-", " ").title()
    else:
        pretty = model
    return f"{pretty} ({_PROVIDER_DISPLAY.get(byok_provider, byok_provider)})"


async def fetch_chat_models() -> List[Dict[str, str]]:
    """Chat-capable models drawn from model_prices, as ``{id, label, provider}``.

    Single source of truth for both the compare list and the judge list; the
    frontend fetches this rather than shipping its own copy.
    """
    async with pg_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT provider, model
            FROM model_prices
            WHERE modality = 'Text'
              AND LOWER(provider) IN ('anthropic', 'openai', 'google', 'moonshot')
            ORDER BY provider, model
            """
        )
    out: List[Dict[str, str]] = []
    for row in rows:
        byok = _BYOK_FOR_PRICE.get((row["provider"] or "").lower())
        model = row["model"]
        if not byok or not _is_chat_model(model):
            continue
        out.append({"id": model, "label": _prettify_model(model, byok), "provider": byok})
    return out


class MetricScore(BaseModel):
    metric: str
    score:  float
    reason: str
    passed: bool


class CompareModelResult(BaseModel):
    model:         str
    output:        Optional[str]   = None
    latency_ms:    Optional[int]   = None
    input_tokens:  Optional[int]   = None
    output_tokens: Optional[int]   = None
    cost_usd:      Optional[float] = None
    error:         Optional[str]   = None
    metrics:       Optional[List[MetricScore]] = None


class CompareRequest(BaseModel):
    prompt:      str       = ""
    models:      List[str] = []
    metrics:     List[str] = []
    judge_model: str       = ""
    context:     str       = ""


class CompareResponse(BaseModel):
    results: List[CompareModelResult]


class PairwiseCandidate(BaseModel):
    id:     str            # model id or free label — echoed back in winner/ranking
    output: str


class PairwiseRequest(BaseModel):
    prompt:      str                    = ""
    context:     str                    = ""
    candidates:  List[PairwiseCandidate] = []
    judge_model: str                    = ""


class PairwiseResult(BaseModel):
    winner:  Optional[str] = None       # candidate id, or None on a tie/no-decision
    ranking: List[str]     = []         # candidate ids, best → worst
    reason:  str           = ""
    tie:     bool          = False


class PairwiseResponse(BaseModel):
    result: PairwiseResult


# ── Provider transports ───────────────────────────────────────────────────────
#
# The transports themselves live in ``shared.completions`` so the dataset task
# runner (which generates the output) and the judge (which grades it) share one
# implementation of provider handling, token accounting, and pricing. Aliased to
# the original private names so every call site below is unchanged.

_complete_anthropic   = complete_anthropic
_complete_openai      = complete_openai
_complete_gemini      = complete_gemini
_complete_for         = complete_for
_provider_for_model   = provider_for_model
_estimate_compare_cost = estimate_cost


async def _run_one(prompt: str, model: str, org_id: uuid.UUID) -> CompareModelResult:
    provider = _provider_for_model(model)
    if provider is None:
        return CompareModelResult(model=model, error=f"Unknown provider for model {model!r}.")
    unsealed = await cred_store.unseal_active(org_id, provider)
    if unsealed is None:
        return CompareModelResult(
            model=model,
            error=f"No {provider} key configured. Add one under Settings → Provider Keys.",
        )
    _, key = unsealed
    t0 = time.monotonic()
    try:
        output, in_tok, out_tok, cached_tok = await _complete_for(provider, key, model, prompt)
        latency_ms = int((time.monotonic() - t0) * 1000)
        cost_usd = await _estimate_compare_cost(provider, model, in_tok, out_tok, cached_tok)
        return CompareModelResult(
            model=model, output=output, latency_ms=latency_ms,
            input_tokens=in_tok, output_tokens=out_tok, cost_usd=cost_usd,
        )
    except httpx.HTTPStatusError as exc:
        # Surface the provider's own message (bad key, model access, rate limit)
        # rather than a bare status code.
        detail = exc.response.text[:300] if exc.response is not None else str(exc)
        return CompareModelResult(
            model=model, latency_ms=int((time.monotonic() - t0) * 1000),
            error=f"{provider} error {exc.response.status_code if exc.response else ''}: {detail}".strip(),
        )
    except Exception as exc:
        return CompareModelResult(
            model=model,
            latency_ms=int((time.monotonic() - t0) * 1000),
            error=str(exc),
        )


# Scores at or above this count as a pass in the compare view. The metric
# prompts are all oriented so 1.0 is good, so a single default threshold reads
# consistently across every metric; per-metric thresholds live on the datasets
# side where a run is persisted and gated.
_COMPARE_PASS_THRESHOLD = 0.5


async def _score_metrics(
    org_id: uuid.UUID,
    judge_model: str,
    *,
    output: str,
    prompt: str,
    context: str,
    metrics: List[str],
) -> List[MetricScore]:
    """LLM-as-judge each metric against one model's ``output`` (BYOK).

    The judge runs on the org's own key for the judge model's provider. If that
    key is missing, every metric comes back as a not-passed row whose reason
    names the key to add, rather than silently dropping the scores.
    """
    wanted = [m for m in metrics if m in SUPPORTED_METRICS]
    if not wanted:
        return []
    provider = _provider_for_model(judge_model)
    unsealed = await cred_store.unseal_active(org_id, provider) if provider else None
    if unsealed is None:
        note = f"No {provider or 'judge'} key configured. Add one under Settings → Provider Keys."
        return [MetricScore(metric=m, score=0.0, reason=note, passed=False) for m in wanted]
    _, key = unsealed

    async def _one(metric: str) -> MetricScore:
        judge_prompt = _METRIC_PROMPTS[metric].format(
            response=output, prompt=prompt, context=context or prompt,
        )
        try:
            raw, *_ = await _complete_for(
                provider, key, judge_model, judge_prompt,
                system=_JUDGE_SYSTEM, max_tokens=300,
            )
            data = _parse_judge_json(raw)
            score = _clamp_score(data.get("score"))
            return MetricScore(
                metric=metric, score=score,
                reason=str(data.get("reason") or ""),
                passed=score >= _COMPARE_PASS_THRESHOLD,
            )
        except Exception as exc:  # noqa: BLE001 — one metric failing must not sink the rest
            return MetricScore(metric=metric, score=0.0, reason=f"judge error: {exc}", passed=False)

    return list(await asyncio.gather(*[_one(m) for m in wanted]))


class ModelInfo(BaseModel):
    id:       str
    label:    str
    provider: str


class ModelsResponse(BaseModel):
    models: List[ModelInfo]


@evaluate_router.get("/evaluate/models", response_model=ModelsResponse)
async def list_compare_models(
    session: dict = Depends(get_current_session),
) -> ModelsResponse:
    """Chat-capable models the playground can compare or judge with.

    Derived live from the model_prices table so a newly-priced model appears
    without a code change; the frontend consumes this instead of a hardcoded
    list. Not gated by which provider keys the org holds — a missing key is
    surfaced per model at run time, not by hiding the model.
    """
    models = await fetch_chat_models()
    return ModelsResponse(models=[ModelInfo(**m) for m in models])


@evaluate_router.post("/evaluate/compare", response_model=CompareResponse)
async def evaluate_compare(
    payload: CompareRequest,
    session: dict = Depends(get_current_session),
) -> CompareResponse:
    """Run the same prompt against every selected model in parallel (BYOK).

    Each model uses the org's own key for its provider; a model whose provider
    has no stored key comes back as a per-model error rather than failing the
    whole comparison, so the user sees exactly which key to add. When ``metrics``
    are requested, each model's output is then scored by the judge model on the
    org's key for the judge's provider.
    """
    if not payload.prompt.strip():
        raise HTTPException(status_code=422, detail="Prompt is required")
    org_id = uuid.UUID(str(session["org_id"]))
    allowed = {m["id"] for m in await fetch_chat_models()}
    valid = [m for m in payload.models if m in allowed]
    if not valid:
        raise HTTPException(
            status_code=422,
            detail="No valid models selected. Pick from GET /evaluate/models.",
        )
    results = await asyncio.gather(*[_run_one(payload.prompt, m, org_id) for m in valid])

    wanted = [m for m in payload.metrics if m in SUPPORTED_METRICS]
    if wanted:
        judge_model = payload.judge_model or "claude-haiku-4-5"

        async def _attach_scores(res: CompareModelResult) -> None:
            # Only score outputs that actually came back; a model that errored
            # has nothing to judge.
            if res.error or not res.output:
                return
            res.metrics = await _score_metrics(
                org_id, judge_model,
                output=res.output, prompt=payload.prompt,
                context=payload.context, metrics=wanted,
            )

        await asyncio.gather(*[_attach_scores(r) for r in results])

    return CompareResponse(results=list(results))


# ── POST /evaluate/pairwise ───────────────────────────────────────────────────
#
# Preference evaluation: instead of scoring each answer against an absolute
# rubric, a judge decides which answer is *better*. This is the standard workflow
# for prompt/model selection — relative quality is easier and more reliable for
# an LLM judge to call than an absolute score. For exactly two candidates we run
# the judge twice with the order swapped and only declare a winner if both runs
# agree, which cancels the well-known position bias (judges favour whichever
# answer they see first). For more than two we ask for a single best→worst
# ranking.

_MAX_PAIRWISE_CANDIDATES = 6


async def _judge_pairwise(
    org_id: uuid.UUID,
    judge_model: str,
    *,
    prompt: str,
    context: str,
    candidates: List[PairwiseCandidate],
) -> PairwiseResult:
    provider = _provider_for_model(judge_model)
    unsealed = await cred_store.unseal_active(org_id, provider) if provider else None
    if unsealed is None:
        note = f"No {provider or 'judge'} key configured. Add one under Settings → Provider Keys."
        return PairwiseResult(winner=None, ranking=[c.id for c in candidates], reason=note, tie=True)
    _, key = unsealed

    ctx_block = f"\n\nCONTEXT:\n{context}" if context.strip() else ""

    async def _ask(a: PairwiseCandidate, b: PairwiseCandidate) -> str:
        """Return the winning candidate id, or "tie"."""
        judge_prompt = (
            "Compare two AI responses to the same QUESTION and decide which is better "
            "overall — accuracy, relevance, completeness, and clarity. If they are "
            "genuinely equal, answer \"tie\"." + ctx_block +
            f"\n\nQUESTION:\n{prompt}\n\nRESPONSE A:\n{a.output}\n\nRESPONSE B:\n{b.output}\n\n"
            'Return JSON: {"winner": "A" | "B" | "tie", "reason": str}'
        )
        raw, *_ = await _complete_for(
            provider, key, judge_model, judge_prompt, system=_JUDGE_SYSTEM, max_tokens=400,
        )
        data = _parse_judge_json(raw)
        pick = str(data.get("winner") or "").strip().upper()
        return a.id if pick == "A" else b.id if pick == "B" else "tie"

    # Two candidates: order-swapped double judging to cancel position bias.
    if len(candidates) == 2:
        a, b = candidates
        try:
            fwd, rev = await asyncio.gather(_ask(a, b), _ask(b, a))
        except Exception as exc:  # noqa: BLE001
            return PairwiseResult(winner=None, ranking=[a.id, b.id], reason=f"judge error: {exc}", tie=True)
        # A real winner must win in BOTH orderings; disagreement ⇒ too close to call.
        if fwd == rev and fwd != "tie":
            loser = b.id if fwd == a.id else a.id
            return PairwiseResult(winner=fwd, ranking=[fwd, loser], reason="Preferred in both orderings.", tie=False)
        return PairwiseResult(
            winner=None, ranking=[a.id, b.id],
            reason="No consistent winner — the two answers are too close to call.", tie=True,
        )

    # Three+ candidates: single best→worst ranking call.
    labelled = "\n\n".join(f"[{i + 1}] {c.output}" for i, c in enumerate(candidates))
    judge_prompt = (
        "Rank the AI responses to the same QUESTION from best to worst — accuracy, "
        "relevance, completeness, and clarity." + ctx_block +
        f"\n\nQUESTION:\n{prompt}\n\nRESPONSES:\n{labelled}\n\n"
        'Return JSON: {"ranking": [response numbers best-first], "reason": str}'
    )
    try:
        raw, *_ = await _complete_for(
            provider, key, judge_model, judge_prompt, system=_JUDGE_SYSTEM, max_tokens=600,
        )
        data = _parse_judge_json(raw)
    except Exception as exc:  # noqa: BLE001
        return PairwiseResult(winner=None, ranking=[c.id for c in candidates], reason=f"judge error: {exc}", tie=True)

    order = data.get("ranking") or []
    ranking: List[str] = []
    for n in order:
        try:
            idx = int(n) - 1
        except (TypeError, ValueError):
            continue
        if 0 <= idx < len(candidates) and candidates[idx].id not in ranking:
            ranking.append(candidates[idx].id)
    # Append any the judge omitted so the ranking always covers every candidate.
    for c in candidates:
        if c.id not in ranking:
            ranking.append(c.id)
    return PairwiseResult(
        winner=ranking[0] if ranking else None,
        ranking=ranking,
        reason=str(data.get("reason") or ""),
        tie=False,
    )


@evaluate_router.post("/evaluate/pairwise", response_model=PairwiseResponse)
async def evaluate_pairwise(
    payload: PairwiseRequest,
    session: dict = Depends(get_current_session),
) -> PairwiseResponse:
    """Judge which candidate answer is best for a prompt (BYOK preference eval)."""
    if not payload.prompt.strip():
        raise HTTPException(status_code=422, detail="Prompt is required")
    cands = [c for c in payload.candidates if c.output and c.output.strip()]
    if len(cands) < 2:
        raise HTTPException(status_code=422, detail="At least two candidate answers are required")
    cands = cands[:_MAX_PAIRWISE_CANDIDATES]
    org_id = uuid.UUID(str(session["org_id"]))
    judge_model = payload.judge_model or "claude-haiku-4-5"
    result = await _judge_pairwise(
        org_id, judge_model, prompt=payload.prompt, context=payload.context, candidates=cands,
    )
    return PairwiseResponse(result=result)


# ── POST /evaluate/trace-metrics ──────────────────────────────────────────────
#
# Dashboard-triggered metric + custom-scorer evaluation of ONE trace's answer.
# The SDK's /evaluate is API-key auth and uses the legacy env-key judge; this is
# the session-authed, BYOK counterpart fired from the trace drawer for single
# (non-agentic) traces. It scores the answer the drawer already has in hand, so
# there's no server-side trace refetch.

async def _score_custom_judges(
    org_id: uuid.UUID,
    judge_model: str,
    *,
    response: str,
    prompt: str,
    context: str,
    judges: Dict[str, str],
) -> Dict[str, Dict[str, Any]]:
    """Run client-defined judge templates (by slug) on one answer, BYOK.

    ``judges`` maps ``{slug: template}``; templates use ``{{question}}`` /
    ``{{answer}}`` / ``{{context}}`` placeholders. Returns
    ``{slug: {"score", "reason"}}``, mirroring the built-in metric path.
    """
    if not judges:
        return {}
    provider = _provider_for_model(judge_model)
    unsealed = await cred_store.unseal_active(org_id, provider) if provider else None
    if unsealed is None:
        note = f"No {provider or 'judge'} key configured. Add one under Settings → Provider Keys."
        return {slug: {"score": 0.0, "reason": note} for slug in judges}
    _, key = unsealed

    async def _one(slug: str, template: str):
        rendered = substitute(
            _ensure_output_contract(template),
            {"question": prompt, "answer": response, "context": context or prompt},
        )
        try:
            raw, *_ = await _complete_for(
                provider, key, judge_model, rendered, system=_JUDGE_SYSTEM, max_tokens=300,
            )
            data = _parse_judge_json(raw)
            return slug, {"score": _clamp_score(data.get("score")), "reason": str(data.get("reason") or "")}
        except Exception as exc:  # noqa: BLE001
            return slug, {"score": 0.0, "reason": f"judge error: {exc}"}

    pairs = await asyncio.gather(*[_one(s, t) for s, t in judges.items()])
    return dict(pairs)


class TraceMetricsRequest(BaseModel):
    trace_id:      str
    root_trace_id: Optional[str]     = None
    prompt:        str               = ""
    response:      str               = ""
    context:       str               = ""
    metrics:       List[str]         = []
    judge_model:   str               = ""
    # {prompt_slug: threshold} — each slug resolves to a kind='judge' prompt this
    # org saved on the Prompts page.
    custom_judges: Dict[str, float]  = {}
    thresholds:    Dict[str, float]  = {}


@evaluate_router.post("/evaluate/trace-metrics", response_model=EvaluateResponse)
async def evaluate_trace_metrics(
    payload: TraceMetricsRequest,
    session: dict = Depends(get_current_session),
) -> EvaluateResponse:
    """Score a single trace's answer on built-in metrics + custom scorers (BYOK).

    Runs the judge on the org's own key, persists to ClickHouse, and streams each
    result over SSE so the trace drawer's Evaluation tab merges them live —
    exactly like the SDK path, but session-authed and judge-BYOK.
    """
    org_id = uuid.UUID(str(session["org_id"]))
    wanted = [m for m in payload.metrics if m in SUPPORTED_METRICS]
    if not wanted and not payload.custom_judges:
        raise HTTPException(
            status_code=422,
            detail=f"Pick at least one metric ({sorted(SUPPORTED_METRICS)}) or a custom scorer.",
        )
    if not payload.response.strip():
        raise HTTPException(status_code=422, detail="This trace has no answer to score.")

    quota = await get_quota_status(org_id)
    if quota.eval_over:
        raise HTTPException(status_code=402, detail=f"Evaluation quota exceeded for {quota.tier} tier.")

    judge_model = payload.judge_model or "claude-haiku-4-5"

    # Built-in metrics via the shared BYOK judge.
    metric_scores = await _score_metrics(
        org_id, judge_model,
        output=payload.response, prompt=payload.prompt, context=payload.context, metrics=wanted,
    )
    raw_results: Dict[str, Dict[str, Any]] = {
        s.metric: {"score": s.score, "reason": s.reason} for s in metric_scores
    }

    # Custom scorers: resolve each slug to its saved template, run BYOK.
    if payload.custom_judges:
        templates: Dict[str, str] = {}
        for slug in payload.custom_judges:
            tmpl = await get_custom_judge_template(org_id, slug)
            if tmpl:
                templates[slug] = tmpl
        raw_results.update(await _score_custom_judges(
            org_id, judge_model,
            response=payload.response, prompt=payload.prompt, context=payload.context,
            judges=templates,
        ))

    thresholds = {**payload.thresholds, **payload.custom_judges}
    root_id = payload.root_trace_id or payload.trace_id

    try:
        await clickhouse_client.insert_evaluations(
            organization_id=org_id, trace_id=payload.trace_id,
            results=raw_results, judge_model=judge_model,
        )
    except Exception:
        pass

    for metric, data in raw_results.items():
        try:
            await trace_broker.publish(
                str(org_id),
                {
                    "kind": "enriched",
                    "enrichment": "evaluation",
                    "organization_id": str(org_id),
                    "api_key_prefix": None,
                    "trace_id": payload.trace_id,
                    "root_trace_id": root_id,
                    "evaluation": {
                        "metric": metric,
                        "score": float(data.get("score", 0.0)),
                        "evaluator": "fluiq.eval",
                        "judge_model": judge_model,
                        "details": {"reason": data.get("reason", "")},
                    },
                },
            )
        except Exception:
            pass

    failures: list[str] = []
    results: list[MetricResult] = []
    for metric, data in raw_results.items():
        score = data["score"]
        threshold = thresholds.get(metric, config.EVAL_DEFAULT_THRESHOLD)
        # See the note above: an unreachable judge must not fail the gate.
        passed = bool(data.get("error")) or score >= threshold
        if not passed and threshold > 0:
            failures.append(metric)
        results.append(MetricResult(metric=metric, score=score, reason=data.get("reason", ""), passed=passed))

    return EvaluateResponse(
        trace_id=payload.trace_id,
        scores={m: d["score"] for m, d in raw_results.items()},
        results=results,
        passed=len(failures) == 0,
        failures=failures,
    )


# ── GET /evaluate/recent-evals  (CI eval gate, API key auth) ──────────────────
#
# Moved here from /api/v1/optimize/evals when the optimization pillar was
# removed; it was always an evaluation endpoint that happened to live under the
# optimize prefix.

class EvalEntry(BaseModel):
    trace_id: Optional[str]
    metric: str
    score: Optional[float]
    evaluator: str
    judge_model: str


class RecentEvalsResponse(BaseModel):
    window_minutes: int
    total: int
    passed: int
    failed: int
    avg_score: Optional[float]
    entries: List[EvalEntry]


@evaluate_router.get("/evaluate/recent-evals", response_model=RecentEvalsResponse)
async def get_recent_evals(
    api_key: Optional[str] = Depends(extract_api_key),
    window_minutes: int = Query(default=30, ge=1, le=1440),
    threshold: float = Query(default=0.7, ge=0.0, le=1.0),
    limit: int = Query(default=200, ge=1, le=1000),
) -> RecentEvalsResponse:
    """Return evaluation scores for the last ``window_minutes`` of traces.

    Designed for CI eval gates.  The caller passes ``threshold`` and
    inspects ``failed`` > 0 to decide whether to block the PR.

    Auth: SDK API key in ``x-api-key`` header (no login required — safe for CI).
    No tier gating — all accounts with at least one evaluation can use this.
    """
    org_id = await _resolve_org(api_key or "")
    rows = await clickhouse_client.fetch_recent_evals(
        organization_id=org_id,
        window_minutes=window_minutes,
        limit=limit,
    )
    entries = [EvalEntry(**r) for r in rows]
    scores = [e.score for e in entries if e.score is not None]
    passed = sum(1 for s in scores if s >= threshold)
    failed = sum(1 for s in scores if s < threshold)
    avg_score = (sum(scores) / len(scores)) if scores else None
    return RecentEvalsResponse(
        window_minutes=window_minutes,
        total=len(entries),
        passed=passed,
        failed=failed,
        avg_score=round(avg_score, 4) if avg_score is not None else None,
        entries=entries,
    )


__all__ = ["evaluate_router", "EvaluateRequest", "EvaluateResponse", "RecentEvalsResponse", "EvalEntry"]
