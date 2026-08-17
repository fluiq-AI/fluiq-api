import asyncio
import json
import re
import config
import uuid
from datetime import datetime, timezone
from typing import Optional

from aiokafka.errors import MessageSizeTooLargeError
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, wait_for_reply
from db_queues.postgresql.auth import get_organization, resolve_api_key
from db_queues.postgresql.guardrails import get_policy
from db_queues.postgresql import online_rules
from realtime import running_registry, trace_broker
from routes.auth.helper import extract_api_key, get_current_session
from shared.cache import cached_json, dash_key
from shared.ids import coerce_trace_uuid
from shared.online_scoring import scorers_of, select_rule
from shared.quotas import (
    QuotaStatus,
    UNLIMITED,
    UNLIMITED_RETENTION_DAYS,
    bump_eval_count,
    bump_trace_count,
    get_quota_status,
)

from .model import (
    IngestPayload,
    SpendingResponse,
    SpendingDay,
    TraceListResponse,
    TraceRecord,
)

import logging
from typing import Any

router = APIRouter()
logger = logging.getLogger(__name__)

_RETRIEVAL_APIS = {
    "query",
    "search",
    "near_text",
    "near_vector",
    "hybrid",
    "bm25",
    "query_points",
    "fetch_objects",
}


def _is_retrieval_event(event: dict) -> bool:
    return (
        event.get("type") == "vectorstore"
        and event.get("api") in _RETRIEVAL_APIS
    )


@router.post("/ingest")
async def ingestion(
    payload: IngestPayload,
    api_key: Optional[str] = Depends(extract_api_key),
):
    api_key = api_key or payload.api_key
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
        )
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    org_id, prefix, _key_id = resolved

    # Enforce tier quotas before doing any Kafka work. Trace quota is a hard
    # stop (402); eval quota gates only the evaluations fan-out so tracing
    # keeps flowing even after the eval cap is reached.
    try:
        quota = await get_quota_status(org_id)
    except Exception:
        # Quota stores unreachable (e.g. ClickHouse restarting). Ingestion must
        # not lose data because a metering read failed — Kafka exists precisely
        # to buffer through store outages. Fail open: accept the trace, skip the
        # eval fan-out for this request (eval is best-effort), and stamp the
        # generous retention so a paid org's rows are never marked for early
        # deletion by a fallback guess.
        logger.warning(
            "[INGEST] quota lookup failed; failing open (trace accepted, eval fan-out skipped)",
            exc_info=True,
        )
        quota = QuotaStatus(
            tier="Unknown",
            trace_count=0, trace_quota=UNLIMITED,
            eval_count=0, eval_quota=0,   # eval_over → True: skips eval fan-out
            retention_days=UNLIMITED_RETENTION_DAYS,
        )
    if quota.trace_over:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"Trace quota exceeded for {quota.tier} tier "
                f"({quota.trace_count}/{quota.trace_quota}). "
                f"Upgrade your plan to resume ingestion."
            ),
        )

    event = payload.event
    # Normalize caller-supplied identifiers to valid UUIDs. ClickHouse stores
    # trace_id/root_trace_id as UUID columns, so a non-UUID string would crash
    # the tracer insert (and drop the trace). Map deterministically so a
    # non-UUID trace tree stays internally linked (see shared.ids).
    trace_id = coerce_trace_uuid(event.get("trace_id")) or str(uuid.uuid4())
    event["trace_id"] = trace_id
    for _link in ("root_trace_id", "parent_id"):
        if event.get(_link):
            event[_link] = coerce_trace_uuid(event[_link])

    # Strip SDK-embedded configs before persisting the trace.
    eval_config     = event.pop("_eval_config",     None)
    security_config = event.pop("_security_config", None)
    # fluiq.eval() sets this on every traced event; instrument() alone never
    # does. Evaluation is opt-in behind it (no ambient auto-eval). Treat an
    # explicit eval_config as also enabling eval so older SDKs (which send
    # _eval_config on LLM warn-mode but not the _eval flag) keep working.
    eval_enabled    = bool(event.pop("_eval", False)) or eval_config is not None
    # Tags stripped off the event and stored in their own table; user metadata
    # stays on the event, where the drawer can render it and JSONExtract can
    # filter it without a second write path.
    sdk_tags        = normalize_tags(event.pop("_tags", None))
    user_metadata   = event.pop("_metadata", None)
    if isinstance(user_metadata, dict) and user_metadata:
        # Kept under a reserved key so it can never collide with a field the
        # tracer or an integration writes.
        event["fluiq_metadata"] = {
            str(k): v for k, v in user_metadata.items()
            if isinstance(v, (str, int, float, bool))
        }

    is_running = event.get("status") == "running"

    # ── Response gate ──────────────────────────────────────────────────────────
    # Runs BEFORE Kafka publish so the result is embedded in the stored event.
    # When scan_responses=True for this org, scan the response synchronously and
    # stamp event["response_gate_blocked"]=True if flagged — this reaches ClickHouse.
    response_gated = False
    ingest_extra: dict = {}

    if not is_running and security_config and event.get("type") == "llm":
        _resp = event.get("response") or event.get("output") or ""
        if isinstance(_resp, list):
            _resp = " ".join(str(x) for x in _resp if x)
        elif not isinstance(_resp, str):
            _resp = str(_resp) if _resp else ""

        if _resp:
            _guardrail = security_config.get("guardrail", "default")
            policy = await get_policy(org_id, slug=_guardrail)
            if policy.scan_responses:
                correlation_id = str(uuid.uuid4())
                try:
                    await kafka_queue.add_job(
                        {
                            "operation":      "response_gate_check",
                            "response":       _resp,
                            "correlation_id": correlation_id,
                            "pii_ignore":     policy.pii_ignore,
                        },
                        topic=config.KAFKA_SECURITY_TOPIC,
                    )
                    gate = await wait_for_reply(correlation_id, timeout=3.0)
                    if gate and gate.get("response_blocked"):
                        event["response_gate_blocked"] = True  # persisted in ClickHouse
                        ingest_extra = {
                            "response_blocked": True,
                            "block_reason":     gate.get("block_reason"),
                            "risk_level":       gate.get("risk_level", "high"),
                            "attack_types":     gate.get("attack_types", []),
                        }
                        response_gated = True
                except Exception:
                    pass  # fail open

    # ── Kafka publish ──────────────────────────────────────────────────────────
    job = {
        "organization_id": str(org_id),
        "api_key_prefix":  prefix,
        "trace_id":        trace_id,
        "event":           event,
        # Per-row retention window for this org's tier (Free=14d, paid=never).
        # Carried through Kafka so the tracer stamps it onto the ClickHouse row.
        "retention_days":  quota.retention_days,
    }
    # A trace event larger than the producer's max_request_size used to crash
    # here with an unhandled MessageSizeTooLargeError → 500. Return an explicit
    # 413 instead so the SDK/client gets an actionable error and we don't log a
    # traceback for an oversized-payload condition. Guarding the trace publish is
    # enough: the eval/security fan-outs below reuse the same `event`, so a
    # rejected trace short-circuits before they run.
    try:
        await kafka_queue.add_job(job, topic=config.KAFKA_TRACE_TOPIC, key=str(org_id))
    except MessageSizeTooLargeError as exc:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                "Trace event exceeds the maximum ingestion size. Reduce the size "
                "of prompts, responses, or tool outputs captured in this event."
            ),
        ) from exc
    if not is_running:
        bump_trace_count(org_id)

    # Tags are written after the trace publish so a tag-store hiccup can never
    # cost the trace itself — the label is the accessory, the trace is the data.
    if sdk_tags and not is_running:
        try:
            await clickhouse_client.add_trace_tags(
                organization_id=org_id,
                trace_id=uuid.UUID(str(trace_id)),
                tags=sdk_tags,
                root_trace_id=(
                    uuid.UUID(str(event["root_trace_id"]))
                    if event.get("root_trace_id") else None
                ),
                source="sdk",
            )
        except Exception:  # noqa: BLE001
            logger.exception("[INGEST] tag write failed org=%s trace=%s", org_id, trace_id)

    # Evaluation is opt-in: it runs ONLY when the caller enabled it via
    # fluiq.eval() (the SDK sets the `_eval` flag, plus `_eval_config` with the
    # metrics/thresholds on LLM calls). instrument() alone never auto-evaluates.
    # Quota-gated; the worker's judge cache keeps repeated prompts cheap.
    eval_skipped = False
    scored_inline = False
    if not is_running and eval_enabled:
        if quota.eval_over:
            eval_skipped = True
        elif _is_retrieval_event(event):
            # Retrieval → context precision (the worker's default for retrieval
            # events); the `_eval` flag alone gates it since retrieval calls
            # carry no `_eval_config`.
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
            scored_inline = True
        elif event.get("type") == "llm" and eval_config:
            await kafka_queue.add_job(
                {**job, "eval_config": eval_config, "operation": "sdk_llm"},
                topic=config.KAFKA_EVAL_TOPIC,
                key=str(org_id),
            )
            bump_eval_count(org_id)
            scored_inline = True

    # Online scoring: continuous evaluation of traffic the SDK said nothing
    # about. Skipped when this trace was already scored above — a rule exists to
    # cover what fluiq.eval() doesn't, not to grade the same call twice.
    online_rule = None
    if not is_running and not scored_inline and not quota.eval_over:
        online_rule = await _apply_online_rules(org_id, event, trace_id, job)

    if not is_running and security_config:
        # Forward the org's warn-mode PII policy so the worker suppresses ignored
        # entity types (cached get_policy — cheap; reuses the response-gate fetch).
        _sec_policy = await get_policy(org_id, slug=security_config.get("guardrail", "default"))
        security_job = {
            **job,
            "security_config": security_config,
            "operation":       "sdk_security",
            "pii_ignore":      _sec_policy.pii_ignore,
            "allowed_tools":   _sec_policy.allowed_tools,
        }
        if response_gated:
            security_job["response_gated"] = True
        await kafka_queue.add_job(security_job, topic=config.KAFKA_SECURITY_TOPIC, key=str(org_id))

    return {
        "ok": True,
        "trace_id": trace_id,
        "eval_skipped": eval_skipped,
        # Named so a developer can see *why* a trace got scored without them
        # asking for it — an unexplained judge bill is a support ticket.
        **({"online_rule": online_rule} if online_rule else {}),
        **ingest_extra,
    }


async def _apply_online_rules(
    org_id: uuid.UUID,
    event: dict,
    trace_id: str,
    job: dict,
) -> Optional[str]:
    """Score this trace if an online rule selects it. Returns the rule's name.

    Best-effort throughout: online scoring is a background quality signal, and
    no failure in it may cost the customer the trace itself. A broken rule, an
    unreachable database, or a full queue all end with the trace stored and
    unscored.
    """
    try:
        rules = await online_rules.active_rules(org_id)
        if not rules:
            return None

        # A root span is one that is its own root: nested spans of an agent run
        # would otherwise each be scored, multiplying the bill by trajectory depth.
        root_id = event.get("root_trace_id") or event.get("trace_id")
        is_root = str(root_id or trace_id) == str(event.get("trace_id") or trace_id)

        rule = select_rule(rules, event, str(trace_id), is_root)
        if rule is None:
            return None

        metrics, custom_judges = scorers_of(rule)
        scoring_job = {
            **job,
            "operation": "sdk_llm",
            "eval_config": {
                "metrics":       metrics,
                "custom_judges": custom_judges,
            },
            # Stamped so a score can be traced back to the rule that ordered it.
            "online_rule_id":   rule.get("rule_id"),
            "online_rule_name": rule.get("name"),
        }
        if rule.get("judge"):
            scoring_job["judge"] = rule["judge"]

        await kafka_queue.add_job(
            scoring_job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id),
        )
        bump_eval_count(org_id)
        return str(rule.get("name") or "")
    except Exception:  # noqa: BLE001
        logger.exception("[INGEST] online scoring failed org=%s trace=%s", org_id, trace_id)
        return None


# ── Tags ──────────────────────────────────────────────────────────────────────

_TAG_RE = re.compile(r"^[a-z0-9][a-z0-9._\-/]{0,62}$")
MAX_TAGS_PER_TRACE = 25


def normalize_tags(raw: Any) -> list[str]:
    """Clean a caller's tags, dropping anything unusable rather than failing.

    Lowercased and de-duplicated, because ``Prompt-A`` and ``prompt-a`` being
    different tags is a trap that only shows up once a filter silently returns
    nothing. Invalid entries are dropped rather than rejected: tagging is a
    side-channel on an ingest call, and failing a whole trace over a stray
    character would cost the customer data to gain them nothing.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, (list, tuple, set)):
        return []
    out: list[str] = []
    for item in raw:
        tag = str(item or "").strip().lower()
        if tag and _TAG_RE.match(tag) and tag not in out:
            out.append(tag)
        if len(out) >= MAX_TAGS_PER_TRACE:
            break
    return out


class TagRequest(BaseModel):
    tags: list[str]


@router.get("/traces/tags")
async def list_org_tags(session: dict = Depends(get_current_session)):
    """Every tag the org uses, most-used first — for the filter dropdown."""
    org_id = uuid.UUID(session["org_id"])
    return {"tags": await clickhouse_client.fetch_org_tags(org_id)}


@router.post("/traces/{trace_id}/tags", status_code=status.HTTP_201_CREATED)
async def add_tags(
    trace_id: uuid.UUID,
    payload: TagRequest,
    session: dict = Depends(get_current_session),
):
    """Tag a trace from the dashboard."""
    tags = normalize_tags(payload.tags)
    if not tags:
        raise HTTPException(
            status_code=422,
            detail=(
                "No usable tags. Use lowercase letters, digits, and . _ - / "
                "(up to 63 characters)."
            ),
        )
    await clickhouse_client.add_trace_tags(
        organization_id=uuid.UUID(session["org_id"]),
        trace_id=trace_id,
        tags=tags,
        source="dashboard",
        created_by=str(session.get("sub") or ""),
    )
    return {"tags": tags}


@router.delete("/traces/{trace_id}/tags/{tag}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_tag(
    trace_id: uuid.UUID,
    tag: str,
    session: dict = Depends(get_current_session),
):
    await clickhouse_client.remove_trace_tag(
        organization_id=uuid.UUID(session["org_id"]),
        trace_id=trace_id,
        tag=tag.strip().lower(),
    )


@router.get("/traces", response_model=TraceListResponse)
async def list_traces(
    session: dict = Depends(get_current_session),
    key_id: Optional[uuid.UUID] = Query(default=None),
    agent_key: Optional[str] = Query(default=None),
    agent_kind: Optional[str] = Query(default=None),
    root_trace_id: Optional[uuid.UUID] = Query(default=None),
    roots_only: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="newest"),
    status: str = Query(default="all"),
    security: str = Query(default="all"),
    integration: str = Query(default="all"),
    quality: str = Query(default="all"),
    # Repeatable: ?tag=prompt-a&tag=canary means "carrying both".
    tag: Optional[list[str]] = Query(default=None),
) -> TraceListResponse:
    """Return traces for the caller's organization.

    When `key_id` is omitted, traces from all of the org's API keys are
    returned. When provided, results are filtered to that key (404 if the
    key does not belong to the org). When `agent_key` and `agent_kind` are
    provided, only root traces for that agent are returned.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )

    selected_prefix: Optional[str] = None
    if key_id is not None:
        match = next(
            (k for k in (organization.api_keys or []) if k.key_id == key_id),
            None,
        )
        if match is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found",
            )
        selected_prefix = match.prefix

    rows = await clickhouse_client.fetch_traces(
        organization_id=org_id,
        api_key_prefix=selected_prefix,
        agent_key=agent_key,
        agent_kind=agent_kind,
        root_trace_id=root_trace_id,
        roots_only=roots_only,
        limit=limit,
        offset=offset,
        sort=sort,
        status=status,
        security=security,
        integration=integration,
        quality=quality,
        tags=[t for t in (tag or []) if t and t.strip()] or None,
    )
    persisted = [TraceRecord(**row) for row in rows]
    # Tags for the page, fetched after: they are many-per-trace, so joining them
    # into the trace query would either fan the page out or add a third
    # groupArray subquery to a query that already has two. Best-effort — a tag
    # lookup failing must not cost the trace list.
    try:
        page_ids = [
            str(p.event.get("trace_id"))
            for p in persisted
            if isinstance(p.event, dict) and p.event.get("trace_id")
        ]
        tag_map = await clickhouse_client.fetch_tags_for_traces(org_id, page_ids)
        for record in persisted:
            tid = str(record.event.get("trace_id")) if isinstance(record.event, dict) else ""
            record.tags = tag_map.get(tid, [])
    except Exception:  # noqa: BLE001
        logger.exception("[TRACES] tag lookup failed org=%s", org_id)
    # In-flight runs only surface on the first page; subsequent pages page
    # through historical (durable) rows where ephemeral entries don't
    # belong. The registry is scoped to this replica only — running rows
    # for connections that landed on a different replica won't appear
    # here, which is the same trade-off the SSE broker already makes.
    running: list[TraceRecord] = []
    if offset == 0 and status in ("all", "running"):
        in_flight = await running_registry.list_for(
            organization_id=str(org_id),
            api_key_prefix=selected_prefix,
        )
        seen: set[str] = {
            str(p.event.get("trace_id"))
            for p in persisted
            if isinstance(p.event, dict) and p.event.get("trace_id")
        }
        for entry in in_flight:
            tid = entry["trace_id"]
            if tid in seen:
                continue
            # When roots_only is requested, skip in-flight spans (those
            # whose root_trace_id differs from their own trace_id).
            if roots_only:
                rtid = entry.get("event", {}).get("root_trace_id")
                if rtid and rtid != tid:
                    continue
            ingested_ms = entry.get("ingested_at_ms")
            ingested_at = (
                datetime.fromtimestamp(ingested_ms / 1000.0, tz=timezone.utc)
                if isinstance(ingested_ms, (int, float))
                else datetime.now(tz=timezone.utc)
            )
            running.append(
                TraceRecord(
                    api_key_prefix=entry.get("api_key_prefix") or "",
                    event=entry.get("event") or {},
                    ingested_at=ingested_at,
                    cost=None,
                    currency=None,
                    evaluations=[],
                )
            )
    return TraceListResponse(
        traces=running + persisted,
        limit=limit,
        offset=offset,
    )


# Provider name (as stored in trace_costs) → spending-chart bucket.
_SPEND_PROVIDER_BUCKET = {
    "openai": "openai",
    "anthropic": "anthropic",
    "google": "google",
}


@router.get("/traces/spending", response_model=SpendingResponse)
async def trace_spending(
    session: dict = Depends(get_current_session),
    days: int = Query(default=30, ge=1, le=365),
) -> SpendingResponse:
    """Daily spend by provider for the dashboard spending chart.

    Server-side aggregation so the chart doesn't pull ~1000 fully-joined trace
    rows on first paint (see fetch_spending_by_day).
    """
    org_id = uuid.UUID(session["org_id"])
    rows = await cached_json(
        dash_key(org_id, "spending_by_day", days=days),
        60,
        lambda: clickhouse_client.fetch_spending_by_day(org_id, days=days),
    )

    by_date: dict[str, SpendingDay] = {}
    for row in rows:
        day = row["day"]
        if not day:
            continue
        bucket = _SPEND_PROVIDER_BUCKET.get((row["provider"] or "").lower(), "other")
        cost = row["cost"]
        entry = by_date.get(day)
        if entry is None:
            entry = SpendingDay(date=day)
            by_date[day] = entry
        setattr(entry, bucket, getattr(entry, bucket) + cost)
        entry.all += cost

    return SpendingResponse(days=sorted(by_date.values(), key=lambda d: d.date))


class RollupItem(BaseModel):
    run_cost: float
    run_tokens: int
    span_count: int
    quality_min: Optional[float] = None
    quality_avg: Optional[float] = None
    quality_count: int
    security_risk_max: float
    security_should_block: bool
    security_detections: int


class RollupRequest(BaseModel):
    root_ids: list[str]


class RollupResponse(BaseModel):
    rollups: dict[str, RollupItem]


# Bound the per-request fan-out so one call can't ask for an unbounded IN list.
_MAX_ROLLUP_IDS = 500


@router.post("/traces/rollups", response_model=RollupResponse)
async def trace_rollups(
    body: RollupRequest,
    session: dict = Depends(get_current_session),
) -> RollupResponse:
    """Precomputed per-run cost / quality / security totals for a set of roots.

    The Traces list and drawer pass the visible root_trace_ids and get back each
    run's rolled-up numbers straight from the AggregatingMergeTree rollups —
    replacing the old per-root prefetch that pulled every child to sum them in
    the browser. Roots with no rollup yet are omitted (caller treats as zero).
    """
    org_id = uuid.UUID(session["org_id"])
    ids = body.root_ids[:_MAX_ROLLUP_IDS]
    data = await clickhouse_client.get_root_rollups(org_id, ids)
    return RollupResponse(rollups={k: RollupItem(**v) for k, v in data.items()})


SSE_HEARTBEAT_SECONDS = 15.0


@router.get("/traces/stream")
async def stream_traces(
    request: Request,
    session: dict = Depends(get_current_session),
    key_id: Optional[uuid.UUID] = Query(default=None),
):
    """Server-Sent Events stream of newly persisted traces for the caller's
    org. Each event payload mirrors the TraceRecord shape (minus cost and
    evaluations, which arrive on later writes and surface on next refresh)
    so the frontend can prepend without re-fetching.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )

    # Mirror the /ingest gate: orgs over their trace cap lose realtime
    # visibility too. The handshake fails fast (402) instead of opening a
    # long-lived connection that would never see new traces anyway.
    quota = await get_quota_status(org_id)
    if quota.trace_over:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"Trace quota exceeded for {quota.tier} tier "
                f"({quota.trace_count}/{quota.trace_quota}). "
                f"Upgrade your plan to resume realtime streaming."
            ),
        )

    selected_prefix: Optional[str] = None
    if key_id is not None:
        match = next(
            (k for k in (organization.api_keys or []) if k.key_id == key_id),
            None,
        )
        if match is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found",
            )
        selected_prefix = match.prefix

    async def event_source():
        # ``trace_broker.subscribe`` is an async context manager yielding
        # the underlying asyncio.Queue. We wrap ``queue.get()`` in
        # ``wait_for`` for heartbeats; cancelling that coroutine on
        # timeout doesn't damage the queue, so the loop keeps running for
        # the lifetime of the connection.
        async with trace_broker.subscribe(str(org_id)) as queue:
            yield {"event": "ready", "data": "{}"}
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(
                        queue.get(),
                        timeout=SSE_HEARTBEAT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": "{}"}
                    continue
                if (
                    selected_prefix is not None
                    and isinstance(message, dict)
                    and message.get("api_key_prefix") != selected_prefix
                ):
                    continue
                # Route by ``kind`` so the frontend can prepend new rows
                # ("trace"), merge cost / evaluation updates by trace_id
                # ("trace.enriched"), or render an in-progress placeholder
                # ("trace.started") that the eventual "trace" event will
                # replace in place. Older producers without ``kind`` fall
                # back to the completed trace event.
                kind = message.get("kind") if isinstance(message, dict) else None
                if kind == "enriched":
                    event_name = "trace.enriched"
                elif kind == "started":
                    event_name = "trace.started"
                else:
                    event_name = "trace"
                yield {
                    "event": event_name,
                    "data": json.dumps(message, default=str),
                }

    return EventSourceResponse(event_source())