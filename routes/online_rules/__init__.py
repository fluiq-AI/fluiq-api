"""fluiq-api — Online scoring rules.

  GET    /api/v1/online-rules            list rules (session)
  POST   /api/v1/online-rules            create (session)
  PATCH  /api/v1/online-rules/{id}       update (session)
  DELETE /api/v1/online-rules/{id}       delete (session)
  POST   /api/v1/online-rules/test       dry-run a rule against recent traces

A rule continuously scores a sample of live traffic, independently of whether
the SDK asked for evaluation. See ``shared.online_scoring`` for the matching and
sampling rules, and ``docs/decision-projects.md`` for why ``project_id`` is
present but unused.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator, model_validator

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql import online_rules as store
from db_queues.postgresql.auth import get_org_tier
from routes.auth.helper import get_current_session
from shared.online_scoring import in_sample, matches, scorers_of

online_rules_router = APIRouter()

#: Built-in metrics a rule may apply. Mirrors the evaluator's registry.
VALID_METRICS = frozenset({
    "hallucination", "faithfulness", "relevance",
    "toxicity", "coherence", "completeness",
})

#: Online scoring spends judge calls continuously rather than on request, so it
#: is a paid capability — the same tiers that get eval alerts.
PAID_TIERS = frozenset({"Starter", "Team", "Growth", "Enterprise"})

MAX_RULES_PER_ORG = 20


class RulePayload(BaseModel):
    name:          str
    description:   Optional[str] = None
    enabled:       bool = True
    metrics:       List[str] = Field(default_factory=list)
    custom_judges: Dict[str, float] = Field(default_factory=dict)
    sample_rate:   float = Field(10.0, ge=0.0, le=100.0)
    span_scope:    str = "root"
    integrations:  List[str] = Field(default_factory=list)
    models:        List[str] = Field(default_factory=list)
    judge:         Optional[str] = None
    project_id:    Optional[uuid.UUID] = None

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 120:
            raise ValueError("name must be 120 characters or fewer")
        return v

    @field_validator("span_scope")
    @classmethod
    def _scope(cls, v: str) -> str:
        v = (v or "root").strip().lower()
        if v not in ("root", "all"):
            raise ValueError("span_scope must be 'root' or 'all'")
        return v

    @field_validator("metrics")
    @classmethod
    def _metrics(cls, v: List[str]) -> List[str]:
        cleaned = [m.strip().lower() for m in v if m and m.strip()]
        unknown = sorted(set(cleaned) - VALID_METRICS)
        if unknown:
            raise ValueError(
                f"unknown metrics: {', '.join(unknown)}. "
                f"Supported: {', '.join(sorted(VALID_METRICS))}"
            )
        return cleaned

    @field_validator("custom_judges")
    @classmethod
    def _judges(cls, v: Dict[str, float]) -> Dict[str, float]:
        for slug, threshold in v.items():
            if not 0.0 <= float(threshold) <= 1.0:
                raise ValueError(f"threshold for {slug!r} must be between 0 and 1")
        return {str(k): float(t) for k, t in v.items()}

    @model_validator(mode="after")
    def _needs_a_scorer(self) -> "RulePayload":
        if not self.metrics and not self.custom_judges:
            # A rule with nothing to score would consume traces from the sample
            # and produce no scores — worse than not existing, because it also
            # shadows any later rule that would have scored them.
            raise ValueError("a rule needs at least one metric or custom scorer")
        return self


class RulePatch(BaseModel):
    """Every field optional — a PATCH writes only what it carries."""
    name:          Optional[str] = None
    description:   Optional[str] = None
    enabled:       Optional[bool] = None
    metrics:       Optional[List[str]] = None
    custom_judges: Optional[Dict[str, float]] = None
    sample_rate:   Optional[float] = Field(default=None, ge=0.0, le=100.0)
    span_scope:    Optional[str] = None
    integrations:  Optional[List[str]] = None
    models:        Optional[List[str]] = None
    judge:         Optional[str] = None


class TestRuleRequest(BaseModel):
    """A rule to dry-run against traffic already recorded."""
    metrics:      List[str] = Field(default_factory=list)
    sample_rate:  float = Field(10.0, ge=0.0, le=100.0)
    span_scope:   str = "root"
    integrations: List[str] = Field(default_factory=list)
    models:       List[str] = Field(default_factory=list)


async def _require_paid(session: dict) -> uuid.UUID:
    org_id = uuid.UUID(session["org_id"])
    tier = await get_org_tier(org_id)
    if tier not in PAID_TIERS:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                "Online scoring continuously evaluates live traffic and is "
                f"available on paid plans. Your plan: {tier}."
            ),
        )
    return org_id


@online_rules_router.get("/online-rules")
async def get_rules(session: dict = Depends(get_current_session)):
    org_id = uuid.UUID(session["org_id"])
    return {"rules": await store.list_rules(org_id)}


@online_rules_router.post("/online-rules", status_code=status.HTTP_201_CREATED)
async def create(
    payload: RulePayload,
    session: dict = Depends(get_current_session),
):
    org_id = await _require_paid(session)
    existing = await store.list_rules(org_id)
    if len(existing) >= MAX_RULES_PER_ORG:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"An organization can hold {MAX_RULES_PER_ORG} rules. "
                "Combine scorers into one rule rather than adding another — "
                "several scorers on one rule cost less than several rules."
            ),
        )
    return await store.create_rule(org_id, **payload.model_dump())


@online_rules_router.patch("/online-rules/{rule_id}")
async def update(
    rule_id: uuid.UUID,
    payload: RulePatch,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update.")
    # Re-validate through the full model so a PATCH can't reach a state POST
    # would have rejected (an unknown metric, an out-of-range threshold).
    if any(k in fields for k in ("metrics", "custom_judges", "span_scope", "name")):
        current = next(
            (r for r in await store.list_rules(org_id) if r["rule_id"] == str(rule_id)), None,
        )
        if current is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
        merged = {**{k: current[k] for k in RulePayload.model_fields if k in current}, **fields}
        merged.pop("project_id", None)
        try:
            RulePayload(**merged)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    rule = await store.update_rule(rule_id, org_id, **fields)
    if rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return rule


@online_rules_router.delete(
    "/online-rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT,
)
async def remove(
    rule_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    if not await store.delete_rule(rule_id, org_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")


@online_rules_router.post("/online-rules/test")
async def test_rule(
    payload: TestRuleRequest,
    session: dict = Depends(get_current_session),
):
    """Dry-run a rule against recent traces: how many would it have scored?

    A sample rate is an abstract number until you see what it costs. This
    replays the rule over traffic already recorded and reports how many traces
    matched and how many of those would have been judged — so the decision is
    made before the money is spent, not after.
    """
    org_id = uuid.UUID(session["org_id"])
    rows = await clickhouse_client.fetch_traces(org_id, limit=500)

    # A fixed id, so the preview is stable between calls on the same traffic.
    # The real rule's own id selects a different slice, which is the point of
    # hashing on it — this only predicts the *rate*, not the exact members.
    probe_id = "preview"
    rule = {
        "span_scope":   payload.span_scope,
        "integrations": payload.integrations,
        "models":       payload.models,
    }

    considered = matched = sampled = 0
    examples: List[Dict[str, Any]] = []
    for row in rows:
        event = row.get("event") or {}
        if not isinstance(event, dict):
            continue
        considered += 1
        trace_id = str(event.get("trace_id") or row.get("trace_id") or "")
        root_id = event.get("root_trace_id") or trace_id
        is_root = str(root_id) == str(trace_id)
        if not matches(rule, event, is_root):
            continue
        matched += 1
        if in_sample(trace_id, probe_id, payload.sample_rate):
            sampled += 1
            if len(examples) < 5:
                examples.append({
                    "trace_id":    trace_id,
                    "model":       event.get("model"),
                    "integration": event.get("integration"),
                })

    metrics, _ = scorers_of({"metrics": payload.metrics})
    return {
        "considered": considered,
        "matched":    matched,
        "sampled":    sampled,
        # What it would have cost, in judge calls — counts not money, matching
        # the rest of the product.
        "est_judge_calls": sampled * max(len(metrics), 1),
        "examples":   examples,
        "window":     "last 500 traces",
    }


__all__ = ["online_rules_router"]
