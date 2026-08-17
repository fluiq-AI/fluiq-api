"""Deciding whether a live trace gets scored by an online rule.

Pure functions, deliberately: this runs on every ingested trace, and the logic
that spends a customer's money on judge calls should be testable without a
database, a queue, or a clock.

Sampling is **deterministic on the trace id**, not random. Two consequences that
both matter:

* the same trace scored twice (a retry, a replay, a second rule evaluation pass)
  makes the same decision, so a duplicate never doubles the bill
* a trace either is or is not in the sample, which means "why wasn't this
  scored?" has an answer you can reproduce, rather than "it rolled badly"

The cost of determinism is that the sample is a fixed subset rather than an
independent draw each time. For quality monitoring that is a feature: the
population being measured is stable between rules and over time.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Mapping, Optional


def _as_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value if v]


def in_sample(trace_id: str, rule_id: str, sample_rate: float) -> bool:
    """Whether this trace falls in a rule's sample.

    Hashed on trace + rule together, so two rules at 10% do not select the same
    10% of traffic. Sampling the identical slice twice would make a second rule
    look free while leaving 90% of traffic unexamined by either.
    """
    if sample_rate >= 100:
        return True
    if sample_rate <= 0:
        return False
    digest = hashlib.sha256(f"{rule_id}:{trace_id}".encode()).digest()
    # 16 bits is finer than any sample rate anyone sets, and cheap.
    bucket = int.from_bytes(digest[:2], "big") / 65535.0 * 100.0
    return bucket < sample_rate


def matches(rule: Mapping[str, Any], event: Mapping[str, Any], is_root: bool) -> bool:
    """Whether a rule applies to this event, ignoring sampling.

    An empty filter list means "any" rather than "none" — a rule with no
    integration filter is a rule about all integrations, which is what someone
    who left the field alone meant.
    """
    if (rule.get("span_scope") or "root") == "root" and not is_root:
        return False

    # Only LLM calls have an answer to grade. Retrieval and tool spans reach the
    # evaluator by other paths with their own metrics.
    if (event.get("type") or "") != "llm":
        return False

    integrations = _as_list(rule.get("integrations"))
    if integrations:
        actual = str(event.get("integration") or "").upper()
        if actual not in {i.upper() for i in integrations}:
            return False

    models = _as_list(rule.get("models"))
    if models:
        actual_model = str(event.get("model") or "")
        if not any(m.lower() == actual_model.lower() for m in models):
            return False

    return True


def scorers_of(rule: Mapping[str, Any]) -> tuple[List[str], Dict[str, float]]:
    """The rule's built-in metrics and custom scorers, normalised."""
    metrics = _as_list(rule.get("metrics"))
    raw_judges = rule.get("custom_judges") or {}
    judges: Dict[str, float] = {}
    if isinstance(raw_judges, Mapping):
        for slug, threshold in raw_judges.items():
            try:
                judges[str(slug)] = float(threshold)
            except (TypeError, ValueError):
                judges[str(slug)] = 0.5
    return metrics, judges


def select_rule(
    rules: List[Mapping[str, Any]],
    event: Mapping[str, Any],
    trace_id: str,
    is_root: bool,
) -> Optional[Mapping[str, Any]]:
    """The first rule that both matches and samples this trace, or None.

    **First, not all.** Two overlapping rules would otherwise judge the same
    trace twice and bill for both, which is a surprising way to spend money. If
    a user wants several scorers on one slice of traffic, that is one rule with
    several scorers — which is also cheaper, because the metrics share a call
    path. Rules are ordered by creation, so the oldest matching rule wins and
    adding a new one cannot silently change what an existing one does.
    """
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        if not matches(rule, event, is_root):
            continue
        metrics, judges = scorers_of(rule)
        if not metrics and not judges:
            # A rule with nothing to score would consume the trace and score
            # nothing, shadowing any later rule that would have scored it.
            continue
        if not in_sample(trace_id, str(rule.get("rule_id") or ""), float(rule.get("sample_rate") or 0)):
            continue
        return rule
    return None


__all__ = ["in_sample", "matches", "scorers_of", "select_rule"]
