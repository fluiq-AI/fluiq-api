"""Weighted composites of several scorers, reported as one number.

A run with six metrics has six answers and no verdict, so everyone invents their
own average in their head — and they invent different ones. One person weights
faithfulness heavily, another treats tone as decisive, and the two of them
disagree about whether the same run was good.

An aggregate makes the weighting explicit and shared: "quality" means 50%
faithfulness, 30% relevance, 20% tone, because somebody decided that once.

Pure functions, so the rules are testable without a database and identical
wherever they are applied.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

MAX_COMPONENTS = 12


class AggregateError(ValueError):
    """A definition that cannot be used, with a message for its author."""


def parse_components(raw: Any) -> List[Dict[str, Any]]:
    """Validate ``[{metric, weight}, ...]``.

    Weights are not required to sum to anything: they are normalised on read, so
    adding a fourth component does not mean redoing the arithmetic on the other
    three. What *is* required is that they are positive — a zero-weight
    component contributes nothing and would sit in the definition looking like
    it does something.
    """
    if not isinstance(raw, (list, tuple)) or not raw:
        raise AggregateError("An aggregate needs at least one component.")
    if len(raw) > MAX_COMPONENTS:
        raise AggregateError(f"At most {MAX_COMPONENTS} components.")

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise AggregateError("Each component must be {metric, weight}.")
        metric = str(item.get("metric") or "").strip()
        if not metric:
            raise AggregateError("Every component needs a metric.")
        if metric.lower() in seen:
            raise AggregateError(f"Duplicate component: {metric}")
        seen.add(metric.lower())
        try:
            weight = float(item.get("weight", 1))
        except (TypeError, ValueError):
            raise AggregateError(f"Component {metric!r} needs a numeric weight.") from None
        if weight <= 0:
            raise AggregateError(
                f"Component {metric!r}: weight must be greater than 0 — a "
                f"zero-weight component contributes nothing."
            )
        out.append({"metric": metric, "weight": weight})
    return out


def compute(
    components: Sequence[Mapping[str, Any]],
    scores: Mapping[str, Optional[float]],
) -> Optional[float]:
    """The aggregate's value for one set of metric scores.

    Missing components are **excluded and the remaining weights renormalised**,
    not treated as zero. A run that didn't happen to include "tone" should
    report the quality of what it did measure, rather than being marked down for
    a metric nobody ran — that would make an aggregate look worse simply for
    being applied to a narrower run.

    Returns None when nothing it names was scored, because averaging no numbers
    is not zero.
    """
    total_weight = 0.0
    total = 0.0
    for component in components:
        value = scores.get(str(component["metric"]))
        if value is None:
            continue
        weight = float(component.get("weight", 1))
        total += float(value) * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return total / total_weight


def coverage(
    components: Sequence[Mapping[str, Any]],
    scores: Mapping[str, Optional[float]],
) -> Dict[str, Any]:
    """Which components contributed and which were missing.

    Reported alongside the number because an aggregate computed from two of its
    five parts is a different claim from one computed from all five, and the
    number alone cannot say which it is.
    """
    present, missing = [], []
    for component in components:
        metric = str(component["metric"])
        (present if scores.get(metric) is not None else missing).append(metric)
    return {
        "present": present,
        "missing": missing,
        "complete": not missing,
    }


def apply_all(
    aggregates: Sequence[Mapping[str, Any]],
    scores: Mapping[str, Optional[float]],
) -> Dict[str, Any]:
    """``{slug: {value, coverage}}`` for every aggregate an org has defined."""
    out: Dict[str, Any] = {}
    for aggregate in aggregates:
        components = aggregate.get("components") or []
        out[str(aggregate["slug"])] = {
            "name":     aggregate.get("name") or aggregate["slug"],
            "value":    compute(components, scores),
            "coverage": coverage(components, scores),
        }
    return out


__all__ = [
    "MAX_COMPONENTS",
    "AggregateError",
    "apply_all",
    "compute",
    "coverage",
    "parse_components",
]
