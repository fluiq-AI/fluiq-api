"""Aggregate scores — weighted composites of several scorers.

The rules that matter are about *missing* components, because a run rarely
includes every metric an aggregate names, and the obvious handling of that
(treat missing as zero) is wrong in a way that looks right.
"""
from __future__ import annotations

import pytest

from shared.aggregates import (
    MAX_COMPONENTS,
    AggregateError,
    apply_all,
    compute,
    coverage,
    parse_components,
)

QUALITY = [
    {"metric": "faithfulness", "weight": 0.5},
    {"metric": "relevance", "weight": 0.3},
    {"metric": "tone", "weight": 0.2},
]


# ══ Definitions ══════════════════════════════════════════════════════════════

def test_a_valid_definition_round_trips():
    assert parse_components(QUALITY) == QUALITY


def test_an_aggregate_needs_a_component():
    with pytest.raises(AggregateError, match="at least one component"):
        parse_components([])


def test_weights_need_not_sum_to_one():
    """They are normalised on read, so adding a fourth component does not mean
    redoing the arithmetic on the other three."""
    parsed = parse_components([
        {"metric": "a", "weight": 3},
        {"metric": "b", "weight": 7},
    ])
    assert [c["weight"] for c in parsed] == [3.0, 7.0]


def test_a_zero_weight_is_rejected():
    """It contributes nothing while sitting in the definition looking like it
    does something."""
    with pytest.raises(AggregateError, match="greater than 0"):
        parse_components([{"metric": "a", "weight": 0}])


def test_a_negative_weight_is_rejected():
    with pytest.raises(AggregateError, match="greater than 0"):
        parse_components([{"metric": "a", "weight": -1}])


def test_a_duplicate_metric_is_rejected():
    """Two entries for one metric would weight it twice under one name."""
    with pytest.raises(AggregateError, match="Duplicate"):
        parse_components([{"metric": "a", "weight": 1}, {"metric": "A", "weight": 1}])


def test_a_missing_metric_name_is_rejected():
    with pytest.raises(AggregateError, match="needs a metric"):
        parse_components([{"weight": 1}])


def test_a_non_numeric_weight_is_rejected():
    with pytest.raises(AggregateError, match="numeric weight"):
        parse_components([{"metric": "a", "weight": "heavy"}])


def test_components_are_bounded():
    with pytest.raises(AggregateError, match="At most"):
        parse_components([
            {"metric": f"m{i}", "weight": 1} for i in range(MAX_COMPONENTS + 1)
        ])


def test_weight_defaults_to_one():
    assert parse_components([{"metric": "a"}])[0]["weight"] == 1.0


# ══ Computing ════════════════════════════════════════════════════════════════

def test_a_complete_set_is_the_weighted_mean():
    value = compute(QUALITY, {"faithfulness": 1.0, "relevance": 1.0, "tone": 0.0})
    assert value == pytest.approx(0.8)   # 0.5 + 0.3


def test_weights_are_normalised_so_they_need_not_sum_to_one():
    value = compute(
        [{"metric": "a", "weight": 3}, {"metric": "b", "weight": 1}],
        {"a": 1.0, "b": 0.0},
    )
    assert value == pytest.approx(0.75)


def test_a_missing_component_is_excluded_not_treated_as_zero():
    """A run that did not include "tone" should report the quality of what it
    *did* measure. Scoring the gap as zero would mark an aggregate down simply
    for being applied to a narrower run."""
    value = compute(QUALITY, {"faithfulness": 1.0, "relevance": 1.0})
    assert value == pytest.approx(1.0)


def test_treating_missing_as_zero_would_have_given_a_very_different_answer():
    """Pins the distinction the previous test depends on."""
    naive = (1.0 * 0.5 + 1.0 * 0.3 + 0.0 * 0.2)
    assert naive == pytest.approx(0.8)
    assert compute(QUALITY, {"faithfulness": 1.0, "relevance": 1.0}) != pytest.approx(naive)


def test_nothing_scored_is_none_rather_than_zero():
    """Averaging no numbers is not zero, and zero is a real, terrible score."""
    assert compute(QUALITY, {}) is None
    assert compute(QUALITY, {"faithfulness": None}) is None


def test_a_single_present_component_is_that_component():
    assert compute(QUALITY, {"tone": 0.4}) == pytest.approx(0.4)


# ══ Coverage ═════════════════════════════════════════════════════════════════

def test_coverage_names_what_was_missing():
    """An aggregate computed from two of five parts is a different claim from
    one computed from all five, and the number alone cannot say which."""
    result = coverage(QUALITY, {"faithfulness": 1.0})
    assert result["present"] == ["faithfulness"]
    assert result["missing"] == ["relevance", "tone"]
    assert result["complete"] is False


def test_full_coverage_says_so():
    result = coverage(QUALITY, {"faithfulness": 1, "relevance": 1, "tone": 1})
    assert result["complete"] is True
    assert result["missing"] == []


def test_a_null_score_counts_as_missing():
    assert coverage(QUALITY, {"tone": None})["missing"] == [
        "faithfulness", "relevance", "tone",
    ]


# ══ Applying a whole set ═════════════════════════════════════════════════════

def test_every_aggregate_is_reported_with_its_coverage():
    result = apply_all(
        [
            {"slug": "quality", "name": "Quality", "components": QUALITY},
            {"slug": "safety", "name": "Safety",
             "components": [{"metric": "toxicity", "weight": 1}]},
        ],
        {"faithfulness": 1.0, "relevance": 1.0, "tone": 1.0},
    )
    assert result["quality"]["value"] == pytest.approx(1.0)
    assert result["quality"]["coverage"]["complete"] is True
    # Nothing scored toxicity, so safety reports None rather than a fabricated 0.
    assert result["safety"]["value"] is None


def test_a_name_falls_back_to_the_slug():
    result = apply_all([{"slug": "quality", "components": QUALITY}], {"tone": 1.0})
    assert result["quality"]["name"] == "quality"
