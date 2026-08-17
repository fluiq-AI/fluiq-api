"""Online scoring: which live traces get judged, and how often.

This logic spends the customer's money on every ingested trace, so the
properties worth pinning are the ones about *not* spending it twice and about
the sample being reproducible.
"""
from __future__ import annotations

import pytest

from shared.online_scoring import in_sample, matches, scorers_of, select_rule


def rule(**over):
    base = {
        "rule_id":      "r1",
        "enabled":      True,
        "metrics":      ["relevance"],
        "custom_judges": {},
        "sample_rate":  100.0,
        "span_scope":   "root",
        "integrations": [],
        "models":       [],
    }
    base.update(over)
    return base


LLM = {"type": "llm", "integration": "OPENAI", "model": "gpt-5-mini"}


# ── Sampling ─────────────────────────────────────────────────────────────────

def test_full_rate_takes_everything():
    assert all(in_sample(f"t{i}", "r", 100) for i in range(50))


def test_zero_rate_takes_nothing():
    assert not any(in_sample(f"t{i}", "r", 0) for i in range(50))


def test_sampling_is_deterministic():
    """A retried or replayed trace must make the same decision, or a duplicate
    doubles the bill."""
    verdicts = [in_sample("trace-abc", "rule-1", 25) for _ in range(20)]
    assert len(set(verdicts)) == 1


def test_two_rules_do_not_sample_the_same_traces():
    """Hashing on trace alone would make every 10% rule pick the identical 10%,
    leaving 90% of traffic unexamined by any of them."""
    traces = [f"trace-{i}" for i in range(2000)]
    a = {t for t in traces if in_sample(t, "rule-a", 10)}
    b = {t for t in traces if in_sample(t, "rule-b", 10)}
    assert a and b
    overlap = len(a & b) / len(a)
    # Independent 10% samples overlap ~10% of the time; identical ones, 100%.
    assert overlap < 0.3, f"rules sampled the same slice ({overlap:.0%} overlap)"


def test_the_rate_is_roughly_honoured():
    traces = [f"trace-{i}" for i in range(4000)]
    hit = sum(1 for t in traces if in_sample(t, "r", 10))
    assert 300 < hit < 500, f"10% of 4000 should be ~400, got {hit}"


def test_a_higher_rate_is_a_superset_of_a_lower_one():
    """Raising the rate must add traces, not reshuffle which ones are watched —
    otherwise a score trend breaks every time the rate is tuned."""
    traces = [f"trace-{i}" for i in range(1000)]
    low = {t for t in traces if in_sample(t, "r", 10)}
    high = {t for t in traces if in_sample(t, "r", 40)}
    assert low <= high


# ── Matching ─────────────────────────────────────────────────────────────────

def test_only_llm_events_are_scored():
    assert matches(rule(), LLM, True)
    assert not matches(rule(), {"type": "retrieval"}, True)
    assert not matches(rule(), {"type": "tool"}, True)


def test_root_scope_skips_nested_spans():
    """Scoring every span of an agent run multiplies the bill by trajectory depth."""
    assert matches(rule(span_scope="root"), LLM, True)
    assert not matches(rule(span_scope="root"), LLM, False)


def test_all_scope_takes_nested_spans_too():
    assert matches(rule(span_scope="all"), LLM, False)


def test_an_empty_filter_means_any_not_none():
    """Someone who left the integration field alone meant "all integrations"."""
    assert matches(rule(integrations=[], models=[]), LLM, True)


def test_integration_filter():
    assert matches(rule(integrations=["OPENAI"]), LLM, True)
    assert not matches(rule(integrations=["ANTHROPIC"]), LLM, True)


def test_integration_filter_ignores_case():
    assert matches(rule(integrations=["openai"]), LLM, True)


def test_model_filter():
    assert matches(rule(models=["gpt-5-mini"]), LLM, True)
    assert not matches(rule(models=["claude-sonnet-4-6"]), LLM, True)


# ── Scorer normalisation ─────────────────────────────────────────────────────

def test_scorers_are_normalised():
    metrics, judges = scorers_of(rule(metrics=["relevance"], custom_judges={"tone": 0.8}))
    assert metrics == ["relevance"]
    assert judges == {"tone": 0.8}


def test_an_unparseable_threshold_falls_back_rather_than_raising():
    """A bad threshold must not stop the rest of a rule from scoring."""
    _, judges = scorers_of(rule(custom_judges={"tone": "high"}))
    assert judges == {"tone": 0.5}


# ── Selection ────────────────────────────────────────────────────────────────

def test_the_first_matching_rule_wins():
    """Two overlapping rules would judge the same trace twice and bill for both."""
    picked = select_rule(
        [rule(rule_id="old", name="old"), rule(rule_id="new", name="new")],
        LLM, "t1", True,
    )
    assert picked["rule_id"] == "old"


def test_a_disabled_rule_is_skipped():
    picked = select_rule(
        [rule(rule_id="off", enabled=False), rule(rule_id="on")], LLM, "t1", True,
    )
    assert picked["rule_id"] == "on"


def test_a_rule_with_no_scorers_does_not_shadow_a_later_one():
    """It would consume the trace and score nothing — worse than not existing."""
    picked = select_rule(
        [rule(rule_id="empty", metrics=[], custom_judges={}), rule(rule_id="real")],
        LLM, "t1", True,
    )
    assert picked["rule_id"] == "real"


def test_a_rule_that_does_not_sample_this_trace_yields_to_the_next():
    picked = select_rule(
        [rule(rule_id="never", sample_rate=0), rule(rule_id="always", sample_rate=100)],
        LLM, "t1", True,
    )
    assert picked["rule_id"] == "always"


def test_no_matching_rule_returns_none():
    assert select_rule([rule(integrations=["ANTHROPIC"])], LLM, "t1", True) is None


def test_no_rules_returns_none():
    assert select_rule([], LLM, "t1", True) is None


def test_a_custom_scorer_alone_is_enough_to_select():
    picked = select_rule(
        [rule(metrics=[], custom_judges={"tone": 0.8})], LLM, "t1", True,
    )
    assert picked is not None
