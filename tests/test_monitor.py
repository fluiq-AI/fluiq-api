"""Monitor: bucket sizing and the headline numbers derived from the series."""
from __future__ import annotations

import pytest

from routes.monitor import _bucket_for, _totals


# ── Bucket sizing ────────────────────────────────────────────────────────────

def test_short_windows_get_fine_buckets():
    assert _bucket_for(1) == 5


def test_long_windows_get_coarse_buckets():
    assert _bucket_for(720) == 1440


def test_every_window_lands_in_a_readable_number_of_points():
    """A fixed bucket gives either one point or two thousand. Each pairing has
    to produce something a chart can actually draw."""
    for hours in (1, 3, 6, 12, 24, 48, 72, 168, 336, 720):
        points = hours * 60 / _bucket_for(hours)
        assert 5 <= points <= 200, f"{hours}h → {points:.0f} points"


def test_bucket_size_never_decreases_as_the_window_grows():
    sizes = [_bucket_for(h) for h in (1, 6, 24, 72, 168, 720)]
    assert sizes == sorted(sizes)


def test_an_out_of_table_window_still_gets_a_bucket():
    assert _bucket_for(10_000) == 1440


# ── Totals ───────────────────────────────────────────────────────────────────

def _series(traffic=None, spend=None, scores=None):
    return {
        "traffic": traffic or [],
        "spend":   spend or [],
        "scores":  scores or [],
    }


def test_counts_are_summed_across_buckets():
    totals = _totals(_series(traffic=[
        {"spans": 10, "runs": 4, "errors": 1, "p95": 1.0},
        {"spans": 5, "runs": 2, "errors": 0, "p95": 2.0},
    ]))
    assert (totals["spans"], totals["runs"], totals["errors"]) == (15, 6, 1)


def test_latency_reports_the_worst_bucket_not_the_average():
    """Percentiles do not average. Quoting the mean of them would understate the
    worst period, which is the one worth knowing about."""
    totals = _totals(_series(traffic=[
        {"p95": 0.5}, {"p95": 9.0}, {"p95": 0.7},
    ]))
    assert totals["worst_p95"] == 9.0


def test_latency_is_none_when_nothing_recorded_one():
    assert _totals(_series(traffic=[{"spans": 3}]))["worst_p95"] is None


def test_judge_score_is_weighted_by_how_many_were_judged():
    """A quiet bucket with one bad score must not drag the headline as far as a
    busy one with the same score."""
    totals = _totals(_series(scores=[
        {"judge_score": 1.0, "judged": 99},
        {"judge_score": 0.0, "judged": 1},
    ]))
    assert totals["judge_score"] == pytest.approx(0.99)
    assert totals["judged"] == 100


def test_an_unweighted_mean_would_have_given_a_very_different_answer():
    """Pins the distinction the previous test relies on."""
    buckets = [
        {"judge_score": 1.0, "judged": 99},
        {"judge_score": 0.0, "judged": 1},
    ]
    naive = sum(b["judge_score"] for b in buckets) / len(buckets)
    assert naive == 0.5
    assert _totals(_series(scores=buckets))["judge_score"] != pytest.approx(naive)


def test_human_feedback_is_reported_separately_from_judge_scores():
    """They answer different questions and move for different reasons; averaging
    them together would hide both."""
    totals = _totals(_series(scores=[
        {"judge_score": 0.9, "judged": 10, "feedback_score": 0.2, "feedback_count": 5},
    ]))
    assert totals["judge_score"] == pytest.approx(0.9)
    assert totals["feedback_score"] == pytest.approx(0.2)
    assert totals["feedback_count"] == 5


def test_scores_are_none_rather_than_zero_when_nothing_was_judged():
    """Zero is a real, terrible score. "Nothing was judged" must not look like it."""
    totals = _totals(_series(scores=[{"judged": 0, "feedback_count": 0}]))
    assert totals["judge_score"] is None
    assert totals["feedback_score"] is None


def test_spend_and_tokens_are_summed():
    totals = _totals(_series(spend=[
        {"cost": 0.5, "tokens": 100}, {"cost": 0.25, "tokens": 50},
    ]))
    assert totals["cost"] == pytest.approx(0.75)
    assert totals["tokens"] == 150


def test_an_empty_window_produces_zeros_not_an_error():
    totals = _totals(_series())
    assert totals["spans"] == 0
    assert totals["cost"] == 0
    assert totals["judge_score"] is None
