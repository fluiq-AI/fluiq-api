"""The judge-scoring-the-judge maths.

The thing worth pinning here is not the arithmetic — it is that a high
agreement number alone must never read as "this judge is good", because that is
the exact false positive the whole feature exists to catch.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.judge_agreement import compare, disagreements


def pairs(*values):
    return [{"judge": j, "human": h} for j, h in values]


def test_a_perfect_judge_is_called_good():
    result = compare(pairs(*[(1.0, 1.0)] * 6, *[(0.0, 0.0)] * 6))
    assert result["agreement"] == 1.0
    assert result["correlation"] == 1.0
    assert result["bias"] == 0.0
    assert result["verdict"] == "good"


def test_a_judge_that_passes_everything_is_not_called_good():
    """The headline failure. 90% of these labels are 'good', so a judge that
    says 'good' unconditionally scores 90% agreement while measuring nothing."""
    data = pairs(*[(1.0, 1.0)] * 18, *[(1.0, 0.0)] * 2)
    result = compare(data)

    assert result["agreement"] == 0.9, "the misleading number is still reported"
    assert result["verdict"] == "matches_base_rate", (
        "but the verdict must not let a 90% agreement pass as a working judge"
    )
    assert "echoing" in result["detail"]


def test_all_positive_labels_cannot_answer_the_question():
    """Humans who labelled nothing bad give the judge nothing to be wrong
    about. That is unanswerable, not answered 'no' — reporting a correlation of
    0 here would read as an indictment of a judge this data cannot assess."""
    result = compare([{"judge": j, "human": 1.0} for j in
                      (0.9, 0.8, 0.7, 0.6, 0.95, 0.55, 0.85, 0.75, 0.65, 0.99, 0.7, 0.6)])
    assert result["correlation"] is None
    assert result["verdict"] == "undetermined"
    assert "human label" in result["detail"]


def test_a_constant_judge_is_diagnosed_as_the_judge_not_the_data():
    """A judge with no variance also has no correlation, but the fix is a new
    prompt — not more labels. The two must not collapse into one message."""
    result = compare(pairs(*[(0.7, 1.0)] * 9, *[(0.7, 0.0)] * 3))
    assert result["correlation"] is None
    assert result["verdict"] == "matches_base_rate"
    assert "same score" in result["detail"]


def test_a_lenient_judge_is_named_as_lenient():
    data = pairs(*[(1.0, 0.6)] * 8, *[(0.6, 0.2)] * 8)
    result = compare(data)
    assert result["bias"] > 0
    assert result["verdict"] == "lenient"
    assert "passing output your reviewers reject" in result["detail"]


def test_a_harsh_judge_is_named_as_harsh():
    data = compare(pairs(*[(0.2, 0.6)] * 8, *[(0.6, 1.0)] * 8))
    assert data["bias"] < 0
    assert data["verdict"] == "harsh"


def test_a_small_sample_refuses_to_render_a_verdict():
    result = compare(pairs((1.0, 1.0), (0.0, 0.0), (1.0, 1.0)))
    assert result["verdict"] == "insufficient"
    assert result["compared"] == 3
    # The numbers are still computed — hiding them would stop someone building
    # intuition while they label — the verdict just declines to endorse them.
    assert result["agreement"] == 1.0


def test_unlabelled_rows_are_dropped_not_zeroed():
    """A missing human label must not become a disagreement with 0.0."""
    result = compare([
        {"judge": 0.9, "human": 1.0},
        {"judge": 0.9},
        {"judge": 0.9, "human": None},
        {"human": 1.0},
    ])
    assert result["compared"] == 1
    assert result["mean_error"] == 0.09999999999999998 or abs(result["mean_error"] - 0.1) < 1e-9


def test_no_overlap_says_so_rather_than_returning_zeros():
    result = compare([{"judge": 0.5}, {"human": 1.0}])
    assert result["compared"] == 0
    assert result["agreement"] is None
    assert result["verdict"] == "no_data"


def test_disagreements_lead_with_the_worst_gap():
    rows = disagreements([
        {"trace_id": "near",  "judge": 0.6, "human": 0.55},
        {"trace_id": "worst", "judge": 1.0, "human": 0.0},
        {"trace_id": "mid",   "judge": 0.9, "human": 0.4},
    ])
    assert [r["trace_id"] for r in rows] == ["worst", "mid"], (
        "agreeing rows are excluded and the rest are worst-first"
    )
    assert rows[0]["direction"] == "lenient"


def test_disagreements_keep_the_context_columns():
    """The reviewer needs the text, not a bare trace id — the point of the list
    is reading what the judge got wrong."""
    rows = disagreements([
        {"trace_id": "t1", "judge": 1.0, "human": 0.0, "preview": "the output",
         "comment": "hallucinated the date"},
    ])
    assert rows[0]["preview"] == "the output"
    assert rows[0]["comment"] == "hallucinated the date"
    assert rows[0]["gap"] == 1.0


def test_a_judge_on_the_right_side_but_far_off_still_counts_as_disagreement():
    """Both above the threshold, but 0.95 vs 0.5 is not agreement worth hiding
    — a judge that inflates every passing score is broken in a way the
    threshold check alone cannot see."""
    rows = disagreements([{"trace_id": "t", "judge": 0.99, "human": 0.5}])
    assert len(rows) == 1
