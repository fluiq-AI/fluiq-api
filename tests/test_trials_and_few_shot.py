"""Trials (TODO-14) and few-shot examples (TODO-23)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from routes.datasets import _fold_trials
from routes.datasets.task_runner import render_few_shot, task_variables


# ---------------------------------------------------------------- trials ----

def item(example_id, scores, **extra):
    return {
        "example_id": example_id,
        "input": f"in-{example_id}",
        "output": f"out-{example_id}",
        "done": True,
        "result": [{"metric": m, "score": s} for m, s in scores.items()],
        **extra,
    }


def test_a_run_without_trials_is_returned_untouched():
    """The fold must cost nothing for the overwhelmingly common case."""
    items = [item("a", {"q": 1.0}), item("b", {"q": 0.5})]
    assert _fold_trials(items) is items


def test_three_trials_of_one_example_become_one_row():
    folded = _fold_trials([
        item("a", {"quality": 1.0}),
        item("a", {"quality": 0.4}),
        item("a", {"quality": 0.7}),
    ])
    assert len(folded) == 1, "one example must not appear as three"
    assert folded[0]["trials"] == 3
    assert abs(folded[0]["result"][0]["score"] - 0.7) < 1e-9


def test_the_spread_survives_the_averaging():
    """The reason to run trials at all. A metric that swings 0.6 between
    identical inputs is the finding — an average alone hides it."""
    folded = _fold_trials([
        item("a", {"quality": 1.0}),
        item("a", {"quality": 0.4}),
    ])
    row = folded[0]["result"][0]
    assert abs(row["spread"] - 0.6) < 1e-9
    assert row["trials"] == 2


def test_a_stable_metric_reports_no_spread():
    folded = _fold_trials([item("a", {"q": 0.8}), item("a", {"q": 0.8})])
    assert folded[0]["result"][0]["spread"] == 0.0


def test_a_group_is_done_only_when_every_trial_is():
    """An average of the two that landed would keep moving as the third
    arrives, so a half-finished group must not present as finished."""
    folded = _fold_trials([
        item("a", {"q": 1.0}),
        item("a", {"q": 0.0}, done=False),
    ])
    assert folded[0]["done"] is False


def test_cost_sums_and_latency_averages():
    """Three trials genuinely cost three times as much; averaging the cost
    would understate a trialled run by exactly the trial count."""
    folded = _fold_trials([
        item("a", {"q": 1.0}, cost_usd=0.01, latency_ms=100),
        item("a", {"q": 1.0}, cost_usd=0.02, latency_ms=300),
    ])
    assert abs(folded[0]["cost_usd"] - 0.03) < 1e-9
    assert folded[0]["latency_ms"] == 200


def test_examples_keep_their_order_and_mixed_counts_work():
    folded = _fold_trials([
        item("a", {"q": 1.0}), item("b", {"q": 0.5}), item("a", {"q": 0.0}),
    ])
    assert [f["example_id"] for f in folded] == ["a", "b"]
    assert folded[0]["trials"] == 2
    assert "trials" not in folded[1], "a single-trial example is left alone"


def test_a_metric_missing_from_one_trial_averages_over_the_rest():
    """A scorer that errored on trial 2 must not drag its average toward zero."""
    folded = _fold_trials([
        item("a", {"q": 1.0, "safety": 1.0}),
        item("a", {"q": 0.0}),
    ])
    by_metric = {r["metric"]: r for r in folded[0]["result"]}
    assert by_metric["q"]["score"] == 0.5
    assert by_metric["safety"]["score"] == 1.0
    assert by_metric["safety"]["trials"] == 1


# -------------------------------------------------------------- few-shot ----

def test_few_shot_pairs_render_as_labelled_blocks():
    text = render_few_shot({"few_shot": [
        {"input": "2+2", "output": "4"},
        {"input": "3+3", "output": "6"},
    ]})
    assert "Input: 2+2\nOutput: 4" in text
    assert "Input: 3+3\nOutput: 6" in text


def test_no_few_shot_renders_empty_not_none():
    """A template with {{few_shot}} must degrade to a zero-shot prompt, not one
    containing the literal word "None"."""
    assert render_few_shot({}) == ""
    assert render_few_shot({"few_shot": []}) == ""
    assert render_few_shot(None) == ""


def test_a_preformatted_string_is_passed_through():
    assert render_few_shot({"few_shot": "Q: a\nA: b"}) == "Q: a\nA: b"


def test_few_shot_is_capped():
    """An example carrying 400 shots would blow the context window of the model
    it is being fed to, and silently, at generation time."""
    text = render_few_shot({"few_shot": [{"input": str(i), "output": str(i)}
                                         for i in range(200)]})
    assert text.count("Input:") == 10


def test_few_shot_reaches_the_template_as_a_variable():
    values = task_variables(
        {"input": "q", "expected_output": "a"},
        {"few_shot": [{"input": "x", "output": "y"}]},
    )
    assert values["few_shot"] == "Input: x\nOutput: y"
    assert values["input"] == "q"


def test_the_raw_few_shot_list_never_leaks_into_the_variables():
    """Metadata scalars become variables; the few_shot list must arrive only in
    its rendered form, or a template would interpolate a Python repr."""
    values = task_variables({"input": "q"}, {"few_shot": [{"input": "x", "output": "y"}]})
    assert "[{" not in values["few_shot"]
    assert all(isinstance(v, str) for v in values.values())
