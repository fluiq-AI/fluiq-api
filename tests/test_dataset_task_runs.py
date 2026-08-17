"""Tests for the dataset-run side of tasks: the request contract, the event a
generated output becomes, and the scoring job published for it.

These are the wiring the playground's "Run over dataset" and the CI gate both
depend on, so a shape change here breaks two callers at once.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from routes import datasets as ds
from routes.datasets import RunTaskRequest


# ── Request contract ──────────────────────────────────────────────────────────

def test_task_accepts_an_inline_template():
    task = RunTaskRequest(template="Answer: {{input}}", model="gpt-5-mini")
    assert task.template == "Answer: {{input}}"


def test_task_accepts_a_prompt_id():
    task = RunTaskRequest(prompt_id=uuid.uuid4(), prompt_version=2)
    assert task.prompt_version == 2


def test_task_accepts_a_prompt_slug():
    """CI references prompts by slug; the dashboard uses ids."""
    assert RunTaskRequest(prompt_slug="support-reply").prompt_slug == "support-reply"


def test_task_rejects_having_nothing_to_run():
    with pytest.raises(ValueError, match="prompt_id, a prompt_slug, a template, or messages"):
        RunTaskRequest(model="gpt-5-mini")


def test_task_rejects_a_blank_template():
    with pytest.raises(ValueError, match="prompt_id, a prompt_slug, a template, or messages"):
        RunTaskRequest(template="   ", model="gpt-5-mini")


def test_run_request_without_a_task_is_still_valid():
    """Grading recorded output stays the default; the task is purely additive."""
    req = ds.CreateRunRequest(kind="metrics")
    assert req.task is None
    assert req.name is None


# ── The event a generated output becomes ──────────────────────────────────────

def test_task_event_grades_the_generated_output_not_the_expectation():
    """The whole point of a task: the answer under test is what the model just
    produced, never the expected output graded against itself."""
    event = ds._task_event("t-1", "gpt-5-mini", "rendered prompt", "the model's answer")
    assert event["response"] == "the model's answer"
    assert event["input"] == "rendered prompt"
    assert event["model"] == "gpt-5-mini"
    assert event["trace_id"] == event["root_trace_id"] == "t-1"
    assert event["integration"] == "DATASET_TASK"


# ── The scoring job ───────────────────────────────────────────────────────────

class _Kafka:
    def __init__(self):
        self.jobs: list[tuple[dict, str]] = []

    async def add_job(self, job, topic, key):  # noqa: ANN001
        self.jobs.append((job, topic))


@pytest.fixture
def kafka(monkeypatch):
    q = _Kafka()
    monkeypatch.setattr(ds, "kafka_queue", q)
    monkeypatch.setattr(ds, "bump_eval_count", lambda _org: None)
    return q


def _publish(kafka, **overrides):
    args = dict(
        org_id=uuid.uuid4(),
        kind="metrics",
        trace_id="t-1",
        event={"response": "answer"},
        expected="",
        run_metrics=["relevance"],
        custom_judges={},
        judge_prompt_overrides={},
        judge=None,
    )
    args.update(overrides)
    asyncio.run(ds._publish_task_score(**args))
    return kafka.jobs[0][0]


def test_metrics_job_carries_the_requested_metrics(kafka):
    job = _publish(kafka, run_metrics=["relevance", "coherence"])
    assert job["operation"] == "sdk_llm"
    assert job["eval_config"]["metrics"] == ["relevance", "coherence"]


def test_expected_output_becomes_the_reference(kafka):
    job = _publish(kafka, expected="the ideal answer")
    assert job["reference"] == "the ideal answer"


def test_no_reference_when_the_example_has_no_expectation(kafka):
    """A reference key with an empty value would have the judge grade against
    nothing rather than skip the comparison."""
    assert "reference" not in _publish(kafka, expected="")


def test_judge_is_forwarded_when_chosen(kafka):
    job = _publish(kafka, judge="anthropic:claude-sonnet-5")
    assert job["judge"] == "anthropic:claude-sonnet-5"


def test_judge_is_absent_when_not_chosen(kafka):
    assert "judge" not in _publish(kafka, judge=None)


def test_custom_scorers_and_dataset_prompts_ride_along(kafka):
    job = _publish(
        kafka,
        custom_judges={"refund-policy": 0.8},
        judge_prompt_overrides={"hallucination": "custom template"},
    )
    assert job["eval_config"]["custom_judges"] == {"refund-policy": 0.8}
    assert job["eval_config"]["judge_prompt_overrides"] == {"hallucination": "custom template"}


def test_agentic_task_falls_back_to_standard_metrics(kafka):
    """One generated turn has no trajectory to score, so it is graded the way a
    synthetic example is rather than sent to the agentic pipeline."""
    job = _publish(kafka, kind="agentic", run_metrics=["relevance"])
    assert job["operation"] == "sdk_llm"
    assert job["eval_config"]["metrics"] == ["hallucination", "relevance"]


def test_security_task_goes_to_the_security_topic(kafka):
    _publish(kafka, kind="security")
    job, topic = kafka.jobs[0]
    assert job["operation"] == "sdk_security"
    assert topic == ds.config.KAFKA_SECURITY_TOPIC


def test_quality_task_goes_to_the_eval_topic(kafka):
    _publish(kafka, kind="metrics")
    assert kafka.jobs[0][1] == ds.config.KAFKA_EVAL_TOPIC


# ── Serialization ─────────────────────────────────────────────────────────────

def test_serialized_run_exposes_task_and_identity():
    from datetime import datetime, timezone

    row = {
        "run_id":      uuid.uuid4(),
        "dataset_id":  uuid.uuid4(),
        "kind":        "metrics",
        "status":      "running",
        "total":       10,
        "summary":     {},
        "created_at":  datetime.now(timezone.utc),
        "name":        "Support v3 on GPT-5",
        "description": "before the refund copy change",
        "task":        {"template": "{{input}}", "model": "gpt-5-mini", "prompt_version": 3},
        "generated":   4,
        "gen_failed":  1,
    }
    out = ds._serialize_run(row)
    assert out["name"] == "Support v3 on GPT-5"
    assert out["task"]["model"] == "gpt-5-mini"
    assert out["generated"] == 4
    assert out["gen_failed"] == 1


def test_serialized_run_parses_a_task_stored_as_json_text():
    """The jsonb codec is not applied on every pool path, so a run read through
    one of those must not surface its task as a string."""
    from datetime import datetime, timezone

    out = ds._serialize_run({
        "run_id":     uuid.uuid4(),
        "dataset_id": uuid.uuid4(),
        "kind":       "metrics",
        "status":     "complete",
        "total":      1,
        "summary":    "{}",
        "created_at": datetime.now(timezone.utc),
        "task":       '{"template": "{{input}}", "model": "gpt-5-mini"}',
    })
    assert out["task"]["model"] == "gpt-5-mini"


def test_serialized_run_without_a_task_reports_none():
    from datetime import datetime, timezone

    out = ds._serialize_run({
        "run_id":     uuid.uuid4(),
        "dataset_id": uuid.uuid4(),
        "kind":       "agentic",
        "status":     "complete",
        "total":      3,
        "summary":    {},
        "created_at": datetime.now(timezone.utc),
    })
    assert out["task"] is None
    assert out["generated"] == 0
