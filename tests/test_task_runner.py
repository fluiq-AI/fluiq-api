"""Tests for dataset run *tasks* — the prompt+model executed against every example.

Covers the pure logic: what variables a template can reference, how a template
without variables still evaluates per-row, and the resolution rules that decide
which template and model a run actually uses.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from routes.datasets import task_runner
from routes.datasets.task_runner import ResolvedTask, TaskError


# ── Variable bag ──────────────────────────────────────────────────────────────

def test_variables_expose_input_and_both_expected_spellings():
    values = task_runner.task_variables(
        {"input": "why is it late?", "expected_output": "apologize + ETA"}, {}
    )
    assert values["input"] == "why is it late?"
    assert values["expected"] == "apologize + ETA"
    assert values["expected_output"] == "apologize + ETA"


def test_variables_include_scalar_metadata():
    """An imported CSV's extra columns land in metadata and must be addressable."""
    values = task_runner.task_variables(
        {"input": "hi", "expected_output": ""},
        {"tier": "gold", "order_count": 3, "vip": True},
    )
    assert values["tier"] == "gold"
    assert values["order_count"] == 3
    assert values["vip"] is True


def test_variables_drop_non_scalar_metadata():
    """Nested structures would stringify as Python reprs inside the prompt."""
    values = task_runner.task_variables(
        {"input": "hi"},
        {"source_trace_id": "abc", "nested": {"a": 1}, "items": [1, 2]},
    )
    assert values["source_trace_id"] == "abc"
    assert "nested" not in values
    assert "items" not in values


def test_example_fields_win_over_metadata():
    """A CSV column literally named 'input' must not shadow the real input."""
    values = task_runner.task_variables(
        {"input": "real", "expected_output": "real-expected"},
        {"input": "impostor", "expected": "impostor-expected"},
    )
    assert values["input"] == "real"
    assert values["expected"] == "real-expected"


# ── Rendering ─────────────────────────────────────────────────────────────────

def _task(template: str) -> ResolvedTask:
    return ResolvedTask(template=template, model="claude-haiku-4-5")


def test_render_substitutes_input():
    out = task_runner.render_task(
        _task("Answer the customer: {{input}}"),
        {"input": "where is my package?"},
        {},
    )
    assert out == "Answer the customer: where is my package?"


def test_render_substitutes_metadata_and_expected():
    out = task_runner.render_task(
        _task("[{{tier}}] {{input}} (target: {{expected}})"),
        {"input": "refund?", "expected_output": "offer refund"},
        {"tier": "gold"},
    )
    assert out == "[gold] refund? (target: offer refund)"


def test_render_appends_input_when_template_has_no_variables():
    """A bare instruction must still evaluate per row rather than sending the
    identical prompt for every example."""
    out = task_runner.render_task(
        _task("You are a support agent. Be concise."),
        {"input": "my order is late"},
        {},
    )
    assert out == "You are a support agent. Be concise.\n\nmy order is late"


def test_render_leaves_bare_template_alone_when_example_has_no_input():
    out = task_runner.render_task(_task("Say hello."), {"input": ""}, {})
    assert out == "Say hello."


def test_render_does_not_append_when_template_uses_any_variable():
    """Referencing {{tier}} but not {{input}} is a deliberate choice, not an
    omission to be helpfully corrected."""
    out = task_runner.render_task(
        _task("Greet a {{tier}} customer."), {"input": "hello"}, {"tier": "gold"}
    )
    assert out == "Greet a gold customer."


def test_render_leaves_unknown_placeholders_untouched():
    """Mirrors safe_substitute: a stray token can never raise mid-run."""
    out = task_runner.render_task(
        _task("{{input}} / {{nope}}"), {"input": "x"}, {}
    )
    assert out == "x / {{nope}}"


# ── Resolution ────────────────────────────────────────────────────────────────

def _resolve(**kwargs):
    return asyncio.run(task_runner.resolve_task(uuid.uuid4(), **kwargs))


def test_resolve_inline_template():
    task = _resolve(template="Answer: {{input}}", model="claude-haiku-4-5")
    assert task.template == "Answer: {{input}}"
    assert task.model == "claude-haiku-4-5"
    assert task.prompt_id is None


def test_resolve_rejects_missing_model():
    with pytest.raises(TaskError, match="needs a model"):
        _resolve(template="Answer: {{input}}")


def test_resolve_rejects_unknown_provider():
    with pytest.raises(TaskError, match="Unknown provider"):
        _resolve(template="Answer: {{input}}", model="llama-3-70b")


def test_resolve_rejects_blank_template():
    with pytest.raises(TaskError, match="needs a prompt template"):
        _resolve(template="   ", model="gpt-5-mini")


def test_resolve_clamps_max_tokens():
    assert _resolve(template="x{{input}}", model="gpt-5-mini", max_tokens=99999).max_tokens == 8192


def test_resolve_treats_zero_max_tokens_as_unspecified():
    """0 is not a budget anyone means; it takes the default rather than asking a
    provider for a one-token completion."""
    assert _resolve(template="x{{input}}", model="gpt-5-mini", max_tokens=0).max_tokens == 2048


def test_resolve_accepts_every_supported_provider_prefix():
    for model in ("claude-sonnet-4-6", "gpt-5-mini", "o3-mini", "gemini-2.5-pro", "kimi-k2"):
        assert _resolve(template="{{input}}", model=model).model == model


def test_resolve_by_slug_uses_the_saved_prompt(monkeypatch):
    """CI references prompts by slug, and must reach undeployed ones — gating a
    PR means testing a prompt precisely before it ships."""
    pid = uuid.uuid4()

    async def by_slug(_org, slug):
        assert slug == "support-reply"
        return {"prompt_id": pid}

    async def full(prompt_id, _org):
        assert prompt_id == pid
        return {
            "name": "Support reply", "template": "Reply to {{input}}",
            "version": 4, "model": "gpt-5-mini",
        }

    monkeypatch.setattr(task_runner, "get_prompt_by_slug", by_slug)
    monkeypatch.setattr(task_runner, "get_prompt_full", full)

    task = _resolve(prompt_slug="support-reply")
    assert task.template == "Reply to {{input}}"
    assert task.model == "gpt-5-mini"          # the prompt's own model is the default
    assert task.prompt_version == 4
    assert task.prompt_name == "Support reply"


def test_resolve_reports_unknown_slug(monkeypatch):
    async def missing(_org, _slug):
        return None

    monkeypatch.setattr(task_runner, "get_prompt_by_slug", missing)
    with pytest.raises(TaskError, match="No saved prompt with slug"):
        _resolve(prompt_slug="nope")


def test_explicit_model_overrides_the_prompts_own(monkeypatch):
    async def full(_pid, _org):
        return {"name": "P", "template": "{{input}}", "version": 1, "model": "gpt-5-mini"}

    monkeypatch.setattr(task_runner, "get_prompt_full", full)
    task = _resolve(prompt_id=uuid.uuid4(), model="claude-sonnet-4-6")
    assert task.model == "claude-sonnet-4-6"


def test_resolve_pins_a_historical_version(monkeypatch):
    """Re-running an experiment must grade the prompt it actually used, not
    whatever that prompt says today."""
    async def full(_pid, _org):
        return {"name": "P", "template": "TODAY {{input}}", "version": 7, "model": "gpt-5-mini"}

    async def versions(_pid, _org):
        return [
            {"version": 7, "template": "TODAY {{input}}", "model": "gpt-5-mini"},
            {"version": 3, "template": "BACK-THEN {{input}}", "model": "gpt-5-mini"},
        ]

    monkeypatch.setattr(task_runner, "get_prompt_full", full)
    monkeypatch.setattr(task_runner, "list_versions", versions)

    task = _resolve(prompt_id=uuid.uuid4(), prompt_version=3)
    assert task.template == "BACK-THEN {{input}}"
    assert task.prompt_version == 3


def test_resolve_rejects_a_version_that_does_not_exist(monkeypatch):
    async def full(_pid, _org):
        return {"name": "P", "template": "{{input}}", "version": 7, "model": "gpt-5-mini"}

    async def versions(_pid, _org):
        return [{"version": 7, "template": "{{input}}"}]

    monkeypatch.setattr(task_runner, "get_prompt_full", full)
    monkeypatch.setattr(task_runner, "list_versions", versions)

    with pytest.raises(TaskError, match="no version 99"):
        _resolve(prompt_id=uuid.uuid4(), prompt_version=99)


def test_to_json_records_everything_needed_to_rerun():
    """The persisted task is what makes a run reproducible, so it stores the
    resolved template rather than only a reference to a prompt that may change."""
    task = ResolvedTask(
        template="Answer: {{input}}", model="gpt-5-mini", system="Be terse",
        max_tokens=512, prompt_id="p1", prompt_version=3, prompt_name="Support v3",
    )
    assert task.to_json() == {
        "template":       "Answer: {{input}}",
        "system":         "Be terse",
        "model":          "gpt-5-mini",
        "max_tokens":     512,
        "prompt_id":      "p1",
        "prompt_version": 3,
        "prompt_name":    "Support v3",
        # Empty for an ordinary single-prompt task; present so a multi-turn or
        # chained task round-trips through the same record.
        "messages":       [],
        "tools":          [],
        "steps":          [],
    }


# ── Generation failure isolation ──────────────────────────────────────────────

def test_generation_failure_is_reported_not_raised(monkeypatch):
    """One bad example must not end the run."""
    async def boom(*_a, **_kw):
        raise RuntimeError("provider exploded")

    monkeypatch.setattr(task_runner, "complete_messages", boom)
    gen = asyncio.run(
        task_runner.generate(_task("{{input}}"), "key", "anthropic", {"input": "hi"}, {})
    )
    assert not gen.ok
    assert "provider exploded" in gen.error
    assert "hi" in gen.rendered


def test_successful_generation_carries_technical_metrics(monkeypatch):
    async def ok(*_a, **_kw):
        from shared.completions import Completion
        return Completion(
            text="the answer", input_tokens=120, output_tokens=40, cached_tokens=10,
        )

    async def price(*_a, **_kw):
        return 0.00042

    monkeypatch.setattr(task_runner, "complete_messages", ok)
    monkeypatch.setattr(task_runner, "estimate_cost", price)
    gen = asyncio.run(
        task_runner.generate(_task("{{input}}"), "key", "openai", {"input": "q"}, {})
    )
    assert gen.ok
    assert gen.output == "the answer"
    assert (gen.input_tokens, gen.output_tokens) == (120, 40)
    assert gen.cost_usd == 0.00042
    assert gen.latency_ms >= 0
