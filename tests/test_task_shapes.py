"""Multi-turn tasks, tools, and prompt chains.

Three features, one mechanism: a task is a list of steps, each of which is
either a prompt or a conversation and may offer tools. Building them separately
would have meant three execution paths where one does.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from routes.datasets import task_runner
from routes.datasets.task_runner import (
    ResolvedTask,
    TaskError,
    TaskStep,
    _parse_messages,
    _parse_step,
    _parse_tools,
    render_messages,
)
from shared.completions import Completion


def _resolve(**kwargs):
    return asyncio.run(task_runner.resolve_task(uuid.uuid4(), **kwargs))


# ══ Multi-turn conversations ═════════════════════════════════════════════════

def test_a_conversation_is_accepted_instead_of_a_template():
    task = _resolve(
        model="gpt-5-mini",
        messages=[
            {"role": "user", "content": "where is my order?"},
            {"role": "assistant", "content": "Let me check."},
            {"role": "user", "content": "{{input}}"},
        ],
    )
    assert len(task.messages) == 3
    assert task.messages[1]["role"] == "assistant"


def test_a_conversation_satisfies_the_needs_something_to_run_check():
    """Requiring a template alongside a conversation would make the caller
    invent one that never gets sent."""
    assert _resolve(model="gpt-5-mini", messages=[{"role": "user", "content": "hi"}])


def test_an_unknown_role_is_rejected_not_coerced():
    """A turn silently re-roled would evaluate a conversation nobody wrote, and
    the scores would look perfectly normal."""
    with pytest.raises(TaskError, match="role must be one of"):
        _parse_messages([{"role": "narrator", "content": "x"}])


def test_a_conversation_needs_a_user_turn():
    """Without one the model has nothing to answer, so the run would score a
    page of empty responses."""
    with pytest.raises(TaskError, match="at least one user turn"):
        _parse_messages([{"role": "assistant", "content": "hello"}])


def test_an_empty_turn_is_rejected():
    with pytest.raises(TaskError, match="no content"):
        _parse_messages([{"role": "user", "content": "   "}])


def test_a_conversation_is_length_bounded():
    with pytest.raises(TaskError, match="at most"):
        _parse_messages([{"role": "user", "content": "x"}] * 41)


def test_conversation_content_is_templated_per_example():
    task = ResolvedTask(
        template="", model="gpt-5-mini",
        messages=[{"role": "user", "content": "Handle: {{input}}"}],
    )
    turns = render_messages(task.all_steps[0], task, {"input": "a refund"})
    assert turns[-1]["content"] == "Handle: a refund"


def test_a_task_system_prompt_is_prepended_to_a_conversation():
    task = ResolvedTask(
        template="", model="gpt-5-mini", system="Be terse",
        messages=[{"role": "user", "content": "hi"}],
    )
    turns = render_messages(task.all_steps[0], task, {})
    assert turns[0] == {"role": "system", "content": "Be terse"}


def test_a_conversations_own_system_turn_wins():
    """A step that writes its own system message is being deliberate; silently
    concatenating both would give it a prompt nobody wrote."""
    task = ResolvedTask(
        template="", model="gpt-5-mini", system="Task-level",
        messages=[
            {"role": "system", "content": "Step-level"},
            {"role": "user", "content": "hi"},
        ],
    )
    turns = render_messages(task.all_steps[0], task, {})
    assert [t["content"] for t in turns if t["role"] == "system"] == ["Step-level"]


# ══ Tools ════════════════════════════════════════════════════════════════════

def test_tools_are_accepted_and_normalised():
    tools = _parse_tools(
        [{"name": "lookup_order", "description": "Find an order"}], where="task",
    )
    assert tools[0]["name"] == "lookup_order"
    # A provider rejects a tool with no schema, so an absent one becomes empty.
    assert tools[0]["parameters"] == {"type": "object", "properties": {}}


def test_a_tool_needs_a_name():
    with pytest.raises(TaskError, match="needs a name"):
        _parse_tools([{"description": "no name"}], where="task")


def test_duplicate_tool_names_are_rejected():
    """Providers reject duplicates with an error naming the schema rather than
    the tool, which is unhelpful at 3am."""
    with pytest.raises(TaskError, match="duplicate tool"):
        _parse_tools([{"name": "a"}, {"name": "a"}], where="task")


def test_a_non_object_parameter_schema_is_rejected():
    with pytest.raises(TaskError, match="JSON schema object"):
        _parse_tools([{"name": "a", "parameters": "string"}], where="task")


def test_tools_are_bounded():
    with pytest.raises(TaskError, match="at most"):
        _parse_tools([{"name": f"t{i}"} for i in range(21)], where="task")


def test_a_tool_call_becomes_the_output_when_there_is_no_text(monkeypatch):
    """A model reaching for `lookup_order` instead of inventing a status has
    answered — that choice is what tool evaluation measures."""
    async def calls_a_tool(*_a, **_kw):
        return Completion(
            text="",
            tool_calls=[{"name": "lookup_order", "arguments": '{"id": 123}'}],
        )

    async def free(*_a, **_kw):
        return None

    monkeypatch.setattr(task_runner, "complete_messages", calls_a_tool)
    monkeypatch.setattr(task_runner, "estimate_cost", free)

    task = ResolvedTask(
        template="{{input}}", model="gpt-5-mini",
        tools=[{"name": "lookup_order", "description": "", "parameters": {}}],
    )
    gen = asyncio.run(task_runner.generate(task, "k", "openai", {"input": "x"}, {}))

    assert gen.ok
    assert "lookup_order" in gen.output
    assert gen.tool_calls[0]["name"] == "lookup_order"


def test_text_wins_over_a_tool_call_when_both_are_present(monkeypatch):
    """A model that answered *and* called a tool has given an answer; replacing
    it with the call would discard the thing being graded."""
    async def both(*_a, **_kw):
        return Completion(
            text="Your order ships tomorrow.",
            tool_calls=[{"name": "lookup_order", "arguments": "{}"}],
        )

    async def free(*_a, **_kw):
        return None

    monkeypatch.setattr(task_runner, "complete_messages", both)
    monkeypatch.setattr(task_runner, "estimate_cost", free)

    task = ResolvedTask(template="{{input}}", model="gpt-5-mini")
    gen = asyncio.run(task_runner.generate(task, "k", "openai", {"input": "x"}, {}))

    assert gen.output == "Your order ships tomorrow."
    assert gen.tool_calls  # still recorded


# ══ Chains ═══════════════════════════════════════════════════════════════════

def test_a_single_prompt_task_has_exactly_one_step():
    """The common case must not acquire a chain it never asked for."""
    task = _resolve(template="{{input}}", model="gpt-5-mini")
    assert task.is_chain is False
    assert len(task.all_steps) == 1


def test_steps_are_parsed_and_numbered_from_two():
    """Step 1 is the task's own template, so an extra step is step 2."""
    step = _parse_step({"template": "Then: {{previous}}"}, 0)
    assert step.name == "step 2"


def test_a_step_needs_a_template_or_a_conversation():
    with pytest.raises(TaskError, match="needs a template or a conversation"):
        _parse_step({}, 0)


def test_each_step_receives_the_previous_output(monkeypatch):
    seen = []

    async def echo(_provider, _key, _model, messages, **_kw):
        seen.append(messages[-1]["content"])
        return Completion(text=f"out{len(seen)}")

    async def free(*_a, **_kw):
        return None

    monkeypatch.setattr(task_runner, "complete_messages", echo)
    monkeypatch.setattr(task_runner, "estimate_cost", free)

    task = ResolvedTask(
        template="Extract from {{input}}", model="gpt-5-mini",
        steps=[TaskStep(template="Draft a reply to {{previous}}", name="draft")],
    )
    gen = asyncio.run(task_runner.generate(task, "k", "openai", {"input": "hi"}, {}))

    assert seen[0] == "Extract from hi"
    assert seen[1] == "Draft a reply to out1"
    # The chain's answer is the last step's output, not the first's.
    assert gen.output == "out2"


def test_a_later_step_can_reach_further_back(monkeypatch):
    seen = []

    async def echo(_provider, _key, _model, messages, **_kw):
        seen.append(messages[-1]["content"])
        return Completion(text=f"out{len(seen)}")

    async def free(*_a, **_kw):
        return None

    monkeypatch.setattr(task_runner, "complete_messages", echo)
    monkeypatch.setattr(task_runner, "estimate_cost", free)

    task = ResolvedTask(
        template="A {{input}}", model="gpt-5-mini",
        steps=[
            TaskStep(template="B {{previous}}"),
            TaskStep(template="C {{step_1}}"),
        ],
    )
    asyncio.run(task_runner.generate(task, "k", "openai", {"input": "x"}, {}))
    assert seen[2] == "C out1"


def test_cost_and_tokens_accumulate_across_a_chain(monkeypatch):
    """What a chain costs is what all of it costs; reporting only the last step
    would understate a three-model pipeline by two thirds."""
    async def each(*_a, **_kw):
        return Completion(text="x", input_tokens=10, output_tokens=5)

    captured = {}

    async def price(_provider, _model, in_tok, out_tok, cached):
        captured.update({"in": in_tok, "out": out_tok})
        return 0.01

    monkeypatch.setattr(task_runner, "complete_messages", each)
    monkeypatch.setattr(task_runner, "estimate_cost", price)

    task = ResolvedTask(
        template="{{input}}", model="gpt-5-mini",
        steps=[TaskStep(template="{{previous}}"), TaskStep(template="{{previous}}")],
    )
    gen = asyncio.run(task_runner.generate(task, "k", "openai", {"input": "x"}, {}))

    assert gen.input_tokens == 30   # 3 steps × 10
    assert gen.output_tokens == 15
    assert captured == {"in": 30, "out": 15}


def test_the_transcript_records_every_step(monkeypatch):
    """The report shows what the task actually asked; a chain that only recorded
    its last prompt would be undebuggable."""
    async def each(*_a, **_kw):
        return Completion(text="x")

    async def free(*_a, **_kw):
        return None

    monkeypatch.setattr(task_runner, "complete_messages", each)
    monkeypatch.setattr(task_runner, "estimate_cost", free)

    task = ResolvedTask(
        template="FIRST {{input}}", model="gpt-5-mini",
        steps=[TaskStep(template="SECOND {{previous}}", name="draft")],
    )
    gen = asyncio.run(task_runner.generate(task, "k", "openai", {"input": "x"}, {}))

    assert "FIRST x" in gen.rendered
    assert "SECOND x" in gen.rendered
    assert "draft" in gen.rendered


def test_a_chain_round_trips_through_the_run_record():
    """A run is only reproducible if every step survives being persisted."""
    task = ResolvedTask(
        template="a", model="m",
        steps=[TaskStep(template="b", name="second")],
        tools=[{"name": "t", "description": "", "parameters": {}}],
        messages=[{"role": "user", "content": "hi"}],
    )
    stored = task.to_json()
    assert stored["steps"][0]["template"] == "b"
    assert stored["tools"][0]["name"] == "t"
    assert stored["messages"][0]["role"] == "user"
