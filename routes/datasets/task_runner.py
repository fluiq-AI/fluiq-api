"""fluiq-api — Dataset run *tasks*.

A task is the thing being evaluated: a prompt + model that is executed against
every example in a dataset to produce fresh output. That output is what the
scorers then grade.

Before this existed a dataset run could only grade output that already existed
(a recorded trace response, or the example's own ``expected_output``), so there
was no way to ask the question evaluation exists to answer: *"if I change this
prompt or this model, do my scores go up or down?"*

Template variables
------------------
A task template is rendered per example with ``{{...}}`` placeholders (the
product-wide syntax — see ``shared.placeholders``). Available names:

  ``input``                    the example's input
  ``expected`` / ``expected_output``   the example's expected output
  any key of the example's ``metadata``  (so an imported CSV's extra columns
                                          are addressable, and few-shot blocks
                                          can be carried per row)

A template with no placeholders at all is still valid — the example's input is
appended, so "You are a support agent. Be concise." works as a bare system-style
task without the user having to learn the syntax first.

Execution is BYOK: the org's own provider key pays for the generation, resolved
once per run rather than once per example.
"""
from __future__ import annotations

import json
import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from db_queues.postgresql import credentials as cred_store
from db_queues.postgresql.prompts import get_prompt_by_slug, get_prompt_full, list_versions
from shared.completions import (
    complete_for,
    complete_messages,
    estimate_cost,
    provider_for_model,
)
from shared.placeholders import identifiers, substitute

logger = logging.getLogger(__name__)

# How many examples generate at once. Generation is the slow, rate-limited,
# money-spending part of a run, so this is deliberately modest: it keeps a
# 500-example run from tripping provider rate limits (which would fail examples
# rather than merely slow them) while still finishing a typical 20-row dataset
# in about the time of two sequential calls.
TASK_CONCURRENCY = 8

# Ceiling on a single run's generations. A dataset run already caps at 500
# examples; this is the same bound restated where the money is spent.
MAX_TASK_EXAMPLES = 500

# Steps are joined by a blank line in the recorded transcript.
TRANSCRIPT_JOIN = "\n\n"

# Few-shot blocks rendered into a prompt from an example's metadata. Bounded
# because each costs input tokens on every call, and a row carrying fifty would
# quietly make that one example the most expensive in the dataset.
MAX_FEW_SHOT = 10


class TaskError(Exception):
    """Task could not be resolved — a user-facing configuration problem."""


@dataclass
class TaskStep:
    """One turn of a task.

    A step is either a single templated prompt or a whole conversation. Both
    may offer tools. A task with several steps is a chain: each step's output is
    available to the next as ``{{previous}}``, which is how a real pipeline —
    extract, then decide, then draft — gets evaluated end to end rather than
    one prompt at a time.
    """
    template: str = ""
    #: A conversation, each entry ``{role, content}``, content templated. Wins
    #: over ``template`` when present.
    messages: List[Dict[str, str]] = field(default_factory=list)
    #: Tool definitions offered to the model, ``{name, description, parameters}``.
    tools: List[Dict[str, Any]] = field(default_factory=list)
    name: str = ""

    def to_json(self) -> Dict[str, Any]:
        return {
            "template": self.template,
            "messages": self.messages,
            "tools":    self.tools,
            "name":     self.name,
        }


@dataclass
class ResolvedTask:
    """A task, flattened to exactly what execution and the run record need."""
    template:       str
    model:          str
    system:         Optional[str] = None
    max_tokens:     int = 2048
    prompt_id:      Optional[str] = None
    prompt_version: Optional[int] = None
    prompt_name:    Optional[str] = None
    #: Steps beyond the first. Empty for the ordinary single-prompt task, so
    #: nothing about the common case changes.
    steps:          List[TaskStep] = field(default_factory=list)
    #: Conversation form of the first step, when the task is multi-turn.
    messages:       List[Dict[str, str]] = field(default_factory=list)
    #: Tools offered on the first step.
    tools:          List[Dict[str, Any]] = field(default_factory=list)

    @property
    def all_steps(self) -> List[TaskStep]:
        """Every step including the first, so execution has one thing to loop."""
        first = TaskStep(
            template=self.template, messages=self.messages,
            tools=self.tools, name="step 1",
        )
        return [first, *self.steps]

    @property
    def is_chain(self) -> bool:
        return bool(self.steps)

    def to_json(self) -> Dict[str, Any]:
        """The shape persisted on ``dataset_runs.task`` — this is what makes the
        run reproducible, so it records the resolved template, not the reference."""
        return {
            "template":       self.template,
            "system":         self.system,
            "model":          self.model,
            "max_tokens":     self.max_tokens,
            "prompt_id":      self.prompt_id,
            "prompt_version": self.prompt_version,
            "prompt_name":    self.prompt_name,
            "messages":       self.messages,
            "tools":          self.tools,
            "steps":          [s.to_json() for s in self.steps],
        }


@dataclass
class Generation:
    """One task execution against one example."""
    output:        str = ""
    rendered:      str = ""
    latency_ms:    int = 0
    input_tokens:  int = 0
    output_tokens: int = 0
    cost_usd:      Optional[float] = None
    error:         Optional[str] = None
    #: Tools the model asked to call. Kept alongside the text because a task
    #: that reaches for a tool has answered — the agentic evaluator scores that
    #: choice, so discarding it would hide the thing being measured.
    tool_calls:    List[Dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None


# ── Resolution ────────────────────────────────────────────────────────────────

async def resolve_task(
    org_id: uuid.UUID,
    *,
    model: Optional[str] = None,
    template: Optional[str] = None,
    system: Optional[str] = None,
    max_tokens: int = 2048,
    prompt_id: Optional[uuid.UUID] = None,
    prompt_slug: Optional[str] = None,
    prompt_version: Optional[int] = None,
    messages: Optional[List[Dict[str, str]]] = None,
    tools: Optional[List[Dict[str, Any]]] = None,
    steps: Optional[List[Dict[str, Any]]] = None,
) -> ResolvedTask:
    """Resolve a task request into a concrete template + model.

    A task references either a saved prompt — by id (the dashboard) or by slug
    (CI, where the slug is what an author has in hand), optionally pinned to a
    version so an experiment can be re-run against exactly the prompt it
    originally used — or an inline template (the playground's ad-hoc case).

    A slug resolves against every prompt the org has saved, deployed or not: a
    PR gate exists precisely to test a prompt before it ships.
    """
    resolved_name:    Optional[str] = None
    resolved_version: Optional[int] = None
    prompt_model:     Optional[str] = None
    prompt_tools:     List[Dict[str, Any]] = []

    if prompt_id is None and prompt_slug:
        row = await get_prompt_by_slug(org_id, prompt_slug.strip())
        if row is None:
            raise TaskError(f"No saved prompt with slug {prompt_slug!r}.")
        prompt_id = row["prompt_id"]

    if prompt_id is not None:
        row = await get_prompt_full(prompt_id, org_id)
        if row is None:
            raise TaskError("Prompt not found.")
        resolved_name = row.get("name")
        template = row.get("template") or ""
        resolved_version = row.get("version")
        prompt_model = row.get("model")
        prompt_tools = _tools_from_prompt(row)

        if prompt_version is not None and prompt_version != resolved_version:
            # Pin to a historical version so re-running an old experiment grades
            # the prompt it actually used, not whatever the prompt says today.
            versions = await list_versions(prompt_id, org_id)
            match = next((v for v in versions if v.get("version") == prompt_version), None)
            if match is None:
                raise TaskError(f"Prompt has no version {prompt_version}.")
            template = match.get("template") or ""
            prompt_model = match.get("model") or prompt_model
            resolved_version = prompt_version

    # An explicit model wins; a saved prompt's own model is the fallback so
    # "run this prompt over the dataset" needs no second choice.
    model = (model or prompt_model or "").strip()
    if not model:
        raise TaskError("A task needs a model.")
    if provider_for_model(model) is None:
        raise TaskError(
            f"Unknown provider for model {model!r}. Supported prefixes: "
            f"claude-, gpt-/o1-/o3-/o4-, gemini-, kimi-/moonshot-"
        )

    parsed_messages = _parse_messages(messages)
    # An explicit toolset on the run wins; otherwise the saved prompt's own
    # tools come along, so evaluating an agentic prompt doesn't silently run it
    # without the tools it was written for — which would look like the model
    # failing to call anything rather than like a misconfigured run.
    parsed_tools = _parse_tools(tools, where="task") if tools else prompt_tools
    parsed_steps = [_parse_step(raw, index) for index, raw in enumerate(steps or [])]

    # A conversation replaces the single template, so one is only required when
    # there is no conversation and no saved prompt to take one from.
    if not parsed_messages and not (template or "").strip():
        raise TaskError(
            "A task needs a prompt template, a conversation, or a prompt_id that has one."
        )

    return ResolvedTask(
        messages=parsed_messages,
        tools=parsed_tools,
        steps=parsed_steps,
        template=template,
        model=model,
        system=(system or None),
        # 0/None means "unspecified" and takes the default; anything else is
        # clamped so a client can't ask a provider for an absurd budget.
        max_tokens=max(1, min(int(max_tokens or 2048), 8192)),
        prompt_id=str(prompt_id) if prompt_id else None,
        prompt_version=resolved_version,
        prompt_name=resolved_name,
    )


# ── Parsing the richer task shapes ────────────────────────────────────────────

VALID_ROLES = ("system", "user", "assistant")
MAX_MESSAGES = 40
MAX_TOOLS = 20
MAX_STEPS = 8


def _parse_messages(raw: Optional[List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """Validate a conversation.

    Rejected rather than repaired: a task whose turns were silently reordered or
    re-roled would evaluate a conversation nobody wrote, and the scores would
    look perfectly normal.
    """
    if not raw:
        return []
    if len(raw) > MAX_MESSAGES:
        raise TaskError(f"A conversation can hold at most {MAX_MESSAGES} turns.")
    out: List[Dict[str, str]] = []
    for index, message in enumerate(raw):
        if not isinstance(message, dict):
            raise TaskError(f"Turn {index + 1} must be an object with a role and content.")
        role = str(message.get("role") or "").strip().lower()
        if role not in VALID_ROLES:
            raise TaskError(
                f"Turn {index + 1}: role must be one of {', '.join(VALID_ROLES)}."
            )
        content = str(message.get("content") or "")
        if not content.strip():
            raise TaskError(f"Turn {index + 1} has no content.")
        out.append({"role": role, "content": content})
    if not any(m["role"] == "user" for m in out):
        # Without a user turn there is no question, so the model has nothing to
        # answer and the run would score empty responses.
        raise TaskError("A conversation needs at least one user turn.")
    return out


def _tools_from_prompt(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The toolset a saved prompt carries, flattened for the provider call.

    A prompt stores plain tools and MCP servers separately, but a model is
    offered one flat list — it selects by name and never sees the distinction.
    The ``kind``/``server`` fields ride along so the agentic evaluator can still
    tell an MCP call from a local one when it grades tool selection.
    """
    config = row.get("config")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except ValueError:
            return []
    if not isinstance(config, dict):
        return []

    out: List[Dict[str, Any]] = []
    for tool in config.get("tools") or []:
        if isinstance(tool, dict) and str(tool.get("name") or "").strip():
            out.append(dict(tool))
    for server in config.get("mcp_servers") or []:
        if not isinstance(server, dict):
            continue
        for tool in server.get("tools") or []:
            if isinstance(tool, dict) and str(tool.get("name") or "").strip():
                out.append({**tool, "kind": "mcp", "server": server.get("label")})
    return out[:MAX_TOOLS]


def _parse_tools(raw: Optional[List[Dict[str, Any]]], *, where: str) -> List[Dict[str, Any]]:
    """Validate tool definitions offered to the model."""
    if not raw:
        return []
    if len(raw) > MAX_TOOLS:
        raise TaskError(f"{where}: at most {MAX_TOOLS} tools.")
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for tool in raw:
        if not isinstance(tool, dict):
            raise TaskError(f"{where}: each tool must be an object.")
        name = str(tool.get("name") or "").strip()
        if not name:
            raise TaskError(f"{where}: every tool needs a name.")
        if name in seen:
            # Providers reject duplicates, and the error they return names the
            # schema rather than the tool, which is unhelpful at 3am.
            raise TaskError(f"{where}: duplicate tool {name!r}.")
        seen.add(name)
        parameters = tool.get("parameters")
        if parameters is not None and not isinstance(parameters, dict):
            raise TaskError(f"{where}: tool {name!r} parameters must be a JSON schema object.")
        out.append({
            "name": name,
            "description": str(tool.get("description") or ""),
            "parameters": parameters or {"type": "object", "properties": {}},
        })
    return out


def _parse_step(raw: Dict[str, Any], index: int) -> TaskStep:
    if not isinstance(raw, dict):
        raise TaskError(f"Step {index + 2} must be an object.")
    messages = _parse_messages(raw.get("messages"))
    template = str(raw.get("template") or "")
    if not messages and not template.strip():
        raise TaskError(f"Step {index + 2} needs a template or a conversation.")
    return TaskStep(
        template=template,
        messages=messages,
        tools=_parse_tools(raw.get("tools"), where=f"step {index + 2}"),
        # Steps are numbered from 2 because the task's own template is step 1.
        name=str(raw.get("name") or f"step {index + 2}"),
    )


# ── Rendering ─────────────────────────────────────────────────────────────────

def task_variables(example: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """The variable bag a task template is rendered against for one example.

    Example fields win over metadata: a CSV column literally named "input"
    must not shadow the example's real input.
    """
    values: Dict[str, Any] = {}
    for key, value in (metadata or {}).items():
        # Nested structures would stringify as Python reprs inside the prompt;
        # only scalars are useful as template variables.
        if isinstance(value, (str, int, float, bool)):
            values[str(key)] = value
    expected = example.get("expected_output") or ""
    values["input"] = example.get("input") or ""
    values["expected"] = expected
    values["expected_output"] = expected
    # Per-row few-shot examples, rendered ready to drop into a prompt. Carried on
    # the example rather than the template because the *useful* few-shots differ
    # per row — the nearest neighbours to this question, not a fixed three
    # pasted into every prompt.
    values["few_shot"] = render_few_shot(metadata)
    return values


def render_few_shot(metadata: Dict[str, Any]) -> str:
    """Format an example's ``few_shot`` metadata as prompt-ready text.

    Accepts the two shapes people store: a list of ``{input, output}`` objects,
    or a string someone already formatted. A string passes through untouched —
    reformatting what an author deliberately laid out would be presumptuous.

    Returns "" when there are none, so a template referencing ``{{few_shot}}``
    degrades to a prompt without examples rather than one containing the word
    "None".
    """
    raw = (metadata or {}).get("few_shot") or (metadata or {}).get("examples")
    if not raw:
        return ""
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, str) or not isinstance(raw, (list, tuple)):
        return ""

    blocks: List[str] = []
    for shot in raw[:MAX_FEW_SHOT]:
        if isinstance(shot, str):
            blocks.append(shot.strip())
            continue
        if not isinstance(shot, dict):
            continue
        question = str(
            shot.get("input") or shot.get("question") or shot.get("user") or ""
        ).strip()
        answer = str(
            shot.get("output") or shot.get("expected") or shot.get("answer")
            or shot.get("assistant") or ""
        ).strip()
        if not question and not answer:
            continue
        blocks.append(f"Input: {question}\nOutput: {answer}")
    return TRANSCRIPT_JOIN.join(blocks)


def render_task(task: ResolvedTask, example: Dict[str, Any], metadata: Dict[str, Any]) -> str:
    """Fill the task template for one example.

    When the template references no variables at all we append the example's
    input, so a plain instruction still evaluates against the dataset instead of
    sending the identical prompt for every row.
    """
    values = task_variables(example, metadata)
    rendered = substitute(task.template, values)
    if not identifiers(task.template):
        user_input = str(values.get("input") or "").strip()
        if user_input:
            rendered = f"{rendered.rstrip()}\n\n{user_input}"
    return rendered


# ── Execution ─────────────────────────────────────────────────────────────────

def render_messages(
    step: TaskStep,
    task: ResolvedTask,
    values: Dict[str, Any],
) -> List[Dict[str, str]]:
    """The message list a step sends, with every content field templated.

    A system prompt set on the task applies to every step, and is prepended
    rather than merged into an existing system turn — a step that writes its own
    system message is being deliberate, and silently concatenating the two would
    give it a prompt nobody wrote.
    """
    if step.messages:
        turns = [
            {
                "role": str(m.get("role") or "user"),
                "content": substitute(str(m.get("content") or ""), values),
            }
            for m in step.messages
        ]
    else:
        rendered = substitute(step.template, values)
        if not identifiers(step.template):
            user_input = str(values.get("input") or "").strip()
            if user_input:
                rendered = f"{rendered.rstrip()}\n\n{user_input}"
        turns = [{"role": "user", "content": rendered}]

    if task.system and not any(t["role"] == "system" for t in turns):
        turns = [{"role": "system", "content": task.system}, *turns]
    return turns


def _describe(turns: List[Dict[str, str]]) -> str:
    """A readable transcript of what was sent, for the run report.

    The report shows what the task actually asked; a JSON blob of roles would be
    accurate and unreadable, which for a debugging surface is the same as wrong.
    """
    return "\n\n".join(f"[{t['role']}] {t['content']}" for t in turns)


async def generate(
    task: ResolvedTask,
    key: str,
    provider: str,
    example: Dict[str, Any],
    metadata: Dict[str, Any],
) -> Generation:
    """Execute the task against one example. Never raises: a failed example is
    recorded as a failure and the rest of the run continues.

    A multi-step task runs its steps in order, threading each output into the
    next as ``{{previous}}``. Token counts and cost accumulate across the whole
    chain, because what a chain costs is what all of it costs.
    """
    values = task_variables(example, metadata)
    started = time.monotonic()
    transcript: List[str] = []
    totals = {"in": 0, "out": 0, "cached": 0}
    output = ""
    tool_calls: List[Dict[str, Any]] = []

    try:
        for index, step in enumerate(task.all_steps):
            turns = render_messages(step, task, values)
            transcript.append(
                f"── {step.name or f'step {index + 1}'} ──\n{_describe(turns)}"
                if task.is_chain else _describe(turns)
            )
            completion = await complete_messages(
                provider, key, task.model, turns,
                tools=step.tools or None, max_tokens=task.max_tokens,
            )
            totals["in"] += completion.input_tokens
            totals["out"] += completion.output_tokens
            totals["cached"] += completion.cached_tokens

            # A tool call is an answer too. The model asking to call
            # `lookup_order` instead of inventing an order status is the
            # behaviour tool evaluation exists to check, so it is recorded and
            # rendered rather than treated as an empty response.
            output = completion.text
            if completion.tool_calls:
                tool_calls.extend(completion.tool_calls)
                if not output.strip():
                    output = _render_tool_calls(completion.tool_calls)

            # Each step sees the last one's result. `previous` is the name a
            # chain author reaches for; `step_1` etc. let a later step reach
            # further back than one hop.
            values["previous"] = output
            values[f"step_{index + 1}"] = output

        latency_ms = int((time.monotonic() - started) * 1000)
        cost = await estimate_cost(
            provider, task.model, totals["in"], totals["out"], totals["cached"],
        )
        return Generation(
            output=output,
            rendered="\n\n".join(transcript),
            latency_ms=latency_ms,
            input_tokens=totals["in"],
            output_tokens=totals["out"],
            cost_usd=cost,
            tool_calls=tool_calls,
        )
    except httpx.HTTPStatusError as exc:
        # Surface the provider's own message (bad key, no model access, rate
        # limit) rather than a bare status code — these are the failures a user
        # can actually act on.
        status_code = exc.response.status_code if exc.response is not None else ""
        detail = exc.response.text[:300] if exc.response is not None else str(exc)
        return Generation(
            rendered=TRANSCRIPT_JOIN.join(transcript),
            latency_ms=int((time.monotonic() - started) * 1000),
            error=f"{provider} error {status_code}: {detail}".strip(),
        )
    except Exception as exc:  # noqa: BLE001 — one bad example must not end the run
        return Generation(
            rendered=TRANSCRIPT_JOIN.join(transcript),
            latency_ms=int((time.monotonic() - started) * 1000),
            error=str(exc)[:300] or exc.__class__.__name__,
        )


def _render_tool_calls(calls: List[Dict[str, Any]]) -> str:
    """A tool request as text, so a scorer can read it.

    Scorers grade a string. A model that answered by calling `refund(order=123)`
    said something specific, and rendering it keeps that legible to a judge and
    to a `contains(output, 'refund')` code scorer alike.
    """
    return "\n".join(
        f"{call.get('name', '?')}({call.get('arguments') or ''})" for call in calls
    )


async def resolve_key(org_id: uuid.UUID, model: str) -> tuple[str, str]:
    """Resolve the BYOK provider + key that will pay for this task's generations.

    Done once per run rather than once per example, and up front so a missing
    key fails the launch with a clear message instead of failing 500 examples.
    """
    provider = provider_for_model(model)
    if provider is None:
        raise TaskError(f"Unknown provider for model {model!r}.")
    unsealed = await cred_store.unseal_active(org_id, provider)
    if unsealed is None:
        raise TaskError(
            f"No {provider} key configured. Add one under Provider Keys to run a task."
        )
    _, key = unsealed
    return provider, key


def semaphore() -> asyncio.Semaphore:
    """Bound on concurrent generations within a single run."""
    return asyncio.Semaphore(TASK_CONCURRENCY)


__all__ = [
    "MAX_TASK_EXAMPLES",
    "TASK_CONCURRENCY",
    "Generation",
    "ResolvedTask",
    "TaskError",
    "generate",
    "render_task",
    "resolve_key",
    "resolve_task",
    "semaphore",
    "task_variables",
]
