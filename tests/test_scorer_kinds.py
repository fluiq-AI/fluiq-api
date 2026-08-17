"""A saved scorer is either an LLM judge or a deterministic expression.

Both are referenced identically at run time, so the request contract is what
decides which one a slug becomes — and getting that wrong is silent: a code
scorer stored as a judge would be handed to a model as a prompt.
"""
from __future__ import annotations

import pytest

from routes.datasets import SaveScorerRequest, _serialize_scorer
from routes.prompts import VALID_PROMPT_KINDS, SavePromptRequest


# ── Dataset scorers ───────────────────────────────────────────────────────────

def test_judge_is_the_default_kind():
    """Every scorer saved before code scorers existed was a judge, so the
    default has to stay 'judge' or those requests change meaning."""
    req = SaveScorerRequest(name="Tone", template="Grade {{answer}}")
    assert req.kind == "judge"


def test_judge_scorer_must_reference_the_answer():
    with pytest.raises(ValueError, match="answer"):
        SaveScorerRequest(name="Tone", template="Grade the vibe.")


def test_code_scorer_is_accepted_without_any_placeholder():
    """A code scorer reads `output` as a variable; requiring {{answer}} would be
    the judge contract leaking into a language that doesn't have placeholders."""
    req = SaveScorerRequest(
        name="Short enough", template="len(output) < 500", kind="code",
    )
    assert req.kind == "code"


def test_invalid_code_is_rejected_at_author_time():
    with pytest.raises(ValueError, match="Syntax error"):
        SaveScorerRequest(name="Broken", template="len(output <", kind="code")


def test_unsafe_code_is_rejected_at_author_time():
    with pytest.raises(ValueError):
        SaveScorerRequest(
            name="Nasty", template="().__class__.__bases__", kind="code",
        )


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="judge.*code|code.*judge"):
        SaveScorerRequest(name="X", template="True", kind="wasm")


def test_empty_body_is_rejected_for_either_kind():
    for kind in ("judge", "code"):
        with pytest.raises(ValueError, match="required"):
            SaveScorerRequest(name="X", template="   ", kind=kind)


def test_threshold_must_be_a_probability():
    with pytest.raises(ValueError, match="between 0 and 1"):
        SaveScorerRequest(name="X", template="True", kind="code", threshold=5)


def test_serialized_scorer_reports_its_kind():
    out = _serialize_scorer({
        "slug": "short", "name": "Short", "template": "len(output) < 500",
        "kind": "code", "threshold": 0.5, "created_at": None,
    })
    assert out["kind"] == "code"


def test_a_scorer_saved_before_kinds_existed_reads_as_a_judge():
    """The column is absent on old links, and only an explicitly-saved code
    scorer is one — so the fallback must be 'judge', not 'code'."""
    out = _serialize_scorer({
        "slug": "tone", "name": "Tone", "template": "Grade {{answer}}",
        "threshold": 0.5, "created_at": None,
    })
    assert out["kind"] == "judge"


# ── Choice scores ─────────────────────────────────────────────────────────────

YN = [{"label": "Y", "score": 1.0}, {"label": "N", "score": 0.0}]


def test_a_judge_can_carry_a_choice_set():
    req = SaveScorerRequest(name="Apologised", template="Did it? {{answer}}", choices=YN)
    assert req.choices == YN


def test_choices_are_optional():
    """Every judge saved before choices existed has none, and must keep working."""
    assert SaveScorerRequest(name="Tone", template="Grade {{answer}}").choices is None


def test_an_invalid_choice_set_is_rejected_at_author_time():
    with pytest.raises(ValueError, match="at least two"):
        SaveScorerRequest(
            name="X", template="Grade {{answer}}", choices=[{"label": "Y", "score": 1}],
        )


def test_a_choice_score_outside_the_unit_range_is_rejected():
    with pytest.raises(ValueError, match="must be 0 to 1"):
        SaveScorerRequest(
            name="X", template="Grade {{answer}}",
            choices=[{"label": "Y", "score": 10}, {"label": "N", "score": 0}],
        )


def test_duplicate_choice_labels_are_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        SaveScorerRequest(
            name="X", template="Grade {{answer}}",
            choices=[{"label": "Y", "score": 1}, {"label": "y", "score": 0}],
        )


def test_a_code_scorer_cannot_have_choices():
    """It returns its own number; there is nothing for a choice table to map."""
    with pytest.raises(ValueError, match="Choices apply to LLM judges"):
        SaveScorerRequest(
            name="X", template="len(output) < 5", kind="code", choices=YN,
        )


def test_serialized_scorer_exposes_its_choices():
    out = _serialize_scorer({
        "slug": "apologised", "name": "Apologised", "template": "Did it? {{answer}}",
        "kind": "judge", "config": {"choices": YN}, "threshold": 0.5, "created_at": None,
    })
    assert out["choices"] == YN


def test_serialized_scorer_parses_config_stored_as_json_text():
    """The jsonb codec is not applied on every pool path."""
    out = _serialize_scorer({
        "slug": "a", "name": "A", "template": "{{answer}}", "kind": "judge",
        "config": '{"choices": [{"label": "Y", "score": 1}, {"label": "N", "score": 0}]}',
        "threshold": 0.5, "created_at": None,
    })
    assert [c["label"] for c in out["choices"]] == ["Y", "N"]


def test_a_scorer_without_config_reports_no_choices():
    out = _serialize_scorer({
        "slug": "a", "name": "A", "template": "{{answer}}", "kind": "judge",
        "threshold": 0.5, "created_at": None,
    })
    assert out["choices"] is None


# ── Prompt library ────────────────────────────────────────────────────────────

def test_prompt_kinds_include_code():
    assert VALID_PROMPT_KINDS == {"completion", "judge", "code"}


def test_saving_a_prompt_with_an_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="kind must be one of"):
        SavePromptRequest(name="X", slug="x-1", template="hi", kind="binary")


def test_saving_a_code_prompt_is_allowed():
    req = SavePromptRequest(
        name="Short", slug="short-enough", template="len(output) < 500", kind="code",
    )
    assert req.kind == "code"
