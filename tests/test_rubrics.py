"""Review rubrics and the annotator role.

A rubric turns "score this 0–1" into the questions a domain expert actually
answers. The scoring rules are pinned here because a wrong one silently
redefines the org's whole review history.
"""
from __future__ import annotations

import pytest

from routes.rubrics import (
    MAX_OPTIONS,
    VALID_KINDS,
    FieldPayload,
    score_answer,
    validate_options,
)
from shared.permissions import (
    ADMIN_ACCESS,
    ANNOTATOR_PATHS,
    FULL_ACCESS,
    ROLES,
    annotator_may_reach,
)


# ══ Field definition ═════════════════════════════════════════════════════════

def test_a_key_must_be_a_stable_identifier():
    """Every recorded answer is filed under the key, so it has to be safe to
    query and safe to keep."""
    for bad in ("Has Spaces", "1leading", "has-dash", "", "with.dot"):
        with pytest.raises(ValueError, match="key must"):
            FieldPayload(key=bad, label="X")


def test_a_good_key_is_accepted_and_lowercased():
    """Case is normalised rather than rejected, matching how slugs and tags
    behave elsewhere — `Dosage` and `dosage` being different keys is a trap that
    only surfaces when a query silently returns nothing."""
    assert FieldPayload(key="  Dosage_Correct ", label="X").key == "dosage_correct"


def test_a_label_is_required():
    """The key is for machines; without a label the reviewer sees nothing."""
    with pytest.raises(ValueError, match="label is required"):
        FieldPayload(key="k", label="   ")


def test_the_four_kinds_cover_how_people_actually_answer():
    assert set(VALID_KINDS) == {"choice", "boolean", "slider", "text"}


def test_an_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="kind must be one of"):
        FieldPayload(key="k", label="X", kind="dropdown")


# ══ Options ══════════════════════════════════════════════════════════════════

def test_a_choice_field_needs_at_least_two_options():
    with pytest.raises(ValueError, match="at least two"):
        validate_options("choice", [{"label": "Yes", "score": 1}])


def test_options_are_bounded():
    with pytest.raises(ValueError, match="at most"):
        validate_options(
            "choice", [{"label": f"o{i}", "score": 0.5} for i in range(MAX_OPTIONS + 1)],
        )


def test_duplicate_option_labels_are_rejected():
    """Two options a reviewer cannot tell apart make the answer meaningless."""
    with pytest.raises(ValueError, match="duplicate option"):
        validate_options(
            "choice", [{"label": "Yes", "score": 1}, {"label": "yes", "score": 0}],
        )


def test_an_option_score_outside_the_unit_range_is_rejected():
    with pytest.raises(ValueError, match="between 0 and 1"):
        validate_options(
            "choice", [{"label": "A", "score": 5}, {"label": "B", "score": 0}],
        )


def test_a_non_numeric_option_score_is_rejected():
    with pytest.raises(ValueError, match="numeric score"):
        validate_options(
            "choice", [{"label": "A", "score": "high"}, {"label": "B", "score": 0}],
        )


def test_non_choice_kinds_carry_no_options():
    """A slider with options would present two contradictory controls."""
    for kind in ("boolean", "slider", "text"):
        assert validate_options(kind, [{"label": "x", "score": 1}]) == []


# ══ Scoring an answer ════════════════════════════════════════════════════════

RUBRIC_CHOICE = {
    "key": "dosage",
    "kind": "choice",
    "options": [
        {"label": "Correct", "score": 1.0},
        {"label": "Unclear", "score": 0.5},
        {"label": "Wrong", "score": 0.0},
    ],
}


def test_a_chosen_label_becomes_its_score():
    assert score_answer(RUBRIC_CHOICE, "Correct") == 1.0
    assert score_answer(RUBRIC_CHOICE, "Unclear") == 0.5


def test_matching_ignores_case_and_padding():
    assert score_answer(RUBRIC_CHOICE, "  correct ") == 1.0


def test_an_unrecognised_choice_raises_rather_than_scoring_zero():
    """A 0 there would be indistinguishable from a reviewer marking it Wrong —
    the same failure the LLM choice-scorer avoids."""
    with pytest.raises(ValueError, match="is not one of"):
        score_answer(RUBRIC_CHOICE, "Maybe")


def test_the_error_lists_the_options_that_were_offered():
    with pytest.raises(ValueError, match="Correct, Unclear, Wrong"):
        score_answer(RUBRIC_CHOICE, "?")


def test_a_boolean_answer_maps_to_one_or_zero():
    field = {"key": "safe", "kind": "boolean"}
    assert score_answer(field, True) == 1.0
    assert score_answer(field, False) == 0.0


def test_a_boolean_field_rejects_a_non_boolean():
    """"yes" is truthy in Python, and accepting it would score a typo as a pass."""
    with pytest.raises(ValueError, match="true or false"):
        score_answer({"key": "safe", "kind": "boolean"}, "yes")


def test_a_slider_passes_a_unit_value_through():
    assert score_answer({"key": "q", "kind": "slider"}, 0.75) == 0.75


def test_a_slider_rejects_a_value_outside_the_range():
    with pytest.raises(ValueError, match="between 0 and 1"):
        score_answer({"key": "q", "kind": "slider"}, 7)


def test_a_slider_rejects_something_unparseable():
    with pytest.raises(ValueError, match="a number"):
        score_answer({"key": "q", "kind": "slider"}, "quite good")


def test_a_text_field_is_recorded_but_not_scored():
    """Scoring prose would invent a number nobody gave."""
    assert score_answer({"key": "notes", "kind": "text"}, "looks fine") is None


# ══ The annotator role ═══════════════════════════════════════════════════════

def test_annotator_is_a_role_but_not_full_access():
    """The whole point: recording verdicts without reaching keys, billing, or
    the ability to delete a dataset."""
    assert "annotator" in ROLES
    assert "annotator" not in FULL_ACCESS
    assert "annotator" not in ADMIN_ACCESS


def test_an_annotator_reaches_the_review_surface():
    for path in (
        "/api/v1/review/queue",
        "/api/v1/review/matrix",
        "/api/v1/rubric",
        "/api/v1/traces/abc/review",
        "/api/v1/traces",
    ):
        assert annotator_may_reach(path), path


def test_an_annotator_is_refused_everywhere_else():
    """An allowlist, not a denylist — a route added tomorrow should be closed to
    a contractor until someone decides otherwise."""
    for path in (
        "/api/v1/credentials",
        "/api/v1/api-keys",
        "/api/v1/billing",
        "/api/v1/datasets",
        "/api/v1/prompts",
        "/api/v1/online-rules",
        "/api/v1/resources/push",
        "/admin/users",
    ):
        assert not annotator_may_reach(path), path


def test_provider_credentials_are_specifically_out_of_reach():
    """The failure this role exists to prevent."""
    assert not annotator_may_reach("/api/v1/credentials")


def test_the_allowlist_has_no_bare_root():
    """A prefix of "/" or "/api/v1" would match everything and quietly turn the
    allowlist into a no-op."""
    for prefix in ANNOTATOR_PATHS:
        assert prefix not in ("/", "/api", "/api/v1")
        assert len(prefix) > len("/api/v1")
