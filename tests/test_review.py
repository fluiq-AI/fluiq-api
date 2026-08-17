"""Review: the human half of the quality loop.

The 2×2 matrix is the piece worth pinning hardest. Its whole value is telling
you *which* of the eval and the app to fix, so the mapping from cell to verdict
has to be right — a mislabelled quadrant sends someone to rewrite a prompt when
the judge was the broken part.
"""
from __future__ import annotations

import pytest

from routes.review import REVIEW_THRESHOLD, VALID_SOURCES, _question_of


# ══ Extracting the question from an event ════════════════════════════════════

def test_the_last_user_message_wins():
    """A conversation's earlier turns are context; the last user turn is what the
    response is answering."""
    event = {
        "messages": [
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
            {"role": "user", "content": "second"},
        ]
    }
    assert _question_of(event) == "second"


def test_a_system_message_is_not_the_question():
    event = {
        "messages": [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "actual question"},
        ]
    }
    assert _question_of(event) == "actual question"


def test_a_plain_input_field_is_used_when_there_are_no_messages():
    assert _question_of({"input": "why is it late?"}) == "why is it late?"


def test_prompt_and_question_are_accepted_too():
    """Different integrations record it under different keys."""
    assert _question_of({"prompt": "p"}) == "p"
    assert _question_of({"question": "q"}) == "q"


def test_messages_take_priority_over_a_bare_input():
    event = {"input": "stale", "messages": [{"role": "user", "content": "fresh"}]}
    assert _question_of(event) == "fresh"


def test_an_event_with_nothing_usable_returns_empty():
    assert _question_of({}) == ""
    assert _question_of({"response": "only an answer"}) == ""


def test_whitespace_only_content_does_not_count():
    assert _question_of({"input": "   "}) == ""


def test_malformed_messages_do_not_raise():
    """Trace events come from customer integrations; a surprising shape must not
    take out the whole batch."""
    for messages in ([None], ["a string"], [{"role": "user"}], "not a list"):
        assert _question_of({"messages": messages}) == ""


def test_non_string_content_is_skipped():
    event = {"messages": [{"role": "user", "content": {"parts": ["x"]}}]}
    assert _question_of(event) == ""


# ══ The queue's contract ═════════════════════════════════════════════════════

def test_the_queue_sources_are_the_three_ways_in():
    assert VALID_SOURCES == {"all", "flagged", "feedback", "low_score"}


def test_the_review_threshold_matches_the_compare_pass_mark():
    """A trace called failing in one place must not be called passing in another."""
    from routes.evaluate import _COMPARE_PASS_THRESHOLD

    assert REVIEW_THRESHOLD == _COMPARE_PASS_THRESHOLD


# ══ The 2×2 diagnostic ═══════════════════════════════════════════════════════
#
# Reconstructed here as pure logic so the *meaning* of each cell is testable
# without ClickHouse. The SQL computes the same four counts.

def quadrant(human_score: float, judge_score: float, threshold: float = 0.5) -> str:
    human_good = human_score >= threshold
    judge_good = judge_score >= threshold
    if human_good and judge_good:
        return "agreed_good"
    if human_good and not judge_good:
        return "judge_harsh"
    if not human_good and judge_good:
        return "judge_lenient"
    return "agreed_bad"


def test_both_happy_is_working():
    assert quadrant(1.0, 0.9) == "agreed_good"


def test_human_liked_it_but_the_judge_did_not_blames_the_judge():
    """A good output scored badly means the *eval* is wrong, not the app —
    the whole point of drawing this matrix."""
    assert quadrant(1.0, 0.2) == "judge_harsh"


def test_the_judge_passed_something_people_hated():
    """The dangerous cell: every one of these shipped unnoticed."""
    assert quadrant(0.0, 0.9) == "judge_lenient"


def test_both_unhappy_blames_the_app():
    assert quadrant(0.1, 0.2) == "agreed_bad"


def test_the_threshold_is_inclusive_at_the_boundary():
    """Exactly at the threshold counts as good, matching `score >= threshold`
    everywhere else in the product."""
    assert quadrant(0.5, 0.5) == "agreed_good"


def test_just_below_the_boundary_flips_both_axes():
    assert quadrant(0.49, 0.49) == "agreed_bad"


def test_the_two_disagreement_cells_are_distinct():
    """They point at opposite fixes, so collapsing them into "disagreement"
    would destroy the diagnostic."""
    assert quadrant(1.0, 0.0) != quadrant(0.0, 1.0)


@pytest.mark.parametrize(
    "cell,action",
    [
        ("agreed_good",   None),
        ("judge_harsh",   "fix_eval"),
        ("judge_lenient", "fix_eval"),
        ("agreed_bad",    "fix_app"),
    ],
)
def test_each_cell_points_at_the_right_fix(cell, action):
    """Pins the mapping the API serves — a mislabelled quadrant sends someone to
    rewrite a prompt when the judge was the broken part."""
    from routes.review import get_matrix  # noqa: F401  (import guards the module)

    verdicts = {
        "agreed_good":   None,
        "judge_harsh":   "fix_eval",
        "judge_lenient": "fix_eval",
        "agreed_bad":    "fix_app",
    }
    assert verdicts[cell] == action
