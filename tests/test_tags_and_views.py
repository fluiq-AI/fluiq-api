"""Trace tags and saved views — the two things that make production data sliceable.

Tags label traffic; views make a slice reusable. Both are contract surfaces the
frontend and the SDK depend on, so the shapes are pinned here.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

from routes.trace import MAX_TAGS_PER_TRACE, normalize_tags
from routes.views import MAX_FILTER_BYTES, ViewPayload, _serialize


# ══ Tags ═════════════════════════════════════════════════════════════════════

def test_a_plain_tag_survives():
    assert normalize_tags(["prompt-a"]) == ["prompt-a"]


def test_tags_are_lowercased():
    """`Prompt-A` and `prompt-a` being different tags is a trap that only shows
    up when a filter silently returns nothing."""
    assert normalize_tags(["Prompt-A", "PROMPT-a"]) == ["prompt-a"]


def test_tags_are_deduplicated():
    assert normalize_tags(["canary", "canary", "canary"]) == ["canary"]


def test_order_is_preserved_for_distinct_tags():
    assert normalize_tags(["b", "a"]) == ["b", "a"]


def test_surrounding_whitespace_is_trimmed():
    assert normalize_tags(["  canary  "]) == ["canary"]


def test_a_bare_string_is_accepted_as_one_tag():
    """A caller passing tags="canary" instead of ["canary"] meant one tag."""
    assert normalize_tags("canary") == ["canary"]


def test_the_permitted_punctuation_works():
    assert normalize_tags(["v1.2", "team_a", "a-b", "svc/checkout"]) == [
        "v1.2", "team_a", "a-b", "svc/checkout",
    ]


def test_invalid_tags_are_dropped_not_raised():
    """Tagging is a side-channel on an ingest call. Failing the whole trace over
    a stray character would cost the customer data to gain them nothing."""
    assert normalize_tags(["good", "has space", "WITH!BANG", ""]) == ["good"]


def test_a_tag_cannot_start_with_punctuation():
    assert normalize_tags(["-leading", "_leading", ".leading"]) == []


def test_an_over_long_tag_is_dropped():
    assert normalize_tags(["a" * 64]) == []
    assert normalize_tags(["a" * 63]) == ["a" * 63]


def test_the_number_of_tags_is_bounded():
    many = [f"tag{i}" for i in range(MAX_TAGS_PER_TRACE + 10)]
    assert len(normalize_tags(many)) == MAX_TAGS_PER_TRACE


def test_empty_input_is_empty_output():
    for empty in (None, [], "", {}):
        assert normalize_tags(empty) == []


def test_a_non_iterable_is_ignored_rather_than_crashing():
    assert normalize_tags(42) == []


def test_the_sdk_and_api_agree_on_what_a_tag_is():
    """A tag the SDK accepts but the server drops would fail silently. The two
    regexes are duplicated because the SDK can't import from the API."""
    import re
    from pathlib import Path

    sdk = (
        Path(__file__).resolve().parents[2]
        / "fluiq-sdk" / "src" / "fluiq" / "__init__.py"
    )
    if not sdk.is_file():
        pytest.skip("SDK not checked out")
    match = re.search(r'_TAG_RE = _re\.compile\(r"(.+?)"\)', sdk.read_text(encoding="utf-8"))
    assert match, "SDK tag regex not found"

    from routes.trace import _TAG_RE as api_re
    assert match.group(1) == api_re.pattern, "SDK and API tag rules have drifted"


# ══ Saved views ══════════════════════════════════════════════════════════════

def test_a_view_needs_a_name():
    with pytest.raises(ValueError, match="name is required"):
        ViewPayload(name="   ")


def test_traces_is_the_default_surface():
    assert ViewPayload(name="Failures").surface == "traces"


def test_an_unknown_surface_is_rejected():
    """A view is meaningless on a list whose filters it doesn't share."""
    with pytest.raises(ValueError, match="surface must be one of"):
        ViewPayload(name="X", surface="billing")


def test_views_default_to_shared():
    """A review queue only works if the reviewers can see it."""
    assert ViewPayload(name="Thumbs down").shared is True


def test_filters_are_opaque():
    """Stored as JSON rather than columns, so adding a filter to a surface
    doesn't need a schema migration."""
    payload = ViewPayload(
        name="Bad GPT-5",
        filters={"quality": "failing", "tag": ["prompt-a"], "model": "gpt-5-mini"},
    )
    assert payload.filters["tag"] == ["prompt-a"]


def test_oversized_filters_are_rejected():
    with pytest.raises(ValueError, match="too large"):
        ViewPayload(name="X", filters={"blob": "x" * (MAX_FILTER_BYTES + 1)})


def test_an_over_long_name_is_rejected():
    with pytest.raises(ValueError, match="80 characters"):
        ViewPayload(name="x" * 81)


def _row(**over):
    base = {
        "view_id":    uuid.uuid4(),
        "surface":    "traces",
        "name":       "Thumbs down",
        "description": None,
        "filters":    {"quality": "failing"},
        "shared":     True,
        "created_by": uuid.uuid4(),
        "project_id": None,
        "created_at": datetime.now(timezone.utc),
    }
    base.update(over)
    return base


def test_serialized_view_carries_its_filters():
    assert _serialize(_row())["filters"] == {"quality": "failing"}


def test_serialized_view_parses_filters_stored_as_json_text():
    """The jsonb codec is not applied on every pool path."""
    out = _serialize(_row(filters=json.dumps({"tag": ["canary"]})))
    assert out["filters"]["tag"] == ["canary"]


def test_unparseable_filters_degrade_to_empty_rather_than_breaking_the_list():
    """One corrupt row must not make every view unreachable."""
    assert _serialize(_row(filters="{not json"))["filters"] == {}


def test_a_view_with_no_filters_serializes_as_an_empty_object():
    assert _serialize(_row(filters=None))["filters"] == {}


def test_ids_are_stringified_for_json():
    out = _serialize(_row())
    assert isinstance(out["view_id"], str)
    assert isinstance(out["created_by"], str)
    assert out["project_id"] is None
