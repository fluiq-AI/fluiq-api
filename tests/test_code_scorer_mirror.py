"""The API's scoring modules must not drift from the evaluator's.

The API validates scorers at author time and runs the editor's test button; the
evaluator applies them to every example of a run. If the two implementations
diverge, a scorer can pass validation and then behave differently — or, far
worse, the API's copy can grow a permissive rule that the sandbox-escape tests
in the evaluator's suite never see.

Everything below each module docstring must be byte-identical. Edit the
evaluator's copy, then re-copy it here.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

API_DIR = Path(__file__).resolve().parent.parent / "shared"
WORKER_DIR = (
    Path(__file__).resolve().parents[2]
    / "fluiq-workers" / "evaluator" / "jobs" / "helper"
)

#: Modules the API mirrors from the evaluator, canonical copy on the right.
MIRRORED = ["code_scorer.py", "choice_scores.py"]


def _body_after_docstring(path: Path) -> str:
    """Source with the module docstring removed, so only the mirror's docstring
    is allowed to differ."""
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    if ast.get_docstring(tree, clean=False) is None:
        return text
    first = tree.body[0]
    lines = text.splitlines(keepends=True)
    return "".join(lines[first.end_lineno:])


@pytest.mark.parametrize("name", MIRRORED)
def test_api_and_worker_copies_are_identical(name):
    api, worker = API_DIR / name, WORKER_DIR / name
    if not worker.is_file():
        pytest.skip("evaluator worker not checked out")
    assert _body_after_docstring(api) == _body_after_docstring(worker), (
        f"shared/{name} has drifted from the evaluator's copy.\n"
        f"Re-copy: cp {worker} {api}\n"
        "then restore the mirror note at the top of the API copy."
    )


@pytest.mark.parametrize("name", MIRRORED)
def test_api_copy_carries_the_drift_warning(name):
    """The note is the only thing telling the next editor which copy is canonical."""
    head = (API_DIR / name).read_text(encoding="utf-8")[:1400]
    assert "mirror" in head.lower()
    assert name in head
