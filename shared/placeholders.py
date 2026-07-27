"""Prompt placeholder syntax, shared by every route that renders or validates one.

``{{variable}}`` is the product-wide standard: the Prompts page has always used
it, and judge prompts and custom scorers now use it too. Prompts saved before
the switch still hold the legacy ``string.Template`` forms (``$var`` /
``${var}``), so both are substituted and both satisfy a required-placeholder
check. That keeps already-saved prompts working instead of silently grading with
an unsubstituted template.

Only ``{{identifier}}`` counts as a placeholder, so the literal JSON braces these
prompts are full of need no escaping.

This mirrors ``jobs/helper/judge_prompts.py`` in the evaluator worker. The two
deployables can't share code at runtime, so the behaviour is duplicated and must
stay in sync.
"""
from __future__ import annotations

import re
from typing import Any, Dict

PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}|\$\{(\w+)\}|\$(\w+)")

# The answer placeholder in either form. A judge prompt that never references the
# output it grades is rejected.
ANSWER_PLACEHOLDER_RE = re.compile(r"\{\{\s*answer\s*\}\}|\$\{answer\}|\$answer\b")


def identifiers(template: str) -> set[str]:
    """Every placeholder name referenced by a template."""
    return {
        m.group(1) or m.group(2) or m.group(3)
        for m in PLACEHOLDER_RE.finditer(template)
    }


def substitute(template: str, values: Dict[str, Any]) -> str:
    """Fill placeholders, leaving unknown names untouched.

    Mirrors ``Template.safe_substitute``: a stray token can never raise.
    """
    def _repl(m: "re.Match[str]") -> str:
        key = m.group(1) or m.group(2) or m.group(3)
        return str(values[key]) if key in values else m.group(0)

    return PLACEHOLDER_RE.sub(_repl, template)


__all__ = ["PLACEHOLDER_RE", "ANSWER_PLACEHOLDER_RE", "identifiers", "substitute"]
