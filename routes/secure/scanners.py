"""Lightweight attack-pattern scanner for the /secure/check pre-call guard.

Only runs injection / jailbreak / skeleton-key pattern matching — no heavy
dependencies (presidio, sentence-transformers).  Full post-call scanning
(PII, secrets, semantic) is handled by the evaluator worker.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


def _compile(patterns: list[str]) -> list[tuple[str, re.Pattern]]:
    return [(p, re.compile(re.escape(p), re.IGNORECASE)) for p in patterns]


_INJECTION_PATTERNS: list[str] = [
    "ignore previous instructions", "ignore all previous instructions",
    "ignore all instructions", "ignore your instructions",
    "ignore your previous instructions", "disregard your",
    "disregard all previous", "disregard the above", "disregard all instructions",
    "override system", "override your instructions", "override the system prompt",
    "forget everything", "forget all previous instructions",
    "forget what you were told", "forget your previous",
    "do not follow", "do not follow your instructions",
    "### instruction", "###instruction", "[system]:", "[instructions]:",
    "<|system|>", "<|im_start|>system", "new instruction:", "begin new instructions",
    "start fresh", "reset your instructions", "clear your context",
    "from now on", "from this point on", "starting now", "henceforth",
    "repeat the above", "print the above", "reveal your system prompt",
    "show me your instructions", "what are your instructions",
    "tell me your system prompt", "output your system prompt",
    "repeat back your prompt", "{{", "{%",
]

_JAILBREAK_PATTERNS: list[str] = [
    "you are now", "pretend you are", "pretend to be", "act as if you are",
    "act as", "act like", "roleplay as", "role play as", "play the role of",
    "simulate a", "simulate being", "you are an AI without", "you are a different AI",
    "you are no longer", "you are not an AI", "you have no restrictions",
    "without any restrictions", "without restrictions", "no restrictions",
    "no limitations", "without limitations", "unrestricted", "uncensored",
    "unfiltered", "without censorship", "without filters",
    "jailbreak", "DAN", "do anything now", "DUDE", "STAN", "AIM",
    "BetterDAN", "BasedGPT", "developer mode", "god mode", "sudo mode",
    "admin mode", "maintenance mode", "test mode", "debug mode",
    "unrestricted mode", "override mode", "chaos mode", "evil mode", "dark mode",
    "hypothetically", "in a fictional world", "in a story", "for a novel",
    "for a movie", "for educational purposes", "for research purposes",
    "for academic purposes", "purely hypothetical", "just imagine", "let's say",
    "as a thought experiment", "theoretically speaking",
    "base64", "rot13", "in pig latin", "backwards", "in leetspeak",
    "i am a developer", "i am the developer", "i am an admin",
    "i am the administrator", "i have permission", "i have clearance",
    "this is authorized", "you are allowed to", "you can say", "it is ok to",
    "your true self", "deep down you want", "you secretly want",
    "your inner self", "if you could", "what would you do if", "new persona", "bypass",
]

_SKELETON_KEY_PATTERNS: list[str] = [
    "add a mode", "add a new mode", "create a mode", "enable a mode",
    "unlock a mode", "switch to a mode", "augment your baseline",
    "augment your directives", "update your baseline", "update your directives",
    "change your baseline", "override your baseline", "add to your instructions",
    "add an exception", "make an exception", "unlock your capabilities",
    "unlock your full capabilities", "unlock your potential",
    "your safety guidelines don't apply", "safety guidelines don't apply",
    "your guidelines don't apply", "treat this as an exception",
    "this is an exception", "exempt this", "waive your", "suspend your",
    "temporarily disable", "turn off your", "ignore your safety",
    "bypass your safety", "disable your safety", "remove your restrictions",
    "add capability", "grant yourself", "give yourself permission",
    "enable yourself to", "allow yourself to", "i authorize you to",
    "you are authorized to", "you now have permission", "your new directive",
    "your updated directive", "new baseline", "new directive",
]

_INJECTION_COMPILED   = _compile(_INJECTION_PATTERNS)
_JAILBREAK_COMPILED   = _compile(_JAILBREAK_PATTERNS)
_SKELETON_COMPILED    = _compile(_SKELETON_KEY_PATTERNS)


@dataclass
class CheckResult:
    allow:        bool
    block_reason: Optional[str]
    risk_level:   str
    attack_types: List[str]


def check(prompt: str) -> CheckResult:
    """Pattern-only pre-call check. Returns allow=False when HIGH risk detected."""
    if not prompt or not prompt.strip():
        return CheckResult(allow=True, block_reason=None, risk_level="clean", attack_types=[])

    attack_types: list[str] = []
    detected = False

    for label, compiled in (
        ("prompt_injection", _INJECTION_COMPILED),
        ("jailbreak",        _JAILBREAK_COMPILED),
        ("skeleton_key",     _SKELETON_COMPILED),
    ):
        if any(rx.search(prompt) for _, rx in compiled):
            attack_types.append(label)
            detected = True

    if not detected:
        return CheckResult(allow=True, block_reason=None, risk_level="clean", attack_types=[])

    block_reason = f"Blocked by fluiq.secure: {', '.join(attack_types)}"
    return CheckResult(
        allow        = False,
        block_reason = block_reason,
        risk_level   = "high",
        attack_types = attack_types,
    )
