"""Response-side scanning for the public demo.

The input-side scanner already lives in ``routes/secure/scanners.py``. This module
covers the other half of the gate: what the model is about to *say*. It is
deliberately dependency-free (no presidio, no spaCy) so it can run inline in the
API — the evaluator worker keeps the heavyweight version.

Detections: planted canaries, credentials, and PII (email / phone / SSN / card,
card confirmed by Luhn so ordinary 16-digit numbers don't trip it).
"""
import math
import re
from dataclasses import dataclass, field
from typing import List


@dataclass
class Finding:
    kind:    str   # "canary" | "secret" | "pii"
    label:   str   # human-readable detector name
    excerpt: str   # redacted sample of what matched


@dataclass
class OutputScan:
    blocked:  bool
    findings: List[Finding] = field(default_factory=list)

    @property
    def kinds(self) -> List[str]:
        seen: list[str] = []
        for f in self.findings:
            if f.kind not in seen:
                seen.append(f.kind)
        return seen


# ── Detectors ─────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]{2,}\b")
_SSN_RE   = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_PHONE_RE = re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b")
_CARD_RE  = re.compile(r"\b(?:\d[ -]?){13,19}\b")

_SECRET_RES = [
    ("OpenAI-style key",   re.compile(r"\bsk-(?:live|test|proj)?-?[A-Za-z0-9]{16,}\b")),
    ("AWS access key",     re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("GitHub token",       re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}\b")),
    ("Slack token",        re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("Private key block",  re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("Connection string",  re.compile(r"\b(?:postgres|postgresql|mysql|mongodb)(?:\+\w+)?://[^\s\"']+:[^\s\"']+@[^\s\"']+")),
    ("Bearer token",       re.compile(r"\bBearer\s+[A-Za-z0-9\-._~+/]{20,}={0,2}")),
]

_ENTROPY_TOKEN_RE = re.compile(r"[A-Za-z0-9+/=_\-]{24,}")
_ENTROPY_THRESHOLD = 4.2


def _luhn_ok(digits: str) -> bool:
    """Card numbers pass Luhn; order IDs and timestamps generally don't."""
    total, alt = 0, False
    for ch in reversed(digits):
        d = ord(ch) - 48
        if alt:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        alt = not alt
    return total % 10 == 0


def _shannon(s: str) -> float:
    if not s:
        return 0.0
    return -sum(
        (n / len(s)) * math.log2(n / len(s))
        for n in (s.count(c) for c in set(s))
    )


def _redact(value: str, keep: int = 4) -> str:
    """Never echo a full secret back to the caller — this response is public."""
    value = value.strip()
    if len(value) <= keep:
        return "•" * len(value)
    return value[:keep] + "•" * min(len(value) - keep, 12)


def scan_output(text: str, canaries: List[str] | None = None) -> OutputScan:
    """Scan a model response for anything that must not reach a user."""
    findings: List[Finding] = []
    if not text or not text.strip():
        return OutputScan(blocked=False)

    # 1. Canaries — planted secrets. Presence is proof, not inference.
    for canary in canaries or []:
        if canary and canary in text:
            findings.append(Finding("canary", "Planted secret disclosed", _redact(canary)))

    # 2. Known credential shapes. Matches are carved out of the text before the
    #    PII pass so a `user:pass@host` connection string isn't also reported as
    #    an email address.
    residual = text
    for label, rx in _SECRET_RES:
        m = rx.search(residual)
        if m:
            findings.append(Finding("secret", label, _redact(m.group(0))))
            residual = residual.replace(m.group(0), " ")

    # 3. High-entropy tokens that aren't ordinary prose. Skipped when a named
    #    detector already fired — "High-entropy token" adds nothing next to
    #    "AWS access key", and a demo that lists one leak three times reads as noise.
    if not any(f.kind == "secret" for f in findings):
        for tok in _ENTROPY_TOKEN_RE.findall(residual):
            if _shannon(tok) >= _ENTROPY_THRESHOLD:
                findings.append(Finding("secret", "High-entropy token", _redact(tok)))
                break

    # 4. PII.
    text = residual
    if m := _EMAIL_RE.search(text):
        findings.append(Finding("pii", "Email address", _redact(m.group(0), keep=3)))
    if m := _SSN_RE.search(text):
        findings.append(Finding("pii", "US Social Security number", _redact(m.group(0), keep=3)))
    if m := _PHONE_RE.search(text):
        findings.append(Finding("pii", "Phone number", _redact(m.group(0), keep=3)))
    for raw in _CARD_RE.findall(text):
        digits = re.sub(r"[ -]", "", raw)
        if 13 <= len(digits) <= 19 and _luhn_ok(digits):
            findings.append(Finding("pii", "Payment card number", _redact(digits, keep=4)))
            break

    # De-duplicate on (kind, label) so one leak doesn't render as five rows.
    unique: List[Finding] = []
    seen: set[tuple[str, str]] = set()
    for f in findings:
        if (f.kind, f.label) not in seen:
            seen.add((f.kind, f.label))
            unique.append(f)

    return OutputScan(blocked=bool(unique), findings=unique)
