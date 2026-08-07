"""Public, unauthenticated demo of the fluiq.secure() response gate.

Costs nothing to run. The model responses are **real transcripts** captured once
from claude-haiku-4-5 and committed to ``transcripts.json`` — no LLM call happens
at request time. What *does* run on every request is the real gate:

  * ``routes.secure.scanners.check``   — the same input scanner /secure/check uses
  * ``routes.demo.output_scan``        — the response-side scan

So the verdicts are genuine even though the model output is recorded. Nothing
here is a mock: paste your own text into /demo/scan and the same scanners run on it.

Because there is no per-request cost, the guards are light and fail **open** —
matching the convention in shared/cache.py. The earlier draft of this endpoint
called the model live and had to fail closed; that tradeoff is gone with the cost.
"""
import json
import logging
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

import config
from routes.secure import scanners
from routes.demo.output_scan import scan_output

logger = logging.getLogger(__name__)
router = APIRouter()

_TRANSCRIPTS = Path(__file__).with_name("transcripts.json")

DEMO_MAX_INPUT   = int(os.getenv("DEMO_MAX_INPUT_CHARS", "20000"))
DEMO_PER_IP_HOUR = int(os.getenv("DEMO_PER_IP_HOURLY", "120"))

_redis: Any = None


@lru_cache(maxsize=1)
def _transcripts() -> List[Dict[str, Any]]:
    with _TRANSCRIPTS.open(encoding="utf-8") as fh:
        return json.load(fh)


def _by_key(key: str) -> Optional[Dict[str, Any]]:
    return next((t for t in _transcripts() if t["key"] == key), None)


def _redis_client() -> Any:
    global _redis
    if _redis is None and config.REDIS_URL:
        import redis.asyncio as aioredis
        _redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


def _client_ip(request: Request) -> str:
    """Left-most XFF hop is the caller; the ALB appends, so the tail is infrastructure."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def _throttle(ip: str) -> None:
    """Light per-IP ceiling. Fails open — a scan costs CPU, not money."""
    r = _redis_client()
    if r is None:
        return
    key = f"fluiq:demo:ip:{ip}:{int(time.time() // 3600)}"
    try:
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, 3600)
        count, _ = await pipe.execute()
    except Exception:
        logger.debug("[DEMO] throttle backend unavailable; allowing", exc_info=True)
        return
    if int(count) > DEMO_PER_IP_HOUR:
        raise HTTPException(
            status_code=429,
            detail="That's a lot of scans this hour. Start a free account to run this against your own traces.",
        )


def _gate(prompt: str, response: str, canaries: List[str] | None) -> Dict[str, Any]:
    """Run the real gate over an (input, output) pair and shape it for the UI."""
    t0 = time.perf_counter()
    verdict = scanners.check(prompt)
    input_ms = (time.perf_counter() - t0) * 1000

    t1 = time.perf_counter()
    out = scan_output(response, canaries or [])
    output_ms = (time.perf_counter() - t1) * 1000

    blocked_at = "input" if not verdict.allow else ("output" if out.blocked else None)

    return {
        "blocked":    blocked_at is not None,
        "blocked_at": blocked_at,
        "input_scan": {
            "allowed":      verdict.allow,
            "risk_level":   verdict.risk_level,
            "attack_types": verdict.attack_types,
        },
        "output_scan": {
            "blocked":  out.blocked,
            "kinds":    out.kinds,
            "findings": [
                {"kind": f.kind, "label": f.label, "excerpt": f.excerpt}
                for f in out.findings
            ],
        },
        "reason": (
            verdict.block_reason if blocked_at == "input"
            else f"Blocked by fluiq.secure: response contained {', '.join(out.kinds)}"
            if blocked_at == "output" else None
        ),
        "timing_ms": {
            "input_scan":  round(input_ms, 3),
            "output_scan": round(output_ms, 3),
        },
    }


class ScanRequest(BaseModel):
    """Free-text mode: bring your own agent's prompt and response."""
    prompt:   str = Field(default="", max_length=DEMO_MAX_INPUT)
    response: str = Field(default="", max_length=DEMO_MAX_INPUT)


@router.get("/demo/scenarios")
async def list_scenarios() -> dict:
    """Recorded scenarios with the gate verdict already computed for each."""
    items = []
    for t in _transcripts():
        gate = _gate(t["prompt"], t["response"], t.get("canaries"))
        items.append({
            "key":       t["key"],
            "label":     t["label"],
            "blurb":     t["blurb"],
            "prompt":    t["prompt"],
            "response":  t["response"],
            "captured":  t["captured"],
            # Did the model itself withhold the sensitive value? Derived from the
            # canaries, not from guessing at refusal language.
            "model_withheld": not any(
                c and c in t["response"] for c in (t.get("canaries") or [])
            ),
            "gate": gate,
        })
    return {"scenarios": items, "mode": "block"}


@router.post("/demo/scan")
async def scan(payload: ScanRequest, request: Request) -> dict:
    """Run the real gate over text the visitor supplies. No model call, no cost."""
    prompt   = (payload.prompt or "").strip()
    response = (payload.response or "").strip()
    if not prompt and not response:
        raise HTTPException(status_code=400, detail="Provide a prompt, a response, or both.")

    await _throttle(_client_ip(request))
    return {"mode": "block", "gate": _gate(prompt, response, canaries=[])}
