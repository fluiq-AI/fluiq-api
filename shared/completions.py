"""Provider completion transports, shared by every path that calls an LLM.

Plain completion calls over httpx (already a dependency) rather than four
provider SDKs. Each transport returns ``(output, input_tokens, output_tokens,
cached_tokens)``.

BYOK only: the caller resolves the key from the org's stored credentials, so
there is no managed key to leak or forget to rotate.

Extracted from ``routes/evaluate`` so the dataset *task runner* (which generates
the output being graded) and the *judge* (which grades it) can't drift on
provider handling, token accounting, or cost estimation. ``routes/evaluate``
re-imports these under its original private names, so its call sites are
unchanged.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx

from db_queues.postgresql import postgres_client as pg_client

MOONSHOT_BASE_URL = "https://api.moonshot.ai/v1"
MILLION = Decimal(1_000_000)

# BYOK provider (what a stored key is filed under) -> the name model_prices uses.
# None means no price sheet, so cost is simply omitted (best-effort).
# From the single provider registry, so a provider added there is priced here
# without a second edit.
from shared.providers import PRICE_PROVIDER  # noqa: E402,F401


# ── Transports ────────────────────────────────────────────────────────────────

async def complete_anthropic(
    key: str, model: str, prompt: str, *, system: str = "", max_tokens: int = 2048,
) -> tuple[str, int, int, int]:
    payload: Dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        payload["system"] = system
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
        )
    r.raise_for_status()
    body = r.json()
    text = "".join(b.get("text", "") for b in body.get("content", []) if b.get("type") == "text")
    usage = body.get("usage") or {}
    cached = (usage.get("cache_read_input_tokens") or 0) + (usage.get("cache_creation_input_tokens") or 0)
    return text, int(usage.get("input_tokens") or 0), int(usage.get("output_tokens") or 0), int(cached)


async def complete_openai(
    key: str, model: str, prompt: str, base_url: str = "https://api.openai.com/v1",
    *, system: str = "", max_tokens: int = 2048,
) -> tuple[str, int, int, int]:
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}
    ]
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {key}", "content-type": "application/json"},
            json={"model": model, "max_tokens": max_tokens, "messages": messages},
        )
    r.raise_for_status()
    body = r.json()
    choices = body.get("choices") or [{}]
    text = (choices[0].get("message") or {}).get("content") or ""
    usage = body.get("usage") or {}
    cached = ((usage.get("prompt_tokens_details") or {}).get("cached_tokens")) or 0
    return text, int(usage.get("prompt_tokens") or 0), int(usage.get("completion_tokens") or 0), int(cached)


async def complete_gemini(
    key: str, model: str, prompt: str, *, system: str = "", max_tokens: int = 2048,
) -> tuple[str, int, int, int]:
    payload: Dict[str, Any] = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens},
    }
    if system:
        payload["systemInstruction"] = {"parts": [{"text": system}]}
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            headers={"x-goog-api-key": key, "content-type": "application/json"},
            json=payload,
        )
    r.raise_for_status()
    body = r.json()
    parts = (((body.get("candidates") or [{}])[0].get("content") or {}).get("parts")) or []
    text = "".join(p.get("text", "") for p in parts)
    usage = body.get("usageMetadata") or {}
    cached = usage.get("cachedContentTokenCount") or 0
    return text, int(usage.get("promptTokenCount") or 0), int(usage.get("candidatesTokenCount") or 0), int(cached)


# Providers that speak the OpenAI chat-completions wire format, and the host to
# reach each on. Everything here shares ``complete_openai`` — the format is the
# de-facto standard, so adding one is a line rather than a transport.
OPENAI_COMPATIBLE_BASES: Dict[str, str] = {
    "openai":     "https://api.openai.com/v1",
    "moonshot":   "https://api.moonshot.ai/v1",
    "groq":       "https://api.groq.com/openai/v1",
    "together":   "https://api.together.xyz/v1",
    "fireworks":  "https://api.fireworks.ai/inference/v1",
    "mistral":    "https://api.mistral.ai/v1",
    "xai":        "https://api.x.ai/v1",
    "perplexity": "https://api.perplexity.ai",
    "cerebras":   "https://api.cerebras.ai/v1",
    "deepseek":   "https://api.deepseek.com",
    # Z.AI's OpenAI-compatible root is /api/paas/v4, not the /v1 nearly every
    # other host uses. A /v1 here 404s in a way that reads like a dead provider.
    "zai":        "https://api.z.ai/api/paas/v4",
    # Gateways. Same wire format, someone else's models behind it.
    "openrouter": "https://openrouter.ai/api/v1",
    "vercel":     "https://ai-gateway.vercel.sh/v1",
    "baseten":    "https://inference.baseten.co/v1",
    "deepinfra":  "https://api.deepinfra.com/v1/openai",
    "sambanova":  "https://api.sambanova.ai/v1",
    "nebius":     "https://api.studio.nebius.com/v1",
    "novita":     "https://api.novita.ai/v3/openai",
    "hyperbolic": "https://api.hyperbolic.xyz/v1",
}

#: Clouds that are routable but have no fixed base. They reach the provider
#: through polygate, which owns their signing and URL shapes; this API's own
#: task-execution transport cannot serve them without an endpoint the caller
#: supplies, so they are listed rather than silently absent.
ENDPOINT_PROVIDERS = frozenset({
    "bedrock", "azure_openai", "vertex", "databricks", "cloudflare",
})


async def complete_for(
    provider: str, key: str, model: str, prompt: str,
    *, system: str = "", max_tokens: int = 2048,
) -> tuple[str, int, int, int]:
    """Dispatch a completion to the right provider transport.

    Anthropic and Gemini have their own request shapes; everything else speaks
    OpenAI's, so they share one transport and differ only by host.
    """
    if provider == "anthropic":
        return await complete_anthropic(key, model, prompt, system=system, max_tokens=max_tokens)
    if provider == "gemini":
        return await complete_gemini(key, model, prompt, system=system, max_tokens=max_tokens)
    base = OPENAI_COMPATIBLE_BASES.get(provider)
    if base:
        return await complete_openai(
            key, model, prompt, base_url=base, system=system, max_tokens=max_tokens
        )
    raise RuntimeError(f"unsupported provider {provider!r}")


# Model→provider routing lives in the registry beside the prefixes it matches on.
from shared.providers import provider_for_model  # noqa: E402,F401


# ── Conversations and tools ───────────────────────────────────────────────────

@dataclass
class Completion:
    """One model turn, including any tool it asked to call.

    ``tool_calls`` is the reason this is a dataclass rather than the tuple the
    single-prompt path returns: a task that offers tools produces an answer, a
    tool request, or both, and collapsing that into a string would throw away
    the thing tool evaluation is about.
    """
    text: str = ""
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    cached_tokens: int = 0


async def complete_messages(
    provider: str,
    key: str,
    model: str,
    messages: List[Dict[str, Any]],
    *,
    tools: Optional[List[Dict[str, Any]]] = None,
    max_tokens: int = 2048,
) -> Completion:
    """A full conversation turn, with optional tool definitions.

    Where :func:`complete_for` sends one prompt, this sends a whole message list
    — which is what a multi-turn task is — and lets the model answer with a tool
    call instead of text.

    Anthropic and Gemini have their own shapes; the rest speak OpenAI's, so they
    share one path and differ only by host.
    """
    if provider == "anthropic":
        return await _messages_anthropic(key, model, messages, tools, max_tokens)
    if provider == "gemini":
        return await _messages_gemini(key, model, messages, tools, max_tokens)
    base = OPENAI_COMPATIBLE_BASES.get(provider)
    if not base:
        raise RuntimeError(f"unsupported provider {provider!r}")
    return await _messages_openai(base, key, model, messages, tools, max_tokens)


async def _messages_openai(
    base_url: str, key: str, model: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]],
    max_tokens: int,
) -> Completion:
    payload: Dict[str, Any] = {
        "model": model, "max_tokens": max_tokens, "messages": messages,
    }
    if tools:
        payload["tools"] = [
            {"type": "function", "function": t} for t in tools
        ]
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {key}", "content-type": "application/json"},
            json=payload,
        )
    r.raise_for_status()
    body = r.json()
    message = ((body.get("choices") or [{}])[0].get("message")) or {}
    usage = body.get("usage") or {}
    return Completion(
        text=message.get("content") or "",
        tool_calls=[
            {
                "name": (call.get("function") or {}).get("name") or "",
                "arguments": (call.get("function") or {}).get("arguments") or "",
                "id": call.get("id") or "",
            }
            for call in (message.get("tool_calls") or [])
        ],
        input_tokens=int(usage.get("prompt_tokens") or 0),
        output_tokens=int(usage.get("completion_tokens") or 0),
        cached_tokens=int((usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0),
    )


async def _messages_anthropic(
    key: str, model: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]],
    max_tokens: int,
) -> Completion:
    # Anthropic takes the system prompt as a top-level field, not a message.
    system = " ".join(
        str(m.get("content") or "") for m in messages if m.get("role") == "system"
    ).strip()
    turns = [m for m in messages if m.get("role") != "system"]
    payload: Dict[str, Any] = {
        "model": model, "max_tokens": max_tokens, "messages": turns,
    }
    if system:
        payload["system"] = system
    if tools:
        payload["tools"] = [
            {
                "name": t.get("name"),
                "description": t.get("description", ""),
                "input_schema": t.get("parameters") or {"type": "object", "properties": {}},
            }
            for t in tools
        ]
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
        )
    r.raise_for_status()
    body = r.json()
    blocks = body.get("content") or []
    usage = body.get("usage") or {}
    return Completion(
        text="".join(b.get("text", "") for b in blocks if b.get("type") == "text"),
        tool_calls=[
            {
                "name": b.get("name") or "",
                "arguments": json.dumps(b.get("input") or {}),
                "id": b.get("id") or "",
            }
            for b in blocks if b.get("type") == "tool_use"
        ],
        input_tokens=int(usage.get("input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
        cached_tokens=int(
            (usage.get("cache_read_input_tokens") or 0)
            + (usage.get("cache_creation_input_tokens") or 0)
        ),
    )


async def _messages_gemini(
    key: str, model: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]],
    max_tokens: int,
) -> Completion:
    system = " ".join(
        str(m.get("content") or "") for m in messages if m.get("role") == "system"
    ).strip()
    contents = [
        {
            # Gemini calls the assistant "model".
            "role": "model" if m.get("role") == "assistant" else "user",
            "parts": [{"text": str(m.get("content") or "")}],
        }
        for m in messages if m.get("role") != "system"
    ]
    payload: Dict[str, Any] = {
        "contents": contents,
        "generationConfig": {"maxOutputTokens": max_tokens},
    }
    if system:
        payload["systemInstruction"] = {"parts": [{"text": system}]}
    if tools:
        payload["tools"] = [{
            "functionDeclarations": [
                {
                    "name": t.get("name"),
                    "description": t.get("description", ""),
                    "parameters": t.get("parameters") or {"type": "object", "properties": {}},
                }
                for t in tools
            ]
        }]
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            headers={"x-goog-api-key": key, "content-type": "application/json"},
            json=payload,
        )
    r.raise_for_status()
    body = r.json()
    parts = (((body.get("candidates") or [{}])[0].get("content") or {}).get("parts")) or []
    usage = body.get("usageMetadata") or {}
    return Completion(
        text="".join(p.get("text", "") for p in parts if "text" in p),
        tool_calls=[
            {
                "name": (p.get("functionCall") or {}).get("name") or "",
                "arguments": json.dumps((p.get("functionCall") or {}).get("args") or {}),
                "id": "",
            }
            for p in parts if "functionCall" in p
        ],
        input_tokens=int(usage.get("promptTokenCount") or 0),
        output_tokens=int(usage.get("candidatesTokenCount") or 0),
        cached_tokens=int(usage.get("cachedContentTokenCount") or 0),
    )


# ── Cost ──────────────────────────────────────────────────────────────────────

def _d(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal(0)
    return value if isinstance(value, Decimal) else Decimal(str(value))


async def estimate_cost(
    provider: str, model: str, in_tok: int, out_tok: int, cached_tok: int = 0
) -> Optional[float]:
    """Best-effort USD cost for one call. None when the model has no price sheet."""
    price_provider = PRICE_PROVIDER.get(provider)
    if price_provider is None:
        return None
    price = await pg_client.fetch_price(price_provider, model, "Text")
    if price is None:
        return None

    billable = max(in_tok - cached_tok, 0)
    threshold = price.get("long_context_consider_token_greater_than")
    long_ctx = bool(threshold) and in_tok > int(threshold)

    if long_ctx:
        in_rate     = _d(price.get("long_context_input_per_million"))
        cached_rate = _d(price.get("long_context_cached_input_per_million"))
        out_rate    = _d(price.get("long_context_output_per_million"))
    else:
        in_rate     = _d(price.get("input_token_cost_per_million"))
        cached_rate = _d(price.get("cached_input_token_cost_per_million"))
        out_rate    = _d(price.get("output_token_cost_per_million"))

    total = (
        Decimal(billable) * in_rate
        + Decimal(cached_tok) * cached_rate
        + Decimal(out_tok) * out_rate
    ) / MILLION
    return float(round(total, 8))


__all__ = [
    "Completion",
    "complete_messages",
    "OPENAI_COMPATIBLE_BASES",
    "MOONSHOT_BASE_URL",
    "MILLION",
    "PRICE_PROVIDER",
    "complete_anthropic",
    "complete_openai",
    "complete_gemini",
    "complete_for",
    "provider_for_model",
    "estimate_cost",
]
