"""Shared helpers for the pull connectors (LangSmith, Braintrust).

A connector's job is thin: fetch runs/spans from a platform's read API, map each
to a Fluiq event (best-effort — get the prompt / response / tool calls in), and
POST them to ``/api/v1/ingest/otel`` as ``{"source": ..., "events": [...]}``.
The endpoint then runs the same persist + eval + security pipeline as native
traces. The mappers here are pure and unit-tested.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def coerce_text(value: Any) -> str:
    """Flatten an arbitrary input/output value into a text string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for k in ("content", "text", "output", "input", "value"):
            if isinstance(value.get(k), str):
                return value[k]
        return json.dumps(value, default=str)[:4000]
    if isinstance(value, list):
        return "\n".join(coerce_text(v) for v in value)[:4000]
    return str(value)


def message_content(msg: Any) -> str:
    """Extract text from a message across LangChain / OpenAI / Braintrust shapes."""
    if isinstance(msg, str):
        return msg
    if not isinstance(msg, dict):
        return str(msg)
    # LangChain serialized: {"kwargs": {"content": ...}}
    kwargs = msg.get("kwargs")
    if isinstance(kwargs, dict) and "content" in kwargs:
        return coerce_text(kwargs["content"])
    return coerce_text(msg.get("content", msg))


def _role_of(msg: Any) -> str:
    if isinstance(msg, dict):
        if msg.get("role"):
            return str(msg["role"])
        # LangChain type in id path, e.g. ["...", "HumanMessage"]
        mid = msg.get("id")
        if isinstance(mid, list) and mid:
            name = str(mid[-1]).lower()
            if "human" in name:
                return "user"
            if "system" in name:
                return "system"
            if "ai" in name or "assistant" in name:
                return "assistant"
    return "user"


def coerce_messages(value: Any) -> List[Dict[str, str]]:
    """Turn a messages-ish value into ``[{role, content}]`` (best-effort)."""
    if value is None:
        return []
    if isinstance(value, str):
        return [{"role": "user", "content": value}]
    if isinstance(value, dict):
        # e.g. {"messages": [...]} or {"input": "..."} or a single message
        if isinstance(value.get("messages"), list):
            return coerce_messages(value["messages"])
        if "content" in value or "kwargs" in value:
            return [{"role": _role_of(value), "content": message_content(value)}]
        for k in ("input", "question", "prompt"):
            if value.get(k):
                return [{"role": "user", "content": coerce_text(value[k])}]
        return []
    if isinstance(value, list):
        out: List[Dict[str, str]] = []
        for m in value:
            if isinstance(m, list):            # LangChain nests message batches
                out.extend(coerce_messages(m))
            else:
                out.append({"role": _role_of(m), "content": message_content(m)})
        return out
    return []


def find_tool_calls(obj: Any) -> Optional[List[Dict[str, Any]]]:
    """Recursively find a ``tool_calls`` list and normalize to OpenAI shape."""
    found = _search_tool_calls(obj)
    if not found:
        return None
    out: List[Dict[str, Any]] = []
    for i, tc in enumerate(found):
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") if isinstance(tc.get("function"), dict) else tc
        name = fn.get("name") or tc.get("name")
        if not name:
            continue
        args = fn.get("arguments")
        if args is None:
            args = tc.get("args") or tc.get("arguments") or {}
        out.append({
            "id": tc.get("id") or f"call_{i}",
            "type": "function",
            "function": {"name": name, "arguments": args if isinstance(args, str) else json.dumps(args, default=str)},
        })
    return out or None


def _search_tool_calls(obj: Any, depth: int = 0) -> Optional[List[Any]]:
    if depth > 6 or obj is None:
        return None
    if isinstance(obj, dict):
        tc = obj.get("tool_calls")
        if isinstance(tc, list) and tc:
            return tc
        for v in obj.values():
            r = _search_tool_calls(v, depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _search_tool_calls(v, depth + 1)
            if r:
                return r
    return None


def post_events(events: List[Dict[str, Any]], *, source: str, endpoint: str, api_key: str,
                eval_config: Optional[Dict] = None, security_config: Optional[Dict] = None) -> Dict[str, Any]:
    """POST mapped events to the Fluiq /ingest/otel endpoint."""
    import requests
    body: Dict[str, Any] = {"api_key": api_key, "source": source, "events": events}
    if eval_config:
        body["eval_config"] = eval_config
    if security_config:
        body["security_config"] = security_config
    resp = requests.post(f"{endpoint.rstrip('/')}/api/v1/ingest/otel", json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()
