"""Convert OpenInference / OTLP spans into Fluiq trace events.

All four external observability platforms (Arize Phoenix, Langfuse, LangSmith,
Braintrust) are OpenTelemetry-based and most emit **OpenInference** semantic
conventions. This module turns those spans into the Fluiq event shape the tracer
/ evaluator / security worker already understand — so once a span is converted,
everything downstream (ClickHouse persistence, per-call eval, agentic eval,
security scan) works exactly as it does for native SDK traces.

Accepts three input forms (see :func:`to_events`):
  * OTLP-JSON       — ``{"resourceSpans": [{"scopeSpans": [{"spans": [...]}]}]}``
  * flat spans      — ``{"spans": [{"span_id", "span_kind", "attributes": {...}}]}``
  * pre-mapped      — ``{"events": [ <fluiq event dicts> ]}`` (used by pull connectors)

Pure functions, no I/O — unit-tested in ``tests``.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple

# OpenInference span kind → Fluiq event type.
_KIND_TO_TYPE = {
    "LLM": "llm", "CHAT": "llm", "TOOL": "tool", "RETRIEVER": "retrieval",
    "EMBEDDING": "llm", "RERANKER": "tool", "AGENT": "chain", "CHAIN": "chain",
}


# ── OTLP value flattening ─────────────────────────────────────────────────────

def _otlp_value(v: Any) -> Any:
    """Unwrap an OTLP AnyValue ({"stringValue": ...}) to a plain Python value."""
    if not isinstance(v, dict):
        return v
    if "stringValue" in v:
        return v["stringValue"]
    if "intValue" in v:
        try:
            return int(v["intValue"])
        except (TypeError, ValueError):
            return v["intValue"]
    if "doubleValue" in v:
        return v["doubleValue"]
    if "boolValue" in v:
        return bool(v["boolValue"])
    if "arrayValue" in v:
        return [_otlp_value(x) for x in (v["arrayValue"] or {}).get("values", [])]
    if "kvlistValue" in v:
        return {kv.get("key"): _otlp_value(kv.get("value")) for kv in (v["kvlistValue"] or {}).get("values", [])}
    return v


def _attrs_to_dict(attributes: Any) -> Dict[str, Any]:
    """Normalize span attributes (OTLP key/value list OR a plain dict) to a dict."""
    if isinstance(attributes, dict):
        return attributes
    out: Dict[str, Any] = {}
    for kv in attributes or []:
        if isinstance(kv, dict) and "key" in kv:
            out[kv["key"]] = _otlp_value(kv.get("value"))
    return out


def _iter_spans(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Yield spans from OTLP-JSON or a flat spans list."""
    if isinstance(payload.get("resourceSpans"), list):
        spans: List[Dict[str, Any]] = []
        for rs in payload["resourceSpans"]:
            for ss in (rs.get("scopeSpans") or rs.get("instrumentationLibrarySpans") or []):
                spans.extend(ss.get("spans") or [])
        return spans
    return payload.get("spans") or []


# ── indexed-attribute reconstruction (llm.input_messages.0.message.role …) ────

def _maybe_json(value: Any) -> Any:
    if isinstance(value, str):
        s = value.strip()
        if s and s[0] in "[{":
            try:
                return json.loads(s)
            except (json.JSONDecodeError, ValueError):
                return value
    return value


_CONTENTS_RE = re.compile(r"^message\.contents\.(\d+)\.message_content\.(.+)$")


def _media_ref_from_url(url: str, mime: Optional[str] = None) -> Dict[str, Any]:
    """Payload-free media reference for an OpenInference image URL, matching the
    Fluiq SDK's ``_media_ref`` shape so the evaluator/security handle it uniformly."""
    ref: Dict[str, Any] = {"kind": "image", "source": "url", "url": url,
                           "sha256": hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]}
    if mime:
        ref["mime"] = mime
    return {"type": "image_url", "_media_ref": ref}


def _collect_messages(attrs: Dict[str, Any], base: str) -> List[Dict[str, Any]]:
    """Rebuild ``{base}.{i}.message.role|content`` into ``[{role, content}]``.

    Multimodal messages use ``message.contents.{j}.message_content.{type|text|
    image.image.url}`` — those are reconstructed into a content-parts list with
    text parts and payload-free image ``_media_ref``s."""
    by_index: Dict[int, Dict[str, Any]] = {}
    # per message index → {content part index → part dict}
    contents: Dict[int, Dict[int, Dict[str, Any]]] = {}
    prefix = base + "."
    for key, val in attrs.items():
        if not key.startswith(prefix):
            continue
        rest = key[len(prefix):]
        idx_str, _, tail = rest.partition(".")
        if not idx_str.isdigit():
            continue
        i = int(idx_str)
        entry = by_index.setdefault(i, {})
        if tail == "message.role":
            entry["role"] = val
        elif tail == "message.content":
            entry["content"] = val
        else:
            m = _CONTENTS_RE.match(tail)
            if m:
                j = int(m.group(1))
                field = m.group(2)
                part = contents.setdefault(i, {}).setdefault(j, {})
                if field == "type":
                    part["type"] = val
                elif field == "text":
                    part["text"] = val
                elif field in ("image.image.url", "image.url"):
                    part["image_url"] = val

    out: List[Dict[str, Any]] = []
    for i in sorted(by_index):
        entry = by_index[i]
        parts = contents.get(i)
        if parts:
            built: List[Dict[str, Any]] = []
            for j in sorted(parts):
                p = parts[j]
                if p.get("image_url"):
                    built.append(_media_ref_from_url(str(p["image_url"])))
                elif p.get("text") is not None:
                    built.append({"type": "text", "text": p["text"]})
            if built:
                entry = {**entry, "content": built}
        out.append(entry)
    return out


def _collect_tool_calls(attrs: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Rebuild output-message tool calls into OpenAI-shaped tool_calls."""
    calls: Dict[str, Dict[str, Any]] = {}
    for key, val in attrs.items():
        if ".tool_call.function." not in key or "tool_calls." not in key:
            continue
        head, _, fn_field = key.partition(".tool_call.function.")
        cid = head  # unique per tool-call position
        entry = calls.setdefault(cid, {"id": cid, "type": "function", "function": {}})
        if fn_field == "name":
            entry["function"]["name"] = val
        elif fn_field in ("arguments", "arguments.json"):
            entry["function"]["arguments"] = val if isinstance(val, str) else json.dumps(val, default=str)
    ordered = [calls[k] for k in sorted(calls)]
    return [c for c in ordered if c["function"].get("name")] or None


def _collect_tools(attrs: Dict[str, Any]) -> Optional[List[Any]]:
    """Available tool schemas from ``llm.tools`` (list) or indexed form."""
    direct = attrs.get("llm.tools")
    if isinstance(direct, list) and direct:
        return [_maybe_json(t) for t in direct]
    tools: Dict[int, Any] = {}
    for key, val in attrs.items():
        if key.startswith("llm.tools.") and key.endswith(".tool.json_schema"):
            idx = key.split(".")[2]
            if idx.isdigit():
                tools[int(idx)] = _maybe_json(val)
    return [tools[i] for i in sorted(tools)] or None


# ── span → event ──────────────────────────────────────────────────────────────

def _span_to_event(span: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
    attrs = _attrs_to_dict(span.get("attributes"))
    span_id = span.get("span_id") or span.get("spanId") or span.get("context", {}).get("span_id")
    if not span_id:
        return None
    parent_id = span.get("parent_id") or span.get("parentSpanId") or span.get("parent_span_id")
    trace_id = span.get("trace_id") or span.get("traceId") or span.get("context", {}).get("trace_id")

    kind = str(attrs.get("openinference.span.kind") or span.get("span_kind") or "").upper()
    etype = _KIND_TO_TYPE.get(kind, "chain")

    event: Dict[str, Any] = {
        "integration": f"OPENINFERENCE:{source}" if source else "OPENINFERENCE",
        "type": etype,
        "trace_id": str(span_id),
        "root_trace_id": str(trace_id) if trace_id else str(span_id),
        "function": span.get("name"),
        "success": True,
        "_external_source": source or "openinference",
    }
    if parent_id:
        event["parent_id"] = str(parent_id)

    # Input → messages (structured preferred, else input.value)
    messages = _collect_messages(attrs, "llm.input_messages")
    if messages:
        event["messages"] = messages
    elif attrs.get("input.value") is not None:
        parsed = _maybe_json(attrs["input.value"])
        if isinstance(parsed, list):
            event["messages"] = parsed
        else:
            event["input"] = parsed
            event["messages"] = [{"role": "user", "content": str(parsed)}]

    # Output → response
    out = attrs.get("output.value")
    if out is None:
        out_msgs = _collect_messages(attrs, "llm.output_messages")
        out = out_msgs[-1].get("content") if out_msgs else None
    if out is not None:
        event["response"] = out if isinstance(out, str) else json.dumps(out, default=str)

    if etype == "llm":
        event["model"] = attrs.get("llm.model_name") or attrs.get("model")
        tools = _collect_tools(attrs)
        if tools:
            event["tools"] = tools
        tcs = _collect_tool_calls(attrs)
        if tcs:
            event["tool_calls"] = tcs
        pt, ct, tt = (attrs.get("llm.token_count.prompt"), attrs.get("llm.token_count.completion"),
                      attrs.get("llm.token_count.total"))
        if any(x is not None for x in (pt, ct, tt)):
            event["tokens"] = {"prompt": pt, "completion": ct, "total": tt}

    if etype == "tool":
        name = attrs.get("tool.name") or attrs.get("tool_call.function.name") or span.get("name")
        raw_args = attrs.get("tool.parameters") or attrs.get("tool_call.function.arguments") or attrs.get("input.value")
        args = _maybe_json(raw_args)
        if name:
            event["tool_calls"] = [{
                "id": str(span_id), "type": "function",
                "function": {"name": name,
                             "arguments": args if isinstance(args, str) else json.dumps(args or {}, default=str)},
            }]
        event["function"] = name or event.get("function")

    return event


def to_events(payload: Dict[str, Any], source: str = "openinference") -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Convert an ingest payload to Fluiq events. Returns (events, root_trace_id).

    ``events`` in the payload are treated as already-mapped Fluiq events
    (used by the pull connectors) and passed through with only a source tag.
    """
    if isinstance(payload.get("events"), list):
        events = []
        for e in payload["events"]:
            if isinstance(e, dict) and e.get("trace_id"):
                e.setdefault("_external_source", source)
                e.setdefault("root_trace_id", e.get("root_trace_id") or e["trace_id"])
                events.append(e)
        root = events[0].get("root_trace_id") if events else None
        return events, root

    events = []
    for span in _iter_spans(payload):
        ev = _span_to_event(span, source)
        if ev:
            events.append(ev)
    root = events[0].get("root_trace_id") if events else None
    return events, root
