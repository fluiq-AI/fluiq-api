"""Braintrust → Fluiq pull connector.

Fetches spans from a Braintrust project's logs and maps each to a Fluiq event,
then POSTs them to ``/api/v1/ingest/otel``.

    python -m connectors.braintrust --limit 500 [--dry-run]

Env: BRAINTRUST_API_KEY, BRAINTRUST_PROJECT_ID, FLUIQ_API_ENDPOINT, FLUIQ_API_KEY.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from connectors.common import coerce_messages, coerce_text, find_tool_calls, post_events

_TYPE_TO_ETYPE = {"llm": "llm", "tool": "tool", "function": "tool", "task": "chain", "retriever": "retrieval"}


def span_to_event(span: Dict[str, Any]) -> Dict[str, Any]:
    """Map one Braintrust span → a Fluiq event (best-effort)."""
    attrs = span.get("span_attributes") or {}
    stype = str(attrs.get("type") or "").lower()
    etype = _TYPE_TO_ETYPE.get(stype, "chain")

    span_id = span.get("span_id") or span.get("id")
    parents = span.get("span_parents") or []
    ev: Dict[str, Any] = {
        "integration": "OPENINFERENCE:braintrust",
        "type": etype,
        "trace_id": str(span_id),
        "root_trace_id": str(span.get("root_span_id") or span_id),
        "function": attrs.get("name") or span.get("name"),
        "success": True,
        "_external_source": "braintrust",
    }
    if parents:
        ev["parent_id"] = str(parents[0])

    inp, out = span.get("input"), span.get("output")
    messages = coerce_messages(inp)
    if messages:
        ev["messages"] = messages
    if out is not None:
        ev["response"] = coerce_text(out)

    metadata = span.get("metadata") or {}
    if etype == "llm":
        ev["model"] = metadata.get("model")
        tcs = find_tool_calls(out)
        if tcs:
            ev["tool_calls"] = tcs
        metrics = span.get("metrics") or {}
        if any(k in metrics for k in ("prompt_tokens", "completion_tokens", "tokens")):
            ev["tokens"] = {
                "prompt": metrics.get("prompt_tokens"),
                "completion": metrics.get("completion_tokens"),
                "total": metrics.get("tokens"),
            }

    if etype == "tool":
        ev["tool_calls"] = [{
            "id": str(span_id), "type": "function",
            "function": {"name": ev["function"], "arguments": json.dumps(inp, default=str) if inp is not None else "{}"},
        }]

    return ev


def fetch_spans(limit: int) -> List[Dict[str, Any]]:
    import os
    import requests
    api_key = os.environ["BRAINTRUST_API_KEY"]
    project_id = os.environ["BRAINTRUST_PROJECT_ID"]
    resp = requests.post(
        "https://api.braintrust.dev/v1/project_logs/fetch",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"project_id": project_id, "limit": limit},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("events") or resp.json().get("data") or []


def main() -> None:
    import os
    import sys
    argv = sys.argv[1:]
    limit = int(argv[argv.index("--limit") + 1]) if "--limit" in argv else 500

    spans = fetch_spans(limit)
    events = [span_to_event(s) for s in spans]
    print(f"Braintrust: {len(spans)} spans → {len(events)} events")
    if "--dry-run" in argv:
        print(json.dumps(events[:2], indent=2, default=str))
        return
    result = post_events(
        events, source="braintrust",
        endpoint=os.environ["FLUIQ_API_ENDPOINT"], api_key=os.environ["FLUIQ_API_KEY"],
    )
    print("Ingested:", result)


if __name__ == "__main__":
    main()
