"""LangSmith → Fluiq pull connector.

Fetches recent runs from LangSmith's read API and maps each to a Fluiq event,
then POSTs them to ``/api/v1/ingest/otel``. LangSmith is LangChain-native (not
OTel-first), so we map its ``run`` shape directly.

    python -m connectors.langsmith --hours 24 --limit 500 [--dry-run]

Env: LANGSMITH_API_KEY, LANGSMITH_PROJECT, FLUIQ_API_ENDPOINT, FLUIQ_API_KEY.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from connectors.common import coerce_messages, coerce_text, find_tool_calls, post_events

_RUN_TYPE_TO_ETYPE = {"llm": "llm", "chat": "llm", "tool": "tool", "retriever": "retrieval", "chain": "chain"}


def run_to_event(run: Dict[str, Any]) -> Dict[str, Any]:
    """Map one LangSmith run → a Fluiq event (best-effort)."""
    rtype = str(run.get("run_type") or "chain").lower()
    etype = _RUN_TYPE_TO_ETYPE.get(rtype, "chain")

    ev: Dict[str, Any] = {
        "integration": "OPENINFERENCE:langsmith",
        "type": etype,
        "trace_id": str(run.get("id")),
        "root_trace_id": str(run.get("trace_id") or run.get("id")),
        "function": run.get("name"),
        "success": run.get("error") in (None, "", False),
        "_external_source": "langsmith",
    }
    if run.get("parent_run_id"):
        ev["parent_id"] = str(run["parent_run_id"])

    inputs = run.get("inputs") or {}
    outputs = run.get("outputs") or {}

    messages = coerce_messages(inputs)
    if messages:
        ev["messages"] = messages

    response = coerce_text(outputs)
    if response:
        ev["response"] = response

    if etype == "llm":
        extra = run.get("extra") or {}
        inv = extra.get("invocation_params") or {}
        meta = extra.get("metadata") or {}
        ev["model"] = inv.get("model") or inv.get("model_name") or meta.get("ls_model_name")
        tcs = find_tool_calls(outputs)
        if tcs:
            ev["tool_calls"] = tcs
        tokens = (outputs.get("llm_output") or {}).get("token_usage") if isinstance(outputs, dict) else None
        if isinstance(tokens, dict):
            ev["tokens"] = {
                "prompt": tokens.get("prompt_tokens"),
                "completion": tokens.get("completion_tokens"),
                "total": tokens.get("total_tokens"),
            }

    if etype == "tool":
        ev["tool_calls"] = [{
            "id": str(run.get("id")), "type": "function",
            "function": {"name": run.get("name"), "arguments": json.dumps(inputs, default=str)},
        }]

    return ev


def fetch_runs(hours: int, limit: int) -> List[Dict[str, Any]]:
    import os
    from langsmith import Client
    client = Client(api_key=os.environ["LANGSMITH_API_KEY"])
    from datetime import datetime, timedelta, timezone
    start = datetime.now(timezone.utc) - timedelta(hours=hours)
    runs = client.list_runs(
        project_name=os.getenv("LANGSMITH_PROJECT"),
        start_time=start, limit=limit,
    )
    return [r.dict() if hasattr(r, "dict") else dict(r) for r in runs]


def main() -> None:
    import os
    import sys
    argv = sys.argv[1:]

    def _arg(flag, default):
        return argv[argv.index(flag) + 1] if flag in argv else default

    runs = fetch_runs(int(_arg("--hours", "24")), int(_arg("--limit", "500")))
    events = [run_to_event(r) for r in runs]
    print(f"LangSmith: {len(runs)} runs → {len(events)} events")
    if "--dry-run" in argv:
        print(json.dumps(events[:2], indent=2, default=str))
        return
    result = post_events(
        events, source="langsmith",
        endpoint=os.environ["FLUIQ_API_ENDPOINT"], api_key=os.environ["FLUIQ_API_KEY"],
    )
    print("Ingested:", result)


if __name__ == "__main__":
    main()
