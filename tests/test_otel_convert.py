"""OpenInference / OTLP → Fluiq event conversion (pure).

Run:  python fluiq-api/tests/test_otel_convert.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "routes", "otel"))

from convert import to_events, _attrs_to_dict, _otlp_value


def test_otlp_value_unwrap():
    assert _otlp_value({"stringValue": "x"}) == "x"
    assert _otlp_value({"intValue": "42"}) == 42
    assert _otlp_value({"boolValue": True}) is True
    assert _otlp_value({"arrayValue": {"values": [{"stringValue": "a"}, {"stringValue": "b"}]}}) == ["a", "b"]


def test_otlp_json_llm_and_tool():
    otlp = {"resourceSpans": [{"scopeSpans": [{"spans": [
        {"spanId": "aaa", "traceId": "root", "name": "agent", "attributes": [
            {"key": "openinference.span.kind", "value": {"stringValue": "LLM"}},
            {"key": "llm.model_name", "value": {"stringValue": "gpt-4o"}},
            {"key": "input.value", "value": {"stringValue": "Weather in Paris?"}},
            {"key": "output.value", "value": {"stringValue": "checking"}},
            {"key": "llm.token_count.total", "value": {"intValue": "20"}},
            {"key": "llm.output_messages.0.message.tool_calls.0.tool_call.function.name",
             "value": {"stringValue": "get_weather"}},
            {"key": "llm.output_messages.0.message.tool_calls.0.tool_call.function.arguments",
             "value": {"stringValue": "{\"location\":\"Paris\"}"}},
        ]},
        {"spanId": "bbb", "traceId": "root", "parentSpanId": "aaa", "name": "get_weather", "attributes": [
            {"key": "openinference.span.kind", "value": {"stringValue": "TOOL"}},
            {"key": "tool.name", "value": {"stringValue": "get_weather"}},
            {"key": "tool.parameters", "value": {"stringValue": "{\"location\":\"Paris\"}"}},
        ]},
    ]}]}]}
    events, root = to_events(otlp, source="phoenix")
    assert root == "root" and len(events) == 2

    llm = events[0]
    assert llm["type"] == "llm" and llm["model"] == "gpt-4o"
    assert llm["messages"] == [{"role": "user", "content": "Weather in Paris?"}]
    assert llm["tool_calls"][0]["function"]["name"] == "get_weather"
    assert llm["tokens"]["total"] == 20
    assert llm["_external_source"] == "phoenix"

    tool = events[1]
    assert tool["type"] == "tool" and tool["parent_id"] == "aaa"
    assert tool["tool_calls"][0]["function"]["name"] == "get_weather"


def test_flat_spans_form():
    flat = {"spans": [
        {"span_id": "s1", "span_kind": "LLM",
         "attributes": {"input.value": "hi", "llm.model_name": "claude-haiku",
                        "llm.tools": [{"type": "function", "function": {"name": "t1"}}]}},
    ]}
    events, root = to_events(flat, source="langfuse")
    assert len(events) == 1
    assert events[0]["type"] == "llm" and events[0]["tools"][0]["function"]["name"] == "t1"


def test_multimodal_message_media_ref():
    # OpenInference multimodal message: text + image content parts
    flat = {"spans": [
        {"span_id": "s1", "span_kind": "LLM", "attributes": {
            "llm.input_messages.0.message.role": "user",
            "llm.input_messages.0.message.contents.0.message_content.type": "text",
            "llm.input_messages.0.message.contents.0.message_content.text": "What is in this image?",
            "llm.input_messages.0.message.contents.1.message_content.type": "image",
            "llm.input_messages.0.message.contents.1.message_content.image.image.url": "https://x/cat.png",
        }},
    ]}
    events, _ = to_events(flat, source="phoenix")
    content = events[0]["messages"][0]["content"]
    assert isinstance(content, list) and len(content) == 2
    assert content[0] == {"type": "text", "text": "What is in this image?"}
    ref = content[1]["_media_ref"]
    assert ref["kind"] == "image" and ref["source"] == "url" and ref["url"] == "https://x/cat.png"
    assert len(ref["sha256"]) == 16


def test_premapped_events_passthrough():
    payload = {"events": [{"trace_id": "x", "type": "llm", "response": "ok"}]}
    events, root = to_events(payload, source="langsmith")
    assert events[0]["trace_id"] == "x" and events[0]["_external_source"] == "langsmith"
    assert events[0]["root_trace_id"] == "x"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
