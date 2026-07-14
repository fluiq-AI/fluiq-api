"""Pull-connector mappers (LangSmith, Braintrust) — pure, offline.

Run:  python fluiq-api/tests/test_connectors.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.common import coerce_messages, find_tool_calls
from connectors.langsmith import run_to_event
from connectors.braintrust import span_to_event


def test_common_helpers():
    assert coerce_messages("hi") == [{"role": "user", "content": "hi"}]
    assert coerce_messages([{"role": "user", "content": "q"}]) == [{"role": "user", "content": "q"}]
    # LangChain serialized message
    lc = [{"id": ["langchain", "schema", "HumanMessage"], "kwargs": {"content": "hello"}}]
    assert coerce_messages(lc) == [{"role": "user", "content": "hello"}]
    tcs = find_tool_calls({"choices": [{"message": {"tool_calls": [{"id": "c1", "function": {"name": "f", "arguments": "{}"}}]}}]})
    assert tcs[0]["function"]["name"] == "f"
    # LangChain-style tool call ({name, args})
    tcs2 = find_tool_calls({"tool_calls": [{"name": "search", "args": {"q": "x"}, "id": "t1"}]})
    assert tcs2[0]["function"]["name"] == "search" and "q" in tcs2[0]["function"]["arguments"]


def test_langsmith_llm_run():
    run = {
        "id": "r1", "trace_id": "root", "run_type": "llm", "name": "ChatOpenAI",
        "inputs": {"messages": [[{"role": "user", "content": "Weather in Paris?"}]]},
        "outputs": {"generations": [[{"message": {"tool_calls": [{"name": "get_weather", "args": {"location": "Paris"}, "id": "tc1"}]}}]],
                    "llm_output": {"token_usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}},
        "extra": {"invocation_params": {"model": "gpt-4o"}},
        "error": None,
    }
    ev = run_to_event(run)
    assert ev["type"] == "llm" and ev["trace_id"] == "r1" and ev["root_trace_id"] == "root"
    assert ev["model"] == "gpt-4o"
    assert ev["messages"] == [{"role": "user", "content": "Weather in Paris?"}]
    assert ev["tool_calls"][0]["function"]["name"] == "get_weather"
    assert ev["tokens"]["total"] == 15


def test_langsmith_tool_run():
    run = {"id": "r2", "trace_id": "root", "parent_run_id": "r1", "run_type": "tool",
           "name": "get_weather", "inputs": {"location": "Paris"}, "outputs": {"output": "sunny"}}
    ev = run_to_event(run)
    assert ev["type"] == "tool" and ev["parent_id"] == "r1"
    assert ev["tool_calls"][0]["function"]["name"] == "get_weather"
    assert ev["response"] == "sunny"


def test_braintrust_llm_span():
    span = {
        "span_id": "s1", "root_span_id": "root", "span_parents": [],
        "span_attributes": {"type": "llm", "name": "chat"},
        "input": [{"role": "user", "content": "Hi there"}],
        "output": {"role": "assistant", "content": "Hello", "tool_calls": [{"id": "c1", "function": {"name": "greet", "arguments": "{}"}}]},
        "metadata": {"model": "claude-haiku"},
        "metrics": {"prompt_tokens": 3, "completion_tokens": 2, "tokens": 5},
    }
    ev = span_to_event(span)
    assert ev["type"] == "llm" and ev["model"] == "claude-haiku"
    assert ev["messages"] == [{"role": "user", "content": "Hi there"}]
    assert ev["tool_calls"][0]["function"]["name"] == "greet"
    assert ev["tokens"]["total"] == 5


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
