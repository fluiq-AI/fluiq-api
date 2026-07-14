"""Row-parsing helpers for ClickHouse result sets.

Pure functions — no I/O, no config dependencies.
"""
import json
from typing import Any, Optional


def parse_event(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}
    return raw if isinstance(raw, dict) else {}


def parse_cost(total_cost: Any) -> Optional[float]:
    if total_cost is None:
        return None
    try:
        return float(total_cost)
    except (TypeError, ValueError):
        return None


def merge_security(event: dict[str, Any], row: tuple) -> None:
    """Merge security scan columns from a result row into the event dict in-place."""
    (
        sec_risk_level, sec_risk_score, sec_should_block,
        sec_injection, sec_injection_patterns,
        sec_jailbreak, sec_jailbreak_patterns,
        sec_skeleton, sec_skeleton_patterns,
        sec_secrets, sec_secret_types,
        sec_indirect, sec_indirect_sources,
        sec_rag, sec_rag_sources, sec_rag_score,
        sec_exfil, sec_exfil_types, sec_exfil_sources,
        sec_tool_policy, sec_tool_policy_violations,
        sec_cross_agent,
        sec_image_injection, sec_image_injection_sources,
        sec_semantic_score,
        sec_pii_prompt, sec_pii_response,
        sec_prompt_redacted, sec_response_redacted, sec_scan_latency,
    ) = row
    if not sec_risk_level:
        return
    event.update({
        "security_risk_level":         sec_risk_level,
        "security_risk_score":         float(sec_risk_score) if sec_risk_score is not None else None,
        "should_block":                bool(sec_should_block),
        "injection_detected":          bool(sec_injection),
        "injection_patterns":          list(sec_injection_patterns or []),
        "jailbreak_detected":          bool(sec_jailbreak),
        "jailbreak_patterns":          list(sec_jailbreak_patterns or []),
        "skeleton_key_detected":       bool(sec_skeleton),
        "skeleton_key_patterns":       list(sec_skeleton_patterns or []),
        "secrets_detected":            bool(sec_secrets),
        "secret_types":                list(sec_secret_types or []),
        "indirect_injection_detected": bool(sec_indirect),
        "indirect_injection_sources":  list(sec_indirect_sources or []),
        "rag_poisoning_detected":      bool(sec_rag),
        "rag_poisoning_sources":       list(sec_rag_sources or []),
        "rag_poisoning_score":         float(sec_rag_score) if sec_rag_score is not None else None,
        "tool_exfiltration_detected":  bool(sec_exfil),
        "tool_exfiltration_types":     list(sec_exfil_types or []),
        "tool_exfiltration_sources":   list(sec_exfil_sources or []),
        "tool_policy_violation_detected": bool(sec_tool_policy),
        "tool_policy_violations":         list(sec_tool_policy_violations or []),
        "cross_agent_injection_detected": bool(sec_cross_agent),
        "image_injection_detected":    bool(sec_image_injection),
        "image_injection_sources":     list(sec_image_injection_sources or []),
        "semantic_attack_score":       float(sec_semantic_score) if sec_semantic_score is not None else None,
        "pii_entities_prompt":         list(sec_pii_prompt or []),
        "pii_entities_response":       list(sec_pii_response or []),
        "prompt_redacted":             sec_prompt_redacted or None,
        "response_redacted":           sec_response_redacted or None,
        "scan_latency":                float(sec_scan_latency) if sec_scan_latency is not None else None,
    })


def parse_evaluations(
    metrics: Any,
    scores: Any,
    evaluators: Any,
    judge_models: Any,
    details_list: Any,
) -> list[dict[str, Any]]:
    if not metrics:
        return []
    metrics_l   = list(metrics)
    scores_l    = list(scores or [])
    evaluators_l = list(evaluators or [])
    judges_l    = list(judge_models or [])
    details_l   = list(details_list or [])
    result: list[dict[str, Any]] = []
    for i, metric in enumerate(metrics_l):
        score_v = scores_l[i] if i < len(scores_l) else None
        try:
            score_f: Optional[float] = float(score_v) if score_v is not None else None
        except (TypeError, ValueError):
            score_f = None
        raw_details = details_l[i] if i < len(details_l) else None
        if isinstance(raw_details, str):
            try:
                parsed_details: Any = json.loads(raw_details)
            except (json.JSONDecodeError, ValueError):
                parsed_details = None
        else:
            parsed_details = raw_details or None
        result.append({
            "metric":      metric,
            "score":       score_f,
            "evaluator":   evaluators_l[i] if i < len(evaluators_l) else "",
            "judge_model": judges_l[i] if i < len(judges_l) else "",
            "details":     parsed_details,
        })
    return result


def parse_trace_row(row: tuple) -> dict[str, Any]:
    """Convert a raw ClickHouse trace result row into the API dict shape."""
    (
        prefix, event_raw, ingested_at, total_cost, currency,
        metrics, scores, evaluators, judge_models, details_list,
        *security_cols,
    ) = row
    parsed = parse_event(event_raw)
    merge_security(parsed, tuple(security_cols))
    return {
        "api_key_prefix": prefix,
        "event":          parsed,
        "ingested_at":    ingested_at,
        "cost":           parse_cost(total_cost),
        "currency":       currency or None,
        "evaluations":    parse_evaluations(metrics, scores, evaluators, judge_models, details_list),
    }
