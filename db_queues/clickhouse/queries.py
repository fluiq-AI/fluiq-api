"""ClickHouse query methods — mixed into ClickHouseClient via inheritance."""
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import config

from .helpers import parse_evaluations, parse_trace_row

logger = logging.getLogger(__name__)

_TRACE_ORDER_MAP: dict[str, str] = {
    "newest":       "t.ingested_at DESC",
    "oldest":       "t.ingested_at ASC",
    "latency_desc": "JSONExtractFloat(toString(t.event), 'latency') DESC NULLS LAST",
    "latency_asc":  "JSONExtractFloat(toString(t.event), 'latency') ASC NULLS LAST",
    "cost_desc":    "c.total_cost DESC NULLS LAST",
    "cost_asc":     "c.total_cost ASC NULLS LAST",
}


class ClickHouseQueryMixin:
    """All read/write query methods. Requires self._client (AsyncClient) and self.start()."""

    # ── Traces ────────────────────────────────────────────────────────────────

    async def fetch_traces(
        self,
        organization_id: uuid.UUID,
        api_key_prefix: Optional[str] = None,
        agent_key: Optional[str] = None,
        agent_kind: Optional[str] = None,
        root_trace_id: Optional[uuid.UUID] = None,
        roots_only: bool = False,
        limit: int = 100,
        offset: int = 0,
        sort: str = "newest",
        status: str = "all",
        security: str = "all",
        integration: str = "all",
        quality: str = "all",
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
        evaluations_table: Optional[str] = None,
        security_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target          = table            or self.default_table  # type: ignore[attr-defined]
        costs_target    = costs_table      or config.CLICKHOUSE_TRACE_COSTS_TABLE
        evals_target    = evaluations_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        security_target = security_table   or config.CLICKHOUSE_SECURITY_TABLE

        where: str = "t.organization_id = {org_id:UUID}"
        params: dict[str, Any] = {
            "org_id": str(organization_id),
            "limit":  limit,
            "offset": offset,
        }

        if api_key_prefix is not None:
            where += " AND t.api_key_prefix = {prefix:String}"
            params["prefix"] = api_key_prefix
        if root_trace_id is not None:
            where += " AND t.root_trace_id = {root_trace_id:UUID}"
            params["root_trace_id"] = str(root_trace_id)
        if roots_only:
            where += (
                " AND (t.trace_id = t.root_trace_id"
                f" OR t.root_trace_id NOT IN ("
                f"   SELECT trace_id FROM {target}"
                "    WHERE organization_id = {org_id:UUID}"
                " ))"
            )
        if agent_key is not None:
            where += " AND t.trace_id = t.root_trace_id"
            params["agent_key"] = agent_key
            if agent_kind == "function":
                where += " AND JSONExtractString(toString(t.event), 'function') = {agent_key:String}"
            elif agent_kind == "chain":
                where += " AND JSONExtractString(toString(t.event), 'name') = {agent_key:String}"
            elif agent_kind == "langgraph_node":
                where += " AND JSONExtractString(JSONExtractRaw(toString(t.event), 'langgraph'), 'langgraph_node') = {agent_key:String}"
            elif agent_kind == "llm":
                where += " AND concat(c.provider, ':', c.model) = {agent_key:String}"
            else:
                where += (
                    " AND ("
                    "JSONExtractString(toString(t.event), 'function') = {agent_key:String}"
                    " OR JSONExtractString(toString(t.event), 'name') = {agent_key:String}"
                    ")"
                )

        # Status filter
        #
        # When the query returns root rows (the trace list's roots_only, or the
        # per-agent view's agent_key), `failed`/`blocked` are subtree-aware: a
        # root matches when it OR any descendant span (anything sharing its
        # root_trace_id) is failed/blocked. This lets a user surface whole trace
        # trees that contain a failure even when the root itself completed —
        # e.g. an agent that recovered from a failed tool call. For the rare
        # non-root fetch (paging raw spans) we keep the row-level predicate so
        # "failed" still means the individual failing spans.
        returning_roots = roots_only or agent_key is not None
        blocked_pred = "JSONExtractString(toString(event), 'status') = 'blocked'"
        failed_pred = (
            "JSONHas(toString(event), 'success')"
            " AND JSONExtractBool(toString(event), 'success') = 0"
            " AND JSONExtractString(toString(event), 'status') != 'blocked'"
        )
        if status == "running":
            where += " AND JSONExtractString(toString(t.event), 'status') = 'running'"
        elif status == "blocked":
            if returning_roots:
                where += (
                    " AND t.root_trace_id IN ("
                    f"   SELECT DISTINCT root_trace_id FROM {target}"
                    "    WHERE organization_id = {org_id:UUID}"
                    f"      AND {blocked_pred}"
                    " )"
                )
            else:
                where += f" AND {blocked_pred.replace('event', 't.event')}"
        elif status == "failed":
            if returning_roots:
                where += (
                    " AND t.root_trace_id IN ("
                    f"   SELECT DISTINCT root_trace_id FROM {target}"
                    "    WHERE organization_id = {org_id:UUID}"
                    f"      AND {failed_pred}"
                    " )"
                )
            else:
                where += f" AND {failed_pred.replace('event', 't.event')}"
        elif status == "completed":
            where += (
                " AND JSONExtractString(toString(t.event), 'status') NOT IN ('running', 'blocked')"
                " AND (NOT JSONHas(toString(t.event), 'success')"
                "   OR JSONExtractBool(toString(t.event), 'success') = 1)"
            )

        # Integration filter
        if integration != "all":
            where += " AND JSONExtractString(toString(t.event), 'integration') = {integration:String}"
            params["integration"] = integration

        # Post-join filters (reference joined table aliases)
        security_filter = ""
        if security == "clean":
            security_filter = " AND (s.security_risk_level = '' OR s.security_risk_level IS NULL)"
        elif security in ("low", "medium", "high"):
            security_filter = f" AND s.security_risk_level = '{security}'"

        quality_filter = ""
        if quality == "none":
            quality_filter = " AND length(e.scores) = 0"
        elif quality == "high":
            quality_filter = " AND length(e.scores) > 0 AND arrayMin(e.scores) >= 0.8"
        elif quality == "medium":
            quality_filter = " AND length(e.scores) > 0 AND arrayMin(e.scores) >= 0.5 AND arrayMin(e.scores) < 0.8"
        elif quality == "low":
            quality_filter = " AND length(e.scores) > 0 AND arrayMin(e.scores) < 0.5"

        order_by = _TRACE_ORDER_MAP.get(sort, "t.ingested_at DESC")

        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT t.api_key_prefix, t.event, t.ingested_at, "
            f"       c.total_cost, c.currency, "
            f"       e.metrics, e.scores, e.evaluators, e.judge_models, e.details_list, "
            f"       s.security_risk_level, s.security_risk_score, s.should_block, "
            f"       s.injection_detected, s.injection_patterns, "
            f"       s.jailbreak_detected, s.jailbreak_patterns, "
            f"       s.skeleton_key_detected, s.skeleton_key_patterns, "
            f"       s.secrets_detected, s.secret_types, "
            f"       s.indirect_injection_detected, s.indirect_injection_sources, "
            f"       s.rag_poisoning_detected, s.rag_poisoning_sources, s.rag_poisoning_score, "
            f"       s.tool_exfiltration_detected, s.tool_exfiltration_types, s.tool_exfiltration_sources, "
            f"       s.tool_policy_violation_detected, s.tool_policy_violations, "
            f"       s.cross_agent_injection_detected, "
            f"       s.semantic_attack_score, "
            f"       s.pii_entities_prompt, s.pii_entities_response, "
            f"       s.prompt_redacted, s.response_redacted, s.scan_latency "
            f"FROM {target} AS t "
            f"LEFT JOIN {costs_target} AS c "
            f"  ON t.organization_id = c.organization_id "
            f" AND t.trace_id = c.trace_id "
            f"LEFT JOIN ("
            f"   SELECT organization_id, trace_id, "
            f"          groupArray(metric)            AS metrics, "
            f"          groupArray(score)             AS scores, "
            f"          groupArray(evaluator)         AS evaluators, "
            f"          groupArray(judge_model)       AS judge_models, "
            f"          groupArray(toString(details)) AS details_list "
            f"   FROM {evals_target} "
            f"   WHERE organization_id = {{org_id:UUID}} "
            f"   GROUP BY organization_id, trace_id"
            f") AS e "
            f"  ON t.organization_id = e.organization_id "
            f" AND t.trace_id = e.trace_id "
            f"LEFT JOIN {security_target} AS s "
            f"  ON t.organization_id = s.organization_id "
            f" AND t.trace_id = s.trace_id "
            f"WHERE {where}{security_filter}{quality_filter} "
            f"ORDER BY {order_by} "
            f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            parameters=params,
        )
        return [parse_trace_row(row) for row in result.result_rows]

    # ── Counts ────────────────────────────────────────────────────────────────

    async def count_rows(self, organization_id: uuid.UUID, table: str) -> int:
        """Count rows for an org in the current calendar month (UTC).

        Tier quotas are advertised per month on the pricing page, so usage is
        scoped to the start of the current month rather than counted lifetime.
        The window resets automatically at each month boundary.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT count() FROM {table} "
            f"WHERE organization_id = {{org_id:UUID}} "
            f"  AND ingested_at >= toStartOfMonth(now('UTC'))",
            parameters={"org_id": str(organization_id)},
        )
        rows = result.result_rows
        return int(rows[0][0] or 0) if rows else 0

    async def count_traces(self, organization_id: uuid.UUID) -> int:
        return await self.count_rows(organization_id, self.default_table)  # type: ignore[attr-defined]

    async def count_evaluations(self, organization_id: uuid.UUID) -> int:
        return await self.count_rows(organization_id, config.CLICKHOUSE_EVALUATIONS_TABLE)

    # ── Spending ────────────────────────────────────────────────────────────────

    async def fetch_spending_by_day(
        self,
        organization_id: uuid.UUID,
        days: int = 30,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Daily spend grouped by provider for the spending chart.

        Aggregates server-side from ``trace_costs`` (provider + total_cost are
        plain columns) joined to ``traces`` only for the ``ingested_at``
        timestamp. This returns at most ``days`` × providers rows instead of
        making the client pull ~1000 fully-joined trace rows (with the eval
        groupArray and security joins) just to sum costs — the single heaviest
        query on the dashboard's first paint.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or self.default_table  # type: ignore[attr-defined]
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        params = {"org_id": str(organization_id), "days": int(days)}
        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT toString(toDate(t.ingested_at)) AS day, "
            f"       c.provider AS provider, "
            f"       sum(c.total_cost) AS cost "
            f"FROM {costs_target} AS c "
            f"INNER JOIN {target} AS t "
            f"  ON t.organization_id = c.organization_id "
            f" AND t.trace_id = c.trace_id "
            f"WHERE c.organization_id = {{org_id:UUID}} "
            f"  AND t.ingested_at >= now() - toIntervalDay({{days:UInt32}}) "
            f"GROUP BY day, provider",
            parameters=params,
        )
        return [
            {"day": row[0], "provider": row[1] or "", "cost": float(row[2] or 0)}
            for row in result.result_rows
        ]

    # ── Cache stats ───────────────────────────────────────────────────────────

    async def fetch_cache_stats(
        self,
        organization_id: uuid.UUID,
        window_hours: int = 24,
        table: Optional[str] = None,
    ) -> dict[str, Any]:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or self.default_table  # type: ignore[attr-defined]
        params = {"org_id": str(organization_id), "window": int(window_hours)}

        legacy_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    JSONExtractString(toString(event), 'cache_kind')      AS kind,
    sum(JSONExtractInt(toString(event), 'cache_hits'))    AS hits,
    sum(JSONExtractInt(toString(event), 'cache_misses'))  AS misses,
    count()                                               AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'cache'
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        llm_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    lower(JSONExtractString(toString(event), 'integration')) AS kind,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 1) AS hits,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'llm'
  AND JSONHas(toString(event), 'cache_hit')
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        fn_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    JSONExtractString(toString(event), 'function')             AS kind,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 1) AS hits,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'function'
  AND JSONHas(toString(event), 'cache_hit')
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        vs_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    lower(JSONExtractString(toString(event), 'integration')) AS kind,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 1) AS hits,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'vectorstore'
  AND JSONHas(toString(event), 'cache_hit')
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        mcp_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    JSONExtractString(toString(event), 'kind')                 AS kind,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 1) AS hits,
    countIf(JSONExtractBool(toString(event), 'cache_hit') = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'mcp'
  AND JSONHas(toString(event), 'cache_hit')
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        per_kind: list[dict[str, Any]] = []
        total_hits = total_misses = total_calls = 0

        def _add_rows(rows: list) -> None:
            nonlocal total_hits, total_misses, total_calls
            for kind, hits, misses, calls in rows:
                h, m, c = int(hits or 0), int(misses or 0), int(calls or 0)
                total_hits += h; total_misses += m; total_calls += c
                lookups = h + m
                per_kind.append({
                    "kind": kind or "unknown",
                    "hits": h, "misses": m, "calls": c,
                    "hit_rate": (h / lookups) if lookups else 0.0,
                })

        _add_rows(legacy_result.result_rows)
        _add_rows(llm_result.result_rows)
        _add_rows(fn_result.result_rows)
        _add_rows(vs_result.result_rows)
        _add_rows(mcp_result.result_rows)

        total_lookups = total_hits + total_misses
        return {
            "window_hours": int(window_hours),
            "hits": total_hits, "misses": total_misses, "calls": total_calls,
            "hit_rate": (total_hits / total_lookups) if total_lookups else 0.0,
            "per_kind": per_kind,
        }

    # ── Prompt cache stats ────────────────────────────────────────────────────

    async def fetch_prompt_cache_stats(
        self,
        organization_id: uuid.UUID,
        window_hours: int = 24,
        table: Optional[str] = None,
    ) -> dict[str, Any]:
        """Return aggregated provider-level prompt cache token counts.

        Sums ``prompt_cache_read_tokens`` / ``prompt_cache_creation_tokens``
        (Anthropic) and ``prompt_cached_tokens`` (OpenAI, Gemini) from LLM
        traces.  Only traces that carry at least one of these fields are
        included so the ``calls`` figure represents instrumented calls only.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or self.default_table  # type: ignore[attr-defined]
        params = {"org_id": str(organization_id), "window": int(window_hours)}

        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    sum(JSONExtractInt(toString(event), 'prompt_cache_read_tokens'))    AS anthropic_read,
    sum(JSONExtractInt(toString(event), 'prompt_cache_creation_tokens')) AS anthropic_creation,
    sum(JSONExtractInt(toString(event), 'prompt_cached_tokens'))         AS provider_cached,
    count()                                                              AS calls,
    countIf(
        JSONExtractInt(toString(event), 'prompt_cache_read_tokens') > 0
        OR JSONExtractInt(toString(event), 'prompt_cached_tokens') > 0
    )                                                                    AS calls_with_hit
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'llm'
  AND (
        JSONHas(toString(event), 'prompt_cache_read_tokens')
     OR JSONHas(toString(event), 'prompt_cached_tokens')
  )
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
""", parameters=params)

        if not result.result_rows:
            return {
                "window_hours": int(window_hours),
                "anthropic_cache_read_tokens": 0,
                "anthropic_cache_creation_tokens": 0,
                "provider_cached_tokens": 0,
                "total_cached_tokens": 0,
                "calls": 0,
                "calls_with_hit": 0,
            }

        row = result.result_rows[0]
        anthropic_read, anthropic_creation, provider_cached, calls, calls_with_hit = (
            int(v or 0) for v in row
        )
        return {
            "window_hours": int(window_hours),
            "anthropic_cache_read_tokens": anthropic_read,
            "anthropic_cache_creation_tokens": anthropic_creation,
            "provider_cached_tokens": provider_cached,
            "total_cached_tokens": anthropic_read + provider_cached,
            "calls": calls,
            "calls_with_hit": calls_with_hit,
        }

    # ── Agents ────────────────────────────────────────────────────────────────

    async def fetch_agent_summary(
        self,
        organization_id: uuid.UUID,
        limit: int = 50,
        offset: int = 0,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target       = table       or self.default_table  # type: ignore[attr-defined]
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        params = {"org_id": str(organization_id), "limit": limit, "offset": offset}
        result = await self._client.query(f"""  # type: ignore[attr-defined]
WITH roots AS (
    SELECT
        trace_id, ingested_at,
        JSONExtractString(toString(event), 'function')                                    AS fn,
        JSONExtractString(toString(event), 'name')                                        AS nm,
        JSONExtractString(toString(event), 'integration')                                 AS intg,
        JSONExtractString(JSONExtractRaw(toString(event), 'langgraph'), 'langgraph_node') AS lg_node,
        JSONExtractFloat(toString(event), 'latency')                                      AS latency
    FROM {target}
    WHERE organization_id = {{org_id:UUID}}
      AND trace_id = root_trace_id
      AND (
            JSONExtractString(toString(event), 'function') != ''
         OR JSONExtractString(toString(event), 'name') != ''
         OR JSONExtractString(JSONExtractRaw(toString(event), 'langgraph'), 'langgraph_node') != ''
      )
),
costs AS (
    SELECT
        root_trace_id,
        sum(total_cost)                                          AS run_cost,
        sum(input_tokens + cached_input_tokens + output_tokens) AS run_tokens
    FROM {costs_target}
    WHERE organization_id = {{org_id:UUID}}
    GROUP BY root_trace_id
)
SELECT
    multiIf(fn != '', fn, nm != '', nm, lg_node)                       AS agent_key,
    multiIf(fn != '', 'function', nm != '', 'chain', 'langgraph_node') AS agent_kind,
    intg                                                                AS integration,
    count()                                                             AS runs,
    sum(ifNull(c.run_cost, 0))                                         AS total_cost,
    sum(ifNull(c.run_cost, 0)) / count()                               AS avg_cost_per_run,
    toUInt64(sum(ifNull(c.run_tokens, 0)))                             AS total_tokens,
    avgIf(r.latency, r.latency > 0)                                    AS avg_latency,
    max(r.ingested_at)                                                  AS last_run
FROM roots AS r
LEFT JOIN costs AS c ON r.trace_id = c.root_trace_id
GROUP BY agent_key, agent_kind, integration
ORDER BY last_run DESC
LIMIT {{limit:UInt32}}
OFFSET {{offset:UInt32}}
""", parameters=params)
        rows: list[dict[str, Any]] = []
        for row in result.result_rows:
            agent_key, agent_kind, integration, runs, total_cost, avg_cost, total_tokens, avg_latency, last_run = row
            rows.append({
                "agent_key":       agent_key or "",
                "agent_kind":      agent_kind or "function",
                "integration":     integration or "",
                "runs":            int(runs or 0),
                "total_cost":      float(total_cost) if total_cost is not None else 0.0,
                "avg_cost_per_run": float(avg_cost) if avg_cost is not None else 0.0,
                "total_tokens":    int(total_tokens or 0),
                "avg_latency":     float(avg_latency) if avg_latency is not None else None,
                "last_run":        last_run,
            })
        return rows

    # ── Optimization ──────────────────────────────────────────────────────────

    async def fetch_optimization_profile(
        self,
        organization_id: uuid.UUID,
        window_hours: int = 168,
        min_calls: int = 10,
        top_n: int = 10,
        table: Optional[str] = None,
    ) -> dict[str, Any]:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or self.default_table  # type: ignore[attr-defined]
        params = {
            "org_id": str(organization_id),
            "window": int(window_hours),
            "min_calls": int(min_calls),
            "top_n": int(top_n),
        }
        model_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    JSONExtractString(toString(event), 'model') AS model,
    count()                                      AS call_count
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND JSONExtractString(toString(event), 'type') = 'llm'
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY model
HAVING call_count >= {{min_calls:UInt32}}
ORDER BY call_count DESC
LIMIT {{top_n:UInt32}}
""", parameters=params)

        repeat_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    sum(call_count)                            AS total_calls,
    sumIf(call_count, call_count > 1)          AS repeating_calls
FROM (
    SELECT
        cityHash64(
            JSONExtractString(toString(event), 'model'),
            toString(JSONExtractRaw(toString(event), 'messages'))
        ) AS prompt_hash,
        count() AS call_count
    FROM {target}
    WHERE organization_id = {{org_id:UUID}}
      AND JSONExtractString(toString(event), 'type') = 'llm'
      AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
    GROUP BY prompt_hash
)
""", parameters=params)

        models = [model for model, _ in model_result.result_rows if model]
        total_calls = repeating_calls = 0
        if repeat_result.result_rows:
            total_calls    = int(repeat_result.result_rows[0][0] or 0)
            repeating_calls = int(repeat_result.result_rows[0][1] or 0)
        return {
            "models": models,
            "estimated_hit_rate": round((repeating_calls / total_calls) if total_calls else 0.0, 4),
            "window_hours": window_hours,
        }

    # ── Evaluations ───────────────────────────────────────────────────────────

    async def fetch_recent_evals(
        self,
        organization_id: uuid.UUID,
        window_minutes: int = 30,
        limit: int = 200,
        table: Optional[str] = None,
        evals_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target       = table       or self.default_table  # type: ignore[attr-defined]
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        params = {"org_id": str(organization_id), "window": int(window_minutes), "limit": int(limit)}
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT e.trace_id, e.metric, e.score, e.evaluator, e.judge_model
FROM {evals_target} AS e
INNER JOIN {target} AS t
    ON e.organization_id = t.organization_id
   AND e.trace_id = t.trace_id
WHERE e.organization_id = {{org_id:UUID}}
  AND t.ingested_at >= now() - toIntervalMinute({{window:UInt32}})
ORDER BY t.ingested_at DESC
LIMIT {{limit:UInt32}}
""", parameters=params)
        return [
            {
                "trace_id":   str(trace_id) if trace_id else None,
                "metric":     metric or "",
                "score":      float(score) if score is not None else None,
                "evaluator":  evaluator or "",
                "judge_model": judge_model or "",
            }
            for trace_id, metric, score, evaluator, judge_model in result.result_rows
        ]

    async def insert_evaluations(
        self,
        organization_id: uuid.UUID,
        trace_id: str,
        results: dict[str, dict[str, Any]],
        judge_model: str,
        table: Optional[str] = None,
    ) -> None:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        evals_target = table or config.CLICKHOUSE_EVALUATIONS_TABLE
        rows = [
            [str(organization_id), trace_id, metric, float(data.get("score", 0.0)), "fluiq.eval", judge_model]
            for metric, data in results.items()
        ]
        if rows:
            await self._client.insert(  # type: ignore[attr-defined]
                evals_target, rows,
                column_names=["organization_id", "trace_id", "metric", "score", "evaluator", "judge_model"],
            )

    # ── Audit log ─────────────────────────────────────────────────────────────

    async def insert_audit_event(
        self,
        organization_id: str,
        actor: str,
        event_type: str,
        http_method: str,
        http_path: str,
        http_status: int,
        ip_address: str,
        latency_ms: int,
        metadata: dict[str, Any],
        hmac_secret: str,
        table: Optional[str] = None,
    ) -> None:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        audit_table = table or config.CLICKHOUSE_AUDIT_TABLE
        event_id   = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc)
        ts_iso     = created_at.isoformat()

        msg = f"{event_id}|{organization_id}|{event_type}|{actor}|{ts_iso}"
        row_hash = hmac.new(hmac_secret.encode(), msg.encode(), hashlib.sha256).hexdigest()

        await self._client.insert(  # type: ignore[attr-defined]
            audit_table,
            [[
                event_id,
                organization_id,
                actor,
                event_type,
                http_method,
                http_path,
                int(http_status),
                ip_address,
                int(latency_ms),
                json.dumps(metadata, default=str),
                row_hash,
                created_at,
            ]],
            column_names=[
                "event_id", "organization_id", "actor", "event_type",
                "http_method", "http_path", "http_status", "ip_address",
                "latency_ms", "metadata", "row_hash", "created_at",
            ],
        )

    async def fetch_audit_log(
        self,
        organization_id: uuid.UUID,
        event_type: Optional[str] = None,
        actor: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        audit_table = table or config.CLICKHOUSE_AUDIT_TABLE

        where  = "organization_id = {org_id:String}"
        params: dict[str, Any] = {
            "org_id": str(organization_id),
            "limit":  limit,
            "offset": offset,
        }
        if event_type:
            where += " AND event_type = {event_type:String}"
            params["event_type"] = event_type
        if actor:
            where += " AND actor = {actor:String}"
            params["actor"] = actor

        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT event_id, organization_id, actor, event_type, "
            f"       http_method, http_path, http_status, ip_address, "
            f"       latency_ms, metadata, row_hash, created_at "
            f"FROM {audit_table} "
            f"WHERE {where} "
            f"ORDER BY created_at DESC "
            f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            parameters=params,
        )
        return [
            {
                "event_id":        str(row[0]),
                "organization_id": str(row[1]),
                "actor":           row[2],
                "event_type":      row[3],
                "http_method":     row[4],
                "http_path":       row[5],
                "http_status":     int(row[6]),
                "ip_address":      row[7],
                "latency_ms":      int(row[8]),
                "metadata":        json.loads(row[9]) if row[9] else {},
                "row_hash":        row[10],
                "created_at":      row[11].isoformat() if row[11] else None,
            }
            for row in result.result_rows
        ]

    # ── Admin ─────────────────────────────────────────────────────────────────

    async def delete_org_data(self, organization_id: uuid.UUID) -> None:
        org_str = str(organization_id)
        for tbl in [
            config.CLICKHOUSE_TRACE_TABLE,
            config.CLICKHOUSE_TRACE_COSTS_TABLE,
            config.CLICKHOUSE_EVALUATIONS_TABLE,
            config.CLICKHOUSE_SECURITY_TABLE,
        ]:
            await self._client.command(  # type: ignore[attr-defined]
                f"ALTER TABLE {tbl} DELETE WHERE organization_id = '{org_str}'"
            )
        logger.info("[CLICKHOUSE] Queued deletion mutations for org %s", org_str)
