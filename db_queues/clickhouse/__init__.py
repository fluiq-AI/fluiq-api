import json
import logging
import uuid
from typing import Any, Optional

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

import config 

logger = logging.getLogger(__name__)

class ClickHouseClient:
    """Async ClickHouse client for reading trace records."""

    def __init__(
        self,
        host: str = config.CLICKHOUSE_HOST,
        port: int = config.CLICKHOUSE_PORT,
        username: str = config.CLICKHOUSE_USER,
        password: str = config.CLICKHOUSE_PASSWORD,
        database: str = config.CLICKHOUSE_DATABASE,
        default_table: str = config.CLICKHOUSE_TRACE_TABLE,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.default_table = default_table
        self._client: Optional[AsyncClient] = None

    async def start(self) -> None:
        if self._client is not None:
            return
        self._client = await clickhouse_connect.get_async_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
        logger.info("[CLICKHOUSE] Client started: %s:%s/%s", self.host, self.port, self.database)

    async def stop(self) -> None:
        if self._client is None:
            return
        await self._client.close()
        self._client = None
        logger.info("[CLICKHOUSE] Client stopped")

    async def fetch_traces(
        self,
        organization_id: uuid.UUID,
        api_key_prefix: Optional[str] = None,
        agent_key: Optional[str] = None,
        agent_kind: Optional[str] = None,
        root_trace_id: Optional[uuid.UUID] = None,
        limit: int = 100,
        offset: int = 0,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
        evaluations_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return traces for an organization, optionally filtered by key prefix.

        Each row is `{api_key_prefix, event, ingested_at, cost, currency,
        evaluations}` with the event JSON already parsed. Cost is left-joined
        from the trace_costs table on (organization_id, trace_id) and may be
        `None` for traces without a matching cost record. Evaluations are
        aggregated per trace from the evaluations table as a list of
        `{metric, score, evaluator, judge_model}` entries (empty when none).
        """
        if self._client is None:
            await self.start()
        target = table or self.default_table
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        evals_target = evaluations_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        where = "t.organization_id = {org_id:UUID}"
        params: dict[str, Any] = {
            "org_id": str(organization_id),
            "limit": limit,
            "offset": offset,
        }
        if api_key_prefix is not None:
            where += " AND t.api_key_prefix = {prefix:String}"
            params["prefix"] = api_key_prefix
        if root_trace_id is not None:
            where += " AND t.root_trace_id = {root_trace_id:UUID}"
            params["root_trace_id"] = str(root_trace_id)
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
        result = await self._client.query(
            f"SELECT t.api_key_prefix, t.event, t.ingested_at, "
            f"       c.total_cost, c.currency, "
            f"       e.metrics, e.scores, e.evaluators, e.judge_models "
            f"FROM {target} AS t "
            f"LEFT JOIN {costs_target} AS c "
            f"  ON t.organization_id = c.organization_id "
            f" AND t.trace_id = c.trace_id "
            f"LEFT JOIN ("
            f"   SELECT organization_id, trace_id, "
            f"          groupArray(metric)      AS metrics, "
            f"          groupArray(score)       AS scores, "
            f"          groupArray(evaluator)   AS evaluators, "
            f"          groupArray(judge_model) AS judge_models "
            f"   FROM {evals_target} "
            f"   WHERE organization_id = {{org_id:UUID}} "
            f"   GROUP BY organization_id, trace_id"
            f") AS e "
            f"  ON t.organization_id = e.organization_id "
            f" AND t.trace_id = e.trace_id "
            f"WHERE {where} "
            f"ORDER BY t.ingested_at DESC "
            f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            parameters=params,
        )
        rows: list[dict[str, Any]] = []
        for (
            prefix, event, ingested_at, total_cost, currency,
            metrics, scores, evaluators, judge_models,
        ) in result.result_rows:
            if isinstance(event, str):
                try:
                    parsed = json.loads(event)
                except json.JSONDecodeError:
                    parsed = {"raw": event}
            else:
                parsed = event
            cost: Optional[float]
            if total_cost is None:
                cost = None
            else:
                try:
                    cost = float(total_cost)
                except (TypeError, ValueError):
                    cost = None
            evaluations: list[dict[str, Any]] = []
            if metrics:
                metrics_l = list(metrics)
                scores_l = list(scores or [])
                evaluators_l = list(evaluators or [])
                judges_l = list(judge_models or [])
                for i, metric in enumerate(metrics_l):
                    score_v = scores_l[i] if i < len(scores_l) else None
                    try:
                        score_f = float(score_v) if score_v is not None else None
                    except (TypeError, ValueError):
                        score_f = None
                    evaluations.append({
                        "metric": metric,
                        "score": score_f,
                        "evaluator": evaluators_l[i] if i < len(evaluators_l) else "",
                        "judge_model": judges_l[i] if i < len(judges_l) else "",
                    })
            rows.append({
                "api_key_prefix": prefix,
                "event": parsed,
                "ingested_at": ingested_at,
                "cost": cost,
                "currency": currency or None,
                "evaluations": evaluations,
            })
        return rows


    async def count_rows(
        self,
        organization_id: uuid.UUID,
        table: str,
    ) -> int:
        """Return the lifetime row count for an org in the given table.

        Used by the quota layer to compare against tier limits. The query is
        a single ``count()`` filtered on the table's leading sort key
        (``organization_id``) so it stays cheap even at large scale.
        """
        if self._client is None:
            await self.start()
        result = await self._client.query(
            f"SELECT count() FROM {table} "
            f"WHERE organization_id = {{org_id:UUID}}",
            parameters={"org_id": str(organization_id)},
        )
        rows = result.result_rows
        if not rows:
            return 0
        return int(rows[0][0] or 0)

    async def count_traces(self, organization_id: uuid.UUID) -> int:
        return await self.count_rows(organization_id, self.default_table)

    async def count_evaluations(self, organization_id: uuid.UUID) -> int:
        return await self.count_rows(organization_id, config.CLICKHOUSE_EVALUATIONS_TABLE)

    async def fetch_cache_stats(
        self,
        organization_id: uuid.UUID,
        window_hours: int = 24,
        table: Optional[str] = None,
    ) -> dict[str, Any]:
        """Aggregate cache hit/miss counts emitted by ``trace=True`` caches.

        Cache spans are stored alongside regular traces with
        ``event.type == 'cache'`` and integer ``cache_hits`` / ``cache_misses``
        counters (batches of N for embeddings, 1/0 for prompts). The
        rollup totals each over the last ``window_hours`` and groups by
        ``cache_kind`` so the dashboard can surface per-cache hit rates.
        """
        if self._client is None:
            await self.start()
        target = table or self.default_table
        params = {
            "org_id": str(organization_id),
            "window": int(window_hours),
        }
        result = await self._client.query(
            f"""
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
""",
            parameters=params,
        )
        per_kind: list[dict[str, Any]] = []
        total_hits = 0
        total_misses = 0
        total_calls = 0
        for kind, hits, misses, calls in result.result_rows:
            h = int(hits or 0)
            m = int(misses or 0)
            c = int(calls or 0)
            total_hits += h
            total_misses += m
            total_calls += c
            lookups = h + m
            per_kind.append({
                "kind": kind or "unknown",
                "hits": h,
                "misses": m,
                "calls": c,
                "hit_rate": (h / lookups) if lookups else 0.0,
            })
        total_lookups = total_hits + total_misses
        return {
            "window_hours": int(window_hours),
            "hits": total_hits,
            "misses": total_misses,
            "calls": total_calls,
            "hit_rate": (total_hits / total_lookups) if total_lookups else 0.0,
            "per_kind": per_kind,
        }


    async def fetch_agent_summary(
        self,
        organization_id: uuid.UUID,
        limit: int = 100,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Aggregate cost/latency/tokens per agent across all history.

        An agent is any root trace that has an explicit identifier:
          1. event.function                     — @trace-decorated entrypoints
          2. event.name                         — LangChain root chain / runnable
          3. event.langgraph.langgraph_node     — LangGraph entry node

        Raw un-decorated LLM calls (provider:model only) are excluded — they
        are not agents, just leaf cost records.

        Cost and token metrics come from a LEFT JOIN on trace_costs so agents
        without LLM calls (e.g. pure function traces) still appear with 0 cost.
        ``runs`` counts distinct root traces — one per agent invocation.
        """
        if self._client is None:
            await self.start()
        target = table or self.default_table
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        params = {"org_id": str(organization_id), "limit": limit}
        result = await self._client.query(
            f"""
WITH roots AS (
    SELECT
        trace_id,
        ingested_at,
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
""",
            parameters=params,
        )
        rows: list[dict[str, Any]] = []
        for row in result.result_rows:
            (
                agent_key, agent_kind, integration, runs, total_cost,
                avg_cost, total_tokens, avg_latency, last_run,
            ) = row
            rows.append({
                "agent_key": agent_key or "",
                "agent_kind": agent_kind or "function",
                "integration": integration or "",
                "runs": int(runs or 0),
                "total_cost": float(total_cost) if total_cost is not None else 0.0,
                "avg_cost_per_run": float(avg_cost) if avg_cost is not None else 0.0,
                "total_tokens": int(total_tokens or 0),
                "avg_latency": float(avg_latency) if avg_latency is not None else None,
                "last_run": last_run,
            })
        return rows


clickhouse_client = ClickHouseClient()

__all__ = [
    "ClickHouseClient",
    "clickhouse_client",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_DATABASE",
    "CLICKHOUSE_TRACE_TABLE",
    "CLICKHOUSE_TRACE_COSTS_TABLE",
    "CLICKHOUSE_EVALUATIONS_TABLE",
]
