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
        """Aggregate cost/latency/tokens per "agent" across all history.

        An agent is identified by the *root* trace's stable name:
          1. event.function           — @trace-decorated entrypoints
          2. event.name               — LangChain root chain / runnable name
          3. event.langgraph.langgraph_node — LangGraph entry node
          4. <integration>:<model>    — fallback for un-decorated LLM calls
                                        (chain_id has no matching trace row)

        ``runs`` counts distinct ``root_trace_id``s — one per agent invocation.
        """
        if self._client is None:
            await self.start()
        target = table or self.default_table
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        params = {"org_id": str(organization_id), "limit": limit}
        # The root trace's own ingestion timestamp is a better "last_run"
        # signal than the leaf cost row's, but root may be missing (synthetic
        # chain_id) — fall back to the cost row's ingested_at via greatest().
        result = await self._client.query(
            f"""
WITH joined AS (
    SELECT
        c.organization_id            AS organization_id,
        c.root_trace_id              AS root_trace_id,
        c.input_tokens               AS leaf_input_tokens,
        c.cached_input_tokens        AS leaf_cached_input_tokens,
        c.output_tokens              AS leaf_output_tokens,
        c.total_cost                 AS leaf_total_cost,
        c.provider                   AS leaf_provider,
        c.model                      AS leaf_model,
        c.ingested_at                AS leaf_ingested_at,
        t.ingested_at                AS root_ingested_at,
        JSONExtractString(toString(t.event), 'function')    AS root_function,
        JSONExtractString(toString(t.event), 'name')        AS root_name,
        JSONExtractString(toString(t.event), 'integration') AS root_integration,
        JSONExtractString(JSONExtractRaw(toString(t.event), 'langgraph'),
                          'langgraph_node')                 AS root_lg_node,
        JSONExtractFloat(toString(t.event), 'latency')      AS root_latency
    FROM {costs_target} AS c
    LEFT JOIN {target} AS t
        ON c.organization_id = t.organization_id
       AND c.root_trace_id   = t.trace_id
    WHERE c.organization_id = {{org_id:UUID}}
)
SELECT
    multiIf(
        root_function   != '', root_function,
        root_name       != '', root_name,
        root_lg_node    != '', root_lg_node,
        concat(leaf_provider, ':', leaf_model)
    )                                                AS agent_key,
    multiIf(
        root_function   != '', 'function',
        root_name       != '', 'chain',
        root_lg_node    != '', 'langgraph_node',
        'llm'
    )                                                AS agent_kind,
    if(root_integration != '', root_integration, leaf_provider) AS integration,
    uniqExact(root_trace_id)                         AS runs,
    sum(leaf_total_cost)                             AS total_cost,
    sum(leaf_total_cost) / uniqExact(root_trace_id)  AS avg_cost_per_run,
    sum(leaf_input_tokens + leaf_cached_input_tokens + leaf_output_tokens) AS total_tokens,
    avgIf(root_latency, root_latency > 0)            AS avg_latency,
    max(greatest(coalesce(root_ingested_at, toDateTime64(0, 3, 'UTC')),
                 leaf_ingested_at))                  AS last_run
FROM joined
GROUP BY agent_key, agent_kind, integration
ORDER BY total_cost DESC
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
                "agent_kind": agent_kind or "llm",
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
