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
    "latency_desc": "t.event.latency.:Float64 DESC NULLS LAST",
    "latency_asc":  "t.event.latency.:Float64 ASC NULLS LAST",
    "cost_desc":    "c.total_cost DESC NULLS LAST",
    "cost_asc":     "c.total_cost ASC NULLS LAST",
}

# Substrings that mark a model as already small/cheap → never suggest a downgrade.
_CHEAP_MARKERS = ("mini", "haiku", "flash", "nano", "lite", "small", "8b", "3b", "1b", "-lite")


def _suggest_downgrade(model: str) -> Optional[tuple[str, float]]:
    """Map a premium model to a cheaper sibling + an estimated savings fraction
    on the candidate (small-output) spend. Returns None when the model is already
    a small model or has no obvious cheaper equivalent. Heuristic, deliberately
    conservative; the UI frames it as a hint, not a guarantee."""
    m = (model or "").lower()
    if not m or any(x in m for x in _CHEAP_MARKERS):
        return None
    if "sonnet" in m or "opus" in m:
        return ("claude-haiku-4-5", 0.85)
    if "gemini" in m and "pro" in m:
        return ("gemini-2.5-flash", 0.88)
    if m.startswith(("o1", "o3")) or "-o1" in m or "-o3" in m:
        return ("o4-mini", 0.75)
    if "gpt-4o" in m or "gpt-4.1" in m or "gpt-4-turbo" in m or m.startswith("gpt-4"):
        return ("gpt-4o-mini", 0.90)
    return None


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
            # Denormalized root flag stamped at ingest — replaces the old
            # whole-org ``root_trace_id NOT IN (SELECT trace_id …)`` scan. is_root
            # already encodes "own root OR orphan (phantom parent)". The
            # ``OR trace_id = root_trace_id`` is a cheap belt-and-suspenders: an
            # own-root span is a root by definition, so it must show even if a
            # self-referential parent_id stamped is_root=0 at ingest.
            where += " AND (t.is_root = 1 OR t.trace_id = t.root_trace_id)"
        if agent_key is not None:
            # Same agent-root definition as fetch_agent_summary, now via the
            # denormalized flag (with the own-root fallback) instead of a
            # per-load orphan-detection subquery.
            where += " AND (t.is_root = 1 OR t.trace_id = t.root_trace_id)"
            params["agent_key"] = agent_key
            if agent_kind in ("function", "chain", "langgraph_node"):
                # Match the denormalized column instead of re-extracting JSON;
                # pin agent_kind too so distinct kinds can't collide on a key.
                where += " AND t.agent_key = {agent_key:String} AND t.agent_kind = {agent_kind:String}"
                params["agent_kind"] = agent_kind
            elif agent_kind == "llm":
                where += " AND concat(c.provider, ':', c.model) = {agent_key:String}"
            else:
                where += " AND t.agent_key = {agent_key:String}"

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
        blocked_pred = "ifNull(event.status.:String,'') = 'blocked'"
        failed_pred = (
            "event.success IS NOT NULL"
            " AND ifNull(event.success.:Bool,false) = 0"
            " AND ifNull(event.status.:String,'') != 'blocked'"
        )
        if status == "running":
            where += " AND ifNull(t.event.status.:String,'') = 'running'"
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
                " AND ifNull(t.event.status.:String,'') NOT IN ('running', 'blocked')"
                " AND (NOT t.event.success IS NOT NULL"
                "   OR ifNull(t.event.success.:Bool,false) = 1)"
            )

        # Integration filter
        if integration != "all":
            where += " AND ifNull(t.event.integration.:String,'') = {integration:String}"
            params["integration"] = integration

        # Security-tab filter: pre-call blocks plus any scanned risk level. Kept
        # as a pre-join predicate (risky root_trace_ids from an org-scoped seek)
        # so the fast page-first path below stays usable; the joined-column
        # filters ("clean"/level) can't, because "no scan row" is only observable
        # after the LEFT JOIN.
        #
        # SUBTREE-AWARE, mirroring the blocked/failed status branches: match on
        # root_trace_id, not trace_id. Security scans run per span, so an agentic
        # attack is usually detected on a CHILD LLM/tool span (trace_id != root),
        # while the Security page lists roots (roots_only). Keying on t.trace_id
        # would miss every run whose detection isn't on the root span itself.
        if security == "flagged":
            where += (
                " AND (t.root_trace_id IN ("
                f"    SELECT DISTINCT root_trace_id FROM {target}"
                "     WHERE organization_id = {org_id:UUID}"
                f"       AND {blocked_pred}"
                "  ) OR t.root_trace_id IN ("
                f"    SELECT DISTINCT root_trace_id FROM {security_target}"
                "     WHERE organization_id = {org_id:UUID}"
                "       AND security_risk_level IN ('low','medium','high')"
                "  ))"
            )

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

        select_cols = (
            "t.api_key_prefix, t.event, t.ingested_at, "
            "c.total_cost, c.currency, "
            "e.metrics, e.scores, e.evaluators, e.judge_models, e.details_list, "
            "s.security_risk_level, s.security_risk_score, s.should_block, "
            "s.injection_detected, s.injection_patterns, "
            "s.jailbreak_detected, s.jailbreak_patterns, "
            "s.skeleton_key_detected, s.skeleton_key_patterns, "
            "s.secrets_detected, s.secret_types, "
            "s.indirect_injection_detected, s.indirect_injection_sources, "
            "s.rag_poisoning_detected, s.rag_poisoning_sources, s.rag_poisoning_score, "
            "s.tool_exfiltration_detected, s.tool_exfiltration_types, s.tool_exfiltration_sources, "
            "s.tool_policy_violation_detected, s.tool_policy_violations, "
            "s.cross_agent_injection_detected, "
            "s.image_injection_detected, s.image_injection_sources, "
            "s.semantic_attack_score, "
            "s.pii_entities_prompt, s.pii_entities_response, "
            "s.prompt_redacted, s.response_redacted, s.scan_latency"
        )
        # Org-wide joins: the eval subquery aggregates every eval row the org has
        # ever produced. Only the slow path (post-join filter / cost sort) needs
        # this shape, because it filters/sorts on the joined columns BEFORE the
        # LIMIT and so can't yet know which trace_ids survive to the page.
        joins = (
            f"LEFT JOIN {costs_target} AS c "
            f"  ON t.organization_id = c.organization_id AND t.trace_id = c.trace_id "
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
            f"  ON t.organization_id = e.organization_id AND t.trace_id = e.trace_id "
            f"LEFT JOIN {security_target} AS s "
            f"  ON t.organization_id = s.organization_id AND t.trace_id = s.trace_id "
        )

        # Page-scoped joins (fast path): every right-side relation is pre-filtered
        # to the <=LIMIT trace_ids already selected into the `page` CTE, so each
        # join seeks those rows along the (organization_id, trace_id) sort key
        # instead of scanning/hashing the whole org. Without this, the eval
        # subquery re-aggregated the org's ENTIRE evaluations table on every load
        # (and on each of the frontend's per-root prefetch calls) — the dominant
        # cost for orgs with a large accumulated history. The `IN (SELECT ...
        # FROM page)` references the CTE defined below, which is legal because
        # CTEs are visible to subqueries throughout the statement. Semantics are
        # identical to `joins`: the added predicate only narrows the right side by
        # the same keys the ON clause already requires.
        page_scoped_joins = (
            f"LEFT JOIN ("
            f"   SELECT organization_id, trace_id, total_cost, currency "
            f"   FROM {costs_target} "
            f"   WHERE organization_id = {{org_id:UUID}} "
            f"     AND trace_id IN (SELECT trace_id FROM page)"
            f") AS c "
            f"  ON t.organization_id = c.organization_id AND t.trace_id = c.trace_id "
            f"LEFT JOIN ("
            f"   SELECT organization_id, trace_id, "
            f"          groupArray(metric)            AS metrics, "
            f"          groupArray(score)             AS scores, "
            f"          groupArray(evaluator)         AS evaluators, "
            f"          groupArray(judge_model)       AS judge_models, "
            f"          groupArray(toString(details)) AS details_list "
            f"   FROM {evals_target} "
            f"   WHERE organization_id = {{org_id:UUID}} "
            f"     AND trace_id IN (SELECT trace_id FROM page) "
            f"   GROUP BY organization_id, trace_id"
            f") AS e "
            f"  ON t.organization_id = e.organization_id AND t.trace_id = e.trace_id "
            f"LEFT JOIN ("
            f"   SELECT * FROM {security_target} "
            f"   WHERE organization_id = {{org_id:UUID}} "
            f"     AND trace_id IN (SELECT trace_id FROM page)"
            f") AS s "
            f"  ON t.organization_id = s.organization_id AND t.trace_id = s.trace_id "
        )

        # Fast path (the default, unfiltered dashboard view): page the base table
        # FIRST — WHERE + ORDER + LIMIT over indexed traces columns — and only then
        # join and reconstruct the heavy `event` JSON for the <=LIMIT rows on the
        # page. Reconstructing `event` across the whole org's joined rows was the
        # dashboard's 15s+ hot spot; deferring it to the page cuts it to ~0.2s.
        #
        # Only valid when nothing after the LIMIT can change which rows qualify:
        # no post-join (security/quality) filters and a sort key that lives on the
        # traces table. Otherwise fall back to the full join-then-filter-then-limit
        # query so pagination stays correct.
        post_join_filter = f"{security_filter}{quality_filter}".strip()
        sort_on_join = sort in ("cost_desc", "cost_asc")
        if not post_join_filter and not sort_on_join:
            query = (
                f"WITH page AS ("
                f"  SELECT t.organization_id, t.trace_id, t.api_key_prefix, "
                f"         t.event, t.ingested_at "
                f"  FROM {target} AS t "
                f"  WHERE {where} "
                f"  ORDER BY {order_by} "
                f"  LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}"
                f") "
                f"SELECT {select_cols} "
                f"FROM page AS t {page_scoped_joins} "
                f"ORDER BY {order_by}"
            )
        else:
            query = (
                f"SELECT {select_cols} "
                f"FROM {target} AS t {joins} "
                f"WHERE {where}{security_filter}{quality_filter} "
                f"ORDER BY {order_by} "
                f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}"
            )

        result = await self._client.query(query, parameters=params)  # type: ignore[attr-defined]
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
    ifNull(event.cache_kind.:String,'')      AS kind,
    sum(ifNull(event.cache_hits.:Int64,0))    AS hits,
    sum(ifNull(event.cache_misses.:Int64,0))  AS misses,
    count()                                               AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'cache'
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        llm_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    lower(ifNull(event.integration.:String,'')) AS kind,
    countIf(ifNull(event.cache_hit.:Bool,false) = 1) AS hits,
    countIf(ifNull(event.cache_hit.:Bool,false) = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'llm'
  AND event.cache_hit IS NOT NULL
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        fn_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    ifNull(event.function.:String,'')             AS kind,
    countIf(ifNull(event.cache_hit.:Bool,false) = 1) AS hits,
    countIf(ifNull(event.cache_hit.:Bool,false) = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'function'
  AND event.cache_hit IS NOT NULL
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        vs_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    lower(ifNull(event.integration.:String,'')) AS kind,
    countIf(ifNull(event.cache_hit.:Bool,false) = 1) AS hits,
    countIf(ifNull(event.cache_hit.:Bool,false) = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'vectorstore'
  AND event.cache_hit IS NOT NULL
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY kind
""", parameters=params)

        mcp_result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    ifNull(event.kind.:String,'')                 AS kind,
    countIf(ifNull(event.cache_hit.:Bool,false) = 1) AS hits,
    countIf(ifNull(event.cache_hit.:Bool,false) = 0) AS misses,
    count()                                                    AS calls
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'mcp'
  AND event.cache_hit IS NOT NULL
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
    sum(ifNull(event.prompt_cache_read_tokens.:Int64,0))    AS anthropic_read,
    sum(ifNull(event.prompt_cache_creation_tokens.:Int64,0)) AS anthropic_creation,
    sum(ifNull(event.prompt_cached_tokens.:Int64,0))         AS provider_cached,
    count()                                                              AS calls,
    countIf(
        ifNull(event.prompt_cache_read_tokens.:Int64,0) > 0
        OR ifNull(event.prompt_cached_tokens.:Int64,0) > 0
    )                                                                    AS calls_with_hit
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'llm'
  AND (
        event.prompt_cache_read_tokens IS NOT NULL
     OR event.prompt_cached_tokens IS NOT NULL
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
        trace_id, root_trace_id, ingested_at,
        agent_key,
        agent_kind,
        ifNull(event.integration.:String,'') AS intg,
        event.latency.:Float64               AS latency
    FROM {target}
    WHERE organization_id = {{org_id:UUID}}
      -- Denormalized at ingest: is_root already encodes "own root OR orphan
      -- (phantom parent)", and agent_key is the function/name/langgraph_node.
      -- Replaces the old whole-org NOT IN scan + per-row JSON extraction. The
      -- own-root fallback guards against a self-referential parent_id (e.g.
      -- CrewAI's crew span) stamping is_root=0.
      AND (is_root = 1 OR trace_id = root_trace_id)
      AND agent_key != ''
),
costs AS (
    -- Precomputed per-run cost rollup (AggregatingMergeTree), read with the
    -- -Merge combinators. Replaces a full GROUP BY scan of {costs_target} on
    -- every Agents load; identical semantics (sum of the run's child costs).
    SELECT
        root_trace_id,
        sumMerge(run_cost)   AS run_cost,
        sumMerge(run_tokens) AS run_tokens
    FROM fluiq.trace_cost_rollup
    WHERE organization_id = {{org_id:UUID}}
    GROUP BY root_trace_id
)
SELECT
    agent_key                                                           AS agent_key,
    agent_kind                                                          AS agent_kind,
    intg                                                                AS integration,
    count()                                                             AS runs,
    sum(ifNull(c.run_cost, 0))                                         AS total_cost,
    sum(ifNull(c.run_cost, 0)) / count()                               AS avg_cost_per_run,
    toUInt64(sum(ifNull(c.run_tokens, 0)))                             AS total_tokens,
    avgIf(r.latency, r.latency > 0)                                    AS avg_latency,
    max(r.ingested_at)                                                  AS last_run
FROM roots AS r
-- Join on root_trace_id (not trace_id) so an orphan-root span still picks up
-- the cost bucket for its own subtree. For true roots the two are equal, so
-- this is a no-op there.
LEFT JOIN costs AS c ON r.root_trace_id = c.root_trace_id
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

    # ── Per-run rollups (root_trace_id) ────────────────────────────────────────

    async def get_root_rollups(
        self,
        organization_id: uuid.UUID,
        root_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Precomputed cost / quality / security aggregates per agent run.

        Reads the AggregatingMergeTree rollup tables (fed by materialized views
        off trace_costs / evaluations / security_scans) with the ``*Merge``
        combinators, so a root's totals come back without re-summing its
        children. Returns ``{root_trace_id: {...}}``; a root with no rollup row
        yet is simply absent, and the caller treats missing as zero / unscored.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        ids = [str(r) for r in root_ids if r]
        if not ids:
            return {}
        params = {"org_id": str(organization_id), "root_ids": ids}
        out: dict[str, dict[str, Any]] = {}

        def _slot(rid: str) -> dict[str, Any]:
            return out.setdefault(rid, {
                "run_cost": 0.0,
                "run_tokens": 0,
                "span_count": 0,
                "quality_min": None,
                "quality_avg": None,
                "quality_count": 0,
                "security_risk_max": 0.0,
                "security_should_block": False,
                "security_detections": 0,
            })

        cost = await self._client.query(  # type: ignore[attr-defined]
            "SELECT root_trace_id, sumMerge(run_cost), sumMerge(run_tokens) "
            "FROM fluiq.trace_cost_rollup "
            "WHERE organization_id = {org_id:UUID} "
            "  AND root_trace_id IN {root_ids:Array(UUID)} "
            "GROUP BY root_trace_id",
            parameters=params,
        )
        for rid, run_cost, run_tokens in cost.result_rows:
            slot = _slot(str(rid))
            slot["run_cost"] = float(run_cost) if run_cost is not None else 0.0
            slot["run_tokens"] = int(run_tokens or 0)

        cnt = await self._client.query(  # type: ignore[attr-defined]
            "SELECT root_trace_id, countMerge(span_count) "
            "FROM fluiq.trace_count_rollup "
            "WHERE organization_id = {org_id:UUID} "
            "  AND root_trace_id IN {root_ids:Array(UUID)} "
            "GROUP BY root_trace_id",
            parameters=params,
        )
        for rid, span_count in cnt.result_rows:
            _slot(str(rid))["span_count"] = int(span_count or 0)

        qual = await self._client.query(  # type: ignore[attr-defined]
            "SELECT root_trace_id, minMerge(quality_min), avgMerge(quality_avg), "
            "       countMerge(quality_count) "
            "FROM fluiq.trace_quality_rollup "
            "WHERE organization_id = {org_id:UUID} "
            "  AND root_trace_id IN {root_ids:Array(UUID)} "
            "GROUP BY root_trace_id",
            parameters=params,
        )
        for rid, qmin, qavg, qcount in qual.result_rows:
            slot = _slot(str(rid))
            slot["quality_min"] = float(qmin) if qmin is not None else None
            slot["quality_avg"] = float(qavg) if qavg is not None else None
            slot["quality_count"] = int(qcount or 0)

        sec = await self._client.query(  # type: ignore[attr-defined]
            "SELECT root_trace_id, maxMerge(risk_score_max), maxMerge(should_block_max), "
            "       sumMerge(detections) "
            "FROM fluiq.trace_security_rollup "
            "WHERE organization_id = {org_id:UUID} "
            "  AND root_trace_id IN {root_ids:Array(UUID)} "
            "GROUP BY root_trace_id",
            parameters=params,
        )
        for rid, risk, block, dets in sec.result_rows:
            slot = _slot(str(rid))
            slot["security_risk_max"] = float(risk) if risk is not None else 0.0
            slot["security_should_block"] = bool(block)
            slot["security_detections"] = int(dets or 0)

        return out

    # ── Dataset trajectory snapshots (no-TTL) ──────────────────────────────────

    async def insert_dataset_trajectory(
        self,
        organization_id: uuid.UUID,
        root_trace_id: uuid.UUID,
        events: list[dict[str, Any]],
    ) -> None:
        """Pin a run's spans into the no-TTL trajectory store (one row per span).

        Idempotent-ish: ReplacingMergeTree keyed on (org, root, trace_id) with a
        captured_at version, so re-pinning the same run replaces its rows.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        rows = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            tid = ev.get("trace_id") or str(uuid.uuid4())
            rows.append([str(organization_id), str(root_trace_id), str(tid), ev])
        if not rows:
            return
        await self._client.insert(  # type: ignore[attr-defined]
            "fluiq.dataset_trajectory_spans",
            rows,
            column_names=["org_id", "root_trace_id", "trace_id", "event"],
        )

    async def get_dataset_trajectory(
        self,
        organization_id: uuid.UUID,
        root_trace_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        """Return the pinned span events for a run, or [] if not snapshotted."""
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        result = await self._client.query(  # type: ignore[attr-defined]
            "SELECT toString(event) "
            "FROM fluiq.dataset_trajectory_spans FINAL "
            "WHERE org_id = {org:UUID} AND root_trace_id = {root:UUID} "
            "ORDER BY captured_at, trace_id",
            parameters={"org": str(organization_id), "root": str(root_trace_id)},
        )
        events: list[dict[str, Any]] = []
        for row in result.result_rows:
            try:
                ev = json.loads(row[0])
                if isinstance(ev, dict):
                    events.append(ev)
            except Exception:
                continue
        return events

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
    ifNull(event.model.:String,'') AS model,
    count()                                      AS call_count
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'llm'
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
            ifNull(event.model.:String,''),
            toString(event.messages)
        ) AS prompt_hash,
        count() AS call_count
    FROM {target}
    WHERE organization_id = {{org_id:UUID}}
      AND ifNull(event.type.:String,'') = 'llm'
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

    async def fetch_optimization_insights(
        self,
        organization_id: uuid.UUID,
        window_hours: int = 168,
        top_n: int = 8,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
    ) -> dict[str, Any]:
        """Developer-facing optimization insights over a time window.

        Returns, in one call: the most-repeated prompts (cache candidates) with
        projected savings, a cacheable-spend headline, the most expensive models
        and agents, the slowest models (p95), and error hotspots. All read-only
        over ``traces`` + ``trace_costs``; safe to cache.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target       = table       or self.default_table  # type: ignore[attr-defined]
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        p = {"org_id": str(organization_id), "window": int(window_hours), "top_n": int(top_n)}

        def f(v: Any) -> float:
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        # Per-LLM-call rows (model, prompt hash, preview, latency, cost) reused by
        # the repeated-prompt and cacheable-spend queries.
        # Prompt text is shape-agnostic: OpenAI/Anthropic use `messages`, Gemini
        # uses `contents`, LangChain/others use `input`/`prompts` — concat so the
        # same logical prompt hashes together regardless of provider shape.
        ptext = ("concat(toString(t.event.messages), toString(t.event.contents), "
                 "toString(t.event.input), toString(t.event.prompts))")
        inner = f"""
            SELECT
                cityHash64(ifNull(t.event.model.:String,''), {ptext}) AS phash,
                ifNull(t.event.model.:String,'')       AS model,
                substring({ptext}, 1, 240)             AS preview,
                t.event.latency.:Float64               AS latency,
                ifNull(c.cost, 0)                      AS cost
            FROM {target} AS t
            LEFT JOIN (
                SELECT trace_id, sum(total_cost) AS cost FROM {costs_target}
                WHERE organization_id = {{org_id:UUID}}
                  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
                GROUP BY trace_id
            ) AS c ON t.trace_id = c.trace_id
            WHERE t.organization_id = {{org_id:UUID}}
              AND ifNull(t.event.type.:String,'') = 'llm'
              AND t.ingested_at >= now() - toIntervalHour({{window:UInt32}})
        """

        # 1. Top repeated prompts (cache candidates).
        top_prompts_res = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT model, any(preview) AS preview, count() AS calls,
       avgIf(latency, latency > 0) AS avg_latency, sum(cost) AS total_cost
FROM ({inner})
GROUP BY phash, model
HAVING calls > 1
ORDER BY calls DESC, total_cost DESC
LIMIT {{top_n:UInt32}}
""", parameters=p)

        top_prompts = []
        for model, preview, calls, avg_latency, total_cost in top_prompts_res.result_rows:
            calls = int(calls or 0)
            tc = f(total_cost)
            top_prompts.append({
                "model": model or "",
                "preview": (preview or "").strip(),
                "calls": calls,
                "avg_latency": f(avg_latency),
                "total_cost": tc,
                # Serving all-but-one call from cache recovers (calls-1)/calls of the spend.
                "projected_savings": round(tc * (calls - 1) / calls, 6) if calls > 1 else 0.0,
            })

        # 2. Cacheable-spend headline.
        cacheable_res = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    sum(group_cost)                                          AS total_spend,
    sumIf(group_cost * (calls - 1) / calls, calls > 1)       AS recoverable_spend,
    sum(calls)                                               AS total_calls,
    sumIf(calls - 1, calls > 1)                              AS recoverable_calls
FROM (
    SELECT phash, count() AS calls, sum(cost) AS group_cost
    FROM ({inner})
    GROUP BY phash
)
""", parameters=p)
        cacheable = {"total_spend": 0.0, "recoverable_spend": 0.0, "total_calls": 0, "recoverable_calls": 0, "pct": 0.0}
        if cacheable_res.result_rows:
            ts, rs, tcalls, rcalls = cacheable_res.result_rows[0]
            total_spend = f(ts)
            cacheable = {
                "total_spend": total_spend,
                "recoverable_spend": f(rs),
                "total_calls": int(tcalls or 0),
                "recoverable_calls": int(rcalls or 0),
                "pct": round(f(rs) / total_spend, 4) if total_spend else 0.0,
            }

        # 3. Most expensive models.
        models_res = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT model, count() AS calls, sum(total_cost) AS cost,
       toUInt64(sum(input_tokens + cached_input_tokens + output_tokens)) AS tokens
FROM {costs_target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY model
ORDER BY cost DESC
LIMIT {{top_n:UInt32}}
""", parameters=p)
        top_models = [{
            "model": m or "", "calls": int(calls or 0), "total_cost": f(cost),
            "avg_cost": round(f(cost) / int(calls), 6) if calls else 0.0, "tokens": int(tokens or 0),
        } for m, calls, cost, tokens in models_res.result_rows]

        # 4. Most expensive agents (per-run cost grouped by denormalized agent_key).
        agents_res = await self._client.query(f"""  # type: ignore[attr-defined]
WITH roots AS (
    SELECT root_trace_id, agent_key, agent_kind, ifNull(event.integration.:String,'') AS integ
    FROM {target}
    WHERE organization_id = {{org_id:UUID}}
      AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
      AND (is_root = 1 OR trace_id = root_trace_id) AND agent_key != ''
),
run_costs AS (
    SELECT root_trace_id, sum(total_cost) AS c FROM {costs_target}
    WHERE organization_id = {{org_id:UUID}}
      AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
    GROUP BY root_trace_id
)
SELECT r.agent_key, any(r.agent_kind), any(r.integ), count() AS runs, sum(ifNull(rc.c, 0)) AS cost
FROM roots AS r LEFT JOIN run_costs AS rc ON r.root_trace_id = rc.root_trace_id
GROUP BY r.agent_key
ORDER BY cost DESC
LIMIT {{top_n:UInt32}}
""", parameters=p)
        top_agents = [{
            "agent_key": k or "", "agent_kind": kind or "", "integration": integ or "",
            "runs": int(runs or 0), "total_cost": f(cost),
            "avg_cost": round(f(cost) / int(runs), 6) if runs else 0.0,
        } for k, kind, integ, runs, cost in agents_res.result_rows]

        # 5. Slowest models (p50 / p95).
        slow_res = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT ifNull(event.model.:String,'') AS model, count() AS calls,
       quantile(0.5)(event.latency.:Float64)  AS p50,
       quantile(0.95)(event.latency.:Float64) AS p95
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ifNull(event.type.:String,'') = 'llm'
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
  AND event.latency.:Float64 > 0
GROUP BY model
HAVING calls >= 5
ORDER BY p95 DESC
LIMIT {{top_n:UInt32}}
""", parameters=p)
        slowest = [{
            "model": m or "", "calls": int(calls or 0), "p50": f(p50), "p95": f(p95),
        } for m, calls, p50, p95 in slow_res.result_rows]

        # 6. Error hotspots (by function/agent).
        errors_res = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    coalesce(nullIf(ifNull(event.function.:String,''), ''), agent_key, ifNull(event.integration.:String,'')) AS name,
    count() AS calls,
    countIf(ifNull(event.status.:String,'') = 'error' OR event.success.:Bool = false) AS errors
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
GROUP BY name
HAVING calls >= 5 AND errors > 0
ORDER BY errors / calls DESC, errors DESC
LIMIT {{top_n:UInt32}}
""", parameters=p)
        errors = [{
            "name": name or "", "calls": int(calls or 0), "errors": int(errs or 0),
            "error_rate": round(int(errs or 0) / int(calls), 4) if calls else 0.0,
        } for name, calls, errs in errors_res.result_rows]

        # 7. Model-downgrade hints — premium models used for small-output (simple)
        # completions, where a cheaper sibling would likely suffice. Detection is
        # in ClickHouse (small-output calls grouped by model); the premium→cheap
        # mapping + savings estimate is applied in Python so it's easy to tune.
        downgrade_res = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT model, count() AS calls, sum(total_cost) AS cost, avg(output_tokens) AS avg_out
FROM {costs_target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
  AND output_tokens > 0 AND output_tokens <= 256
GROUP BY model
HAVING calls >= 3
ORDER BY cost DESC
LIMIT 30
""", parameters=p)

        downgrades = []
        for model, calls, cost, avg_out in downgrade_res.result_rows:
            suggestion = _suggest_downgrade(model or "")
            if suggestion is None:
                continue
            cheaper, factor = suggestion
            spend = f(cost)
            downgrades.append({
                "model": model or "",
                "suggested": cheaper,
                "calls": int(calls or 0),
                "candidate_spend": spend,
                "est_savings": round(spend * factor, 6),
                "avg_output_tokens": round(f(avg_out), 1),
            })
        downgrades.sort(key=lambda d: d["est_savings"], reverse=True)
        downgrades = downgrades[:top_n]

        return {
            "window_hours": window_hours,
            "top_prompts": top_prompts,
            "cacheable": cacheable,
            "top_models": top_models,
            "top_agents": top_agents,
            "slowest": slowest,
            "errors": errors,
            "downgrades": downgrades,
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

    async def fetch_agentic_summary(
        self,
        organization_id: uuid.UUID,
        window_hours: int = 24,
        evals_table: Optional[str] = None,
    ) -> dict[str, Any]:
        """Aggregate agentic-eval health over a time window.

        Agentic evals write one row per layer per run (``evaluator =
        'fluiq.agent_eval'``). The ``deterministic``-layer row is the run
        representative — it carries the run-level ``run_score`` / ``run_passed``
        — so run counts and pass-rate key off that layer, while per-layer average
        scores are computed across all rows of each layer.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        params = {"org_id": str(organization_id), "window": int(window_hours)}
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT
    countIf(layer = 'deterministic')                       AS runs,
    avgIf(run_passed, layer = 'deterministic')             AS pass_rate,
    avgIf(run_score,  layer = 'deterministic')             AS avg_run_score,
    avgIf(score, layer = 'deterministic')                  AS det_score,
    avgIf(score, layer = 'tool_selection')                 AS tsq_score,
    countIf(layer = 'tool_selection')                      AS tsq_count,
    avgIf(score, layer = 'trajectory')                     AS traj_score,
    countIf(layer = 'trajectory')                          AS traj_count,
    avgIf(score, layer = 'coordination')                   AS coord_score,
    countIf(layer = 'coordination')                        AS coord_count
FROM {evals_target}
WHERE organization_id = {{org_id:UUID}}
  AND evaluator = 'fluiq.agent_eval'
  AND ingested_at >= now() - toIntervalHour({{window:UInt32}})
""", parameters=params)

        def _f(v: Any) -> Optional[float]:
            return float(v) if v is not None else None

        if not result.result_rows:
            return {"runs": 0, "pass_rate": None, "avg_run_score": None, "layers": []}

        (runs, pass_rate, avg_run_score,
         det_score, tsq_score, tsq_count, traj_score, traj_count,
         coord_score, coord_count) = result.result_rows[0]

        return {
            "runs":          int(runs or 0),
            "pass_rate":     _f(pass_rate),
            "avg_run_score": _f(avg_run_score),
            "layers": [
                {"layer": "deterministic",  "score": _f(det_score),   "count": int(runs or 0)},
                {"layer": "tool_selection", "score": _f(tsq_score),   "count": int(tsq_count or 0)},
                {"layer": "trajectory",     "score": _f(traj_score),  "count": int(traj_count or 0)},
                {"layer": "coordination",   "score": _f(coord_score), "count": int(coord_count or 0)},
            ],
        }

    async def fetch_dataset_eval_results(
        self,
        organization_id: uuid.UUID,
        trace_ids: list[str],
        evals_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Eval rows (standard metrics + agentic layers) for a set of trace ids.

        The per-item results a dataset *agentic-eval* run aggregates into its
        report — one row per (trace_id, metric/layer).
        """
        if not trace_ids:
            return []
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        params = {"org_id": str(organization_id), "tids": [str(t) for t in trace_ids]}
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT toString(trace_id) AS trace_id, evaluator, metric, score, layer, run_score, run_passed
FROM {evals_target}
WHERE organization_id = {{org_id:UUID}}
  AND trace_id IN {{tids:Array(UUID)}}
""", parameters=params)
        return [
            {
                "trace_id":   tid,
                "evaluator":  ev or "",
                "metric":     metric or "",
                "score":      float(score) if score is not None else None,
                "layer":      layer or "",
                "run_score":  float(run_score) if run_score is not None else None,
                "run_passed": bool(run_passed),
            }
            for tid, ev, metric, score, layer, run_score, run_passed in result.result_rows
        ]

    async def fetch_dataset_security_results(
        self,
        organization_id: uuid.UUID,
        trace_ids: list[str],
        table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Security-scan rows for a set of trace ids — the per-item results a
        dataset *security* run aggregates into its report."""
        if not trace_ids:
            return []
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        sec_target = table or config.CLICKHOUSE_SECURITY_TABLE
        params = {"org_id": str(organization_id), "tids": [str(t) for t in trace_ids]}
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT toString(trace_id) AS trace_id, security_risk_level, security_risk_score, should_block,
       injection_detected, jailbreak_detected, skeleton_key_detected, secrets_detected,
       indirect_injection_detected, rag_poisoning_detected, tool_exfiltration_detected,
       tool_policy_violation_detected, cross_agent_injection_detected, image_injection_detected
FROM {sec_target}
WHERE organization_id = {{org_id:UUID}}
  AND trace_id IN {{tids:Array(UUID)}}
""", parameters=params)
        cols = [
            "trace_id", "risk_level", "risk_score", "should_block",
            "injection", "jailbreak", "skeleton_key", "secrets",
            "indirect_injection", "rag_poisoning", "tool_exfiltration",
            "tool_policy_violation", "cross_agent_injection", "image_injection",
        ]
        out: list[dict[str, Any]] = []
        for row in result.result_rows:
            d = dict(zip(cols, row))
            d["risk_score"] = float(d["risk_score"]) if d["risk_score"] is not None else None
            for k in cols[3:]:
                d[k] = int(d[k] or 0)
            out.append(d)
        return out

    async def fetch_costs_for_traces(
        self,
        organization_id: uuid.UUID,
        trace_ids: list[str],
        costs_table: Optional[str] = None,
    ) -> dict[str, dict[str, Any]]:
        """Run-level cost per trace id (summed over the root's subtree), keyed by
        root_trace_id. Used to enrich dataset examples with their run cost."""
        if not trace_ids:
            return {}
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        params = {"org_id": str(organization_id), "tids": [str(t) for t in trace_ids]}
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT toString(root_trace_id) AS tid, sum(total_cost) AS cost, any(currency) AS cur
FROM {costs_target}
WHERE organization_id = {{org_id:UUID}}
  AND root_trace_id IN {{tids:Array(UUID)}}
GROUP BY root_trace_id
""", parameters=params)
        return {
            tid: {"cost": float(cost) if cost is not None else None, "currency": cur or "USD"}
            for tid, cost, cur in result.result_rows
        }

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

    async def insert_human_score(
        self,
        organization_id: uuid.UUID,
        trace_id: str,
        root_trace_id: str,
        evaluator: str,
        metric: str,
        score: float,
        details: dict[str, Any],
        api_key_prefix: str = "",
        table: Optional[str] = None,
    ) -> None:
        """One human signal (end-user feedback or a dashboard annotation) into
        the evaluations table, so it rides the same read paths as judge scores.
        The quality rollup MV excludes evaluator LIKE 'human.%'."""
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        evals_target = table or config.CLICKHOUSE_EVALUATIONS_TABLE
        await self._client.insert(  # type: ignore[attr-defined]
            evals_target,
            [[
                str(organization_id), api_key_prefix, trace_id, root_trace_id,
                evaluator, metric, float(score), "", details or {},
            ]],
            column_names=[
                "organization_id", "api_key_prefix", "trace_id", "root_trace_id",
                "evaluator", "metric", "score", "judge_model", "details",
            ],
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
