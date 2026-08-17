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
        tags: Optional[list[str]] = None,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
        evaluations_table: Optional[str] = None,
        security_table: Optional[str] = None,
        tags_table: Optional[str] = None,
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
        if tags:
            # A pre-join predicate on the base table, deliberately: putting it
            # here keeps the fast path (page first, join after) valid, whereas a
            # join on the tags table would force every tag-filtered load down the
            # slow full-join branch.
            #
            # AND, not OR: two tags means "traces carrying both". Filters
            # everywhere else in this list narrow, and one that widened instead
            # would be a trap.
            tags_target = tags_table or config.CLICKHOUSE_TRACE_TAGS_TABLE
            for i, tag in enumerate(tags):
                where += (
                    f" AND t.trace_id IN ("
                    f"   SELECT trace_id FROM {tags_target} FINAL"
                    f"   WHERE organization_id = {{org_id:UUID}}"
                    f"     AND tag = {{tag_{i}:String}} AND deleted = 0"
                    f" )"
                )
                params[f"tag_{i}"] = tag
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

    async def count_security_scans(self, organization_id: uuid.UUID) -> int:
        return await self.count_rows(organization_id, config.CLICKHOUSE_SECURITY_TABLE)

    async def fetch_judge_usage(
        self,
        organization_id: uuid.UUID,
        days: int = 30,
    ) -> dict[str, Any]:
        """Judge-token spend for an org, in total and split by evaluator.

        Judge tokens are the only part of an evaluation whose cost varies —
        with trace size, jury size, and judge model — so this is what a
        usage-based price has to be built on. Row counts cannot substitute:
        a jury writes one row and makes three calls.

        ``judge_*`` columns hold the totals for a whole eval *message* on its
        first row, with later rows of the same message carrying zeros (a jury's
        calls are not divisible per metric). That makes SUM exact and any
        per-row or AVG reading meaningless — hence the aggregation here rather
        than in a caller.

        Returns tokens, not money: what a token costs is a pricing decision
        that changes, while the count is ground truth.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]

        table = config.CLICKHOUSE_EVALUATIONS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT evaluator, "
            f"       sum(judge_input_tokens)  AS input_tokens, "
            f"       sum(judge_output_tokens) AS output_tokens, "
            f"       sum(judge_calls)         AS judge_calls, "
            # Distinct messages, not rows: this is the billable unit.
            f"       uniqExact(trace_id)      AS eval_runs "
            f"FROM {table} "
            f"WHERE organization_id = {{org_id:UUID}} "
            f"  AND ingested_at >= now('UTC') - INTERVAL {{days:UInt32}} DAY "
            f"GROUP BY evaluator "
            f"ORDER BY input_tokens + output_tokens DESC",
            parameters={"org_id": str(organization_id), "days": int(days)},
        )

        by_evaluator = [
            {
                "evaluator":     row[0] or "",
                "input_tokens":  int(row[1] or 0),
                "output_tokens": int(row[2] or 0),
                "judge_calls":   int(row[3] or 0),
                "eval_runs":     int(row[4] or 0),
            }
            for row in (result.result_rows or [])
        ]
        return {
            "window_days":   int(days),
            "input_tokens":  sum(r["input_tokens"] for r in by_evaluator),
            "output_tokens": sum(r["output_tokens"] for r in by_evaluator),
            "judge_calls":   sum(r["judge_calls"] for r in by_evaluator),
            "eval_runs":     sum(r["eval_runs"] for r in by_evaluator),
            "by_evaluator":  by_evaluator,
        }

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

    # ── Trace tags ────────────────────────────────────────────────────────────

    async def add_trace_tags(
        self,
        organization_id: uuid.UUID,
        trace_id: uuid.UUID,
        tags: list[str],
        root_trace_id: Optional[uuid.UUID] = None,
        source: str = "dashboard",
        created_by: str = "",
        table: Optional[str] = None,
    ) -> None:
        """Attach tags to a trace. Idempotent — re-tagging replaces the row.

        Writes ``deleted = 0`` explicitly so re-adding a previously removed tag
        supersedes the tombstone instead of racing it.
        """
        if not tags:
            return
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or config.CLICKHOUSE_TRACE_TAGS_TABLE
        now = datetime.now(timezone.utc)
        rows = [
            [
                organization_id, trace_id, root_trace_id or trace_id,
                tag, source, created_by, 0, now,
            ]
            for tag in tags
        ]
        await self._client.insert(  # type: ignore[attr-defined]
            target, rows,
            column_names=[
                "organization_id", "trace_id", "root_trace_id",
                "tag", "source", "created_by", "deleted", "updated_at",
            ],
        )

    async def remove_trace_tag(
        self,
        organization_id: uuid.UUID,
        trace_id: uuid.UUID,
        tag: str,
        table: Optional[str] = None,
    ) -> None:
        """Untag by inserting a tombstone. ReplacingMergeTree collapses it onto
        the original row; reads filter ``deleted = 0`` after FINAL."""
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or config.CLICKHOUSE_TRACE_TAGS_TABLE
        await self._client.insert(  # type: ignore[attr-defined]
            target,
            [[organization_id, trace_id, trace_id, tag, "dashboard", "", 1,
              datetime.now(timezone.utc)]],
            column_names=[
                "organization_id", "trace_id", "root_trace_id",
                "tag", "source", "created_by", "deleted", "updated_at",
            ],
        )

    async def fetch_tags_for_traces(
        self,
        organization_id: uuid.UUID,
        trace_ids: list[str],
        table: Optional[str] = None,
    ) -> dict[str, list[str]]:
        """``{trace_id: [tag, …]}`` for a page of traces.

        Fetched separately rather than joined into the trace query: tags are a
        many-per-trace dimension, and joining them would either fan the page out
        or need another groupArray subquery in a query that already has two.
        """
        if not trace_ids:
            return {}
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or config.CLICKHOUSE_TRACE_TAGS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT toString(trace_id), tag FROM {target} FINAL "
            f"WHERE organization_id = {{org_id:UUID}} "
            f"  AND trace_id IN {{trace_ids:Array(UUID)}} "
            f"  AND deleted = 0",
            parameters={"org_id": str(organization_id), "trace_ids": trace_ids},
        )
        out: dict[str, list[str]] = {}
        for trace_id, tag in result.result_rows:
            out.setdefault(str(trace_id), []).append(tag)
        return out

    async def fetch_org_tags(
        self,
        organization_id: uuid.UUID,
        limit: int = 100,
        table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Every tag the org uses, with counts — for the filter's dropdown.

        Ordered by frequency: a tag applied to one trace by accident should not
        sit above the one applied to ten thousand on purpose.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or config.CLICKHOUSE_TRACE_TAGS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT tag, count() AS n FROM {target} FINAL "
            f"WHERE organization_id = {{org_id:UUID}} AND deleted = 0 "
            f"GROUP BY tag ORDER BY n DESC LIMIT {{limit:UInt32}}",
            parameters={"org_id": str(organization_id), "limit": int(limit)},
        )
        return [{"tag": r[0], "count": int(r[1] or 0)} for r in result.result_rows]

    # ── Review ────────────────────────────────────────────────────────────────

    async def set_review_flag(
        self,
        organization_id: uuid.UUID,
        trace_id: uuid.UUID,
        *,
        status: str = "open",
        reason: str = "manual",
        note: str = "",
        actor: str = "",
        root_trace_id: Optional[uuid.UUID] = None,
        table: Optional[str] = None,
    ) -> None:
        """Raise or clear a review flag. Idempotent — re-flagging replaces."""
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or config.CLICKHOUSE_REVIEW_FLAGS_TABLE
        await self._client.insert(  # type: ignore[attr-defined]
            target,
            [[
                organization_id, trace_id, root_trace_id or trace_id,
                status, reason, note,
                actor if status == "open" else "",
                actor if status == "resolved" else "",
                datetime.now(timezone.utc),
            ]],
            column_names=[
                "organization_id", "trace_id", "root_trace_id",
                "status", "reason", "note",
                "flagged_by", "resolved_by", "updated_at",
            ],
        )

    async def fetch_flags_for_traces(
        self,
        organization_id: uuid.UUID,
        trace_ids: list[str],
        table: Optional[str] = None,
    ) -> dict[str, dict[str, Any]]:
        """``{trace_id: {status, reason, note}}`` for a page of traces."""
        if not trace_ids:
            return {}
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = table or config.CLICKHOUSE_REVIEW_FLAGS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"SELECT toString(trace_id), status, reason, note FROM {target} FINAL "
            f"WHERE organization_id = {{org_id:UUID}} "
            f"  AND trace_id IN {{trace_ids:Array(UUID)}}",
            parameters={"org_id": str(organization_id), "trace_ids": trace_ids},
        )
        return {
            str(r[0]): {"status": r[1], "reason": r[2], "note": r[3]}
            for r in result.result_rows
        }

    async def fetch_review_queue(
        self,
        organization_id: uuid.UUID,
        hours: int = 168,
        limit: int = 100,
        offset: int = 0,
        source: str = "all",
        table: Optional[str] = None,
        evals_table: Optional[str] = None,
        flags_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Traces that want a human, worst first.

        Three things put a trace here, and the queue unions them rather than
        making the reviewer check three places:

          ``flagged``   someone raised it by hand
          ``feedback``  an end user said it was bad
          ``low_score`` a judge scored it below the review threshold

        Ordered by human verdict then judge score, so the rows a person already
        told us are bad lead — those are the ones with the most information in
        them, and the ones a reviewer can act on fastest.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target       = table       or self.default_table  # type: ignore[attr-defined]
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        flags_target = flags_table or config.CLICKHOUSE_REVIEW_FLAGS_TABLE

        # Built as a scored per-trace summary first, then filtered — the three
        # sources overlap constantly (a thumbs-down usually also scores badly),
        # and unioning row sets would double-count those.
        having = {
            "flagged":  "flag_status = 'open'",
            "feedback": "human_score IS NOT NULL AND human_score < 0.5",
            "low_score": "judge_score IS NOT NULL AND judge_score < 0.5",
        }.get(source) or (
            "flag_status = 'open' "
            "OR (human_score IS NOT NULL AND human_score < 0.5) "
            "OR (judge_score IS NOT NULL AND judge_score < 0.5)"
        )

        result = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT toString(t.trace_id)          AS trace_id,
       toString(t.root_trace_id)     AS root_trace_id,
       t.ingested_at                 AS ingested_at,
       JSONExtractString(toString(t.event), 'model')       AS model,
       JSONExtractString(toString(t.event), 'integration') AS integration,
       substring(JSONExtractString(toString(t.event), 'response'), 1, 300) AS preview,
       e.judge_score                 AS judge_score,
       e.human_score                 AS human_score,
       e.comment                     AS comment,
       f.status                      AS flag_status,
       f.reason                      AS flag_reason,
       f.note                        AS flag_note
FROM {target} AS t
LEFT JOIN (
    SELECT trace_id,
           avgIf(score, evaluator NOT LIKE 'human.%')  AS judge_score,
           avgIf(score, evaluator LIKE 'human.%')      AS human_score,
           anyIf(JSONExtractString(toString(details), 'comment'),
                 evaluator LIKE 'human.%')             AS comment
    FROM {evals_target}
    WHERE organization_id = {{org_id:UUID}}
    GROUP BY trace_id
) AS e ON t.trace_id = e.trace_id
LEFT JOIN (
    SELECT trace_id, status, reason, note FROM {flags_target} FINAL
    WHERE organization_id = {{org_id:UUID}}
) AS f ON t.trace_id = f.trace_id
WHERE t.organization_id = {{org_id:UUID}}
  AND t.ingested_at >= now() - toIntervalHour({{hours:UInt32}})
  AND (t.is_root = 1 OR t.trace_id = t.root_trace_id)
  AND ({having})
  AND (f.status != 'resolved' OR f.status = '')
ORDER BY human_score ASC NULLS LAST, judge_score ASC NULLS LAST, t.ingested_at DESC
LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}
""",
            parameters={
                "org_id": str(organization_id),
                "hours":  int(hours),
                "limit":  int(limit),
                "offset": int(offset),
            },
        )

        def _f(v: Any) -> Optional[float]:
            return float(v) if v is not None else None

        return [
            {
                "trace_id":      r[0],
                "root_trace_id": r[1],
                "ingested_at":   r[2].isoformat() if r[2] else None,
                "model":         r[3] or None,
                "integration":   r[4] or None,
                "preview":       r[5] or "",
                "judge_score":   _f(r[6]),
                "human_score":   _f(r[7]),
                "comment":       r[8] or "",
                "flag_status":   r[9] or "",
                "flag_reason":   r[10] or "",
                "flag_note":     r[11] or "",
            }
            for r in result.result_rows
        ]

    async def fetch_judge_agreement_pairs(
        self,
        organization_id: uuid.UUID,
        judge_metric: str,
        human_field: Optional[str] = None,
        hours: int = 720,
        limit: int = 2000,
        table: Optional[str] = None,
        evals_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Every trace where one judge metric and a human both left a score.

        This is the raw material for asking whether a judge is any good: pairs
        of (what the judge said, what a person said) about the same output.

        ``human_field`` names a rubric field. Left unset, every human annotation
        on the trace is averaged — right when the rubric is a single quality
        question, wrong when it holds several unrelated ones, so callers that
        know which field they mean should say so.

        Only traces carrying both sides come back. A judge score with no human
        label is not evidence about the judge, and silently treating an absent
        label as a zero would manufacture disagreement out of nothing.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target       = table       or self.default_table  # type: ignore[attr-defined]
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE

        # Restricting the human side to one field, when asked, happens inside the
        # aggregate rather than as a row filter: a WHERE would also drop the
        # judge rows on the same trace and leave nothing to pair with.
        human_expr = (
            "avgIf(score, evaluator LIKE 'human.%' AND metric = {human_field:String})"
            if human_field else
            "avgIf(score, evaluator LIKE 'human.%')"
        )
        human_count = (
            "countIf(evaluator LIKE 'human.%' AND metric = {human_field:String})"
            if human_field else
            "countIf(evaluator LIKE 'human.%')"
        )

        result = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT toString(e.trace_id) AS trace_id,
       e.judge              AS judge,
       e.human              AS human,
       e.comment            AS comment,
       substring(JSONExtractString(toString(t.event), 'response'), 1, 300) AS preview,
       substring(JSONExtractString(toString(t.event), 'prompt'), 1, 300)   AS input
FROM (
    SELECT trace_id,
           avgIf(score, evaluator NOT LIKE 'human.%'
                        AND metric = {{judge_metric:String}})  AS judge,
           {human_expr}                                        AS human,
           anyIf(JSONExtractString(toString(details), 'comment'),
                 evaluator LIKE 'human.%')                     AS comment
    FROM {evals_target}
    WHERE organization_id = {{org_id:UUID}}
      AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
    GROUP BY trace_id
    HAVING countIf(evaluator NOT LIKE 'human.%'
                   AND metric = {{judge_metric:String}}) > 0
       AND {human_count} > 0
) AS e
LEFT JOIN {target} AS t ON t.trace_id = e.trace_id
WHERE t.organization_id = {{org_id:UUID}}
LIMIT {{limit:UInt32}}
""",
            parameters={
                "org_id":       str(organization_id),
                "judge_metric": str(judge_metric),
                "human_field":  str(human_field or ""),
                "hours":        int(hours),
                "limit":        int(limit),
            },
        )
        return [
            {
                "trace_id": r[0],
                "judge":    float(r[1]) if r[1] is not None else None,
                "human":    float(r[2]) if r[2] is not None else None,
                "comment":  r[3] or "",
                "preview":  r[4] or "",
                "input":    r[5] or "",
            }
            for r in result.result_rows
        ]

    async def fetch_labelled_metrics(
        self,
        organization_id: uuid.UUID,
        hours: int = 720,
        table: Optional[str] = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Which judge metrics and rubric fields have enough overlap to compare.

        Offered so the UI can present only pairings that would actually produce
        an answer. Without it the obvious design is two free dropdowns, most of
        whose combinations return "0 examples compared" — an empty result that
        reads like a bug rather than like a choice that was never viable.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        evals_target = table or config.CLICKHOUSE_EVALUATIONS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT metric,
       evaluator LIKE 'human.%' AS is_human,
       count()                  AS scored,
       uniqExact(trace_id)      AS traces
FROM {evals_target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
  AND metric != ''
GROUP BY metric, is_human
ORDER BY traces DESC
LIMIT 200
""",
            parameters={"org_id": str(organization_id), "hours": int(hours)},
        )
        judges: list[dict[str, Any]] = []
        humans: list[dict[str, Any]] = []
        for metric, is_human, scored, traces in result.result_rows:
            row = {"metric": metric, "scored": int(scored), "traces": int(traces)}
            (humans if is_human else judges).append(row)
        return {"judge_metrics": judges, "human_fields": humans}

    async def fetch_review_matrix(
        self,
        organization_id: uuid.UUID,
        hours: int = 168,
        threshold: float = 0.5,
        evals_table: Optional[str] = None,
    ) -> dict[str, Any]:
        """Human verdict crossed with judge score, per trace.

        The diagnostic from the workshop (09:25–11:28): the interesting cells are
        the two where the human and the judge disagree, because those say the
        *eval* is wrong rather than the app. Counting them is the difference
        between "our scores went down" and knowing which of the two to fix.

        Only traces carrying both signals can be placed, so the response also
        reports how many were skipped — a matrix built from four traces should
        not be read as confidently as one built from four hundred.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT countIf(human_good AND judge_good)          AS agreed_good,
       countIf(human_good AND NOT judge_good)      AS judge_harsh,
       countIf(NOT human_good AND judge_good)      AS judge_lenient,
       countIf(NOT human_good AND NOT judge_good)  AS agreed_bad
FROM (
    SELECT trace_id,
           avgIf(score, evaluator LIKE 'human.%')     >= {{threshold:Float64}} AS human_good,
           avgIf(score, evaluator NOT LIKE 'human.%') >= {{threshold:Float64}} AS judge_good
    FROM {evals_target}
    WHERE organization_id = {{org_id:UUID}}
      AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
    GROUP BY trace_id
    HAVING countIf(evaluator LIKE 'human.%') > 0
       AND countIf(evaluator NOT LIKE 'human.%') > 0
)
""",
            parameters={
                "org_id":    str(organization_id),
                "hours":     int(hours),
                "threshold": float(threshold),
            },
        )
        row = result.result_rows[0] if result.result_rows else (0, 0, 0, 0)

        # How much of the window could not be placed, so the matrix can say what
        # it is a matrix *of* rather than implying it covers everything.
        coverage = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT countIf(has_human > 0 AND has_judge > 0) AS both,
       countIf(has_judge > 0 AND has_human = 0) AS judge_only,
       countIf(has_human > 0 AND has_judge = 0) AS human_only
FROM (
    SELECT trace_id,
           countIf(evaluator LIKE 'human.%')     AS has_human,
           countIf(evaluator NOT LIKE 'human.%') AS has_judge
    FROM {evals_target}
    WHERE organization_id = {{org_id:UUID}}
      AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
    GROUP BY trace_id
)
""",
            parameters={"org_id": str(organization_id), "hours": int(hours)},
        )
        cov = coverage.result_rows[0] if coverage.result_rows else (0, 0, 0)

        return {
            "agreed_good":   int(row[0] or 0),
            "judge_harsh":   int(row[1] or 0),
            "judge_lenient": int(row[2] or 0),
            "agreed_bad":    int(row[3] or 0),
            "coverage": {
                "both":       int(cov[0] or 0),
                "judge_only": int(cov[1] or 0),
                "human_only": int(cov[2] or 0),
            },
        }

    # ── Monitor (operational time series) ─────────────────────────────────────

    async def fetch_monitor_series(
        self,
        organization_id: uuid.UUID,
        hours: int = 72,
        bucket_minutes: int = 60,
        model: Optional[str] = None,
        table: Optional[str] = None,
        costs_table: Optional[str] = None,
        evals_table: Optional[str] = None,
    ) -> dict[str, Any]:
        """Operational metrics over time: volume, latency, spend, tokens, scores.

        Three separate grouped queries rather than one joined one. Joining
        traces to costs to evaluations would fan out — a trace with four eval
        rows would count four times toward latency and spend — and the fix
        (nested aggregation before the join) reads worse and runs slower than
        asking each table its own question. Each returns at most
        ``hours × 60 / bucket_minutes`` rows.

        Latency percentiles come from the event JSON, which is the only place a
        span's duration is recorded.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target       = table       or self.default_table  # type: ignore[attr-defined]
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        evals_target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE

        bucket = max(1, int(bucket_minutes))
        params = {
            "org_id": str(organization_id),
            "hours":  int(hours),
            "bucket": bucket,
        }
        # An empty model filter must not become `model = ''`, which would match
        # nothing rather than everything.
        model_filter = ""
        if model:
            params["model"] = model
            model_filter = " AND JSONExtractString(toString(event), 'model') = {model:String}"

        traffic = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT toString(toStartOfInterval(ingested_at, INTERVAL {{bucket:UInt32}} MINUTE)) AS bucket,
       count()                                                        AS spans,
       countIf(is_root = 1)                                           AS runs,
       quantile(0.5)(JSONExtractFloat(toString(event), 'latency'))    AS p50,
       quantile(0.95)(JSONExtractFloat(toString(event), 'latency'))   AS p95,
       countIf(JSONExtractBool(toString(event), 'success') = 0)       AS errors
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{hours:UInt32}}){model_filter}
GROUP BY bucket
ORDER BY bucket
""",
            parameters=params,
        )

        spend = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT toString(toStartOfInterval(ingested_at, INTERVAL {{bucket:UInt32}} MINUTE)) AS bucket,
       sum(total_cost)                        AS cost,
       sum(input_tokens + output_tokens)      AS tokens,
       sum(cached_input_tokens)               AS cached_tokens
FROM {costs_target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
GROUP BY bucket
ORDER BY bucket
""",
            parameters={k: v for k, v in params.items() if k != "model"},
        )

        # Human feedback rides in the same table under evaluator 'human.%', and
        # must not be averaged into an automated quality score — they answer
        # different questions and move for different reasons.
        scores = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT toString(toStartOfInterval(ingested_at, INTERVAL {{bucket:UInt32}} MINUTE)) AS bucket,
       avgIf(score, evaluator NOT LIKE 'human.%')     AS judge_score,
       countIf(evaluator NOT LIKE 'human.%')          AS judged,
       avgIf(score, evaluator = 'human.feedback')     AS feedback_score,
       countIf(evaluator = 'human.feedback')          AS feedback_count
FROM {evals_target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
GROUP BY bucket
ORDER BY bucket
""",
            parameters={k: v for k, v in params.items() if k != "model"},
        )

        def _f(value: Any) -> Optional[float]:
            return float(value) if value is not None else None

        return {
            "traffic": [
                {
                    "bucket": r[0], "spans": int(r[1] or 0), "runs": int(r[2] or 0),
                    "p50": _f(r[3]), "p95": _f(r[4]), "errors": int(r[5] or 0),
                }
                for r in traffic.result_rows
            ],
            "spend": [
                {
                    "bucket": r[0], "cost": float(r[1] or 0),
                    "tokens": int(r[2] or 0), "cached_tokens": int(r[3] or 0),
                }
                for r in spend.result_rows
            ],
            "scores": [
                {
                    "bucket": r[0], "judge_score": _f(r[1]), "judged": int(r[2] or 0),
                    "feedback_score": _f(r[3]), "feedback_count": int(r[4] or 0),
                }
                for r in scores.result_rows
            ],
        }

    async def fetch_monitor_breakdown(
        self,
        organization_id: uuid.UUID,
        hours: int = 72,
        limit: int = 8,
        costs_table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Per-model totals for the window: calls, tokens, spend.

        The companion to the time series: a line going up tells you something
        changed, and this tells you which model it was.
        """
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        costs_target = costs_table or config.CLICKHOUSE_TRACE_COSTS_TABLE
        result = await self._client.query(  # type: ignore[attr-defined]
            f"""
SELECT model,
       provider,
       count()                            AS calls,
       sum(input_tokens + output_tokens)  AS tokens,
       sum(total_cost)                    AS cost
FROM {costs_target}
WHERE organization_id = {{org_id:UUID}}
  AND ingested_at >= now() - toIntervalHour({{hours:UInt32}})
  AND model != ''
GROUP BY model, provider
ORDER BY cost DESC
LIMIT {{limit:UInt32}}
""",
            parameters={
                "org_id": str(organization_id),
                "hours":  int(hours),
                "limit":  int(limit),
            },
        )
        return [
            {
                "model": r[0] or "", "provider": r[1] or "",
                "calls": int(r[2] or 0), "tokens": int(r[3] or 0),
                "cost": float(r[4] or 0),
            }
            for r in result.result_rows
        ]

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
        # NB: do NOT alias `toString(trace_id) AS trace_id` — in ClickHouse the
        # SELECT alias shadows the raw UUID column in WHERE, so `trace_id IN
        # {tids:Array(UUID)}` would compare the stringified id to UUIDs and match
        # nothing. Left unaliased, WHERE binds the real column; the result is
        # unpacked positionally below.
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT toString(trace_id), evaluator, metric, score, layer, run_score, run_passed
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

    async def fetch_run_judge_usage(
        self,
        organization_id: uuid.UUID,
        trace_ids: list[str],
        evals_table: Optional[str] = None,
    ) -> dict[str, int]:
        """Actual judge spend for one dataset run, summed over its trace ids.

        Same contract as :meth:`fetch_judge_usage`: the ``judge_*`` columns carry
        a whole eval message's totals on its first row and zeros on the rest, so
        SUM is exact and per-row readings are meaningless.

        Returns counts, not money, matching the rest of the product: a token
        count is ground truth, while what it costs is a pricing decision.
        """
        empty = {"judge_calls": 0, "input_tokens": 0, "output_tokens": 0}
        if not trace_ids:
            return empty
        if self._client is None:  # type: ignore[attr-defined]
            await self.start()  # type: ignore[attr-defined]
        target = evals_table or config.CLICKHOUSE_EVALUATIONS_TABLE
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT sum(judge_calls)         AS judge_calls,
       sum(judge_input_tokens)  AS input_tokens,
       sum(judge_output_tokens) AS output_tokens
FROM {target}
WHERE organization_id = {{org_id:UUID}}
  AND trace_id IN {{tids:Array(UUID)}}
""", parameters={"org_id": str(organization_id), "tids": [str(t) for t in trace_ids]})
        rows = result.result_rows or []
        if not rows:
            return empty
        row = rows[0]
        return {
            "judge_calls":   int(row[0] or 0),
            "input_tokens":  int(row[1] or 0),
            "output_tokens": int(row[2] or 0),
        }

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
        # Unaliased trace_id — see fetch_dataset_eval_results: an `AS trace_id`
        # alias shadows the UUID column in WHERE and matches nothing.
        result = await self._client.query(f"""  # type: ignore[attr-defined]
SELECT toString(trace_id), security_risk_level, security_risk_score, should_block,
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
