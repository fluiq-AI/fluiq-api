-- ── Backfill for the denormalized agent-run columns on fluiq.traces ─────────
--
-- Populates is_root / agent_key / agent_kind for rows that predate the columns
-- (or were inserted before the tracer started stamping them). Run AFTER the
-- ADD COLUMN migrations in schema.sql, and BEFORE switching the API reads to
-- filter on is_root / agent_key (queries.py already does — so run this as part
-- of the same deploy, before the new API serves traffic).
--
-- Unlike rollup_backfill.sql, these are IDEMPOTENT: each ALTER … UPDATE
-- recomputes a value that's a pure function of existing columns, so re-running
-- is safe (just wasteful — mutations rewrite parts). They are kept out of
-- schema.sql so they don't re-mutate the whole table on every API boot.
--
-- ClickHouse mutations are asynchronous. Watch completion with:
--   SELECT * FROM system.mutations WHERE table = 'traces' AND is_done = 0;

-- agent_key / agent_kind — mirror the read-side multiIf(): function > name >
-- langgraph_node. Empty for spans carrying none (e.g. bare LLM calls).
ALTER TABLE fluiq.traces UPDATE
    agent_key = multiIf(
        ifNull(event.function.:String, '') != '', ifNull(event.function.:String, ''),
        ifNull(event.name.:String, '') != '',     ifNull(event.name.:String, ''),
        ifNull(event.langgraph.langgraph_node.:String, '')
    ),
    agent_kind = multiIf(
        ifNull(event.function.:String, '') != '', 'function',
        ifNull(event.name.:String, '') != '',     'chain',
        ifNull(event.langgraph.langgraph_node.:String, '') != '', 'langgraph_node',
        ''
    )
WHERE 1;

-- is_root — the column DEFAULTs to 1, so only resolved children need demoting:
-- a span is a child when it isn't its own root AND its root_trace_id points at
-- a persisted trace. Everything else (own-root or orphan/phantom parent) stays
-- 1, matching the resolver's ingest-time classification. trace_id is a UUID and
-- effectively unique across orgs, so the existence check needs no org filter.
ALTER TABLE fluiq.traces UPDATE is_root = 0
WHERE trace_id != root_trace_id
  AND root_trace_id IN (SELECT trace_id FROM fluiq.traces);
