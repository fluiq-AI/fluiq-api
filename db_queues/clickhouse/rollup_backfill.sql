-- ── One-time backfill for the per-run rollup tables ─────────────────────────
--
-- The materialized views in schema.sql only capture rows inserted AFTER the MVs
-- are created. Run this ONCE, after the rollup tables + MVs exist, to populate
-- the aggregate state for all history that predates them.
--
-- ⚠️  RUN EXACTLY ONCE. Each statement inserts summed aggregate states; running
--     it again ADDS the historical totals a second time (AggregatingMergeTree
--     folds states by summing), inflating every run's cost/tokens/detections.
--     This file is deliberately NOT part of schema.sql (which re-runs on boot).
--
-- Safe to run while the MVs are live: a row inserted concurrently is captured by
-- either the MV or this backfill, not both, only if you cut over cleanly. The
-- simplest correct procedure on a low-traffic deploy is:
--   1) apply schema.sql (creates tables + MVs)
--   2) immediately run this backfill for rows with ingested_at < now()
-- Any tiny overlap window is negligible for a dashboard rollup; if you need it
-- exact, pause ingestion briefly around the cutover.

INSERT INTO fluiq.trace_cost_rollup
SELECT
    organization_id,
    root_trace_id,
    sumState(total_cost)                                                   AS run_cost,
    sumState(toUInt64(input_tokens + cached_input_tokens + output_tokens)) AS run_tokens,
    max(ingested_at)                                                       AS updated_at
FROM fluiq.trace_costs
GROUP BY organization_id, root_trace_id;

INSERT INTO fluiq.trace_quality_rollup
SELECT
    organization_id,
    root_trace_id,
    minState(score)  AS quality_min,
    avgState(score)  AS quality_avg,
    countState()     AS quality_count,
    max(ingested_at) AS updated_at
FROM fluiq.evaluations
WHERE evaluator != 'fluiq.security'
GROUP BY organization_id, root_trace_id;

INSERT INTO fluiq.trace_security_rollup
SELECT
    organization_id,
    root_trace_id,
    maxState(security_risk_score) AS risk_score_max,
    maxState(should_block)        AS should_block_max,
    sumState(toUInt64(
        injection_detected + jailbreak_detected + skeleton_key_detected +
        secrets_detected + indirect_injection_detected + rag_poisoning_detected +
        tool_exfiltration_detected + tool_policy_violation_detected +
        cross_agent_injection_detected + image_injection_detected
    ))                            AS detections,
    max(ingested_at)              AS updated_at
FROM fluiq.security_scans
GROUP BY organization_id, root_trace_id;

INSERT INTO fluiq.trace_count_rollup
SELECT
    organization_id,
    root_trace_id,
    countState()     AS span_count,
    max(ingested_at) AS updated_at
FROM fluiq.traces
GROUP BY organization_id, root_trace_id;
