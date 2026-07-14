CREATE DATABASE IF NOT EXISTS fluiq;

CREATE TABLE IF NOT EXISTS fluiq.traces
(
    organization_id UUID,
    api_key_prefix  String,
    trace_id        UUID,
    root_trace_id   UUID,
    event           JSON,
    -- Raw-trace retention window, stamped per row at ingest from the org's
    -- tier. Free = 14; paid = 36500 (~100y ≈ never). The DEFAULT is the
    -- "never" sentinel so any row inserted without an explicit value fails
    -- safe (kept, never silently deleted).
    retention_days  UInt16 DEFAULT 36500,
    -- Denormalized agent-run identity, stamped at ingest by the tracer so the
    -- read side skips a whole-org ``root_trace_id NOT IN (SELECT trace_id …)``
    -- scan (root detection) and JSON extraction (agent grouping).
    -- is_root: 1 for an agent-run root — its own root OR an orphan whose parent
    -- was never persisted; 0 for a resolved child. DEFAULT 1 biases unstamped
    -- rows toward visibility, matching the orphan-root philosophy.
    is_root         UInt8 DEFAULT 1,
    agent_key       String DEFAULT '',
    agent_kind      LowCardinality(String) DEFAULT '',
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, ingested_at)
-- Per-row TTL: Free traces roll off after their 14-day window; paid rows
-- carry the ~100y sentinel so they never expire. Observability is free and
-- unlimited on every tier — retention is the only thing that differs.
--
-- NOTE: keep this on DateTime64 (do NOT wrap in toDateTime()). toDateTime()
-- downcasts to 32-bit DateTime, whose max is year 2106, so the ~100y sentinel
-- (retention_days=36500) OVERFLOWS and wraps into the past → rows get deleted
-- almost immediately. DateTime64 spans to year 2299, so the sentinel lands at
-- ~2126 as intended.
TTL ingested_at + toIntervalDay(retention_days);

-- Migrations for existing deployments — per-row retention + TTL.
-- Must run BEFORE the tracer worker starts stamping retention_days by name.
ALTER TABLE fluiq.traces ADD COLUMN IF NOT EXISTS retention_days UInt16 DEFAULT 36500;
ALTER TABLE fluiq.traces MODIFY TTL ingested_at + toIntervalDay(retention_days);

-- Denormalized agent-run identity (see column comments above). Add BEFORE the
-- tracer deploys (it inserts these by name); backfill existing rows ONCE via
-- rollup_backfill.sql, and switch the API reads to is_root only AFTER backfill.
ALTER TABLE fluiq.traces ADD COLUMN IF NOT EXISTS is_root    UInt8 DEFAULT 1;
ALTER TABLE fluiq.traces ADD COLUMN IF NOT EXISTS agent_key  String DEFAULT '';
ALTER TABLE fluiq.traces ADD COLUMN IF NOT EXISTS agent_kind LowCardinality(String) DEFAULT '';

CREATE TABLE IF NOT EXISTS fluiq.trace_costs
(
    organization_id      UUID,
    api_key_prefix       String,
    trace_id             UUID,
    root_trace_id        UUID,
    provider             String,
    model                String,
    modality             String,
    input_tokens         UInt64,
    cached_input_tokens  UInt64,
    output_tokens        UInt64,
    input_cost           Decimal(18, 10),
    cached_input_cost    Decimal(18, 10),
    output_cost          Decimal(18, 10),
    total_cost           Decimal(18, 10),
    currency             String DEFAULT 'USD',
    long_context        UInt8 DEFAULT 0,
    ingested_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, ingested_at, trace_id);

CREATE TABLE IF NOT EXISTS fluiq.evaluations
(
    organization_id  UUID,
    api_key_prefix   String,
    trace_id         UUID,
    root_trace_id    UUID,
    evaluator        String,
    metric           String,
    score            Float32,
    judge_model      String DEFAULT '',
    details          JSON,
    -- Agentic eval fields. Non-agentic rows leave these at their defaults;
    -- agentic dashboards filter on evaluator = 'fluiq.agent_eval'.
    layer            LowCardinality(String) DEFAULT '',
    step_id          String DEFAULT '',
    run_score        Float32 DEFAULT 0,
    run_passed       UInt8 DEFAULT 1,
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, trace_id, metric);

-- Migrations for existing deployments — agentic evaluator fields (L1-L4).
ALTER TABLE fluiq.evaluations ADD COLUMN IF NOT EXISTS layer      LowCardinality(String) DEFAULT '';
ALTER TABLE fluiq.evaluations ADD COLUMN IF NOT EXISTS step_id    String  DEFAULT '';
ALTER TABLE fluiq.evaluations ADD COLUMN IF NOT EXISTS run_score  Float32 DEFAULT 0;
ALTER TABLE fluiq.evaluations ADD COLUMN IF NOT EXISTS run_passed UInt8   DEFAULT 1;

CREATE TABLE IF NOT EXISTS fluiq.security_scans
(
    organization_id              UUID,
    api_key_prefix               String,
    trace_id                     UUID,
    root_trace_id                UUID,
    mode                         LowCardinality(String) DEFAULT 'warn',
    prompt_redacted              String,
    response_redacted            String,
    pii_entities_prompt          Array(String),
    pii_entities_response        Array(String),
    injection_detected           UInt8,
    injection_patterns           Array(String),
    jailbreak_detected           UInt8,
    jailbreak_patterns           Array(String),
    skeleton_key_detected        UInt8,
    skeleton_key_patterns        Array(String),
    secrets_detected             UInt8,
    secret_types                 Array(String),
    indirect_injection_detected  UInt8,
    indirect_injection_sources   Array(String),
    rag_poisoning_detected         UInt8 DEFAULT 0,
    rag_poisoning_sources          Array(String),
    rag_poisoning_score            Float32 DEFAULT 0,
    tool_exfiltration_detected     UInt8 DEFAULT 0,
    tool_exfiltration_types        Array(String),
    tool_exfiltration_sources      Array(String),
    tool_policy_violation_detected UInt8 DEFAULT 0,
    tool_policy_violations         Array(String),
    cross_agent_injection_detected UInt8 DEFAULT 0,
    image_injection_detected       UInt8 DEFAULT 0,
    image_injection_sources        Array(String),
    semantic_attack_score        Float32,
    security_risk_level          LowCardinality(String),
    security_risk_score          Float32,
    should_block                 UInt8,
    scan_latency                 Float32,
    extra                        JSON,
    ingested_at                  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, trace_id, ingested_at);

-- Migrations for existing deployments — agentic-threat signals (A.2/B.2/B.3/C.1)
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS rag_poisoning_detected         UInt8 DEFAULT 0;
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS rag_poisoning_sources          Array(String);
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS rag_poisoning_score            Float32 DEFAULT 0;
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS tool_exfiltration_detected     UInt8 DEFAULT 0;
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS tool_exfiltration_types        Array(String);
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS tool_exfiltration_sources      Array(String);
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS tool_policy_violation_detected UInt8 DEFAULT 0;
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS tool_policy_violations         Array(String);
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS cross_agent_injection_detected UInt8 DEFAULT 0;
-- Image-embedded injection (found via OCR of image media)
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS image_injection_detected       UInt8 DEFAULT 0;
ALTER TABLE fluiq.security_scans ADD COLUMN IF NOT EXISTS image_injection_sources        Array(String);

-- ── Per-run rollups (root_trace_id) ──────────────────────────────────────────
-- Precomputed cost / quality / security aggregates per agent run, so the
-- dashboard shows a root's totals without re-summing its children on every read
-- (previously an org-wide GROUP BY on the Agents view and a per-root child
-- prefetch on the Traces list/drawer).
--
-- These are AggregatingMergeTree tables fed by materialized views off the three
-- source tables. Each child row inserted contributes a *partial aggregate
-- state*; ClickHouse folds states with the same (organization_id,
-- root_trace_id) key on background merge. That means the rollup updates
-- incrementally as children land — no "subtree finished" signal is needed, and
-- late / out-of-order spans just merge in and the total converges. Reads use the
-- matching ``*Merge`` combinators (see queries.get_root_rollups).
--
-- IMPORTANT: MVs only capture rows inserted AFTER they exist. Existing history
-- is populated ONCE via rollup_backfill.sql — do NOT put those INSERTs here,
-- since this file re-runs on every boot and would double-count the states.

CREATE TABLE IF NOT EXISTS fluiq.trace_cost_rollup
(
    organization_id UUID,
    root_trace_id   UUID,
    run_cost        AggregateFunction(sum, Decimal(18, 10)),
    run_tokens      AggregateFunction(sum, UInt64),
    updated_at      SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (organization_id, root_trace_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fluiq.mv_trace_cost_rollup
TO fluiq.trace_cost_rollup AS
SELECT
    organization_id,
    root_trace_id,
    sumState(total_cost)                                                   AS run_cost,
    sumState(toUInt64(input_tokens + cached_input_tokens + output_tokens)) AS run_tokens,
    max(ingested_at)                                                       AS updated_at
FROM fluiq.trace_costs
GROUP BY organization_id, root_trace_id;

CREATE TABLE IF NOT EXISTS fluiq.trace_quality_rollup
(
    organization_id UUID,
    root_trace_id   UUID,
    quality_min     AggregateFunction(min, Float32),
    quality_avg     AggregateFunction(avg, Float32),
    quality_count   AggregateFunction(count),
    updated_at      SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (organization_id, root_trace_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fluiq.mv_trace_quality_rollup
TO fluiq.trace_quality_rollup AS
SELECT
    organization_id,
    root_trace_id,
    minState(score)  AS quality_min,
    avgState(score)  AS quality_avg,
    countState()     AS quality_count,
    max(ingested_at) AS updated_at
FROM fluiq.evaluations
-- Exclude security-evaluator rows so quality mirrors the UI's minTraceScore,
-- which summarizes *quality* by its weakest metric and ignores fluiq.security
-- (security is surfaced separately via the security rollup).
WHERE evaluator != 'fluiq.security'
GROUP BY organization_id, root_trace_id;

CREATE TABLE IF NOT EXISTS fluiq.trace_security_rollup
(
    organization_id  UUID,
    root_trace_id    UUID,
    risk_score_max   AggregateFunction(max, Float32),
    should_block_max AggregateFunction(max, UInt8),
    detections       AggregateFunction(sum, UInt64),
    updated_at       SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (organization_id, root_trace_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fluiq.mv_trace_security_rollup
TO fluiq.trace_security_rollup AS
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

-- Span count per run — lets the Traces list show a root's child count without
-- pulling its children first. Sourced from fluiq.traces (running events are
-- never persisted, so one row == one real span).
CREATE TABLE IF NOT EXISTS fluiq.trace_count_rollup
(
    organization_id UUID,
    root_trace_id   UUID,
    span_count      AggregateFunction(count),
    updated_at      SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (organization_id, root_trace_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fluiq.mv_trace_count_rollup
TO fluiq.trace_count_rollup AS
SELECT
    organization_id,
    root_trace_id,
    countState()     AS span_count,
    max(ingested_at) AS updated_at
FROM fluiq.traces
GROUP BY organization_id, root_trace_id;

-- Pinned agent-run trajectories for datasets — a retention-independent copy of
-- a run's spans (one row per span), so a dataset example can drive full agentic
-- eval offline even after the source trace hits its TTL. Keyed by the run's
-- root_trace_id (shared across datasets/examples that reference the same run).
-- Media in the spans is offloaded to S3 (see shared.dataset_media); the stored
-- event carries an ``_media_ref.s3_key`` instead of the payload. NO TTL.
-- ReplacingMergeTree(captured_at) so re-import replaces a run's spans in place;
-- reads use FINAL to collapse to the latest capture.
CREATE TABLE IF NOT EXISTS fluiq.dataset_trajectory_spans
(
    org_id         UUID,
    root_trace_id  UUID,
    trace_id       UUID,
    event          JSON,
    captured_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(captured_at)
ORDER BY (org_id, root_trace_id, trace_id);

CREATE TABLE IF NOT EXISTS audit_log (
    event_id        UUID,
    organization_id String,
    actor           String,
    event_type      LowCardinality(String),
    http_method     LowCardinality(String),
    http_path       String,
    http_status     UInt16,
    ip_address      String,
    latency_ms      UInt32,
    metadata        String,
    row_hash        String,
    created_at      DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (organization_id, created_at)
TTL created_at + INTERVAL 10 YEAR;