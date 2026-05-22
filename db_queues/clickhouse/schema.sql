CREATE DATABASE IF NOT EXISTS fluiq;

CREATE TABLE IF NOT EXISTS fluiq.traces
(
    organization_id UUID,
    api_key_prefix  String,
    trace_id        UUID,
    root_trace_id   UUID,
    event           JSON,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, ingested_at);

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
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, trace_id, metric);

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