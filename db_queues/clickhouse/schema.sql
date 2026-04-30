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