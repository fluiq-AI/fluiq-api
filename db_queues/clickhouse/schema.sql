CREATE DATABASE IF NOT EXISTS fluiq;

CREATE TABLE IF NOT EXISTS fluiq.traces
(
    organization_id UUID,
    api_key_prefix  String,
    event           JSON,
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (organization_id, ingested_at);