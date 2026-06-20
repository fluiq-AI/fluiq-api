-- Run once against the fluiq database to provision auth tables.

CREATE TABLE IF NOT EXISTS organizations (
    org_id        UUID PRIMARY KEY,
    name          TEXT NOT NULL,
    user_id       UUID NOT NULL,
    team_ids      UUID[] NOT NULL DEFAULT '{}',
    api_keys      JSONB NOT NULL DEFAULT '[]'::jsonb,
    api_key_limit INTEGER NOT NULL DEFAULT 1,
    api_key_usage INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS users (
    user_id         UUID PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    hashed_password TEXT NOT NULL,
    name            TEXT NOT NULL,
    user_type       TEXT NOT NULL DEFAULT 'Free'
                    CHECK (user_type IN ('Free', 'Team', 'Growth', 'Enterprise')),
    org_id          UUID NOT NULL REFERENCES organizations(org_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_organizations_user_id ON organizations(user_id);

CREATE TABLE IF NOT EXISTS revoked_refresh_tokens (
    jti        UUID PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_revoked_refresh_tokens_expires_at
    ON revoked_refresh_tokens(expires_at);

CREATE TABLE IF NOT EXISTS password_resets (
    token_id   UUID PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    otp_hash   TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_password_resets_user_id
    ON password_resets(user_id);
CREATE INDEX IF NOT EXISTS idx_password_resets_expires_at
    ON password_resets(expires_at);

CREATE TABLE IF NOT EXISTS model_prices (
    id                                       BIGSERIAL PRIMARY KEY,
    provider                                 TEXT NOT NULL,
    model                                    TEXT NOT NULL,
    modality                                 TEXT NOT NULL,
    input_token_cost_per_million             NUMERIC,
    cached_input_token_cost_per_million      NUMERIC,
    output_token_cost_per_million            NUMERIC,
    long_context_consider_token_greater_than INTEGER,
    long_context_input_per_million           NUMERIC,
    long_context_cached_input_per_million    NUMERIC,
    long_context_output_per_million          NUMERIC,
    cache_valid_5_minutes_per_million        NUMERIC,
    cache_valid_60_minutes_per_million       NUMERIC,
    training_cost_per_hour                   NUMERIC,
    video_size                               TEXT,
    video_portrait                           TEXT,
    video_landscape                          TEXT,
    video_price_per_second                   NUMERIC,
    audio_price_per_second                   NUMERIC,
    created_at                               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_model_prices_provider_model
    ON model_prices(provider, model);
CREATE INDEX IF NOT EXISTS idx_model_prices_provider_model_modality
    ON model_prices(provider, model, modality);

ALTER TABLE users DROP CONSTRAINT users_user_type_check;

ALTER TABLE users ADD CONSTRAINT users_user_type_check CHECK (user_type IN ('Free', 'Starter', 'Team', 'Growth', 'Enterprise', 'Admin'));

CREATE TABLE IF NOT EXISTS prompts (
    prompt_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    slug        TEXT        NOT NULL,
    template    TEXT        NOT NULL,
    model       TEXT,
    variables   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    is_deployed BOOLEAN     NOT NULL DEFAULT FALSE,
    deployed_at TIMESTAMPTZ,
    version     INTEGER     NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ,
    UNIQUE (org_id, slug)
);
CREATE INDEX IF NOT EXISTS idx_prompts_org_id ON prompts(org_id);
CREATE INDEX IF NOT EXISTS idx_prompts_org_slug ON prompts(org_id, slug);

CREATE TABLE IF NOT EXISTS prompt_versions (
    version_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    prompt_id   UUID        NOT NULL REFERENCES prompts(prompt_id) ON DELETE CASCADE,
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    version     INTEGER     NOT NULL,
    name        TEXT        NOT NULL,
    template    TEXT        NOT NULL,
    model       TEXT,
    variables   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prompt_versions_prompt_id ON prompt_versions(prompt_id);
CREATE INDEX IF NOT EXISTS idx_prompt_versions_prompt_version ON prompt_versions(prompt_id, version);

-- Stores a snapshot of the prompt at the moment it was promoted to each named
-- environment. One row per (prompt, environment). On re-deploy the row is
-- replaced via ON CONFLICT DO UPDATE so the history stays in prompt_versions.
CREATE TABLE IF NOT EXISTS prompt_environments (
    env_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    prompt_id   UUID        NOT NULL REFERENCES prompts(prompt_id) ON DELETE CASCADE,
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    environment TEXT        NOT NULL CHECK (environment IN ('development', 'staging', 'production')),
    version     INTEGER     NOT NULL,
    template    TEXT        NOT NULL,
    model       TEXT,
    variables   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (prompt_id, environment)
);
CREATE INDEX IF NOT EXISTS idx_prompt_environments_prompt_id ON prompt_environments(prompt_id);
CREATE INDEX IF NOT EXISTS idx_prompt_environments_org_env  ON prompt_environments(org_id, environment);

-- Datasets: named collections of input/output pairs for evaluation
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ,
    UNIQUE (org_id, name)
);
CREATE INDEX IF NOT EXISTS idx_datasets_org_id ON datasets(org_id);

CREATE TABLE IF NOT EXISTS dataset_examples (
    example_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id      UUID        NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    org_id          UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    input           TEXT        NOT NULL,
    expected_output TEXT,
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dataset_examples_dataset_id ON dataset_examples(dataset_id);
CREATE INDEX IF NOT EXISTS idx_dataset_examples_org_id     ON dataset_examples(org_id);

-- Feedback collected when a user deletes their account.
-- user_id is NOT a FK so the record survives after the user row is removed.
CREATE TABLE IF NOT EXISTS account_deletion_feedback (
    feedback_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        NOT NULL,
    email       TEXT        NOT NULL,
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_deletion_feedback_created_at ON account_deletion_feedback(created_at);

-- Per-org guardrail policies — one row per (org, slug); "default" is the fallback
CREATE TABLE IF NOT EXISTS guardrail_policies (
    org_id            UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    slug              TEXT        NOT NULL DEFAULT 'default'
                      CHECK (slug ~ '^[a-z0-9][a-z0-9\-_]{0,62}$'),
    block_threshold   TEXT        NOT NULL DEFAULT 'high'
                      CHECK (block_threshold IN ('medium', 'high')),
    warn_threshold    TEXT        NOT NULL DEFAULT 'medium'
                      CHECK (warn_threshold IN ('low', 'medium', 'high')),
    block_categories  TEXT[]      NOT NULL DEFAULT '{}',
    custom_deny_list  TEXT[]      NOT NULL DEFAULT '{}',
    custom_allow_list TEXT[]      NOT NULL DEFAULT '{}',
    pii_ignore        TEXT[]      NOT NULL DEFAULT '{}',
    allowed_tools     TEXT[]      NOT NULL DEFAULT '{}',
    alert_webhook     TEXT,
    alert_on          TEXT[]      NOT NULL DEFAULT '{high}',
    scan_responses    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ,
    PRIMARY KEY (org_id, slug)
);
-- Migrations for existing deployments
ALTER TABLE guardrail_policies ADD COLUMN IF NOT EXISTS slug          TEXT    NOT NULL DEFAULT 'default';
ALTER TABLE guardrail_policies ADD COLUMN IF NOT EXISTS scan_responses BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE guardrail_policies ADD COLUMN IF NOT EXISTS pii_ignore    TEXT[]  NOT NULL DEFAULT '{}';
ALTER TABLE guardrail_policies ADD COLUMN IF NOT EXISTS allowed_tools TEXT[]  NOT NULL DEFAULT '{}';
ALTER TABLE guardrail_policies DROP CONSTRAINT IF EXISTS guardrail_policies_pkey;
ALTER TABLE guardrail_policies ADD CONSTRAINT guardrail_policies_pkey PRIMARY KEY (org_id, slug);

-- Per-org alert settings — one row per org. Drives the Slack alert dispatcher
-- that watches enriched eval / security events. The webhook is stored as-is;
-- delivery is server-side only (the URL is never returned to non-owners).
CREATE TABLE IF NOT EXISTS alert_settings (
    org_id                  UUID        PRIMARY KEY REFERENCES organizations(org_id) ON DELETE CASCADE,
    slack_webhook           TEXT,
    digest                  TEXT        NOT NULL DEFAULT 'realtime'
                            CHECK (digest IN ('realtime', 'hourly', 'daily')),
    -- Eval alerts
    eval_enabled            BOOLEAN     NOT NULL DEFAULT FALSE,
    eval_metrics            TEXT[]      NOT NULL DEFAULT '{}',
    eval_score_below        DOUBLE PRECISION NOT NULL DEFAULT 0.7,
    eval_failure_rate_above DOUBLE PRECISION NOT NULL DEFAULT 10,
    -- Security alerts
    security_enabled        BOOLEAN     NOT NULL DEFAULT FALSE,
    security_alert_on       TEXT[]      NOT NULL DEFAULT '{high}',
    security_categories     TEXT[]      NOT NULL DEFAULT '{}',
    security_blocked_only   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ
);

-- Blog: marketing/content posts authored in the admin panel (WYSIWYG -> HTML).
-- Platform-wide content (not org-scoped); only Admin users can write.
CREATE TABLE IF NOT EXISTS blog_posts (
    post_id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT        NOT NULL UNIQUE
                    CHECK (slug ~ '^[a-z0-9][a-z0-9\-]{0,126}$'),
    title           TEXT        NOT NULL,
    excerpt         TEXT        NOT NULL DEFAULT '',
    body_html       TEXT        NOT NULL DEFAULT '',
    cover_image_url TEXT,
    author          TEXT        NOT NULL DEFAULT 'Fluiq',
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'published')),
    seo_title       TEXT,
    seo_description TEXT,
    reading_minutes INTEGER     NOT NULL DEFAULT 1,
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_blog_posts_status_pub
    ON blog_posts(status, published_at DESC);

-- Blog media stored in a private S3 bucket; only the object key lives here.
-- The public media endpoint redirects to a short-lived presigned GET URL.
CREATE TABLE IF NOT EXISTS blog_media (
    media_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    filename     TEXT        NOT NULL,
    content_type TEXT        NOT NULL,
    s3_key       TEXT        NOT NULL,
    byte_size    INTEGER     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);