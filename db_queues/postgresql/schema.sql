-- Run once against the fluiq database to provision auth tables.

CREATE TABLE IF NOT EXISTS organizations (
    org_id           UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    user_id          UUID NOT NULL,
    team_ids         UUID[] NOT NULL DEFAULT '{}',
    api_keys         JSONB NOT NULL DEFAULT '[]'::jsonb,
    api_key_limit    INTEGER NOT NULL DEFAULT 1,
    api_key_usage    INTEGER NOT NULL DEFAULT 0,
    -- Admin-granted adjustment (in evaluations) added on top of the tier's
    -- monthly evaluation quota. May be negative to deduct allowance. The
    -- effective monthly eval cap is max(0, tier_quota + eval_quota_bonus).
    eval_quota_bonus INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ
);

-- Migration for existing deployments (no-op once the column exists):
ALTER TABLE organizations
    ADD COLUMN IF NOT EXISTS eval_quota_bonus INTEGER NOT NULL DEFAULT 0;

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

-- Self-serve 5-day trial of a paid plan (Team / Growth), no card required.
-- ``trial_ends_at`` is the expiry instant while a trial is active (NULL
-- otherwise); ``get_org_tier`` lazily downgrades to Free once it passes.
-- ``trial_used`` is a one-shot latch so a single account can't loop trials.
ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_ends_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_used    BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS prompts (
    prompt_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    slug        TEXT        NOT NULL,
    template    TEXT        NOT NULL,
    model       TEXT,
    variables   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    -- 'completion' = ordinary prompt template (fetched via fluiq.fetch_prompt).
    -- 'judge'      = an LLM-as-judge prompt the client references by slug in
    --                fluiq.eval(custom_judges={...}). Judge templates use
    --                string.Template $question/$answer/$context placeholders and
    --                are expected to return {"score": float, "reason": str}.
    kind        TEXT        NOT NULL DEFAULT 'completion'
                CHECK (kind IN ('completion', 'judge')),
    is_deployed BOOLEAN     NOT NULL DEFAULT FALSE,
    deployed_at TIMESTAMPTZ,
    version     INTEGER     NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ,
    UNIQUE (org_id, slug)
);
CREATE INDEX IF NOT EXISTS idx_prompts_org_id ON prompts(org_id);
CREATE INDEX IF NOT EXISTS idx_prompts_org_slug ON prompts(org_id, slug);
-- Migration for existing deployments
ALTER TABLE prompts ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'completion';

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
    -- 'single'  = single-prompt dataset (input/output + custom scorer).
    -- 'agentic' = full-trajectory dataset (all inputs/outputs/tools/MCP calls).
    kind        TEXT        NOT NULL DEFAULT 'agentic',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ,
    UNIQUE (org_id, name)
);
CREATE INDEX IF NOT EXISTS idx_datasets_org_id ON datasets(org_id);
-- Existing datasets predate typing and are trajectory-capable → default 'agentic'.
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'agentic';

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

-- Batch jobs that evaluate (or security-scan) an entire dataset. One row per
-- launch; per-example progress/results live in dataset_run_items. Results
-- themselves are produced by the eval/security workers into ClickHouse (keyed
-- by trace_id) and aggregated back into `summary` on read.
CREATE TABLE IF NOT EXISTS dataset_runs (
    run_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id  UUID        NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    kind        TEXT        NOT NULL,                       -- 'agentic' | 'security'
    depth       TEXT,                                        -- agentic depth, when kind='agentic'
    status      TEXT        NOT NULL DEFAULT 'running',      -- 'running' | 'complete' | 'failed'
    total       INT         NOT NULL DEFAULT 0,
    summary     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dataset_runs_dataset ON dataset_runs(dataset_id, created_at DESC);
-- Judge model ("provider:model") this run graded with. Multi-model comparison
-- launches one run per model, so this is what labels the columns in the report.
ALTER TABLE dataset_runs ADD COLUMN IF NOT EXISTS model TEXT;
-- Groups the runs launched together by one multi-model comparison, so the UI
-- can tell when every model in a batch has finished.
ALTER TABLE dataset_runs ADD COLUMN IF NOT EXISTS batch_id UUID;
CREATE INDEX IF NOT EXISTS idx_dataset_runs_batch ON dataset_runs(batch_id);

-- One row per example enrolled in a run. `trace_id` is the id the job was
-- published under (the example's source trace, or a synthesized id for
-- text-only examples); the report joins ClickHouse results on it. `source`
-- records whether we re-ran the real trace or a synthetic text event.
CREATE TABLE IF NOT EXISTS dataset_run_items (
    item_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID        NOT NULL REFERENCES dataset_runs(run_id) ON DELETE CASCADE,
    example_id  UUID        NOT NULL,
    org_id      UUID        NOT NULL,
    trace_id    TEXT        NOT NULL,
    source      TEXT        NOT NULL DEFAULT 'trace',        -- 'trace' | 'text'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dataset_run_items_run ON dataset_run_items(run_id);

-- Agents linked to a dataset so their future runs are auto-appended as examples
-- (the import of past runs happens immediately at link time, client-side).
CREATE TABLE IF NOT EXISTS dataset_agent_links (
    dataset_id  UUID        NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    agent_key   TEXT        NOT NULL,
    agent_kind  TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, agent_key, agent_kind)
);
CREATE INDEX IF NOT EXISTS idx_dataset_agent_links_agent ON dataset_agent_links(org_id, agent_key, agent_kind);

-- Custom scorers (client-defined LLM-as-judge prompts) saved to a dataset, so a
-- dataset remembers its own scorers for future metrics runs. The prompt itself
-- lives in the org-wide prompt library (prompts, kind='judge') and is reusable
-- across datasets; this table just links a slug + threshold to the dataset.
CREATE TABLE IF NOT EXISTS dataset_scorers (
    dataset_id  UUID             NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    org_id      UUID             NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    slug        TEXT             NOT NULL,
    threshold   DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, slug)
);
CREATE INDEX IF NOT EXISTS idx_dataset_scorers_dataset ON dataset_scorers(dataset_id);

-- Per-dataset LLM-as-Judge prompt overrides. Lets two datasets grade the same
-- built-in metric with different prompts (e.g. hallucination tuned per domain).
-- Sent with the run as eval_config.judge_prompt_overrides; the evaluator applies
-- them above the org → platform → default chain for that message only.
CREATE TABLE IF NOT EXISTS dataset_judge_prompts (
    dataset_id  UUID        NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    template    TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_id, name)
);
CREATE INDEX IF NOT EXISTS idx_dataset_judge_prompts_dataset ON dataset_judge_prompts(dataset_id);

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

-- Visitor submissions from the public LLM cost calculator.
CREATE TABLE IF NOT EXISTS model_requests (
    id         BIGSERIAL   PRIMARY KEY,
    provider   TEXT,
    model      TEXT        NOT NULL,
    email      TEXT,
    note       TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_price_reports (
    id              BIGSERIAL   PRIMARY KEY,
    model_price_id  BIGINT,
    provider        TEXT,
    model           TEXT        NOT NULL,
    reported_input  NUMERIC,
    reported_output NUMERIC,
    source_url      TEXT,
    email           TEXT,
    note            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- LLM-as-Judge prompts used by the evaluator worker.
--
-- Platform-global (NOT org-scoped): these are Fluiq's own internal judge
-- prompts, edited from the Admin console so they can be reworked without a
-- worker redeploy. The evaluator seeds the canonical defaults on startup
-- (ON CONFLICT DO NOTHING so admin edits are never clobbered) and reads the
-- effective ``template`` with a TTL cache, failing open to its built-in
-- constants if a row is missing or a template is invalid.
--
--   name             stable key the worker renders by (e.g. 'hallucination_verify')
--   template         effective template ($-placeholders, string.Template syntax)
--   default_template pristine seed copy, used by "Reset to default"
--   required_vars    JSON array of placeholder names that MUST appear; the API
--                    rejects an edit that drops any of them
--   is_overridden    TRUE once an admin has edited it away from the default
CREATE TABLE IF NOT EXISTS eval_judge_prompts (
    name             TEXT        PRIMARY KEY,
    template         TEXT        NOT NULL,
    default_template TEXT        NOT NULL,
    description      TEXT,
    required_vars    JSONB       NOT NULL DEFAULT '[]'::jsonb,
    is_overridden    BOOLEAN     NOT NULL DEFAULT FALSE,
    version          INTEGER     NOT NULL DEFAULT 1,
    updated_by       UUID,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Immutable history: one row per saved version, for rollback.
CREATE TABLE IF NOT EXISTS eval_judge_prompt_versions (
    version_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL REFERENCES eval_judge_prompts(name) ON DELETE CASCADE,
    version     INTEGER     NOT NULL,
    template    TEXT        NOT NULL,
    updated_by  UUID,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_eval_judge_prompt_versions_name
    ON eval_judge_prompt_versions(name, version);

-- Per-organization judge-prompt overrides: a customer edit of a built-in
-- judge prompt, visible only to that org. Resolution order in the evaluator is
-- org override → platform template (eval_judge_prompts) → code default, so
-- deleting a row here reverts the org to the platform prompt. The API rejects
-- an edit that drops any required_vars of the underlying prompt.
CREATE TABLE IF NOT EXISTS eval_judge_prompt_org_overrides (
    org_id     UUID        NOT NULL REFERENCES organizations(org_id)   ON DELETE CASCADE,
    name       TEXT        NOT NULL REFERENCES eval_judge_prompts(name) ON DELETE CASCADE,
    template   TEXT        NOT NULL,
    version    INTEGER     NOT NULL DEFAULT 1,
    updated_by UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, name)
);

-- Immutable per-org history, for rollback. Survives deletion of the live
-- override so "restore" works after a reset.
CREATE TABLE IF NOT EXISTS eval_judge_prompt_org_versions (
    version_id  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID        NOT NULL,
    name        TEXT        NOT NULL,
    version     INTEGER     NOT NULL,
    template    TEXT        NOT NULL,
    updated_by  UUID,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_eval_judge_prompt_org_versions
    ON eval_judge_prompt_org_versions(org_id, name, version);

-- ---------------------------------------------------------------------------
-- Multi-user organizations: team membership + email invitations.
--
-- Historically an org had exactly one user (organizations.user_id = the owner,
-- users.org_id = that user's only org). ``organization_members`` is now the
-- source of truth for "who may access which org"; users.org_id remains the
-- user's *current/active* org (what the JWT is minted with) and
-- organizations.user_id remains the *owner* (whose users.user_type drives the
-- org's plan unless organizations.plan_tier overrides it).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS organization_members (
    org_id     UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    user_id    UUID        NOT NULL REFERENCES users(user_id)        ON DELETE CASCADE,
    role       TEXT        NOT NULL DEFAULT 'member'
               CHECK (role IN ('owner', 'admin', 'member')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_org_members_user ON organization_members(user_id);

-- One-time backfill for pre-existing deployments: every org's owner becomes an
-- 'owner' member. Idempotent via ON CONFLICT.
INSERT INTO organization_members (org_id, user_id, role)
SELECT org_id, user_id, 'owner' FROM organizations
ON CONFLICT DO NOTHING;

-- Tokenized email invitations. The raw token is only ever in the accept link
-- (emailed once); we persist its SHA-256 like password-reset OTPs / API keys.
CREATE TABLE IF NOT EXISTS organization_invitations (
    invite_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    email       TEXT        NOT NULL,
    role        TEXT        NOT NULL DEFAULT 'member'
                CHECK (role IN ('admin', 'member')),
    token_hash  TEXT        NOT NULL,
    invited_by  UUID,
    status      TEXT        NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'accepted', 'revoked')),
    expires_at  TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_org_invites_org   ON organization_invitations(org_id, status);
CREATE INDEX IF NOT EXISTS idx_org_invites_email ON organization_invitations(lower(email), status);

-- Per-org plan override. NULL => fall back to the owner's users.user_type (the
-- legacy path, so existing single-user orgs are unchanged). A secondary org a
-- user creates is stamped 'Free' here so it can be upgraded independently of
-- any paid plan the creator holds on their home org.
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS plan_tier TEXT;
-- ── Customer provider credentials (BYOK) ─────────────────────────────────────
-- Envelope-encrypted OpenAI/Anthropic/etc. keys supplied by the customer, used
-- to run their judge calls on their own provider account. See shared/crypto.py.
--
-- What is NOT here, deliberately: the plaintext key, and the plaintext data key
-- that would decrypt it. `ciphertext` is AES-256-GCM under a per-credential DEK
-- that only KMS can unwrap, with the org id bound in as additional
-- authenticated data — so a row lifted into another org's context fails its tag
-- check rather than decrypting. A leaked snapshot of this table is inert.
--
-- `fingerprint` is sha256(plaintext)[:16]: it dedupes re-pasted keys and scopes
-- caches without being reversible. `last4` is display-only.
CREATE TABLE IF NOT EXISTS org_provider_credentials (
    credential_id    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id           UUID        NOT NULL REFERENCES organizations(org_id) ON DELETE CASCADE,
    provider         TEXT        NOT NULL
                     CHECK (provider IN ('openai', 'anthropic', 'gemini', 'moonshot', 'azure_openai', 'bedrock')),
    label            TEXT,
    ciphertext       BYTEA       NOT NULL,
    nonce            BYTEA       NOT NULL,
    wrapped_dek      BYTEA       NOT NULL,
    key_version      INTEGER     NOT NULL DEFAULT 1,
    last4            TEXT        NOT NULL,
    fingerprint      TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'active'
                     CHECK (status IN ('active', 'invalid', 'revoked')),
    last_verified_at TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Re-pasting the same key is an update, not a second row.
    UNIQUE (org_id, provider, fingerprint)
);
-- The evaluator's hot path: "the active key for this org+provider".
CREATE INDEX IF NOT EXISTS idx_org_provider_credentials_lookup
    ON org_provider_credentials(org_id, provider, status);

-- CREATE TABLE above only applies to fresh databases, so widen the provider
-- list on any deployment that already created the table.
ALTER TABLE org_provider_credentials DROP CONSTRAINT IF EXISTS org_provider_credentials_provider_check;
ALTER TABLE org_provider_credentials ADD CONSTRAINT org_provider_credentials_provider_check
    CHECK (provider IN ('openai', 'anthropic', 'gemini', 'moonshot', 'azure_openai', 'bedrock'));
