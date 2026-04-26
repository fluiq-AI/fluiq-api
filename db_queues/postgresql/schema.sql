-- Run once against the fluiq database to provision auth tables.

CREATE TABLE IF NOT EXISTS organizations (
    org_id        UUID PRIMARY KEY,
    name          TEXT NOT NULL,
    user_id       UUID NOT NULL,
    team_ids      UUID[] NOT NULL DEFAULT '{}',
    api_keys      UUID[] NOT NULL DEFAULT '{}',
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
                    CHECK (user_type IN ('Free', 'Team', 'Enterprise')),
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
