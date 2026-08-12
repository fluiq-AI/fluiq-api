import os
import ssl
from dotenv import load_dotenv

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES"))
JWT_REFRESH_EXPIRE_DAYS = int(os.getenv("JWT_REFRESH_EXPIRE_DAYS"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TRACE_TOPIC = os.getenv("KAFKA_TRACE_TOPIC")
KAFKA_TRACE_PERSISTED_TOPIC = os.getenv("KAFKA_TRACE_PERSISTED_TOPIC")
KAFKA_EVAL_TOPIC = os.getenv("KAFKA_EVAL_TOPIC")

# Fraction of LLM calls that get an ambient single-shot eval when the caller did
# NOT configure fluiq.eval(). 1.0 = every call (default); 0 = none. An explicit
# fluiq.eval() is always evaluated regardless of this rate.
EVAL_AUTO_SAMPLE_RATE = float(os.getenv("EVAL_AUTO_SAMPLE_RATE", "1.0"))
# Dedicated topic for the standalone security worker (separate from evals so the
# heavy torch/spaCy security deps don't run in the evaluator).
KAFKA_SECURITY_TOPIC = os.getenv("KAFKA_SECURITY_TOPIC")
# Consumer group for the alert dispatcher. Unlike the SSE consumer (which uses a
# per-replica UUID group so every replica sees every event), this is a STABLE
# shared group so each enriched event is handled by exactly one replica — alerts
# must fire once, not once-per-replica.
KAFKA_ALERTS_GROUP_ID = os.getenv("KAFKA_ALERTS_GROUP_ID", "api-alerts")
# PLAINTEXT (local docker) | SASL_SSL (AWS MSK SASL/SCRAM)
KAFKA_SECURITY_PROTOCOL=os.getenv("KAFKA_SECURITY_PROTOCOL")
KAFKA_SASL_MECHANISM=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_SASL_USERNAME=os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD=os.getenv("KAFKA_SASL_PASSWORD")

# Max producer request size (bytes) and matching consumer fetch ceiling. Trace
# events can legitimately reach a few MB (large prompts / responses / tool
# outputs); the aiokafka + broker default of ~1MB rejected them in _serialize
# with MessageSizeTooLargeError, surfacing as a 500 on POST /api/v1/ingest.
# This MUST stay <= the broker `message.max.bytes` / topic `max.message.bytes`
# and <= the consumers' fetch sizes, or producing/replicating will still fail.
# Note: the per-message guard runs on the *uncompressed* serialized size, so
# raising this is required even with compression enabled.
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", str(10 * 1024 * 1024)))
KAFKA_MAX_FETCH_BYTES = int(os.getenv("KAFKA_MAX_FETCH_BYTES", str(10 * 1024 * 1024)))


def kafka_auth_kwargs() -> dict:
    """aiokafka security kwargs derived from env, shared by producer + consumers.

    PLAINTEXT (default, local docker-compose) → no auth.
    SASL_SSL → SCRAM-SHA-512 username/password over TLS (AWS MSK). MSK broker
    certs chain to Amazon Trust Services (in the default CA bundle), so no CA
    file is needed.
    """
    protocol = (KAFKA_SECURITY_PROTOCOL or "PLAINTEXT").upper()
    if protocol == "SASL_SSL":
        return {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_SASL_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PASSWORD,
            "ssl_context": ssl.create_default_context(),
        }
    return {"security_protocol": "PLAINTEXT"}


POSTGRES_DSN = os.getenv("POSTGRES_DSN")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX"))
# Path to a CA certificate (.pem) used to verify the Postgres server's TLS
# certificate. When set, the connection uses full verification (verify-full).
# Leave empty/unset for local development (no CA verification).
POSTGRES_SSL_CA_FILE = os.getenv("POSTGRES_SSL_CA_FILE")

POSTGRES_USER_TABLE = os.getenv("POSTGRES_USER_TABLE")
POSTGRES_ORG_TABLE = os.getenv("POSTGRES_ORG_TABLE")
POSTGRES_REVOKED_TOKEN_TABLE  = os.getenv("POSTGRES_REVOKED_TOKEN_TABLE")
POSTGRES_PASSWORD_RESET_TABLE = os.getenv("POSTGRES_PASSWORD_RESET_TABLE")
POSTGRES_GUARDRAILS_TABLE     = os.getenv("POSTGRES_GUARDRAILS_TABLE", "guardrail_policies")
POSTGRES_ALERTS_TABLE         = os.getenv("POSTGRES_ALERTS_TABLE", "alert_settings")
POSTGRES_CREDENTIALS_TABLE    = os.getenv("POSTGRES_CREDENTIALS_TABLE", "org_provider_credentials")

SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

PASSWORD_RESET_OTP_LENGTH = int(os.getenv("PASSWORD_RESET_OTP_LENGTH"))
PASSWORD_RESET_EXPIRE_MINUTES = int(os.getenv("PASSWORD_RESET_EXPIRE_MINUTES"))
FRONTEND_BASE_URLS: list[str] = [
    url.strip().rstrip("/")
    for url in os.getenv("FRONTEND_BASE_URLS", "").split(",")
    if url.strip()
]

# Every entry above is CORS-allowed, but links we *generate* — password-reset
# emails, OAuth redirects, invite links, Slack alert links — have to pick one.
# The first entry is the canonical domain.
FRONTEND_BASE_URL = FRONTEND_BASE_URLS[0] if FRONTEND_BASE_URLS else ""

# Render "Deploy Hook" URL for the frontend static site. When a blog post is
# published / updated / unpublished we POST here to trigger a rebuild, which
# re-runs the prerender step so the post is baked into static HTML for SEO.
# Optional — publishing still works (fails open) when this is unset.
RENDER_DEPLOY_HOOK_URL = os.getenv("RENDER_DEPLOY_HOOK_URL")

# ── Blog media on S3 ──────────────────────────────────────────────────────────
# Blog images live in a private S3 bucket; only the object key is stored in
# Postgres. The public media endpoint redirects to a short-lived presigned GET
# URL, so the bucket never needs public access. Credentials come from the ECS
# task role (no static keys). Locally, configure AWS_* env / profile to test.
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BLOG_MEDIA_BUCKET = os.getenv("S3_BLOG_MEDIA_BUCKET")
S3_PRESIGN_TTL = int(os.getenv("S3_PRESIGN_TTL", "3600"))  # seconds
# Bucket for user uploads (dataset imports). Defaults to the blog media bucket so
# no new infra is required, but can point at a dedicated bucket in prod.
S3_UPLOADS_BUCKET = os.getenv("S3_UPLOADS_BUCKET") or S3_BLOG_MEDIA_BUCKET
# Optional S3-compatible endpoint override (e.g. MinIO for local dev). Empty in
# prod → boto3 uses the regional AWS endpoint.
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
# The endpoint used to SIGN presigned URLs the browser hits. With MinIO the API
# reaches the server internally (S3_ENDPOINT_URL=http://minio:9000) but the
# browser must hit it via a published host (http://localhost:9000), so the two
# differ locally. Both empty in prod → the regional AWS endpoint is used for both.
S3_PUBLIC_ENDPOINT_URL = os.getenv("S3_PUBLIC_ENDPOINT_URL") or None

# ── Customer provider credentials (BYOK) ─────────────────────────────────────
# Provider keys are envelope-encrypted (see shared/crypto.py): a per-credential
# data key from KMS, AES-256-GCM ciphertext in Postgres, plaintext data key
# never persisted. Recovering a key needs the DB row *and* kms:Decrypt, so an
# RDS snapshot alone is inert.
#
# CREDENTIAL_KMS_KEY_ID is the CMK arn/alias; the ECS task role needs
# kms:GenerateDataKey + kms:Decrypt on it. When unset, the BYOK surface is
# disabled rather than degraded — there is deliberately no unencrypted path.
#
# The `local` backend exists only so the feature can be developed and tested
# without AWS. It must be selected explicitly and needs a base64 32-byte key.
CREDENTIAL_ENCRYPTION_BACKEND = os.getenv("CREDENTIAL_ENCRYPTION_BACKEND", "kms").lower()
CREDENTIAL_KMS_KEY_ID = os.getenv("CREDENTIAL_KMS_KEY_ID")
CREDENTIAL_ENCRYPTION_LOCAL_KEY = os.getenv("CREDENTIAL_ENCRYPTION_LOCAL_KEY")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
CLICKHOUSE_TRACE_TABLE = os.getenv("CLICKHOUSE_TRACE_TABLE")
CLICKHOUSE_TRACE_COSTS_TABLE = os.getenv("CLICKHOUSE_TRACE_COSTS_TABLE")
CLICKHOUSE_EVALUATIONS_TABLE = os.getenv("CLICKHOUSE_EVALUATIONS_TABLE")
CLICKHOUSE_SECURITY_TABLE    = os.getenv("CLICKHOUSE_SECURITY_TABLE")
CLICKHOUSE_AUDIT_TABLE       = os.getenv("CLICKHOUSE_AUDIT_TABLE", "audit_log")
AUDIT_HMAC_SECRET            = os.getenv("AUDIT_HMAC_SECRET", "change-me-in-production")

REDIS_URL = os.getenv("REDIS_URL")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
 
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET")
API_BASE_URL = os.getenv("API_BASE_URL")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

KAFKA_SECURITY_REPLY_TOPIC     = os.getenv("KAFKA_SECURITY_REPLY_TOPIC")
KAFKA_SECURITY_CHECK_TIMEOUT   = float(os.getenv("KAFKA_SECURITY_CHECK_TIMEOUT"))
# When the full worker scan is unavailable (timeout / Kafka error / reply topic
# unset), /secure/check falls back to the pattern-only scanner, which cannot see
# PII, secrets, or semantic attacks. Default preserves the documented fail-OPEN
# behavior (allow on degraded). Set SECURE_FAIL_CLOSED=true to block instead when
# running degraded — safer for block-critical deployments.
SECURE_FAIL_CLOSED = os.getenv("SECURE_FAIL_CLOSED", "false").lower() in ("1", "true", "yes")

KAFKA_PLAYGROUND_REPLY_TOPIC   = os.getenv("KAFKA_PLAYGROUND_REPLY_TOPIC")
KAFKA_PLAYGROUND_CHECK_TIMEOUT = float(os.getenv("KAFKA_PLAYGROUND_CHECK_TIMEOUT", "30"))