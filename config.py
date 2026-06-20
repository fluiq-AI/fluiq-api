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
# Dedicated topic for the standalone security worker (separate from evals so the
# heavy torch/spaCy security deps don't run in the evaluator).
KAFKA_SECURITY_TOPIC = os.getenv("KAFKA_SECURITY_TOPIC")
# PLAINTEXT (local docker) | SASL_SSL (AWS MSK SASL/SCRAM)
KAFKA_SECURITY_PROTOCOL=os.getenv("KAFKA_SECURITY_PROTOCOL")
KAFKA_SASL_MECHANISM=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_SASL_USERNAME=os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD=os.getenv("KAFKA_SASL_PASSWORD")


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

SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")

PASSWORD_RESET_OTP_LENGTH = int(os.getenv("PASSWORD_RESET_OTP_LENGTH"))
PASSWORD_RESET_EXPIRE_MINUTES = int(os.getenv("PASSWORD_RESET_EXPIRE_MINUTES"))
FRONTEND_BASE_URL = (os.getenv("FRONTEND_BASE_URL") or "").rstrip("/")

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
REDIS_SDK_URL = os.getenv("REDIS_SDK_URL") or REDIS_URL
REDIS_DEFAULT_TTL_SECONDS = int(os.getenv("REDIS_DEFAULT_TTL_SECONDS"))

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
 
GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET")
API_BASE_URL = os.getenv("API_BASE_URL")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

KAFKA_SECURITY_REPLY_TOPIC     = os.getenv("KAFKA_SECURITY_REPLY_TOPIC")
KAFKA_SECURITY_CHECK_TIMEOUT   = float(os.getenv("KAFKA_SECURITY_CHECK_TIMEOUT"))

KAFKA_PLAYGROUND_REPLY_TOPIC   = os.getenv("KAFKA_PLAYGROUND_REPLY_TOPIC")
KAFKA_PLAYGROUND_CHECK_TIMEOUT = float(os.getenv("KAFKA_PLAYGROUND_CHECK_TIMEOUT", "30"))