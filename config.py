import os
from dotenv import load_dotenv

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES"))
JWT_REFRESH_EXPIRE_DAYS = int(os.getenv("JWT_REFRESH_EXPIRE_DAYS"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TRACE_TOPIC = os.getenv("KAFKA_TRACE_TOPIC", "traces")
KAFKA_TRACE_PERSISTED_TOPIC = os.getenv("KAFKA_TRACE_PERSISTED_TOPIC", "traces.persisted")
KAFKA_SECURITY_PROTOCOL=os.getenv("KAFKA_SECURITY_PROTOCOL","SSL")
KAFKA_SSL_CA_FILE=os.getenv("KAFKA_SSL_CA_FILE")
KAFKA_SSL_CERT_FILE=os.getenv("KAFKA_SSL_CERT_FILE")
KAFKA_SSL_KEY_FILE=os.getenv("KAFKA_SSL_KEY_FILE")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fluiq:fluiq@localhost:5432/fluiq")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX", "10"))

POSTGRES_USER_TABLE = os.getenv("POSTGRES_USER_TABLE", "users")
POSTGRES_ORG_TABLE = os.getenv("POSTGRES_ORG_TABLE", "organizations")
POSTGRES_REVOKED_TOKEN_TABLE = os.getenv(
    "POSTGRES_REVOKED_TOKEN_TABLE", "revoked_refresh_tokens"
)
POSTGRES_PASSWORD_RESET_TABLE = os.getenv(
    "POSTGRES_PASSWORD_RESET_TABLE", "password_resets"
)

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "no-reply@getfluiq.com")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Fluiq")

PASSWORD_RESET_OTP_LENGTH = int(os.getenv("PASSWORD_RESET_OTP_LENGTH", "6"))
PASSWORD_RESET_EXPIRE_MINUTES = int(os.getenv("PASSWORD_RESET_EXPIRE_MINUTES", "15"))
FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "http://localhost:5173").rstrip("/")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "fluiq")
CLICKHOUSE_TRACE_TABLE = os.getenv("CLICKHOUSE_TRACE_TABLE", "traces")
CLICKHOUSE_TRACE_COSTS_TABLE = os.getenv("CLICKHOUSE_TRACE_COSTS_TABLE", "trace_costs")
CLICKHOUSE_EVALUATIONS_TABLE = os.getenv("CLICKHOUSE_EVALUATIONS_TABLE", "evaluations")
