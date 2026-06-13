import json
import os
import ssl
import logging
import config
from pathlib import Path
from typing import Optional

import asyncpg

async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )

logger = logging.getLogger(__name__)


def _build_ssl_context() -> Optional[ssl.SSLContext]:
    """Build a TLS context that verifies the server against POSTGRES_SSL_CA_FILE.

    Returns None when no CA file is configured, so asyncpg falls back to the
    sslmode (if any) embedded in the DSN — preserving local/dev behavior.
    """
    ca_file = getattr(config, "POSTGRES_SSL_CA_FILE", None)
    if not ca_file:
        return None
    if not Path(ca_file).is_file():
        raise FileNotFoundError(
            f"POSTGRES_SSL_CA_FILE is set but the file was not found: {ca_file}"
        )
    ctx = ssl.create_default_context(cafile=ca_file)
    # create_default_context already sets check_hostname=True and
    # verify_mode=CERT_REQUIRED (equivalent to libpq sslmode=verify-full).
    return ctx

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

class PostgresClient:
    """Async PostgreSQL connection pool wrapper."""

    def __init__(
        self,
        dsn: str = config.POSTGRES_DSN,
        pool_min: int = config.POSTGRES_POOL_MIN,
        pool_max: int = config.POSTGRES_POOL_MAX,
    ) -> None:
        self.dsn = dsn
        self.pool_min = pool_min
        self.pool_max = pool_max
        self._pool: Optional[asyncpg.Pool] = None

    async def start(self) -> None:
        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.pool_min,
            max_size=self.pool_max,
            init=_init_connection,
            ssl=_build_ssl_context(),
        )
        logger.info("[POSTGRES] Connection pool started: %s", self.dsn)
        await self._apply_schema()

    async def _apply_schema(self) -> None:
        if not SCHEMA_PATH.is_file():
            logger.warning("[POSTGRES] schema.sql not found at %s", SCHEMA_PATH)
            return
        ddl = SCHEMA_PATH.read_text(encoding="utf-8")
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)
        logger.info("[POSTGRES] schema applied from %s", SCHEMA_PATH)

    async def stop(self) -> None:
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None
        logger.info("[POSTGRES] Connection pool stopped")

    def acquire(self):
        """Return the pool's acquire() context manager."""
        if self._pool is None:
            raise RuntimeError("PostgresClient pool not started; call start() first")
        return self._pool.acquire()

    async def fetch_price(
        self,
        provider: str,
        model: str,
        modality: str = "Text",
    ) -> Optional[dict]:
        """Return the model_prices row for (provider, model, modality) or None.

        Falls back to any modality when an exact modality match is not found.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM model_prices
                WHERE LOWER(provider) = LOWER($1)
                  AND LOWER(model)    = LOWER($2)
                  AND LOWER(modality) = LOWER($3)
                ORDER BY id
                LIMIT 1
                """,
                provider, model, modality,
            )
            if row is None:
                row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM model_prices
                    WHERE LOWER(provider) = LOWER($1)
                      AND LOWER(model)    = LOWER($2)
                    ORDER BY id
                    LIMIT 1
                    """,
                    provider, model,
                )
        return dict(row) if row is not None else None


postgres_client = PostgresClient()

__all__ = ["PostgresClient", "postgres_client", "POSTGRES_DSN"]
