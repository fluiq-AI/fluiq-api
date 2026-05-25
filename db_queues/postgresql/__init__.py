import json
import os
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
