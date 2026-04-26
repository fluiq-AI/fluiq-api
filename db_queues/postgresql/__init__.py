import json
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional

import asyncpg

load_dotenv()


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )

logger = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://fluiq:fluiq@localhost:5432/fluiq")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX", "10"))
SCHEMA_PATH = Path(__file__).parent / "schema.sql"

class PostgresClient:
    """Async PostgreSQL connection pool wrapper."""

    def __init__(
        self,
        dsn: str = POSTGRES_DSN,
        pool_min: int = POSTGRES_POOL_MIN,
        pool_max: int = POSTGRES_POOL_MAX,
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


postgres_client = PostgresClient()

__all__ = ["PostgresClient", "postgres_client", "POSTGRES_DSN"]
