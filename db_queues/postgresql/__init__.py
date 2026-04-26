import os
import json
import logging
from dotenv import load_dotenv
from typing import Any, Optional

import asyncpg

load_dotenv()

logger = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/fluiq")
POSTGRES_USER_TABLE = os.getenv("POSTGRES_USER_TABLE", "user")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX", "10"))


class PostgresClient:
    """Async PostgreSQL client for storing trace records."""

    def __init__(
        self,
        dsn: str = POSTGRES_DSN,
        default_table: str = POSTGRES_USER_TABLE,
        pool_min: int = POSTGRES_POOL_MIN,
        pool_max: int = POSTGRES_POOL_MAX,
    ) -> None:
        self.dsn = dsn
        self.default_table = default_table
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
        )
        logger.info("[POSTGRES] Connection pool started: %s", self.dsn)

    async def stop(self) -> None:
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None
        logger.info("[POSTGRES] Connection pool stopped")

    async def insert_user(
        self,
        trace: dict[str, Any],
        table: Optional[str] = None,
    ) -> None:
        if self._pool is None:
            await self.start()
        target = table or self.default_table
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO {target} (api_key, event, created_at) "
                f"VALUES ($1, $2::jsonb, NOW())",
                trace.get("api_key"),
                json.dumps(trace.get("event"), default=str),
            )


postgres_client = PostgresClient()

__all__ = ["PostgresClient", "postgres_client", "POSTGRES_DSN", "POSTGRES_TRACE_TABLE"]
