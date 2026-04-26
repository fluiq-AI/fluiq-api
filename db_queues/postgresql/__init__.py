import os
import logging
from dotenv import load_dotenv
from typing import Optional

import asyncpg

load_dotenv()

logger = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/fluiq")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX", "10"))

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
        )
        logger.info("[POSTGRES] Connection pool started: %s", self.dsn)

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
