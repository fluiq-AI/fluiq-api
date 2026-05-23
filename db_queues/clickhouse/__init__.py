import logging
from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

import config

from .queries import ClickHouseQueryMixin

logger = logging.getLogger(__name__)


class ClickHouseClient(ClickHouseQueryMixin):
    """Async ClickHouse client.

    Connection management lives here; all query/write methods come from
    ClickHouseQueryMixin so each concern stays in its own file.
    """

    def __init__(
        self,
        host: str = config.CLICKHOUSE_HOST,
        port: int = config.CLICKHOUSE_PORT,
        username: str = config.CLICKHOUSE_USER,
        password: str = config.CLICKHOUSE_PASSWORD,
        database: str = config.CLICKHOUSE_DATABASE,
        default_table: str = config.CLICKHOUSE_TRACE_TABLE,
    ) -> None:
        self.host          = host
        self.port          = port
        self.username      = username
        self.password      = password
        self.database      = database
        self.default_table = default_table
        self._client: Optional[AsyncClient] = None

    async def start(self) -> None:
        if self._client is not None:
            return
        self._client = await clickhouse_connect.get_async_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
        logger.info("[CLICKHOUSE] Client started: %s:%s/%s", self.host, self.port, self.database)

    async def stop(self) -> None:
        if self._client is None:
            return
        await self._client.close()
        self._client = None
        logger.info("[CLICKHOUSE] Client stopped")


clickhouse_client = ClickHouseClient()

__all__ = [
    "ClickHouseClient",
    "clickhouse_client",
]
