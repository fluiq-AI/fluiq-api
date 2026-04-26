import os
import json
import logging
import uuid
from dotenv import load_dotenv
from typing import Any, Optional

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

load_dotenv()

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "fluiq")
CLICKHOUSE_TRACE_TABLE = os.getenv("CLICKHOUSE_TRACE_TABLE", "traces")


class ClickHouseClient:
    """Async ClickHouse client for reading trace records."""

    def __init__(
        self,
        host: str = CLICKHOUSE_HOST,
        port: int = CLICKHOUSE_PORT,
        username: str = CLICKHOUSE_USER,
        password: str = CLICKHOUSE_PASSWORD,
        database: str = CLICKHOUSE_DATABASE,
        default_table: str = CLICKHOUSE_TRACE_TABLE,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
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

    async def fetch_traces(
        self,
        organization_id: uuid.UUID,
        api_key_prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        table: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return traces for an organization, optionally filtered by key prefix.

        Each row is `{api_key_prefix, event, ingested_at}` with the event JSON
        already parsed.
        """
        if self._client is None:
            await self.start()
        target = table or self.default_table
        where = "organization_id = {org_id:UUID}"
        params: dict[str, Any] = {
            "org_id": str(organization_id),
            "limit": limit,
            "offset": offset,
        }
        if api_key_prefix is not None:
            where += " AND api_key_prefix = {prefix:String}"
            params["prefix"] = api_key_prefix
        result = await self._client.query(
            f"SELECT api_key_prefix, event, ingested_at FROM {target} "
            f"WHERE {where} "
            f"ORDER BY ingested_at DESC "
            f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}",
            parameters=params,
        )
        rows: list[dict[str, Any]] = []
        for prefix, event, ingested_at in result.result_rows:
            if isinstance(event, str):
                try:
                    parsed = json.loads(event)
                except json.JSONDecodeError:
                    parsed = {"raw": event}
            else:
                parsed = event
            rows.append({
                "api_key_prefix": prefix,
                "event": parsed,
                "ingested_at": ingested_at,
            })
        return rows


clickhouse_client = ClickHouseClient()

__all__ = [
    "ClickHouseClient",
    "clickhouse_client",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_DATABASE",
    "CLICKHOUSE_TRACE_TABLE",
]
