import logging
from pathlib import Path
from typing import Optional

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

import config

from .queries import ClickHouseQueryMixin

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def _split_statements(ddl: str) -> list[str]:
    """Split a multi-statement .sql file into individual statements.

    ClickHouse's HTTP interface runs one statement per request, so the schema
    file (CREATE DATABASE / CREATE TABLE / ALTER …) must be split. Full-line
    ``--`` comments are stripped *before* splitting on ``;`` so a semicolon
    inside a comment can't cut a statement in half (the schema contains no
    semicolons inside string literals, so ``;`` split is then safe).
    """
    cleaned = "\n".join(
        ln for ln in ddl.splitlines()
        if ln.strip() and not ln.strip().startswith("--")
    )
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


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
        try:
            self._client = await clickhouse_connect.get_async_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
            # Warm the connection during startup so the first real query on the
            # dashboard's first paint doesn't also pay the TCP/TLS + handshake cost
            # (which, when it pushed requests past the gateway timeout, surfaced as
            # phantom CORS errors on first login).
            try:
                await self._client.query("SELECT 1")
            except Exception:
                logger.exception("[CLICKHOUSE] Warm-up query failed")
            await self._apply_schema()
        except Exception:
            # Leave the client unset so the next caller retries the FULL start —
            # including the schema apply. A half-initialized client (connected
            # but schema not applied) would serve queries against missing
            # tables/columns and never heal.
            client, self._client = self._client, None
            if client is not None:
                try:
                    await client.close()
                except Exception:
                    pass
            raise
        logger.info("[CLICKHOUSE] Client started: %s:%s/%s", self.host, self.port, self.database)

    async def _apply_schema(self) -> None:
        """Apply schema.sql on startup, mirroring the Postgres client.

        ClickHouse does not auto-run DDL, so new tables/columns (e.g. the
        agentic-threat security columns) must be applied here. Every statement is
        idempotent (``CREATE … IF NOT EXISTS`` / ``ADD COLUMN IF NOT EXISTS``),
        so this is safe to run on every boot and across concurrent instances.
        Best-effort: a single failed statement is logged and the rest continue,
        so a transient DDL hiccup never blocks API startup.
        """
        if not SCHEMA_PATH.is_file():
            logger.warning("[CLICKHOUSE] schema.sql not found at %s", SCHEMA_PATH)
            return
        statements = _split_statements(SCHEMA_PATH.read_text(encoding="utf-8"))
        applied = failed = 0
        for stmt in statements:
            try:
                await self._client.command(stmt)
                applied += 1
            except Exception as exc:
                failed += 1
                logger.warning("[CLICKHOUSE] schema stmt failed (%s): %s",
                               str(exc)[:120], stmt.splitlines()[0][:80])
        logger.info("[CLICKHOUSE] schema applied: %d ok, %d failed (%s)",
                    applied, failed, SCHEMA_PATH)

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
