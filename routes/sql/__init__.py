"""fluiq-api — A read-only SQL sandbox, scoped to the caller's organization.

  GET  /api/v1/sql/schema   the tables and columns available (session)
  POST /api/v1/sql/query    run one read-only query (session)

Every dashboard is somebody's second-favourite question. A SQL box is the escape
hatch for the first one — "which model regressed on Tuesday for customers on the
gold plan" is not a screen anyone will build, and without a way to ask it the
answer is simply unavailable.

Tenancy
-------
The admin console has had a SQL editor for a while, and it is safe there for a
reason that does not transfer: an admin is *supposed* to see every org. Handing
the same thing to a customer would let them read every other customer's traces
with one `WHERE 1=1`.

So scoping here is **structural, not a filter the user could omit**. The query
never names a real table. It names a set of virtual ones that this module
defines as org-filtered subqueries, and any reference to anything else is
rejected before execution:

    -- what the user writes
    SELECT model, count() FROM traces GROUP BY model

    -- what actually runs
    WITH traces AS (SELECT ... FROM fluiq.traces WHERE organization_id = {org})
    SELECT model, count() FROM traces GROUP BY model

There is no syntax available to them that reaches another tenant's rows, because
the only names that resolve are already filtered. That is a stronger guarantee
than validating a WHERE clause, which has to be right every time forever.
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Any, Dict, List, Optional

import config
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from db_queues.clickhouse import clickhouse_client
from routes.auth.helper import get_current_session

sql_router = APIRouter()

MAX_ROWS = 1000
TIMEOUT_SECONDS = 20
MAX_QUERY_CHARS = 8000

#: Only these may start a statement. Everything that mutates is absent, and
#: ClickHouse's own ``readonly`` setting is the second line rather than the first.
_ALLOWED_START = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)

#: Anything following FROM or JOIN. Used to check every table the query names.
_TABLE_REF = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w.]*)", re.IGNORECASE)

#: Names a user may define themselves in a leading WITH clause.
_CTE_NAME = re.compile(r"(?:^\s*with\s+|,\s*)([a-zA-Z_]\w*)\s+as\s*\(", re.IGNORECASE)


def _virtual_tables() -> Dict[str, str]:
    """The org-filtered views a query may read, as SQL fragments.

    Each is a complete subquery whose WHERE already pins the organization. The
    columns are chosen rather than ``SELECT *``: the raw tables carry internal
    fields (retention stamps, api-key prefixes) that are noise at best and
    another tenant's shape at worst.
    """
    return {
        "traces": f"""
            SELECT toString(trace_id)      AS trace_id,
                   toString(root_trace_id) AS root_trace_id,
                   is_root,
                   agent_key,
                   agent_kind,
                   ingested_at,
                   JSONExtractString(toString(event), 'model')       AS model,
                   JSONExtractString(toString(event), 'integration') AS integration,
                   JSONExtractString(toString(event), 'type')        AS type,
                   JSONExtractFloat(toString(event), 'latency')      AS latency,
                   JSONExtractBool(toString(event), 'success')       AS success,
                   JSONExtractString(toString(event), 'response')    AS response
            FROM {config.CLICKHOUSE_TRACE_TABLE}
            WHERE organization_id = {{org_id:UUID}}
        """,
        "costs": f"""
            SELECT toString(trace_id) AS trace_id,
                   toString(root_trace_id) AS root_trace_id,
                   provider, model, modality,
                   input_tokens, cached_input_tokens, output_tokens,
                   total_cost, currency, ingested_at
            FROM {config.CLICKHOUSE_TRACE_COSTS_TABLE}
            WHERE organization_id = {{org_id:UUID}}
        """,
        "evaluations": f"""
            SELECT toString(trace_id) AS trace_id,
                   toString(root_trace_id) AS root_trace_id,
                   evaluator, metric, score, judge_model,
                   judge_input_tokens, judge_output_tokens, judge_calls,
                   ingested_at
            FROM {config.CLICKHOUSE_EVALUATIONS_TABLE}
            WHERE organization_id = {{org_id:UUID}}
        """,
        "security": f"""
            SELECT toString(trace_id) AS trace_id,
                   risk_level, risk_score, should_block, ingested_at
            FROM {config.CLICKHOUSE_SECURITY_TABLE}
            WHERE organization_id = {{org_id:UUID}}
        """,
        "tags": f"""
            SELECT toString(trace_id) AS trace_id, tag, source, updated_at
            FROM {config.CLICKHOUSE_TRACE_TAGS_TABLE} FINAL
            WHERE organization_id = {{org_id:UUID}} AND deleted = 0
        """,
    }


#: Documented for the editor's help panel, so nobody has to guess column names.
SCHEMA_DOC: Dict[str, List[str]] = {
    "traces": [
        "trace_id", "root_trace_id", "is_root", "agent_key", "agent_kind",
        "ingested_at", "model", "integration", "type", "latency", "success",
        "response",
    ],
    "costs": [
        "trace_id", "root_trace_id", "provider", "model", "modality",
        "input_tokens", "cached_input_tokens", "output_tokens", "total_cost",
        "currency", "ingested_at",
    ],
    "evaluations": [
        "trace_id", "root_trace_id", "evaluator", "metric", "score",
        "judge_model", "judge_input_tokens", "judge_output_tokens",
        "judge_calls", "ingested_at",
    ],
    "security": ["trace_id", "risk_level", "risk_score", "should_block", "ingested_at"],
    "tags": ["trace_id", "tag", "source", "updated_at"],
}


class QueryRequest(BaseModel):
    sql: str


def clean_sql(raw: str) -> str:
    """Strip comments, enforce a single read-only statement.

    Comments are removed before anything else is checked: ``SELECT 1 --\\nDROP``
    would otherwise pass a naive semicolon test while hiding a second statement
    from it.
    """
    if not raw or not raw.strip():
        raise HTTPException(400, "Empty query.")
    if len(raw) > MAX_QUERY_CHARS:
        raise HTTPException(400, f"Query exceeds {MAX_QUERY_CHARS} characters.")

    text = re.sub(r"--[^\n]*", " ", raw)
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    text = text.strip().rstrip(";").strip()
    if not text:
        raise HTTPException(400, "Empty query.")
    if ";" in text:
        raise HTTPException(400, "Only a single statement is allowed.")
    if not _ALLOWED_START.match(text):
        raise HTTPException(400, "Read-only: a query must start with SELECT or WITH.")
    return text


def check_tables(sql: str, allowed: Dict[str, str]) -> None:
    """Every table the query names must be virtual or its own CTE.

    This is the tenancy boundary. A reference to a real table — ``fluiq.traces``,
    ``system.tables`` — is refused, because those are not filtered by
    organization and nothing downstream would add the filter for them.
    """
    own_ctes = {m.lower() for m in _CTE_NAME.findall(sql)}
    for reference in _TABLE_REF.findall(sql):
        name = reference.lower().strip()
        if name in allowed or name in own_ctes:
            continue
        raise HTTPException(
            400,
            f"Unknown table {reference!r}. Available: "
            f"{', '.join(sorted(allowed))}. "
            f"(Schema-qualified names like 'fluiq.traces' are not available — "
            f"the tables above are already scoped to your organization.)",
        )


def build_query(sql: str, allowed: Dict[str, str]) -> str:
    """Prefix the org-scoped definitions the query will resolve against.

    A user's own ``WITH`` is preserved by folding it into the same clause: the
    definitions here come first, so a CTE of theirs may read a virtual table,
    and one that shadows a virtual name simply wins for their own query — which
    is harmless, because it can only be built from tables that are already
    filtered.
    """
    definitions = ",\n".join(f"{name} AS ({body})" for name, body in allowed.items())
    stripped = sql.strip()
    if re.match(r"^with\s+", stripped, re.IGNORECASE):
        rest = re.sub(r"^with\s+", "", stripped, flags=re.IGNORECASE)
        return f"WITH {definitions},\n{rest}"
    return f"WITH {definitions}\n{stripped}"


@sql_router.get("/sql/schema")
async def get_schema(_session: dict = Depends(get_current_session)):
    """What a query may read. Served rather than documented in the frontend so
    the two cannot disagree about a column name."""
    return {
        "tables": [
            {"name": name, "columns": columns}
            for name, columns in SCHEMA_DOC.items()
        ],
        "notes": [
            "Every table is already filtered to your organization.",
            "Read-only: SELECT and WITH only.",
            f"Results are capped at {MAX_ROWS} rows and {TIMEOUT_SECONDS}s.",
        ],
    }


@sql_router.post("/sql/query")
async def run_query(
    payload: QueryRequest,
    session: dict = Depends(get_current_session),
):
    """Run one read-only, org-scoped query."""
    org_id = uuid.UUID(session["org_id"])
    allowed = _virtual_tables()

    sql = clean_sql(payload.sql)
    check_tables(sql, allowed)
    prepared = build_query(sql, allowed)

    client = clickhouse_client._client
    if client is None:
        raise HTTPException(503, "The query engine is not available right now.")

    started = time.perf_counter()
    try:
        result = await client.query(
            prepared,
            parameters={"org_id": str(org_id)},
            settings={
                # readonly=2 is the second line of defence: scoping already makes
                # a write unexpressible, but a write that somehow parsed would
                # still be refused by the engine.
                "readonly": 2,
                "max_execution_time": TIMEOUT_SECONDS,
                "max_result_rows": MAX_ROWS,
                "result_overflow_mode": "break",
            },
        )
    except Exception as exc:  # noqa: BLE001
        # The engine's own message is the useful part of a syntax error, but it
        # names the rewritten query, so the prefix is trimmed off the reply.
        message = str(exc)
        raise HTTPException(400, _readable_error(message)) from exc

    elapsed = int((time.perf_counter() - started) * 1000)
    rows = [[_cell(v) for v in row] for row in result.result_rows[:MAX_ROWS]]
    return {
        "columns": list(result.column_names),
        "rows": rows,
        "row_count": len(rows),
        "truncated": len(result.result_rows) >= MAX_ROWS,
        "elapsed_ms": elapsed,
    }


def _cell(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def _readable_error(message: str) -> str:
    """Trim the engine's echo of the rewritten query out of an error.

    A syntax error that quoted three hundred characters of injected CTEs would
    bury the one line the author actually wrote.
    """
    first = message.split("\n", 1)[0]
    cut = first.find("(query:")
    if cut > 0:
        first = first[:cut]
    return first.strip()[:400] or "Query failed."


__all__ = ["sql_router", "clean_sql", "check_tables", "build_query", "SCHEMA_DOC"]
