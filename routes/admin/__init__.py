import re
import time
import uuid
from datetime import datetime
from typing import Any, Optional

import boto3
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

import config

from db_queues.postgresql import postgres_client
from db_queues.postgresql.auth import (
    admin_adjust_eval_bonus,
    admin_get_user_eval_account,
    admin_list_organizations,
    admin_list_users,
    admin_update_user_type,
    get_platform_stats,
    get_user_by_id,
)
from db_queues.clickhouse import clickhouse_client
from routes.auth.helper import get_current_session
from shared.quotas import (
    DEFAULT_TIER,
    TIER_QUOTAS,
    UNLIMITED,
    get_quota_status,
    invalidate as invalidate_quota_cache,
)

admin_router = APIRouter()

# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

async def require_admin(session: dict = Depends(get_current_session)) -> dict:
    user = await get_user_by_id(uuid.UUID(session["sub"]))
    if user is None or user.user_type != "Admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return session

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class PlatformStatsResponse(BaseModel):
    total_users: int
    total_orgs: int
    users_by_type: dict[str, int]


class UserAdminView(BaseModel):
    user_id: uuid.UUID
    email: str
    name: str
    user_type: str
    org_id: uuid.UUID
    org_name: Optional[str]
    created_at: datetime


class UserListResponse(BaseModel):
    users: list[UserAdminView]
    total: int
    page: int
    limit: int


class OrgAdminView(BaseModel):
    org_id: uuid.UUID
    org_name: str
    owner_email: Optional[str]
    owner_name: Optional[str]
    owner_type: Optional[str]
    api_key_usage: int
    api_key_limit: int
    created_at: datetime


class OrgListResponse(BaseModel):
    organizations: list[OrgAdminView]
    total: int
    page: int
    limit: int


class UpdateUserTypeRequest(BaseModel):
    user_type: str


class UpdateUserTypeResponse(BaseModel):
    ok: bool


class UserEvalAccountResponse(BaseModel):
    user_id: uuid.UUID
    email: str
    name: str
    user_type: str
    org_id: uuid.UUID
    org_name: Optional[str]
    tier: str
    # Evaluations consumed this calendar month (ClickHouse count).
    eval_used: int
    # Admin-granted adjustment added on top of the tier quota (may be negative).
    eval_bonus: int
    # Tier's base monthly eval quota before any bonus; None means unlimited.
    base_eval_quota: Optional[int]
    # Effective monthly cap after the bonus; None means unlimited.
    eval_limit: Optional[int]


class AdjustEvalRequest(BaseModel):
    # Evaluations to add (positive) or subtract (negative) from the org's
    # monthly allowance. Must be non-zero.
    delta: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

_BILLABLE_TYPES = {"Free", "Team", "Growth", "Enterprise"}


@admin_router.get("/stats", response_model=PlatformStatsResponse)
async def platform_stats(
    _session: dict = Depends(require_admin),
) -> PlatformStatsResponse:
    data = await get_platform_stats()
    return PlatformStatsResponse(**data)


@admin_router.get("/users", response_model=UserListResponse)
async def list_users(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    search: str = Query(""),
    user_type: Optional[str] = Query(None),
    _session: dict = Depends(require_admin),
) -> UserListResponse:
    rows, total = await admin_list_users(
        page=page,
        limit=limit,
        search=search,
        user_type_filter=user_type,
    )
    return UserListResponse(
        users=[UserAdminView(**r) for r in rows],
        total=total,
        page=page,
        limit=limit,
    )


@admin_router.patch("/users/{user_id}", response_model=UpdateUserTypeResponse)
async def update_user(
    user_id: uuid.UUID,
    body: UpdateUserTypeRequest,
    _session: dict = Depends(require_admin),
) -> UpdateUserTypeResponse:
    if body.user_type not in _BILLABLE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"user_type must be one of: {', '.join(sorted(_BILLABLE_TYPES))}",
        )
    ok = await admin_update_user_type(user_id, body.user_type)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return UpdateUserTypeResponse(ok=True)


async def _build_eval_account(account: dict) -> UserEvalAccountResponse:
    """Assemble the eval-account view for a resolved user/org row."""
    org_id: uuid.UUID = account["org_id"]
    status_ = await get_quota_status(org_id, force_count=True)

    base_quota = TIER_QUOTAS.get(status_.tier, TIER_QUOTAS[DEFAULT_TIER])[1]
    return UserEvalAccountResponse(
        user_id=account["user_id"],
        email=account["email"],
        name=account["name"],
        user_type=account["user_type"],
        org_id=org_id,
        org_name=account.get("org_name"),
        tier=status_.tier,
        eval_used=status_.eval_count,
        eval_bonus=int(account.get("eval_quota_bonus") or 0),
        base_eval_quota=None if base_quota == UNLIMITED else base_quota,
        eval_limit=None if status_.eval_quota == UNLIMITED else status_.eval_quota,
    )


@admin_router.get(
    "/users/{user_id}/evaluations", response_model=UserEvalAccountResponse
)
async def get_user_evaluations(
    user_id: uuid.UUID,
    _session: dict = Depends(require_admin),
) -> UserEvalAccountResponse:
    account = await admin_get_user_eval_account(user_id)
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return await _build_eval_account(account)


@admin_router.post(
    "/users/{user_id}/evaluations", response_model=UserEvalAccountResponse
)
async def adjust_user_evaluations(
    user_id: uuid.UUID,
    body: AdjustEvalRequest,
    _session: dict = Depends(require_admin),
) -> UserEvalAccountResponse:
    if body.delta == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="delta must be a non-zero integer",
        )
    account = await admin_get_user_eval_account(user_id)
    if account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    org_id: uuid.UUID = account["org_id"]
    new_bonus = await admin_adjust_eval_bonus(org_id, body.delta)
    if new_bonus is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found"
        )
    # Drop the org's cached tier/bonus/counts so the new allowance takes effect
    # immediately on the hot /ingest path instead of after the TTL window.
    invalidate_quota_cache(org_id)

    account["eval_quota_bonus"] = new_bonus
    return await _build_eval_account(account)


@admin_router.get("/organizations", response_model=OrgListResponse)
async def list_organizations(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    _session: dict = Depends(require_admin),
) -> OrgListResponse:
    rows, total = await admin_list_organizations(page=page, limit=limit)
    return OrgListResponse(
        organizations=[OrgAdminView(**r) for r in rows],
        total=total,
        page=page,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Infrastructure — read-only SQL console (Postgres + ClickHouse)
# ---------------------------------------------------------------------------

_INFRA_MAX_ROWS = 1000
_INFRA_TIMEOUT_MS = 15000
# Only statements that begin with one of these may run. Combined with DB-level
# read-only enforcement below, this prevents any write/DDL (incl. CTE writes).
_INFRA_ALLOWED_START = re.compile(r"^\s*(with|select|show|describe|desc|explain)\b", re.I)


class InfraQueryRequest(BaseModel):
    database: str  # "postgres" | "clickhouse"
    sql: str


class InfraQueryResponse(BaseModel):
    database: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    truncated: bool
    elapsed_ms: int


def _infra_clean_sql(raw: str) -> str:
    """Strip comments + trailing semicolon; enforce single read-only statement."""
    s = re.sub(r"--[^\n]*", " ", raw)
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    s = s.strip().rstrip(";").strip()
    if not s:
        raise HTTPException(status_code=400, detail="Empty query.")
    if ";" in s:
        raise HTTPException(status_code=400, detail="Only a single statement is allowed.")
    if not _INFRA_ALLOWED_START.match(s):
        raise HTTPException(
            status_code=400,
            detail="Read-only only: queries must start with SELECT, WITH, SHOW, DESCRIBE, or EXPLAIN.",
        )
    return s


def _infra_cell(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool, list, dict)):
        return v
    return str(v)


@admin_router.post("/infra/query", response_model=InfraQueryResponse)
async def infra_query(
    payload: InfraQueryRequest,
    _session: dict = Depends(require_admin),
) -> InfraQueryResponse:
    db = payload.database.lower()
    sql = _infra_clean_sql(payload.sql)
    started = time.perf_counter()

    if db == "postgres":
        async with postgres_client.acquire() as conn:
            # Read-only transaction blocks any data/DDL write at the PG level.
            async with conn.transaction(readonly=True):
                await conn.execute(f"SET LOCAL statement_timeout = {_INFRA_TIMEOUT_MS}")
                records = await conn.fetch(sql)
        columns = list(records[0].keys()) if records else []
        total = len(records)
        rows = [[_infra_cell(v) for v in r.values()] for r in records[:_INFRA_MAX_ROWS]]

    elif db == "clickhouse":
        client = clickhouse_client._client
        if client is None:
            raise HTTPException(status_code=503, detail="ClickHouse is not connected.")
        # readonly=2 forbids writes/DDL while still allowing the result caps below.
        result = await client.query(
            sql,
            settings={
                "readonly": 2,
                "max_execution_time": _INFRA_TIMEOUT_MS // 1000,
                "max_result_rows": _INFRA_MAX_ROWS,
                "result_overflow_mode": "break",
            },
        )
        columns = list(result.column_names)
        all_rows = result.result_rows
        total = len(all_rows)
        rows = [[_infra_cell(v) for v in row] for row in all_rows[:_INFRA_MAX_ROWS]]

    else:
        raise HTTPException(status_code=400, detail="database must be 'postgres' or 'clickhouse'.")

    return InfraQueryResponse(
        database=db,
        columns=columns,
        rows=rows,
        row_count=min(total, _INFRA_MAX_ROWS),
        truncated=total > _INFRA_MAX_ROWS,
        elapsed_ms=int((time.perf_counter() - started) * 1000),
    )


# ---------------------------------------------------------------------------
# Infrastructure — ECS workers (status + logs) and SSM secrets (names only)
# ---------------------------------------------------------------------------

_ECS_CLUSTER = "fluiq"
_WORKER_SERVICES = ["fluiq-api", "fluiq-tracer", "fluiq-evaluator", "fluiq-security"]
_SSM_PREFIX = "/fluiq/prod/"


def _aws(service: str):
    return boto3.client(service, region_name=config.AWS_REGION)


@admin_router.get("/infra/workers")
async def infra_workers(_session: dict = Depends(require_admin)):
    """ECS service status for the API + workers."""
    def _describe():
        ecs = _aws("ecs")
        resp = ecs.describe_services(cluster=_ECS_CLUSTER, services=_WORKER_SERVICES)
        out = []
        for s in resp.get("services", []):
            dep = (s.get("deployments") or [{}])[0]
            updated = dep.get("updatedAt")
            out.append({
                "name": s.get("serviceName"),
                "status": s.get("status"),
                "running": s.get("runningCount", 0),
                "desired": s.get("desiredCount", 0),
                "pending": s.get("pendingCount", 0),
                "task_definition": (s.get("taskDefinition") or "").split("/")[-1],
                "rollout": dep.get("rolloutState"),
                "updated_at": updated.isoformat() if updated else None,
            })
        return out

    try:
        return {"workers": await run_in_threadpool(_describe)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"ECS describe failed: {exc}")


@admin_router.get("/infra/workers/{service}/logs")
async def infra_worker_logs(
    service: str,
    limit: int = Query(120, ge=1, le=500),
    _session: dict = Depends(require_admin),
):
    """Tail the most recent CloudWatch log stream for a service."""
    if service not in _WORKER_SERVICES:
        raise HTTPException(status_code=404, detail="Unknown service.")

    def _logs():
        logs = _aws("logs")
        group = f"/ecs/{service}"
        streams = logs.describe_log_streams(
            logGroupName=group, orderBy="LastEventTime", descending=True, limit=1
        ).get("logStreams", [])
        if not streams:
            return []
        resp = logs.get_log_events(
            logGroupName=group,
            logStreamName=streams[0]["logStreamName"],
            limit=limit,
            startFromHead=False,
        )
        return [
            {"ts": e["timestamp"], "message": (e.get("message") or "").rstrip()}
            for e in resp.get("events", [])
        ]

    try:
        return {"service": service, "events": await run_in_threadpool(_logs)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"CloudWatch logs failed: {exc}")


@admin_router.get("/infra/secrets")
async def infra_secrets(_session: dict = Depends(require_admin)):
    """List SSM parameter names/metadata under the prod prefix. Never returns values."""
    def _list():
        ssm = _aws("ssm")
        out = []
        paginator = ssm.get_paginator("describe_parameters")
        for page in paginator.paginate(
            ParameterFilters=[{"Key": "Path", "Option": "Recursive", "Values": [_SSM_PREFIX]}]
        ):
            for p in page.get("Parameters", []):
                mod = p.get("LastModifiedDate")
                out.append({
                    "name": p.get("Name"),
                    "type": p.get("Type"),
                    "last_modified": mod.isoformat() if mod else None,
                })
        return sorted(out, key=lambda x: x["name"] or "")

    try:
        return {"parameters": await run_in_threadpool(_list)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"SSM describe failed: {exc}")


__all__ = ["admin_router"]
