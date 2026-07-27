import logging
import re
import time
import uuid
from datetime import datetime
from typing import Any, List, Optional

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
from db_queues.postgresql.organizations import (
    admin_add_member,
    admin_list_user_organizations,
    admin_set_org_plan,
    list_members,
    remove_member,
    rename_organization,
    set_member_role,
)
from db_queues.postgresql.eval_prompts import (
    create_judge_prompt,
    get_judge_prompt,
    list_judge_prompt_versions,
    list_judge_prompts,
    reset_judge_prompt,
    restore_judge_prompt_version,
    update_judge_prompt,
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

logger = logging.getLogger(__name__)

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
    plan: Optional[str] = None
    member_count: int = 0
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
# Admin: organization membership + plan management (god-mode; any org)
# ---------------------------------------------------------------------------

class AdminMemberView(BaseModel):
    user_id: uuid.UUID
    name: str
    email: str
    role: str
    created_at: datetime


class AdminMembersResponse(BaseModel):
    members: list[AdminMemberView]


class AdminAddMemberRequest(BaseModel):
    email: str
    role: str = "member"


class AdminSetRoleRequest(BaseModel):
    role: str


class AdminOrgUpdateRequest(BaseModel):
    name: Optional[str] = None
    plan_tier: Optional[str] = None


_ORG_ROLES = {"owner", "admin", "member"}
_ORG_PLANS = {"Free", "Starter", "Team", "Growth", "Enterprise"}


@admin_router.get(
    "/organizations/{org_id}/members", response_model=AdminMembersResponse
)
async def admin_org_members(
    org_id: uuid.UUID, _session: dict = Depends(require_admin)
) -> AdminMembersResponse:
    rows = await list_members(org_id)
    return AdminMembersResponse(members=[AdminMemberView(**r) for r in rows])


@admin_router.post("/organizations/{org_id}/members", response_model=AdminMembersResponse)
async def admin_org_add_member(
    org_id: uuid.UUID,
    body: AdminAddMemberRequest,
    _session: dict = Depends(require_admin),
) -> AdminMembersResponse:
    role = body.role if body.role in _ORG_ROLES else "member"
    ok, err = await admin_add_member(org_id, body.email.strip(), role)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err)
    rows = await list_members(org_id)
    return AdminMembersResponse(members=[AdminMemberView(**r) for r in rows])


@admin_router.patch("/organizations/{org_id}/members/{member_id}", response_model=UpdateUserTypeResponse)
async def admin_org_set_member_role(
    org_id: uuid.UUID,
    member_id: uuid.UUID,
    body: AdminSetRoleRequest,
    _session: dict = Depends(require_admin),
) -> UpdateUserTypeResponse:
    if body.role not in _ORG_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role.")
    ok = await set_member_role(org_id, member_id, body.role)
    if not ok:
        raise HTTPException(status_code=404, detail="Member not found.")
    return UpdateUserTypeResponse(ok=True)


@admin_router.delete(
    "/organizations/{org_id}/members/{member_id}", response_model=UpdateUserTypeResponse
)
async def admin_org_remove_member(
    org_id: uuid.UUID,
    member_id: uuid.UUID,
    _session: dict = Depends(require_admin),
) -> UpdateUserTypeResponse:
    ok, err = await remove_member(org_id, member_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err)
    return UpdateUserTypeResponse(ok=True)


@admin_router.patch("/organizations/{org_id}", response_model=UpdateUserTypeResponse)
async def admin_update_org(
    org_id: uuid.UUID,
    body: AdminOrgUpdateRequest,
    _session: dict = Depends(require_admin),
) -> UpdateUserTypeResponse:
    if body.name is not None:
        renamed = await rename_organization(org_id, body.name)
        if renamed is None:
            raise HTTPException(status_code=404, detail="Organization not found")
    if body.plan_tier is not None:
        if body.plan_tier not in _ORG_PLANS:
            raise HTTPException(
                status_code=400,
                detail=f"plan_tier must be one of: {', '.join(sorted(_ORG_PLANS))}",
            )
        ok = await admin_set_org_plan(org_id, body.plan_tier)
        if not ok:
            raise HTTPException(status_code=404, detail="Organization not found")
        invalidate_quota_cache(org_id)
    return UpdateUserTypeResponse(ok=True)


class AdminUserOrgsResponse(BaseModel):
    organizations: list[dict]


@admin_router.get("/users/{user_id}/organizations", response_model=AdminUserOrgsResponse)
async def admin_user_organizations(
    user_id: uuid.UUID, _session: dict = Depends(require_admin)
) -> AdminUserOrgsResponse:
    rows = await admin_list_user_organizations(user_id)
    orgs = [
        {
            "org_id": str(r["org_id"]),
            "name": r["name"],
            "role": r["role"],
            "plan": r.get("plan"),
            "member_count": int(r["member_count"]),
        }
        for r in rows
    ]
    return AdminUserOrgsResponse(organizations=orgs)


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

# Self-hosted infra boxes, for the Kafka/ClickHouse/Postgres log viewers.
_KAFKA_INSTANCE_ID = "i-0ce87c91a2778da50"   # fluiq-kafka EC2 (Docker KRaft broker)
_KAFKA_CONTAINER = "fluiq-kafka"
_RDS_INSTANCE_ID = "fluiq-postgres"


def _aws(service: str):
    return boto3.client(service, region_name=config.AWS_REGION)


# Mutations are confined to this prefix and to known env-var names.
_SECRET_NAME_RE = re.compile(r"^/fluiq/prod/[A-Za-z0-9_./-]+$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Fields accepted by register_task_definition (the rest of describe_task_definition
# is read-only and must be dropped before re-registering).
_TASK_DEF_KEYS = (
    "family", "taskRoleArn", "executionRoleArn", "networkMode",
    "containerDefinitions", "volumes", "placementConstraints",
    "requiresCompatibilities", "cpu", "memory", "pidMode", "ipcMode",
    "proxyConfiguration", "inferenceAccelerators", "ephemeralStorage",
    "runtimePlatform",
)


def _require_secret_prefix(name: str) -> None:
    if not _SECRET_NAME_RE.match(name or ""):
        raise HTTPException(
            status_code=400,
            detail="Name must look like /fluiq/prod/<NAME> (letters, digits, and _ . - /).",
        )


def _audit(session: dict, action: str, **fields: Any) -> None:
    """Emit one structured audit line per infra mutation."""
    detail = " ".join(f"{k}={v}" for k, v in fields.items())
    logger.info("infra-audit admin=%s action=%s %s", session.get("sub"), action, detail)


def _ssm_arn(name: str) -> str:
    account = _aws("sts").get_caller_identity()["Account"]
    return f"arn:aws:ssm:{config.AWS_REGION}:{account}:parameter{name}"


def _primary_container(containers: list[dict], service: str) -> dict:
    """The container a worker's secrets attach to: matched by name, else the first."""
    for c in containers:
        if c.get("name") == service:
            return c
    if not containers:
        raise HTTPException(status_code=502, detail="Task definition has no containers.")
    return containers[0]


def _task_def_for_register(td: dict) -> dict:
    return {k: td[k] for k in _TASK_DEF_KEYS if td.get(k) not in (None, [], {})}


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


# How many filter_log_events pages to scan before giving up (each page ≤ 10k
# events); bounds the work when a query matches sparsely over a wide window.
_INFRA_LOG_MAX_PAGES = 20


@admin_router.get("/infra/workers/{service}/logs")
async def infra_worker_logs(
    service: str,
    limit: int = Query(200, ge=1, le=2000),
    q: str | None = Query(None, description="Substring to search for (case-sensitive)."),
    start: int | None = Query(None, ge=0, description="Window start, epoch ms."),
    end: int | None = Query(None, ge=0, description="Window end, epoch ms."),
    _session: dict = Depends(require_admin),
):
    """Worker logs from CloudWatch.

    With no ``q``/``start``/``end`` this tails the most recent log stream (fast
    "live" view). When a query term or time window is supplied it searches across
    *every* stream in the group via ``filter_log_events`` — i.e. the whole history,
    not just the latest stream. Events are returned ascending by timestamp.
    """
    if service not in _WORKER_SERVICES:
        raise HTTPException(status_code=404, detail="Unknown service.")
    group = f"/ecs/{service}"
    searching = bool(q) or start is not None or end is not None

    def _tail():
        logs = _aws("logs")
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

    def _search():
        logs = _aws("logs")
        kwargs: dict[str, Any] = {"logGroupName": group, "limit": min(limit, 10000)}
        if q:
            # Literal substring match across all streams (CloudWatch is case-sensitive).
            kwargs["filterPattern"] = f'"{q}"'
        if start is not None:
            kwargs["startTime"] = start
        if end is not None:
            kwargs["endTime"] = end
        events: list[dict] = []
        token: str | None = None
        for _ in range(_INFRA_LOG_MAX_PAGES):
            if token:
                kwargs["nextToken"] = token
            resp = logs.filter_log_events(**kwargs)
            events.extend(resp.get("events", []))
            token = resp.get("nextToken")
            if not token or len(events) >= limit:
                break
        events.sort(key=lambda e: e.get("timestamp", 0))
        return [
            {"ts": e["timestamp"], "message": (e.get("message") or "").rstrip()}
            for e in events[:limit]
        ]

    try:
        events = await run_in_threadpool(_search if searching else _tail)
        return {"service": service, "events": events, "searched": searching}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"CloudWatch logs failed: {exc}")


# ---------------------------------------------------------------------------
# Infrastructure — data-store logs (Kafka · ClickHouse · Postgres)
# ---------------------------------------------------------------------------
# Each source lives somewhere different, so each has its own fetch strategy:
#   kafka      → SSM RunShellScript `docker logs` on the broker EC2 (on-demand)
#   clickhouse → query system.text_log / system.query_log over the live conn
#   postgres   → RDS DownloadDBLogFilePortion on the latest log file
# All fail SOFT: a missing IAM permission surfaces as a 502 with the AWS error,
# which the Admin UI renders inline (nothing else breaks).

_LOG_SOURCES = ("kafka", "clickhouse", "postgres")


async def _clickhouse_logs(limit: int) -> dict:
    client = clickhouse_client._client
    if client is None:
        raise HTTPException(status_code=503, detail="ClickHouse is not connected.")
    settings = {"readonly": 2, "max_execution_time": 10}
    # Prefer the server text log; fall back to recent query history + errors.
    text_sql = (
        "SELECT event_time, level, message FROM system.text_log "
        f"ORDER BY event_time DESC LIMIT {limit}"
    )
    query_sql = (
        "SELECT event_time, "
        "       concat(type, ' ', "
        "         if(exception != '', concat('EXCEPTION ', exception), "
        "            concat(toString(query_duration_ms), 'ms')), ' ', "
        "         substring(replaceRegexpAll(query, '\\\\s+', ' '), 1, 200)) "
        "FROM system.query_log WHERE type != 'QueryStart' "
        f"ORDER BY event_time DESC LIMIT {limit}"
    )
    last_exc: Exception | None = None
    for sql in (text_sql, query_sql):
        try:
            res = await client.query(sql, settings=settings)
            lines = [" ".join(str(c) for c in row) for row in res.result_rows]
            lines.reverse()  # oldest → newest for a natural tail
            return {"source": "clickhouse", "lines": lines}
        except Exception as exc:  # noqa: BLE001 — try the fallback query
            last_exc = exc
    raise HTTPException(status_code=502, detail=f"ClickHouse logs unavailable: {last_exc}")


def _kafka_logs(limit: int) -> dict:
    ssm = _aws("ssm")
    cmd_id = ssm.send_command(
        InstanceIds=[_KAFKA_INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [
            f"docker logs --tail {limit} {_KAFKA_CONTAINER} 2>&1 | tail -n {limit}"
        ]},
    )["Command"]["CommandId"]
    deadline = time.time() + 15
    while time.time() < deadline:
        time.sleep(1.0)
        try:
            inv = ssm.get_command_invocation(CommandId=cmd_id, InstanceId=_KAFKA_INSTANCE_ID)
        except ssm.exceptions.InvocationDoesNotExist:
            continue
        if inv["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            out = (inv.get("StandardOutputContent") or "") + (inv.get("StandardErrorContent") or "")
            return {"source": "kafka", "lines": out.splitlines()[-limit:], "status": inv["Status"]}
    raise HTTPException(status_code=504, detail="Kafka log fetch timed out (SSM still running).")


def _postgres_logs(limit: int) -> dict:
    rds = _aws("rds")
    files = rds.describe_db_log_files(DBInstanceIdentifier=_RDS_INSTANCE_ID).get("DescribeDBLogFiles", [])
    if not files:
        return {"source": "postgres", "lines": []}
    latest = max(files, key=lambda f: f.get("LastWritten", 0))["LogFileName"]
    portion = rds.download_db_log_file_portion(
        DBInstanceIdentifier=_RDS_INSTANCE_ID,
        LogFileName=latest, Marker="0", NumberOfLines=limit,
    )
    data = portion.get("LogFileData") or ""
    return {"source": "postgres", "lines": data.splitlines()[-limit:], "file": latest}


@admin_router.get("/infra/logs/{source}")
async def infra_logs(
    source: str,
    limit: int = Query(200, ge=1, le=2000),
    _session: dict = Depends(require_admin),
):
    """Tail logs for a data-store box. See module notes for per-source strategy."""
    src = source.lower()
    if src not in _LOG_SOURCES:
        raise HTTPException(status_code=404, detail=f"source must be one of {', '.join(_LOG_SOURCES)}.")
    try:
        if src == "clickhouse":
            return await _clickhouse_logs(limit)
        if src == "kafka":
            return await run_in_threadpool(_kafka_logs, limit)
        return await run_in_threadpool(_postgres_logs, limit)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 — fail soft; UI shows the AWS/IAM error
        raise HTTPException(status_code=502, detail=f"{src} logs failed: {exc}")


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


class SecretWriteRequest(BaseModel):
    name: str
    value: str
    type: str = "SecureString"  # "String" | "SecureString"
    description: Optional[str] = None


@admin_router.put("/infra/secrets")
async def infra_put_secret(
    payload: SecretWriteRequest,
    session: dict = Depends(require_admin),
):
    """Create or update an SSM parameter under /fluiq/prod/. Values are write-only."""
    _require_secret_prefix(payload.name)
    if payload.type not in ("String", "SecureString"):
        raise HTTPException(status_code=400, detail="type must be String or SecureString.")
    if not payload.value:
        raise HTTPException(status_code=400, detail="value is required.")

    def _put():
        ssm = _aws("ssm")
        kwargs: dict[str, Any] = {
            "Name": payload.name, "Value": payload.value,
            "Type": payload.type, "Overwrite": True,
        }
        if payload.description:
            kwargs["Description"] = payload.description
        return ssm.put_parameter(**kwargs).get("Version")

    try:
        version = await run_in_threadpool(_put)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"SSM put failed: {exc}")
    _audit(session, "secret.put", name=payload.name, type=payload.type, version=version)
    return {"name": payload.name, "type": payload.type, "version": version}


@admin_router.delete("/infra/secrets")
async def infra_delete_secret(
    name: str = Query(..., description="Full SSM parameter name under /fluiq/prod/."),
    session: dict = Depends(require_admin),
):
    """Delete an SSM parameter. Irreversible."""
    _require_secret_prefix(name)

    def _del():
        _aws("ssm").delete_parameter(Name=name)

    try:
        await run_in_threadpool(_del)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "ParameterNotFound" in msg:
            raise HTTPException(status_code=404, detail="Parameter not found.")
        raise HTTPException(status_code=502, detail=f"SSM delete failed: {exc}")
    _audit(session, "secret.delete", name=name)
    return {"deleted": name}


# ── Per-worker secret bindings (task-definition `secrets` → env vars) ──────────


class WorkerSecretRequest(BaseModel):
    env_name: str            # env var injected into the container
    ssm_name: str            # SSM parameter the value is read from
    value: Optional[str] = None   # if set, the SSM parameter is created/updated too
    type: str = "SecureString"


@admin_router.get("/infra/workers/{service}/secrets")
async def infra_worker_secrets(service: str, _session: dict = Depends(require_admin)):
    """List the SSM-backed env vars wired into a worker's task definition."""
    if service not in _WORKER_SERVICES:
        raise HTTPException(status_code=404, detail="Unknown service.")

    def _get():
        ecs = _aws("ecs")
        svc = ecs.describe_services(cluster=_ECS_CLUSTER, services=[service]).get("services", [])
        if not svc:
            return []
        td = ecs.describe_task_definition(taskDefinition=svc[0]["taskDefinition"])["taskDefinition"]
        container = _primary_container(td.get("containerDefinitions", []), service)
        return [
            {"name": s.get("name"), "value_from": s.get("valueFrom")}
            for s in container.get("secrets", [])
        ]

    try:
        return {"service": service, "secrets": await run_in_threadpool(_get)}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"ECS describe failed: {exc}")


@admin_router.post("/infra/workers/{service}/secrets")
async def infra_worker_add_secret(
    service: str,
    payload: WorkerSecretRequest,
    session: dict = Depends(require_admin),
):
    """Wire an SSM parameter into a worker as an env var (registers a new task-def
    revision and updates the service — this redeploys the worker). When ``value`` is
    provided the SSM parameter is created/updated first."""
    if service not in _WORKER_SERVICES:
        raise HTTPException(status_code=404, detail="Unknown service.")
    if not _ENV_NAME_RE.match(payload.env_name):
        raise HTTPException(status_code=400, detail="env_name must be a valid env var identifier.")
    _require_secret_prefix(payload.ssm_name)
    if payload.value is not None and payload.type not in ("String", "SecureString"):
        raise HTTPException(status_code=400, detail="type must be String or SecureString.")

    def _add():
        if payload.value:
            _aws("ssm").put_parameter(
                Name=payload.ssm_name, Value=payload.value,
                Type=payload.type, Overwrite=True,
            )
        ecs = _aws("ecs")
        svc = ecs.describe_services(cluster=_ECS_CLUSTER, services=[service]).get("services", [])
        if not svc:
            raise HTTPException(status_code=404, detail="Service not found.")
        td = ecs.describe_task_definition(taskDefinition=svc[0]["taskDefinition"])["taskDefinition"]
        reg = _task_def_for_register(td)
        container = _primary_container(reg.get("containerDefinitions", []), service)
        secrets = [s for s in container.get("secrets", []) if s.get("name") != payload.env_name]
        secrets.append({"name": payload.env_name, "valueFrom": _ssm_arn(payload.ssm_name)})
        container["secrets"] = secrets
        new = ecs.register_task_definition(**reg)["taskDefinition"]
        ecs.update_service(
            cluster=_ECS_CLUSTER, service=service, taskDefinition=new["taskDefinitionArn"],
        )
        return new["revision"]

    try:
        revision = await run_in_threadpool(_add)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"ECS update failed: {exc}")
    _audit(
        session, "worker.secret.add", service=service,
        env_name=payload.env_name, ssm_name=payload.ssm_name,
        created_param=bool(payload.value), revision=revision,
    )
    return {"service": service, "env_name": payload.env_name, "revision": revision}


@admin_router.delete("/infra/workers/{service}/secrets/{env_name}")
async def infra_worker_delete_secret(
    service: str,
    env_name: str,
    session: dict = Depends(require_admin),
):
    """Unwire an env-var secret from a worker (new task-def revision + service update,
    redeploying the worker). The underlying SSM parameter is left intact — delete it
    from the Secrets tab if it is no longer used."""
    if service not in _WORKER_SERVICES:
        raise HTTPException(status_code=404, detail="Unknown service.")

    def _del():
        ecs = _aws("ecs")
        svc = ecs.describe_services(cluster=_ECS_CLUSTER, services=[service]).get("services", [])
        if not svc:
            raise HTTPException(status_code=404, detail="Service not found.")
        td = ecs.describe_task_definition(taskDefinition=svc[0]["taskDefinition"])["taskDefinition"]
        reg = _task_def_for_register(td)
        container = _primary_container(reg.get("containerDefinitions", []), service)
        before = container.get("secrets", [])
        kept = [s for s in before if s.get("name") != env_name]
        if len(kept) == len(before):
            raise HTTPException(status_code=404, detail="No such secret binding on this worker.")
        container["secrets"] = kept
        new = ecs.register_task_definition(**reg)["taskDefinition"]
        ecs.update_service(
            cluster=_ECS_CLUSTER, service=service, taskDefinition=new["taskDefinitionArn"],
        )
        return new["revision"]

    try:
        revision = await run_in_threadpool(_del)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"ECS update failed: {exc}")
    _audit(session, "worker.secret.delete", service=service, env_name=env_name, revision=revision)
    return {"service": service, "env_name": env_name, "revision": revision}


# ── Scaling (desired task count) ───────────────────────────────────────────────

_MAX_DESIRED = 10  # guard rail; raise if a service legitimately needs more


class ScaleRequest(BaseModel):
    desired: int


@admin_router.post("/infra/workers/{service}/scale")
async def infra_worker_scale(
    service: str,
    payload: ScaleRequest,
    session: dict = Depends(require_admin),
):
    """Set the desired task count for a service (0 stops it, capped at _MAX_DESIRED)."""
    if service not in _WORKER_SERVICES:
        raise HTTPException(status_code=404, detail="Unknown service.")
    if payload.desired < 0 or payload.desired > _MAX_DESIRED:
        raise HTTPException(status_code=400, detail=f"desired must be between 0 and {_MAX_DESIRED}.")

    def _scale():
        ecs = _aws("ecs")
        svc = ecs.update_service(
            cluster=_ECS_CLUSTER, service=service, desiredCount=payload.desired,
        )["service"]
        return {
            "desired": svc.get("desiredCount", payload.desired),
            "running": svc.get("runningCount", 0),
            "pending": svc.get("pendingCount", 0),
        }

    try:
        result = await run_in_threadpool(_scale)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"ECS scale failed: {exc}")
    _audit(session, "worker.scale", service=service, desired=payload.desired)
    return {"service": service, **result}


# ---------------------------------------------------------------------------
# LLM-as-Judge prompts (platform-global; edited here, read by the evaluator)
# ---------------------------------------------------------------------------

# Mirrors the worker's placeholder scanner: the {{var}} standard plus the legacy
# string.Template forms ($var / ${var}) that older saved prompts still use.
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}|\$\{(\w+)\}|\$(\w+)")


def _template_identifiers(template: str) -> set[str]:
    return {
        m.group(1) or m.group(2) or m.group(3)
        for m in _PLACEHOLDER_RE.finditer(template)
    }


class JudgePromptView(BaseModel):
    name: str
    template: str
    default_template: str
    description: Optional[str]
    required_vars: List[str]
    is_overridden: bool
    version: int
    updated_at: datetime


class JudgePromptListResponse(BaseModel):
    prompts: list[JudgePromptView]


class UpdateJudgePromptRequest(BaseModel):
    template: str


class CreateJudgePromptRequest(BaseModel):
    name: str
    template: str
    description: Optional[str] = None
    required_vars: List[str] = []


# Lowercase identifier: starts with a letter, then letters/digits/underscores.
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,63}$")


class JudgePromptVersionView(BaseModel):
    version_id: uuid.UUID
    name: str
    version: int
    template: str
    updated_by: Optional[uuid.UUID]
    created_at: datetime


class JudgePromptVersionsResponse(BaseModel):
    versions: list[JudgePromptVersionView]


def _validate_template(prompt_row: dict, template: str) -> None:
    """Reject an edit that is empty or drops a required placeholder."""
    if not template or not template.strip():
        raise HTTPException(status_code=400, detail="Template cannot be empty.")
    required = set(prompt_row.get("required_vars") or [])
    missing = sorted(required - _template_identifiers(template))
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                "Template is missing required placeholder(s): "
                + ", ".join(f"${{{m}}}" for m in missing)
            ),
        )


@admin_router.get("/judge-prompts", response_model=JudgePromptListResponse)
async def judge_prompts_list(
    _session: dict = Depends(require_admin),
) -> JudgePromptListResponse:
    rows = await list_judge_prompts()
    return JudgePromptListResponse(prompts=[JudgePromptView(**r) for r in rows])


@admin_router.post("/judge-prompts", response_model=JudgePromptView, status_code=201)
async def judge_prompt_create(
    body: CreateJudgePromptRequest,
    session: dict = Depends(require_admin),
) -> JudgePromptView:
    name = body.name.strip().lower()
    if not _NAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail="Name must be lowercase letters, digits, or underscores (e.g. 'my_metric').",
        )
    # Reuse the same placeholder check used by edits.
    _validate_template({"required_vars": body.required_vars}, body.template)
    created = await create_judge_prompt(
        name=name,
        template=body.template,
        description=body.description,
        required_vars=[v.strip() for v in body.required_vars if v.strip()],
        updated_by=uuid.UUID(session["sub"]),
    )
    if created is None:
        raise HTTPException(status_code=409, detail=f"A prompt named '{name}' already exists.")
    return JudgePromptView(**created)


@admin_router.get("/judge-prompts/{name}", response_model=JudgePromptView)
async def judge_prompt_get(
    name: str,
    _session: dict = Depends(require_admin),
) -> JudgePromptView:
    row = await get_judge_prompt(name)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    return JudgePromptView(**row)


@admin_router.put("/judge-prompts/{name}", response_model=JudgePromptView)
async def judge_prompt_update(
    name: str,
    body: UpdateJudgePromptRequest,
    session: dict = Depends(require_admin),
) -> JudgePromptView:
    row = await get_judge_prompt(name)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    _validate_template(row, body.template)
    updated = await update_judge_prompt(
        name, body.template, updated_by=uuid.UUID(session["sub"])
    )
    return JudgePromptView(**updated)


@admin_router.post("/judge-prompts/{name}/reset", response_model=JudgePromptView)
async def judge_prompt_reset(
    name: str,
    session: dict = Depends(require_admin),
) -> JudgePromptView:
    updated = await reset_judge_prompt(name, updated_by=uuid.UUID(session["sub"]))
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    return JudgePromptView(**updated)


@admin_router.get("/judge-prompts/{name}/versions", response_model=JudgePromptVersionsResponse)
async def judge_prompt_versions(
    name: str,
    _session: dict = Depends(require_admin),
) -> JudgePromptVersionsResponse:
    rows = await list_judge_prompt_versions(name)
    return JudgePromptVersionsResponse(versions=[JudgePromptVersionView(**r) for r in rows])


@admin_router.post(
    "/judge-prompts/{name}/restore/{version}", response_model=JudgePromptView
)
async def judge_prompt_restore(
    name: str,
    version: int,
    session: dict = Depends(require_admin),
) -> JudgePromptView:
    updated = await restore_judge_prompt_version(
        name, version, updated_by=uuid.UUID(session["sub"])
    )
    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Prompt or version not found"
        )
    return JudgePromptView(**updated)


__all__ = ["admin_router"]
