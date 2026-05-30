"""GET /api/v1/audit — query the org's audit log."""
from __future__ import annotations

import uuid
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from db_queues.clickhouse import clickhouse_client
from routes.auth.helper import get_current_session

audit_router = APIRouter()


class AuditEvent(BaseModel):
    event_id:        str
    organization_id: str
    actor:           str
    event_type:      str
    http_method:     str
    http_path:       str
    http_status:     int
    ip_address:      str
    latency_ms:      int
    metadata:        Any
    row_hash:        str
    created_at:      Optional[str]


class AuditLogResponse(BaseModel):
    events: List[AuditEvent]
    limit:  int
    offset: int


@audit_router.get("/audit", response_model=AuditLogResponse)
async def get_audit_log(
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    actor:      Optional[str] = Query(None, description="Filter by actor (user_id or api_key_prefix)"),
    limit:      int           = Query(100, ge=1, le=500),
    offset:     int           = Query(0, ge=0),
    session:    dict          = Depends(get_current_session),
) -> AuditLogResponse:
    org_id = uuid.UUID(session["org_id"])
    rows = await clickhouse_client.fetch_audit_log(
        organization_id=org_id,
        event_type=event_type,
        actor=actor,
        limit=limit,
        offset=offset,
    )
    return AuditLogResponse(events=rows, limit=limit, offset=offset)
