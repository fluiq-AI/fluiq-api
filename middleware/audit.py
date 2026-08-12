"""Audit log middleware — fires an append-only audit event for every API request.

Covers EU AI Act (Aug 2026), China Generative AI Regulations, Colorado AI Act
(Feb 2026), and similar frameworks requiring tamper-evident request logs.

Each row is signed with HMAC-SHA256 so tampering can be detected by
recomputing the hash over (event_id, organization_id, event_type, actor, ts).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

import config
from db_queues.clickhouse import clickhouse_client

logger = logging.getLogger(__name__)

_SKIP_PATHS = frozenset({"/", "/health", "/docs", "/openapi.json", "/redoc"})

_EVENT_TYPE_MAP: list[tuple[str, str, str]] = [
    # (method, path_prefix, event_type)
    ("POST",   "/auth/login",    "auth_login"),
    ("POST",   "/auth/register", "auth_register"),
    ("POST",   "/auth/logout",   "auth_logout"),
    ("POST",   "/auth/refresh",  "auth_refresh"),
    ("POST",   "/api-keys",      "api_key_created"),
    ("DELETE", "/api-keys",      "api_key_deleted"),
    ("POST",   "/api/v1/secure", "security_check"),
    ("POST",   "/api/v1/evaluate", "eval_run"),
    ("POST",   "/api/v1/trace",  "llm_call"),
    ("POST",   "/admin",         "admin_action"),
    ("DELETE", "/admin",         "admin_action"),
]


def _classify(method: str, path: str) -> str:
    for m, prefix, etype in _EVENT_TYPE_MAP:
        if method == m and path.startswith(prefix):
            return etype
    return "api_request"


def _extract_claims(request: Request) -> tuple[str, str]:
    """Return (organization_id, actor) from Bearer JWT without re-verifying."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return "", "anonymous"
    token = auth[7:]
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
        org_id = claims.get("org_id") or ""
        actor  = claims.get("sub") or "unknown"
        return str(org_id), str(actor)
    except Exception:
        return "", "anonymous"


async def _emit(
    org_id: str,
    actor: str,
    event_type: str,
    method: str,
    path: str,
    status: int,
    ip: str,
    latency_ms: int,
    metadata: dict,
) -> None:
    try:
        await clickhouse_client.insert_audit_event(
            organization_id=org_id,
            actor=actor,
            event_type=event_type,
            http_method=method,
            http_path=path,
            http_status=status,
            ip_address=ip,
            latency_ms=latency_ms,
            metadata=metadata,
            hmac_secret=config.AUDIT_HMAC_SECRET,
        )
    except Exception:
        logger.exception("[AUDIT] Failed to write audit event %s %s", method, path)


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if path in _SKIP_PATHS or path.startswith("/static"):
            return await call_next(request)

        t0       = time.monotonic()
        response = await call_next(request)
        latency  = int((time.monotonic() - t0) * 1000)

        org_id, actor  = _extract_claims(request)
        event_type     = _classify(request.method, path)
        ip             = request.client.host if request.client else ""

        asyncio.create_task(_emit(
            org_id=org_id,
            actor=actor,
            event_type=event_type,
            method=request.method,
            path=path,
            status=response.status_code,
            ip=ip,
            latency_ms=latency,
            metadata={"query": str(request.query_params) or None},
        ))

        return response
