"""Public lead capture + admin read-back.

  POST /api/v1/leads          public — capture an email from a marketing page
  GET  /api/v1/leads          admin  — read them back
  GET  /api/v1/leads/summary  admin  — counts per source page

The write is persisted *before* the notification email is attempted. The
contact form does the opposite (email only, no row), which is why nothing that
came through it is recoverable — do not repeat that here: a Resend outage must
never lose a lead.
"""
from __future__ import annotations

import html as html_lib
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel

import config
from db_queues.postgresql import leads as leads_db
from routes.admin import require_admin
from shared.email import email_service

logger = logging.getLogger(__name__)
router = APIRouter()

LEADS_TO = "fluiqai@gmail.com"

# Only pages we actually ship a form on. An unknown value is stored as
# "unknown" rather than rejected, so a typo never costs us the email.
KNOWN_SOURCES = {
    "response-gate-demo",
    "llm-cost-calculator",
    "langsmith-alternative",
    "braintrust-alternative",
    "langfuse-alternative",
    "helicone-alternative",
    "lakera-alternative",
    "portkey-alternative",
    "contact",
    "pricing",
    "home",
}


class LeadPayload(BaseModel):
    email: str
    source_page: str
    # Free-form, small: what the visitor was looking at when they submitted
    # (e.g. the scan verdict on the demo). Never store the pasted text itself.
    context: Optional[dict[str, Any]] = None


@router.post("/leads", status_code=status.HTTP_201_CREATED)
async def capture_lead(payload: LeadPayload, request: Request):
    email = payload.email.strip().lower()
    if not leads_db.is_valid_email(email):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Enter a valid email address.",
        )

    source = payload.source_page.strip().lower()
    if source not in KNOWN_SOURCES:
        logger.warning("[LEADS] unknown source_page=%r — storing as 'unknown'", source)
        source = "unknown"

    context = payload.context or {}
    # Guard against a caller stuffing the visitor's pasted agent output in here.
    if len(str(context)) > 2000:
        context = {"truncated": True}

    try:
        is_new = await leads_db.capture(
            email=email,
            source_page=source,
            context=context,
            referrer=request.headers.get("referer"),
            user_agent=request.headers.get("user-agent"),
        )
    except Exception:
        logger.exception("[LEADS] capture failed email=%s source=%s", email, source)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not save that right now. Please try again.",
        )

    logger.info("[LEADS] captured email=%s source=%s new=%s", email, source, is_new)

    # Best-effort notification. The row is already durable; a mail failure is
    # logged and swallowed so the visitor still sees success.
    if is_new:
        try:
            safe_email  = html_lib.escape(email)
            safe_source = html_lib.escape(source)
            await email_service.send_email(
                to=LEADS_TO,
                subject=f"[Fluiq Lead] {email} — {source}",
                html=(
                    '<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;'
                    'max-width:600px;margin:0 auto;padding:32px 24px;color:#0f172a">'
                    '<h2 style="margin:0 0 6px;font-size:22px;font-weight:700">New lead</h2>'
                    f'<p style="margin:0 0 18px;color:#64748b;font-size:14px">From <strong>{safe_source}</strong></p>'
                    f'<p style="font-size:16px;font-weight:600"><a href="mailto:{safe_email}" style="color:#0f172a">{safe_email}</a></p>'
                    '<p style="margin:28px 0 0;font-size:12px;color:#94a3b8">'
                    'Saved to the leads table. Reply to them today — inbound goes cold fast.</p></div>'
                ),
                text=f"New lead: {email}\nSource: {source}\n",
            )
        except Exception:
            logger.exception("[LEADS] notification email failed for %s (row saved)", email)

    return {"ok": True, "new": is_new}


@router.get("/leads")
async def get_leads(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: dict = Depends(require_admin),
):
    return {
        "leads": await leads_db.list_leads(limit=limit, offset=offset),
        "total": await leads_db.total(),
    }


@router.get("/leads/summary")
async def get_leads_summary(_: dict = Depends(require_admin)):
    return {
        "by_source": await leads_db.counts_by_source(),
        "total":     await leads_db.total(),
    }
