"""Public model-pricing API for the LLM cost calculator.

- ``GET  /api/v1/models``         list models + per-million token prices
- ``POST /api/v1/models/request`` visitor asks us to add a model
- ``POST /api/v1/models/report``  visitor reports a price change

Reads come from the existing ``model_prices`` table; submissions are stored in
``model_requests`` / ``model_price_reports`` and emailed to the team. Everything
fails soft: a mail outage never fails the visitor's request.
"""
import html as html_lib
import logging
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

from db_queues.postgresql import postgres_client
from shared.email import email_service

logger = logging.getLogger(__name__)
models_router = APIRouter()

NOTIFY_TO = "fluiqai@gmail.com"


def _f(value) -> Optional[float]:
    """Coerce a NUMERIC/Decimal column to float, preserving NULL."""
    return float(value) if value is not None else None


# ── Read ──────────────────────────────────────────────────────────────────────

@models_router.get("/models")
async def list_models():
    """Text-modality models that have both an input and output price."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, provider, model, modality,
                   input_token_cost_per_million,
                   cached_input_token_cost_per_million,
                   output_token_cost_per_million
            FROM model_prices
            WHERE LOWER(modality) = 'text'
              AND input_token_cost_per_million  IS NOT NULL
              AND output_token_cost_per_million IS NOT NULL
            ORDER BY provider, model
            """
        )
    models = [
        {
            "id": r["id"],
            "provider": r["provider"],
            "model": r["model"],
            "input_per_million": _f(r["input_token_cost_per_million"]),
            "output_per_million": _f(r["output_token_cost_per_million"]),
            "cached_input_per_million": _f(r["cached_input_token_cost_per_million"]),
        }
        for r in rows
    ]
    return {"models": models, "count": len(models)}


# ── Submissions ───────────────────────────────────────────────────────────────

class ModelRequest(BaseModel):
    provider: Optional[str] = None
    model: str
    email: Optional[str] = None
    note: Optional[str] = None


class PriceReport(BaseModel):
    model_price_id: Optional[int] = None
    provider: Optional[str] = None
    model: str
    reported_input: Optional[float] = None
    reported_output: Optional[float] = None
    source_url: Optional[str] = None
    email: Optional[str] = None
    note: Optional[str] = None


async def _notify(subject: str, fields: dict[str, Optional[str]]) -> None:
    """Best-effort team email. Swallows failures so the visitor still gets 200."""
    rows = "".join(
        f"<tr><td style='padding:8px 0;color:#64748b;font-size:13px;font-weight:600;"
        f"width:140px;vertical-align:top;border-bottom:1px solid #f1f5f9'>{html_lib.escape(k)}</td>"
        f"<td style='padding:8px 0;font-size:14px;border-bottom:1px solid #f1f5f9'>"
        f"{html_lib.escape(str(v))}</td></tr>"
        for k, v in fields.items()
        if v not in (None, "")
    )
    html = (
        "<div style='font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:600px;"
        "margin:0 auto;padding:32px 24px;color:#0f172a'>"
        "<span style='display:inline-block;background:#1860D3;color:#fff;font-size:11px;"
        "font-weight:700;letter-spacing:0.08em;text-transform:uppercase;padding:4px 10px;"
        f"border-radius:4px'>Fluiq</span><h2 style='margin:18px 0 16px;font-size:20px'>{html_lib.escape(subject)}</h2>"
        f"<table style='width:100%;border-collapse:collapse'>{rows}</table></div>"
    )
    text = "\n".join(f"{k}: {v}" for k, v in fields.items() if v not in (None, ""))
    try:
        await email_service.send_email(to=NOTIFY_TO, subject=f"[Fluiq] {subject}", html=html, text=text)
    except Exception:
        logger.exception("[MODELS] notification email failed for %r", subject)


@models_router.post("/models/request")
async def request_model(payload: ModelRequest):
    async with postgres_client.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO model_requests (provider, model, email, note)
            VALUES ($1, $2, $3, $4)
            """,
            payload.provider, payload.model, payload.email, payload.note,
        )
    logger.info("[MODELS] request model=%r provider=%r", payload.model, payload.provider)
    await _notify(
        "New model request",
        {
            "Model": payload.model,
            "Provider": payload.provider,
            "Requested by": payload.email,
            "Note": payload.note,
        },
    )
    return {"ok": True}


@models_router.post("/models/report")
async def report_price(payload: PriceReport):
    async with postgres_client.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO model_price_reports
              (model_price_id, provider, model, reported_input, reported_output, source_url, email, note)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            payload.model_price_id, payload.provider, payload.model,
            payload.reported_input, payload.reported_output,
            payload.source_url, payload.email, payload.note,
        )
    logger.info("[MODELS] price report model=%r provider=%r", payload.model, payload.provider)
    await _notify(
        "Price change reported",
        {
            "Model": payload.model,
            "Provider": payload.provider,
            "Reported input / 1M": payload.reported_input,
            "Reported output / 1M": payload.reported_output,
            "Source": payload.source_url,
            "Reported by": payload.email,
            "Note": payload.note,
        },
    )
    return {"ok": True}


__all__ = ["models_router"]
