"""fluiq-api — Dataset management routes.

  GET    /api/v1/datasets                                   list datasets (session)
  POST   /api/v1/datasets                                   create dataset (session)
  DELETE /api/v1/datasets/{id}                              delete dataset (session)
  GET    /api/v1/datasets/{id}/examples                     list examples (session)
  POST   /api/v1/datasets/{id}/examples                     add example (session)
  DELETE /api/v1/datasets/{id}/examples/{example_id}        delete example (session)
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import config
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, field_validator, model_validator

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, wait_for_playground_reply
from db_queues.postgresql.datasets import (
    add_example,
    add_examples_bulk,
    create_dataset,
    delete_dataset,
    delete_example,
    existing_source_trace_ids,
    get_example,
    list_datasets,
    list_examples,
)
from db_queues.postgresql.dataset_runs import (
    add_run_items,
    create_run,
    finalize_run,
    get_run,
    get_run_items,
    link_agent,
    list_agent_links,
    list_runs,
    unlink_agent,
)
from db_queues.postgresql.dataset_scorers import (
    dataset_owned,
    delete_dataset_judge_prompt,
    link_dataset_scorer,
    list_dataset_judge_prompts,
    list_dataset_scorers,
    unlink_dataset_scorer,
    upsert_dataset_judge_prompt,
)
from db_queues.postgresql.prompts import (
    create_prompt,
    get_prompt_by_slug,
    update_prompt,
)
from db_queues.postgresql.auth import resolve_api_key
from db_queues.postgresql.eval_prompts_org import get_org_judge_prompt
from routes.auth.helper import extract_api_key, get_current_session
# Reuse the org judge-prompt editor's validator so the two paths can't drift on
# what makes an override safe (non-empty, keeps every required placeholder).
from routes.evaluate.judge_prompts import _validate_template as _validate_judge_template
from shared.placeholders import ANSWER_PLACEHOLDER_RE
from shared.quotas import bump_eval_count, get_quota_status
from shared.dataset_media import store_trajectory_media, hydrate_trajectory_media
from shared import s3

datasets_router = APIRouter()
logger = logging.getLogger(__name__)


# ── Pydantic models ───────────────────────────────────────────────────────────

class CreateDatasetRequest(BaseModel):
    name:        str
    description: Optional[str] = None
    # 'single'  = single-prompt dataset (input/output pairs + custom scorer).
    # 'agentic' = full-trajectory dataset (all inputs/outputs/tools/MCP calls).
    kind:        str = "agentic"

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 200:
            raise ValueError("name must be 200 characters or fewer")
        return v

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, v: str) -> str:
        v = (v or "agentic").strip().lower()
        if v not in ("single", "agentic"):
            raise ValueError("kind must be 'single' or 'agentic'")
        return v


class AddExampleRequest(BaseModel):
    input:           str
    expected_output: Optional[str]       = None
    metadata:        Dict[str, Any]      = {}

    @model_validator(mode="after")
    def _require_content(self) -> "AddExampleRequest":
        if self.input.strip():
            return self
        # Trace-backed examples (a run added from the drawer, e.g. CrewAI's crew
        # root) may have no direct input — the input is derived from the pinned
        # trajectory server-side — so accept them when a source_trace_id or a
        # final output is present. Only truly empty drafts are rejected.
        meta = self.metadata if isinstance(self.metadata, dict) else {}
        if meta.get("source_trace_id"):
            return self
        if self.expected_output and self.expected_output.strip():
            return self
        raise ValueError("input is required")


# Metrics a 'metrics' dataset run may request — mirrors the evaluator worker's
# _build_evaluator names (plus per-org custom judges, referenced by slug).
RUN_METRICS = frozenset({
    "hallucination", "faithfulness", "relevance",
    "toxicity", "coherence", "completeness",
    # NOTE: context_precision / context_recall are RAG *retrieval* metrics that
    # need real retrieved contexts. Text dataset examples have none (the synthetic
    # event's response is the expected_output), so the dataset path can't score
    # them meaningfully yet — deliberately not offered here. They ARE offered in
    # the Traces/Prompts drawers where the user supplies the context. Surfacing
    # them for trace-backed RAG examples needs retrieved-context capture first.
})


class CreateRunRequest(BaseModel):
    kind:  str                      # 'agentic' | 'security' | 'metrics'
    depth: Optional[str] = None     # agentic depth: fast | standard | deep
    # Judge selection as "provider:model" (e.g. "anthropic:claude-sonnet-5"),
    # applied to every example in the run. Omitted uses the server default.
    # Batch runs are where judge choice costs the most: the model is paid for
    # once per example, so a 10k-example dataset multiplies the difference.
    judge: Optional[str] = None
    # Jury members for depth='deep'. Ignored at shallower depths.
    jury:  Optional[List[str]] = None
    # kind='metrics' only: which metrics to grade each example on, plus
    # optional per-org custom judges {slug: threshold}.
    metrics:       Optional[List[str]]      = None
    custom_judges: Optional[Dict[str, float]] = None
    # Groups the runs of one multi-model comparison so the UI can tell when
    # every model in the batch has finished.
    batch_id:      Optional[uuid.UUID]      = None

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, v: str) -> str:
        if v not in ("agentic", "security", "metrics"):
            raise ValueError("kind must be 'agentic', 'security', or 'metrics'")
        return v

    @field_validator("metrics")
    @classmethod
    def _validate_metrics(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return None
        cleaned = [m.strip().lower() for m in v if m and m.strip()]
        unknown = sorted(set(cleaned) - RUN_METRICS)
        if unknown:
            raise ValueError(
                f"unknown metrics: {', '.join(unknown)}. "
                f"Supported: {', '.join(sorted(RUN_METRICS))}"
            )
        return cleaned or None


class LinkAgentRequest(BaseModel):
    agent_key:  str
    agent_kind: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@datasets_router.get("/datasets")
async def get_datasets(session: dict = Depends(get_current_session)):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_datasets(org_id)
    return {"datasets": [_serialize_dataset(r) for r in rows]}


@datasets_router.post("/datasets", status_code=status.HTTP_201_CREATED)
async def create_new_dataset(
    payload: CreateDatasetRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    try:
        row = await create_dataset(org_id, payload.name, payload.description, payload.kind)
    except Exception as exc:
        if "unique" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A dataset named '{payload.name}' already exists.",
            )
        raise
    return _serialize_dataset({**row, "example_count": 0})


# ── Import from a spreadsheet (CSV / Excel) ───────────────────────────────────
#
# People keep golden Q&A sets in spreadsheets; this lets them upload one and turn
# each row into a dataset example. Parsing is server-side (CSV via the stdlib,
# Excel via openpyxl) so the frontend just uploads the file, previews the
# detected columns, maps input / expected-output, and imports.

_IMPORT_MAX_BYTES = 10 * 1024 * 1024   # 10 MB
_IMPORT_MAX_ROWS = 5000
_IMPORT_MAX_COLS = 60
_IMPORT_PREVIEW_ROWS = 5

# Header names we auto-map to input / expected-output so the mapping is usually
# pre-filled correctly.
_INPUT_HEADER_HINTS = ("input", "question", "prompt", "query", "user", "text", "message")
_EXPECTED_HEADER_HINTS = (
    "expected_output", "expected output", "expected", "output", "answer",
    "response", "target", "label", "gold", "reference", "ideal",
)


def _parse_tabular(filename: str, content: bytes) -> tuple[List[str], List[List[str]]]:
    """Return (headers, rows) from a CSV or Excel file. Raises HTTPException on bad input."""
    name = (filename or "").lower()
    if name.endswith(".csv") or name.endswith(".tsv"):
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")
        delimiter = "\t" if name.endswith(".tsv") else ","
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        rows = [row for row in reader]
    elif name.endswith(".xlsx") or name.endswith(".xlsm") or name.endswith(".xls"):
        try:
            import openpyxl  # imported lazily so the module loads even without it
        except ImportError:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Excel import is unavailable on this server. Upload a CSV instead.",
            )
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        except Exception:
            raise HTTPException(status_code=422, detail="Could not read that Excel file.")
        ws = wb.active
        rows = []
        for r in ws.iter_rows(values_only=True):
            rows.append(["" if c is None else str(c) for c in r])
        wb.close()
    else:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Unsupported file type. Upload a .csv or .xlsx file.",
        )

    # Drop fully blank rows, then split off the header.
    rows = [r for r in rows if any((str(c).strip() for c in r))]
    if not rows:
        raise HTTPException(status_code=422, detail="The file has no rows.")
    headers = [str(h).strip() for h in rows[0][:_IMPORT_MAX_COLS]]
    if not any(headers):
        raise HTTPException(status_code=422, detail="The first row must be column headers.")
    body = [r[:_IMPORT_MAX_COLS] for r in rows[1:]]
    return headers, body


def _suggest_columns(headers: List[str]) -> Dict[str, Optional[str]]:
    lower = {h: h.strip().lower() for h in headers}

    def _match(hints: tuple) -> Optional[str]:
        for h in headers:
            if lower[h] in hints:
                return h
        for h in headers:
            if any(hint in lower[h] for hint in hints):
                return h
        return None

    inp = _match(_INPUT_HEADER_HINTS) or (headers[0] if headers else None)
    exp = _match(_EXPECTED_HEADER_HINTS)
    if exp == inp:
        exp = None
    return {"input": inp, "expected_output": exp}


_ALLOWED_IMPORT_EXT = (".csv", ".tsv", ".xlsx", ".xlsm", ".xls")


class ImportPresignRequest(BaseModel):
    filename: str


class ImportPreviewRequest(BaseModel):
    key: str


class ImportCreateRequest(BaseModel):
    key: str
    name: str
    input_column: str
    expected_column: Optional[str] = None
    kind: str = "single"


class ImportAppendRequest(BaseModel):
    key: str
    input_column: str
    expected_column: Optional[str] = None


def _import_ext(filename: str) -> str:
    name = (filename or "").lower()
    for ext in _ALLOWED_IMPORT_EXT:
        if name.endswith(ext):
            return ext
    raise HTTPException(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        detail="Unsupported file type. Upload a .csv or .xlsx file.",
    )


def _import_prefix(org_id: uuid.UUID) -> str:
    return f"dataset-imports/{org_id}/"


async def _load_import_object(key: str, org_id: uuid.UUID) -> bytes:
    """Read an uploaded object from S3, enforcing it belongs to this org and is
    within the size cap (so a huge upload can't OOM the API)."""
    if not key or not key.startswith(_import_prefix(org_id)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid upload key.")
    if not config.S3_UPLOADS_BUCKET:
        raise HTTPException(status_code=503, detail="File uploads are not configured on this server.")
    try:
        size = await asyncio.to_thread(s3.object_size, key, config.S3_UPLOADS_BUCKET)
    except Exception:
        raise HTTPException(status_code=404, detail="Uploaded file not found. Re-upload and try again.")
    if size > _IMPORT_MAX_BYTES:
        raise HTTPException(status_code=413, detail="File is larger than 10 MB.")
    if size == 0:
        raise HTTPException(status_code=422, detail="The file is empty.")
    return await asyncio.to_thread(s3.get_object_bytes, key, config.S3_UPLOADS_BUCKET)


@datasets_router.post("/datasets/import/presign")
async def import_dataset_presign(
    payload: ImportPresignRequest,
    session: dict = Depends(get_current_session),
):
    """Return a presigned PUT URL so the browser uploads the file directly to S3.

    The key is namespaced by org, and the preview/import endpoints only read keys
    under the caller's own prefix — so one org can't read another's upload.
    """
    if not config.S3_UPLOADS_BUCKET:
        raise HTTPException(status_code=503, detail="File uploads are not configured on this server.")
    ext = _import_ext(payload.filename)
    org_id = uuid.UUID(session["org_id"])
    key = f"{_import_prefix(org_id)}{uuid.uuid4().hex}{ext}"
    url = s3.presigned_put_url(key, bucket=config.S3_UPLOADS_BUCKET)
    return {"upload_url": url, "key": key}


@datasets_router.post("/datasets/import/preview")
async def import_dataset_preview(
    payload: ImportPreviewRequest,
    session: dict = Depends(get_current_session),
):
    """Parse the uploaded (S3) file and return its columns + a small preview so the
    user can map which column is the input and which is the expected output."""
    org_id = uuid.UUID(session["org_id"])
    content = await _load_import_object(payload.key, org_id)
    headers, rows = _parse_tabular(payload.key, content)
    preview = [
        {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        for row in rows[:_IMPORT_PREVIEW_ROWS]
    ]
    return {
        "columns": headers,
        "preview": preview,
        "row_count": len(rows),
        "suggested": _suggest_columns(headers),
        "truncated": len(rows) > _IMPORT_MAX_ROWS,
    }


def _rows_to_examples(
    headers: List[str],
    rows: List[List[str]],
    input_column: str,
    expected_column: Optional[str],
    src_name: str,
) -> List[tuple]:
    """Map parsed rows to (input, expected, metadata) examples. Raises on bad input."""
    if input_column not in headers:
        raise HTTPException(status_code=422, detail=f"Column '{input_column}' not found in the file.")
    if expected_column and expected_column not in headers:
        raise HTTPException(status_code=422, detail=f"Column '{expected_column}' not found in the file.")

    in_idx = headers.index(input_column)
    exp_idx = headers.index(expected_column) if expected_column else None

    examples: List[tuple] = []
    for i, row in enumerate(rows[:_IMPORT_MAX_ROWS]):
        inp = (row[in_idx].strip() if in_idx < len(row) else "")
        if not inp:
            continue  # skip rows with no input
        expected = None
        if exp_idx is not None and exp_idx < len(row):
            expected = row[exp_idx].strip() or None
        # Keep every other column as metadata so nothing from the sheet is lost.
        extra = {
            headers[j]: row[j]
            for j in range(len(headers))
            if j not in (in_idx, exp_idx) and j < len(row) and str(row[j]).strip()
        }
        meta: Dict[str, Any] = {"imported_from": src_name, "row": i + 2}
        if extra:
            meta["columns"] = extra
        examples.append((inp, expected, meta))

    if not examples:
        raise HTTPException(status_code=422, detail="No rows had a non-empty input value.")
    return examples


@datasets_router.post("/datasets/import", status_code=status.HTTP_201_CREATED)
async def import_dataset(
    payload: ImportCreateRequest,
    session: dict = Depends(get_current_session),
):
    """Create a new dataset and import each row of the (S3-uploaded) file."""
    if payload.kind not in ("single", "agentic"):
        raise HTTPException(status_code=422, detail="kind must be 'single' or 'agentic'.")
    if not payload.name.strip():
        raise HTTPException(status_code=422, detail="Dataset name is required.")

    org_id = uuid.UUID(session["org_id"])
    content = await _load_import_object(payload.key, org_id)
    headers, rows = _parse_tabular(payload.key, content)
    examples = _rows_to_examples(
        headers, rows, payload.input_column, payload.expected_column, "import"
    )

    try:
        row = await create_dataset(org_id, payload.name.strip(), "Imported from a file", payload.kind)
    except Exception as exc:
        if "unique" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A dataset named '{payload.name.strip()}' already exists.",
            )
        raise

    inserted = await add_examples_bulk(uuid.UUID(str(row["dataset_id"])), org_id, examples)
    # The raw upload isn't retained — only the rows matter.
    await asyncio.to_thread(s3.delete_object, payload.key, config.S3_UPLOADS_BUCKET)
    return _serialize_dataset({**row, "example_count": inserted})


@datasets_router.post("/datasets/{dataset_id}/examples/import", status_code=status.HTTP_201_CREATED)
async def import_examples_into_dataset(
    dataset_id: uuid.UUID,
    payload: ImportAppendRequest,
    session: dict = Depends(get_current_session),
):
    """Append each row of the (S3-uploaded) file to an EXISTING dataset."""
    org_id = uuid.UUID(session["org_id"])
    content = await _load_import_object(payload.key, org_id)
    headers, rows = _parse_tabular(payload.key, content)
    examples = _rows_to_examples(
        headers, rows, payload.input_column, payload.expected_column, "import"
    )
    inserted = await add_examples_bulk(dataset_id, org_id, examples)
    if inserted == 0:
        # bulk insert returns 0 when the dataset isn't this org's (or absent).
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found.")
    await asyncio.to_thread(s3.delete_object, payload.key, config.S3_UPLOADS_BUCKET)
    return {"dataset_id": str(dataset_id), "imported": inserted}


@datasets_router.post("/datasets/import/discard")
async def import_dataset_discard(
    payload: ImportPreviewRequest,
    session: dict = Depends(get_current_session),
):
    """Best-effort delete of an uploaded-but-not-imported object — e.g. when the
    user cancels the dialog or picks a different file. A bucket lifecycle rule on
    the `dataset-imports/` prefix is the catch-all for anything this misses."""
    org_id = uuid.UUID(session["org_id"])
    if payload.key.startswith(_import_prefix(org_id)) and config.S3_UPLOADS_BUCKET:
        await asyncio.to_thread(s3.delete_object, payload.key, config.S3_UPLOADS_BUCKET)
    return {"ok": True}


@datasets_router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_dataset(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    deleted = await delete_dataset(dataset_id, org_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")


@datasets_router.get("/datasets/{dataset_id}/examples")
async def get_examples(
    dataset_id: uuid.UUID,
    limit:  int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_examples(dataset_id, org_id, limit=limit, offset=offset)
    examples = [_serialize_example(r) for r in rows]
    # Auto-enrich each example with its run's eval / security / cost signals,
    # looked up live from ClickHouse by source_trace_id. This "backfills" as the
    # eval/security workers finish (a refresh picks up new scores). Best-effort:
    # a ClickHouse hiccup must never break the examples list.
    try:
        await _attach_enrichment(org_id, examples)
    except Exception:
        pass
    return {"examples": examples}


@datasets_router.post("/datasets/{dataset_id}/examples", status_code=status.HTTP_201_CREATED)
async def add_dataset_example(
    dataset_id: uuid.UUID,
    payload: AddExampleRequest,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    input_text = payload.input
    expected = payload.expected_output
    # Trace-backed example (a run added from the drawer): pin the whole trajectory
    # (media → S3) into the no-TTL store so the example is self-contained for
    # agentic eval, and derive a representative IO summary from it for agentic
    # wrapper roots (e.g. CrewAI's crew) that have no direct input of their own.
    src = payload.metadata.get("source_trace_id") if isinstance(payload.metadata, dict) else None
    if src:
        try:
            events = await _snapshot_run(org_id, uuid.UUID(str(src)))
            if events and not input_text.strip():
                d_inp, d_out = _trajectory_input_output(events)
                if d_inp.strip():
                    input_text = d_inp
                if not expected and d_out:
                    expected = d_out
        except Exception:
            pass
    row = await add_example(
        dataset_id, org_id,
        input_text=input_text,
        expected_output=expected,
        metadata=payload.metadata,
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    return _serialize_example(row)


@datasets_router.delete(
    "/datasets/{dataset_id}/examples/{example_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_example(
    dataset_id: uuid.UUID,
    example_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    deleted = await delete_example(example_id, dataset_id, org_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Example not found")


@datasets_router.get("/datasets/{dataset_id}/examples/{example_id}/trajectory")
async def get_example_trajectory(
    dataset_id: uuid.UUID,
    example_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """The pinned agent trajectory behind a trace-backed example.

    A dataset example imported from an agent run carries the whole run
    (all spans: LLM calls, tasks/agents, tool calls, MCP calls, media) in the
    no-TTL ``dataset_trajectory_spans`` store, keyed by its ``source_trace_id``.
    The examples list only shows input/expected_output; this returns a compact,
    display-ready summary of the trajectory so the UI can show *how* the run got
    there — the steps, agents, tools and MCP calls — not just its IO.
    """
    org_id = uuid.UUID(session["org_id"])
    # Resolve the example (dataset + org scoped) and its source trace.
    example = await get_example(example_id, dataset_id, org_id)
    if example is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Example not found")
    meta = example.get("metadata") or {}
    if isinstance(meta, str):  # asyncpg returns jsonb as a string
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    src = meta.get("source_trace_id") if isinstance(meta, dict) else None
    if not src:
        # Text-only example (no run behind it) — nothing to show.
        return {"trace_id": None, "has_trajectory": False, "stats": {}, "steps": []}

    try:
        src_uuid = uuid.UUID(str(src))
    except (ValueError, TypeError):
        return {"trace_id": str(src), "has_trajectory": False, "stats": {}, "steps": []}

    # Prefer the pinned snapshot; fall back to a live snapshot (older examples /
    # a run pinned before this feature). Best-effort — never 500 the drawer.
    events: List[dict] = []
    try:
        events = await clickhouse_client.get_dataset_trajectory(org_id, src_uuid)
        if not events:
            events = await _snapshot_run(org_id, src_uuid)
        events = hydrate_trajectory_media(events)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[DATASET-TRAJECTORY] load failed trace=%s: %s", src, exc)
        events = []

    summary = _trajectory_summary(events)
    summary["trace_id"] = str(src)
    summary["has_trajectory"] = bool(summary["steps"])
    return summary


# ── Dataset batch runs (agentic eval / security) ──────────────────────────────

def _synthetic_event(example: dict, trace_id: str) -> dict:
    """Build a single LLM-shaped event from a text-only example so the eval /
    security workers (which read messages/input + response) can score it."""
    raw = example.get("input") or ""
    event: dict = {
        "trace_id":      trace_id,
        "root_trace_id": trace_id,
        "type":          "llm",
        "integration":   "DATASET",
        "input":         raw,
        "response":      example.get("expected_output") or "",
    }
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            event["messages"] = parsed
    except (ValueError, TypeError):
        pass
    return event


def _root_event(events: List[dict], trace_id: str) -> dict:
    """Pick the run's root span (matching trace_id) or fall back to the first."""
    for e in events:
        if e.get("trace_id") == trace_id and (e.get("root_trace_id") in (None, trace_id)):
            return e
    return events[0]


@datasets_router.post("/datasets/{dataset_id}/runs", status_code=status.HTTP_201_CREATED)
async def create_dataset_run(
    dataset_id: uuid.UUID,
    payload: CreateRunRequest,
    session: dict = Depends(get_current_session),
):
    """Launch a batch agentic-eval, security, or metrics run over every example.

    For each example we re-run the *real* source trace when one is recorded
    (``metadata.source_trace_id``) — fetching its full span tree so agentic /
    tool context is preserved — and fall back to a synthetic single-turn event
    for text-only examples. Results land in ClickHouse (keyed by trace_id) via
    the normal workers; ``GET /datasets/runs/{run_id}`` aggregates the report.
    """
    return await _launch_run(uuid.UUID(session["org_id"]), dataset_id, payload)


async def _launch_run(org_id: uuid.UUID, dataset_id: uuid.UUID, payload: CreateRunRequest):
    """Shared run-launch body for the session route and the CI (API-key) route."""
    if payload.kind in ("agentic", "metrics"):
        quota = await get_quota_status(org_id)
        if quota.eval_over:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail=f"Evaluation quota exceeded for {quota.tier} tier.",
            )

    run_metrics = payload.metrics or ["hallucination", "relevance"]
    # Per-run selection: the caller (the Run-evaluation drawer) sends exactly the
    # custom scorers to apply this run, chosen from the dataset's saved library
    # (``dataset_scorers``). So the request is authoritative — no auto-merge.
    custom_judges: Dict[str, float] = payload.custom_judges or {}
    # This dataset's own metric prompts ride along with the job so the evaluator
    # grades with them instead of the org/platform prompt — two datasets can use
    # different hallucination prompts without affecting each other or production.
    judge_prompt_overrides: Dict[str, str] = {}
    if payload.kind in ("metrics", "agentic"):
        judge_prompt_overrides = await list_dataset_judge_prompts(dataset_id, org_id)
    # For metrics runs the kind-specific config column records which metrics
    # ran (the way `depth` records the agentic depth).
    depth = ",".join(run_metrics) if payload.kind == "metrics" else payload.depth
    # Record the judge model so a multi-model comparison can label each run.
    run = await create_run(
        org_id, dataset_id, payload.kind, depth, payload.judge, payload.batch_id,
    )
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    run_id = run["run_id"]

    examples = await list_examples(dataset_id, org_id, limit=500)
    if not examples:
        await finalize_run(run_id, org_id, "complete", {"total": 0, "completed": 0})
        return _serialize_run({**run, "item_count": 0})

    items: List[tuple] = []
    for ex in examples:
        meta = ex.get("metadata") or {}
        # metadata can come back as a JSON string (the jsonb codec isn't applied
        # on every pool path), so parse it — otherwise a trace-backed example's
        # source_trace_id is missed and it silently falls back to a text example
        # with an empty answer, which the evaluator then skips.
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        src = meta.get("source_trace_id") if isinstance(meta, dict) else None
        events: List[dict] = []
        trace_id = ""
        source = "text"

        if src:
            try:
                src_uuid = uuid.UUID(str(src))
                # Prefer the pinned, retention-independent snapshot; if this run
                # was never snapshotted (e.g. auto-appended), pin it now (lazily)
                # while the source trace is still alive.
                events = await clickhouse_client.get_dataset_trajectory(org_id, src_uuid)
                if not events:
                    events = await _snapshot_run(org_id, src_uuid)
                # Rehydrate S3-stored media into fresh presigned URLs so the
                # evaluator's vision/OCR paths can fetch it.
                events = hydrate_trajectory_media(events)
            except Exception:
                events = []
            if events:
                trace_id = str(src)
                source = "trace"

        if source == "text":
            trace_id = str(uuid.uuid4())
            events = [_synthetic_event(ex, trace_id)]

        # ── publish the job the workers already know how to run ──
        if payload.kind == "metrics":
            # Grade the example's recorded answer with the chosen LLM metrics
            # against its expected output. Always under a FRESH trace_id so the
            # eval rows are uniquely attributable to this run (source traces may
            # already carry production/agentic eval rows).
            root = _root_event(events, trace_id)
            answer = str(
                (meta.get("output") if isinstance(meta, dict) else None)
                or root.get("response") or root.get("output") or ""
            )
            event = {**root, "response": answer}
            trace_id = str(uuid.uuid4())
            event["trace_id"] = event["root_trace_id"] = trace_id
            expected = str(ex.get("expected_output") or "").strip()
            job = {
                "operation":       "sdk_llm",
                "organization_id": str(org_id),
                "trace_id":        trace_id,
                "event":           event,
                "eval_config": {
                    "metrics":       run_metrics,
                    "custom_judges": custom_judges,
                    "judge_prompt_overrides": judge_prompt_overrides,
                },
            }
            if payload.judge:
                job["judge"] = payload.judge
            # The reference is the expected output — unless that IS the answer
            # being graded (text-only example with no recorded output), where a
            # self-comparison would trivially score 1.0.
            if expected and expected != answer.strip():
                job["reference"] = expected
            # Trace-backed examples: carry the run's tool/MCP results (across all
            # pinned spans, not just the root) so factual metrics ground the answer
            # on what the agent retrieved — matching the dashboard's Traces eval.
            if source == "trace":
                grounding = _extract_grounding_from_events(events)
                if grounding:
                    job["tool_grounding"] = grounding
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
        elif payload.kind == "agentic":
            if source == "trace":
                job = {
                    "operation":       "agent_eval",
                    "organization_id": str(org_id),
                    "trace_id":        trace_id,
                    "root_trace_id":   trace_id,
                    "events":          events,
                }
                if payload.depth:
                    job["depth"] = payload.depth
                if payload.judge:
                    job["judge"] = payload.judge
                if payload.jury:
                    job["jury"] = payload.jury
                # Custom scorers grade the run's final answer alongside the
                # agentic layers; the dataset's own prompts apply here too.
                if custom_judges or judge_prompt_overrides:
                    job["eval_config"] = {
                        "custom_judges":          custom_judges,
                        "judge_prompt_overrides": judge_prompt_overrides,
                    }
            else:
                # A single synthetic turn has no agentic trajectory; score it with
                # the standard LLM metrics so the example still gets a number.
                job = {
                    "operation":       "sdk_llm",
                    "organization_id": str(org_id),
                    "trace_id":        trace_id,
                    "event":           events[0],
                    "eval_config": {
                        "metrics":                ["hallucination", "relevance"],
                        "custom_judges":          custom_judges,
                        "judge_prompt_overrides": judge_prompt_overrides,
                    },
                }
                if payload.judge:
                    job["judge"] = payload.judge
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
        else:  # security
            job = {
                "operation":       "sdk_security",
                "organization_id": str(org_id),
                "trace_id":        trace_id,
                "event":           _root_event(events, trace_id),
                "security_config": {"guardrail": "default"},
            }
            await kafka_queue.add_job(job, topic=config.KAFKA_SECURITY_TOPIC, key=str(org_id))

        items.append((ex["example_id"], trace_id, source))

    await add_run_items(run_id, org_id, items)
    return _serialize_run({**run, "total": len(items), "item_count": len(items)})


@datasets_router.get("/datasets/{dataset_id}/runs")
async def get_dataset_runs(
    dataset_id: uuid.UUID,
    limit:  int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_runs(dataset_id, org_id, limit=limit, offset=offset)
    # Finalize any run on this page that has quietly finished, so the list
    # reflects reality even when the run's own report was never opened. Bounded
    # to the still-running runs on this page; nothing runs on a timer.
    out: List[dict] = []
    for r in rows:
        if r["status"] == "running" and (r.get("total") or 0) > 0:
            try:
                items = await get_run_items(r["run_id"], org_id)
                r, _summary, _bt = await _summarize_and_finalize(org_id, r, items)
            except Exception:  # noqa: BLE001 — a finalize hiccup must not break the list
                logger.exception("[DATASET] finalize-on-list failed run=%s", r.get("run_id"))
        out.append(_serialize_run(r))
    return {"runs": out}


@datasets_router.get("/datasets/runs/{run_id}")
async def get_dataset_run_report(
    run_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """Aggregate a run's per-example worker results (from ClickHouse) into a
    report, and finalize the run once every example has a result."""
    return await _build_report(uuid.UUID(session["org_id"]), run_id)


async def _summarize_and_finalize(
    org_id: uuid.UUID, run: dict, items: List[dict],
) -> tuple[dict, dict, dict]:
    """Aggregate a run's worker results (from ClickHouse) into a summary and
    finalize the run once every example has one. Returns the (possibly updated)
    run row, the summary, and the by-trace result index. Shared by the report
    and the runs-list endpoints so a run reaches ``complete`` on either fetch —
    not only when its individual report is opened."""
    trace_ids = [it["trace_id"] for it in items]

    if run["kind"] == "agentic":
        rows = await clickhouse_client.fetch_dataset_eval_results(org_id, trace_ids)
        by_trace: Dict[str, Any] = {}
        for r in rows:
            by_trace.setdefault(r["trace_id"], []).append(r)
        summary = _agentic_summary(by_trace, len(items))
    elif run["kind"] == "metrics":
        rows = await clickhouse_client.fetch_dataset_eval_results(org_id, trace_ids)
        by_trace = {}
        for r in rows:
            if r["evaluator"] == "fluiq.eval" and r["score"] is not None:
                by_trace.setdefault(r["trace_id"], []).append(r)
        summary = _metrics_summary(by_trace, len(items))
    else:
        rows = await clickhouse_client.fetch_dataset_security_results(org_id, trace_ids)
        by_trace = {}
        for r in rows:
            by_trace[r["trace_id"]] = r  # dedupe: keep one row per trace
        summary = _security_summary(by_trace, len(items))

    if summary["completed"] >= len(items) and run["status"] == "running":
        await finalize_run(run["run_id"], org_id, "complete", summary)
        run = {
            **run,
            "status": "complete",
            "summary": summary,
            # finalize_run stamps NOW() in Postgres; mirror it here (≈ that time)
            # so the just-finalized row already carries a duration to render.
            "finished_at": datetime.now(timezone.utc),
        }

    return run, summary, by_trace


async def _build_report(org_id: uuid.UUID, run_id: uuid.UUID):
    """Shared report body for the session route and the CI (API-key) route."""
    run = await get_run(run_id, org_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    items = await get_run_items(run_id, org_id)
    trace_ids = [it["trace_id"] for it in items]

    run, summary, by_trace = await _summarize_and_finalize(org_id, run, items)

    item_out = [
        {
            "example_id":      str(it["example_id"]),
            "trace_id":        it["trace_id"],
            "source":          it["source"],
            "input":           it.get("input"),
            "expected_output": it.get("expected_output"),
            "done":            it["trace_id"] in by_trace,
            "result": (
                by_trace.get(it["trace_id"])
                if run["kind"] == "security"
                else [
                    {"metric": r["metric"], "score": r["score"]}
                    for r in by_trace.get(it["trace_id"], [])
                ]
                if run["kind"] == "metrics"
                else None
            ),
        }
        for it in items
    ]
    # What the run actually cost in judge work. Counts, not money, matching the
    # rest of the product: a token count is ground truth while pricing changes.
    # Best-effort so a ClickHouse hiccup can't break the report.
    usage: Dict[str, int] = {"judge_calls": 0, "input_tokens": 0, "output_tokens": 0}
    if run["kind"] != "security":
        try:
            usage = await clickhouse_client.fetch_run_judge_usage(org_id, trace_ids)
        except Exception:  # noqa: BLE001
            pass

    return {
        "run":     _serialize_run(run),
        "summary": summary,
        "items":   item_out,
        "usage":   usage,
    }


def _agentic_summary(by_trace: Dict[str, List[dict]], total: int) -> dict:
    """Blend per-example eval rows into run-level scores + per-layer averages."""
    completed = len(by_trace)
    run_scores: List[float] = []
    passes: List[bool] = []
    layer_scores: Dict[str, List[float]] = {}
    metric_scores: Dict[str, List[float]] = {}
    for rows in by_trace.values():
        for r in rows:
            if r["layer"] == "deterministic" and r["run_score"] is not None:
                run_scores.append(r["run_score"])
                passes.append(bool(r["run_passed"]))
            if r["layer"] and r["score"] is not None:
                layer_scores.setdefault(r["layer"], []).append(r["score"])
            if not r["layer"] and r["score"] is not None and r["evaluator"] == "fluiq.eval":
                metric_scores.setdefault(r["metric"], []).append(r["score"])

    def _avg(xs: List[float]) -> Optional[float]:
        return round(sum(xs) / len(xs), 4) if xs else None

    return {
        "kind":          "agentic",
        "total":         total,
        "completed":     completed,
        "avg_run_score": _avg(run_scores),
        "pass_rate":     round(sum(passes) / len(passes), 4) if passes else None,
        "layers":        {k: _avg(v) for k, v in layer_scores.items()},
        "metrics":       {k: _avg(v) for k, v in metric_scores.items()},
    }


def _metrics_summary(by_trace: Dict[str, List[dict]], total: int) -> dict:
    """Blend per-example metric rows into run-level per-metric averages."""
    completed = len(by_trace)
    metric_scores: Dict[str, List[float]] = {}
    example_avgs: List[float] = []
    for rows in by_trace.values():
        scores = [r["score"] for r in rows]
        for r in rows:
            metric_scores.setdefault(r["metric"], []).append(r["score"])
        if scores:
            example_avgs.append(sum(scores) / len(scores))

    def _avg(xs: List[float]) -> Optional[float]:
        return round(sum(xs) / len(xs), 4) if xs else None

    return {
        "kind":      "metrics",
        "total":     total,
        "completed": completed,
        "avg_score": _avg(example_avgs),
        "metrics":   {k: _avg(v) for k, v in metric_scores.items()},
    }


# Score movements smaller than this are noise (LLM judges are not perfectly
# deterministic), so the comparison classifies them as unchanged.
_COMPARE_EPSILON = 0.05


def _example_scores(kind: str, rows: List[dict]) -> tuple[Optional[float], Dict[str, float]]:
    """One example's (overall score, per-metric/layer scores) for comparison."""
    per: Dict[str, float] = {}
    overall: Optional[float] = None
    if kind == "agentic":
        for r in rows:
            if r["layer"] and r["score"] is not None:
                per[r["layer"]] = r["score"]
            if r["layer"] == "deterministic" and r["run_score"] is not None:
                overall = r["run_score"]
        if overall is None and per:
            overall = min(per.values())
    else:  # metrics
        scored = [r for r in rows if r["evaluator"] == "fluiq.eval" and r["score"] is not None]
        for r in scored:
            per[r["metric"]] = r["score"]
        if scored:
            overall = sum(r["score"] for r in scored) / len(scored)
    return overall, per


@datasets_router.get("/datasets/runs/{run_id}/compare")
async def compare_dataset_runs(
    run_id: uuid.UUID,
    against: uuid.UUID = Query(..., description="Baseline run to compare against"),
    session: dict = Depends(get_current_session),
):
    """Run-vs-run regression report: per-metric deltas plus every example that
    regressed, improved, or stayed flat versus the baseline run. Examples are
    joined by example_id (trace ids differ between runs)."""
    org_id = uuid.UUID(session["org_id"])
    run = await get_run(run_id, org_id)
    baseline = await get_run(against, org_id)
    if run is None or baseline is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
    if run["dataset_id"] != baseline["dataset_id"]:
        raise HTTPException(status_code=400, detail="Runs belong to different datasets")
    if run["kind"] != baseline["kind"]:
        raise HTTPException(status_code=400, detail="Runs are of different kinds")
    if run["kind"] not in ("agentic", "metrics"):
        raise HTTPException(
            status_code=400, detail="Comparison supports agentic and metrics runs"
        )

    items_a = await get_run_items(run_id, org_id)
    items_b = await get_run_items(against, org_id)
    rows_a = await clickhouse_client.fetch_dataset_eval_results(
        org_id, [it["trace_id"] for it in items_a]
    )
    rows_b = await clickhouse_client.fetch_dataset_eval_results(
        org_id, [it["trace_id"] for it in items_b]
    )
    by_trace_a: Dict[str, List[dict]] = {}
    for r in rows_a:
        by_trace_a.setdefault(r["trace_id"], []).append(r)
    by_trace_b: Dict[str, List[dict]] = {}
    for r in rows_b:
        by_trace_b.setdefault(r["trace_id"], []).append(r)

    a_by_example = {str(it["example_id"]): it for it in items_a}
    b_by_example = {str(it["example_id"]): it for it in items_b}

    examples: List[dict] = []
    counts = {"regressed": 0, "improved": 0, "unchanged": 0, "added": 0, "missing": 0}
    metric_pairs: Dict[str, List[tuple[float, float]]] = {}

    for ex_id, it in a_by_example.items():
        score_a, per_a = _example_scores(run["kind"], by_trace_a.get(it["trace_id"], []))
        base_it = b_by_example.get(ex_id)
        if base_it is None:
            counts["added"] += 1
            status_label = "added"
            score_b, per_b = None, {}
        else:
            score_b, per_b = _example_scores(
                run["kind"], by_trace_b.get(base_it["trace_id"], [])
            )
            if score_a is None or score_b is None:
                status_label = "pending"
            elif score_a < score_b - _COMPARE_EPSILON:
                counts["regressed"] += 1
                status_label = "regressed"
            elif score_a > score_b + _COMPARE_EPSILON:
                counts["improved"] += 1
                status_label = "improved"
            else:
                counts["unchanged"] += 1
                status_label = "unchanged"
            for name in set(per_a) & set(per_b):
                metric_pairs.setdefault(name, []).append((per_a[name], per_b[name]))
        examples.append({
            "example_id":     ex_id,
            "input":          (str(it.get("input") or ""))[:300],
            "score":          score_a,
            "baseline_score": score_b,
            "delta": round(score_a - score_b, 4) if score_a is not None and score_b is not None else None,
            "scores":          per_a,
            "baseline_scores": per_b,
            "status":          status_label,
        })
    counts["missing"] = len(set(b_by_example) - set(a_by_example))

    # Per-metric deltas over examples present in BOTH runs, so a changed
    # example mix can't masquerade as a score movement.
    metrics_out = [
        {
            "metric":       name,
            "avg":          round(sum(a for a, _ in pairs) / len(pairs), 4),
            "baseline_avg": round(sum(b for _, b in pairs) / len(pairs), 4),
            "delta":        round(sum(a - b for a, b in pairs) / len(pairs), 4),
        }
        for name, pairs in sorted(metric_pairs.items())
    ]

    # Regressions first, biggest drop on top.
    order = {"regressed": 0, "improved": 1, "unchanged": 2, "added": 3, "pending": 4}
    examples.sort(key=lambda e: (order.get(e["status"], 5), e["delta"] if e["delta"] is not None else 0))

    return {
        "run":      _serialize_run(run),
        "baseline": _serialize_run(baseline),
        "metrics":  metrics_out,
        "examples": examples,
        "summary":  counts,
    }


# Distinct path (not /datasets/runs/...) so it can't be shadowed by the
# /datasets/runs/{run_id} route regardless of registration order.
@datasets_router.get("/datasets/run-comparison")
async def compare_runs_multi(
    runs: str = Query(..., description="Comma-separated run ids (2-6)"),
    metric: Optional[str] = Query(
        None,
        description=(
            "Drill the per-example table into one metric/layer instead of the "
            "blended overall score. Must be one of the metrics in the response."
        ),
    ),
    session: dict = Depends(get_current_session),
):
    """Compare N runs of the same dataset side by side.

    Built for multi-model comparison: launching a metrics run per model produces
    one run each, and this aggregates them into a per-metric matrix (a column per
    run/model) plus a per-example table sorted by disagreement — the examples
    where the models most disagree are the ones worth reading.
    """
    org_id = uuid.UUID(session["org_id"])

    ids: List[uuid.UUID] = []
    for raw in runs.split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            ids.append(uuid.UUID(raw))
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Invalid run id '{raw}'")
    if len(ids) < 2:
        raise HTTPException(status_code=400, detail="Pick at least two runs to compare")
    if len(ids) > 6:
        raise HTTPException(status_code=400, detail="Compare at most six runs at once")

    run_rows: List[dict] = []
    for rid in ids:
        row = await get_run(rid, org_id)
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        run_rows.append(row)

    base = run_rows[0]
    if any(r["dataset_id"] != base["dataset_id"] for r in run_rows):
        raise HTTPException(status_code=400, detail="Runs belong to different datasets")
    if any(r["kind"] != base["kind"] for r in run_rows):
        raise HTTPException(status_code=400, detail="Runs are of different kinds")
    if base["kind"] not in ("agentic", "metrics"):
        raise HTTPException(
            status_code=400, detail="Comparison supports agentic and metrics runs"
        )

    # run_id -> example_id -> (overall, {metric: score})
    per_run: Dict[str, Dict[str, tuple]] = {}
    inputs: Dict[str, str] = {}
    for row in run_rows:
        rid = str(row["run_id"])
        items = await get_run_items(row["run_id"], org_id)
        rows = await clickhouse_client.fetch_dataset_eval_results(
            org_id, [it["trace_id"] for it in items]
        )
        by_trace: Dict[str, List[dict]] = {}
        for r in rows:
            by_trace.setdefault(r["trace_id"], []).append(r)
        scored: Dict[str, tuple] = {}
        for it in items:
            ex_id = str(it["example_id"])
            inputs.setdefault(ex_id, (str(it.get("input") or ""))[:300])
            scored[ex_id] = _example_scores(base["kind"], by_trace.get(it["trace_id"], []))
        per_run[rid] = scored

    def _avg(xs: List[float]) -> Optional[float]:
        return round(sum(xs) / len(xs), 4) if xs else None

    # Per-metric matrix: one row per metric/layer, one column per run.
    metric_names = sorted(
        {name for scored in per_run.values() for (_o, per) in scored.values() for name in per}
    )
    metrics_out = [
        {
            "metric": name,
            "scores": {
                rid: _avg([per[name] for (_o, per) in scored.values() if name in per])
                for rid, scored in per_run.items()
            },
        }
        for name in metric_names
    ]

    overall = {
        rid: _avg([o for (o, _per) in scored.values() if o is not None])
        for rid, scored in per_run.items()
    }
    completed = {
        rid: sum(1 for (o, _per) in scored.values() if o is not None)
        for rid, scored in per_run.items()
    }

    # Per-example scores, ordered by widest disagreement. Without a metric this
    # is each run's overall score; with one it drills into that metric alone,
    # which is where a model-vs-model difference usually actually lives.
    drill = metric if metric and metric in metric_names else None
    example_ids = {ex for scored in per_run.values() for ex in scored}
    examples_out: List[dict] = []
    for ex_id in example_ids:
        scores = {}
        for rid, scored in per_run.items():
            overall_score, per_metric = scored.get(ex_id) or (None, {})
            scores[rid] = per_metric.get(drill) if drill else overall_score
        present = [v for v in scores.values() if v is not None]
        spread = round(max(present) - min(present), 4) if len(present) >= 2 else None
        examples_out.append({
            "example_id": ex_id,
            "input":      inputs.get(ex_id, ""),
            "scores":     scores,
            "spread":     spread,
        })
    examples_out.sort(key=lambda e: (e["spread"] is None, -(e["spread"] or 0.0)))

    return {
        "runs":       [_serialize_run(r) for r in run_rows],
        "overall":    overall,
        "completed":  completed,
        "metrics":    metrics_out,
        "examples":   examples_out,
        # Which metric the per-example table is showing (None = overall), and
        # what else can be drilled into.
        "metric":     drill,
        "metric_options": metric_names,
    }


_THREAT_KEYS = [
    "injection", "jailbreak", "skeleton_key", "secrets", "indirect_injection",
    "rag_poisoning", "tool_exfiltration", "tool_policy_violation",
    "cross_agent_injection", "image_injection",
]


def _security_summary(by_trace: Dict[str, dict], total: int) -> dict:
    completed = len(by_trace)
    risk_levels: Dict[str, int] = {}
    threats = {k: 0 for k in _THREAT_KEYS}
    blocked = 0
    flagged = 0
    for r in by_trace.values():
        lvl = (r.get("risk_level") or "clean") or "clean"
        risk_levels[lvl] = risk_levels.get(lvl, 0) + 1
        is_blocked = bool(r.get("should_block"))
        if is_blocked:
            blocked += 1
        detected = False
        for k in _THREAT_KEYS:
            if r.get(k):
                threats[k] += 1
                detected = True
        # "Flagged" = worth a look: a real threat, a block, or medium/high risk.
        # A plain 'low' score with nothing detected is the clean baseline every
        # scan starts from, so it does not count.
        if is_blocked or detected or lvl in ("medium", "high"):
            flagged += 1
    return {
        "kind":        "security",
        "total":       total,
        "completed":   completed,
        "risk_levels": risk_levels,
        "flagged":     flagged,
        "blocked":     blocked,
        "threats":     {k: v for k, v in threats.items() if v > 0},
    }


# ── CI (API-key) runs — `python -m fluiq.ci` / GitHub Actions ────────────────
# Distinct /ci/eval-runs prefix so these can't collide with the
# /datasets/{dataset_id}/... session routes.

class CIRunRequest(CreateRunRequest):
    api_key:      Optional[str] = None       # also accepted via header
    dataset_id:   Optional[uuid.UUID] = None
    dataset_name: Optional[str] = None       # case-insensitive name lookup


async def _resolve_ci_org(header_key: Optional[str], body_key: Optional[str]) -> uuid.UUID:
    key = header_key or body_key
    if not key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, _prefix, _key_id = resolved
    return org_id


@datasets_router.post("/ci/eval-runs", status_code=status.HTTP_201_CREATED)
async def ci_create_run(
    payload: CIRunRequest,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Launch a dataset eval run from CI, authenticated by API key."""
    org_id = await _resolve_ci_org(api_key, payload.api_key)

    dataset_id = payload.dataset_id
    if dataset_id is None:
        name = (payload.dataset_name or "").strip().lower()
        if not name:
            raise HTTPException(status_code=422, detail="dataset_id or dataset_name required")
        rows = await list_datasets(org_id)
        match = next((r for r in rows if str(r.get("name") or "").strip().lower() == name), None)
        if match is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No dataset named '{payload.dataset_name}'",
            )
        dataset_id = match["dataset_id"]

    return await _launch_run(org_id, dataset_id, payload)


@datasets_router.get("/ci/eval-runs/{run_id}")
async def ci_run_report(
    run_id: uuid.UUID,
    api_key: Optional[str] = Depends(extract_api_key),
):
    """Fetch a run's report from CI, authenticated by API key."""
    org_id = await _resolve_ci_org(api_key, None)
    return await _build_report(org_id, run_id)


# ── Agent links (Connect Agents) ──────────────────────────────────────────────

# Import safety bounds: page the agent's root traces and cap a single connect so
# a very high-volume agent can't enqueue an unbounded insert.
_IMPORT_PAGE = 200
_IMPORT_CAP = 2000


def _event_to_example(event: dict, cost: Any) -> tuple:
    """Turn a trace's root event into an (input, expected_output, metadata)
    example — the server-side mirror of the frontend ``traceToDatasetExample``."""
    req = (
        event.get("messages")
        or event.get("input")
        or event.get("contents")
        or event.get("prompts")
    )
    if isinstance(req, str):
        input_text = req
    elif req is not None:
        input_text = json.dumps(req, default=str)
    else:
        input_text = ""

    resp = event.get("output")
    if resp is None:
        resp = event.get("response")
    if resp is None:
        expected: Optional[str] = None
    elif isinstance(resp, str):
        expected = resp
    else:
        expected = json.dumps(resp, default=str)

    metadata: Dict[str, Any] = {}
    model = event.get("model")
    if isinstance(model, str) and model:
        metadata["model"] = model
    if isinstance(cost, (int, float)):
        metadata["cost"] = cost
    tid = event.get("trace_id")
    if isinstance(tid, str) and tid:
        metadata["source_trace_id"] = tid
    integ = event.get("integration")
    if isinstance(integ, str) and integ:
        metadata["integration"] = integ
    return input_text, expected, metadata


def _trajectory_input_output(events: List[dict]) -> tuple:
    """Derive (input, expected) from a run's full trajectory.

    Used for agentic wrapper roots that carry no direct IO of their own (e.g.
    CrewAI's crew span, whose ``kickoff()`` had no inputs): take the first real
    input among the run's spans and the last real output, so the example
    represents the whole multi-agent run instead of an empty root.
    """
    inp = ""
    for ev in events:
        req = ev.get("messages") or ev.get("input") or ev.get("contents") or ev.get("prompts")
        if req:
            inp = req if isinstance(req, str) else json.dumps(req, default=str)
            if inp.strip():
                break
    out = ""
    for ev in reversed(events):
        r = ev.get("output")
        if r is None:
            r = ev.get("response")
        if r:
            out = r if isinstance(r, str) else json.dumps(r, default=str)
            if out.strip():
                break
    return inp, (out or None)


# ── Trajectory display summary ────────────────────────────────────────────────
# Compact, display-ready view of a pinned run. Mirrors (a lean subset of) the
# agentic evaluator's Layer-0 adapter so the Datasets UI shows the same steps /
# tools / MCP calls the evaluator scores, without importing worker code.

def _traj_preview(value: Any, limit: int = 600) -> Optional[str]:
    """A short text preview of an input/output/arg value (JSON-encode non-str)."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value
    else:
        try:
            s = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            s = str(value)
    s = s.strip()
    if not s:
        return None
    return s[:limit] + ("…" if len(s) > limit else "")


def _traj_agent(event: dict) -> Optional[str]:
    """The logical agent a step belongs to (LangGraph node / crew role / fn)."""
    lg = event.get("langgraph")
    if isinstance(lg, dict) and lg.get("langgraph_node"):
        return str(lg["langgraph_node"])
    for k in ("agent", "agent_name", "crew_agent", "role"):
        v = event.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return None


def _traj_tool_calls(event: dict) -> List[dict]:
    """Tool + MCP calls on an event, across OpenAI/Anthropic/Gemini/MCP shapes."""
    calls: List[dict] = []

    def add(name, arguments, kind="tool", server=None, output=None):
        if not name:
            return
        calls.append({
            "name": str(name),
            "kind": kind,
            "server": server,
            "arguments": _traj_preview(arguments, 300),
            "output": _traj_preview(output, 300),
        })

    for tc in event.get("tool_calls") or []:
        if isinstance(tc, dict):
            fn = tc.get("function") or {}
            add(fn.get("name") or tc.get("name"),
                fn.get("arguments") if "arguments" in fn else tc.get("arguments"))
    for tu in event.get("tool_uses") or []:
        if isinstance(tu, dict):
            add(tu.get("name"), tu.get("input"))
    for fc in event.get("function_calls") or []:
        if isinstance(fc, dict):
            add(fc.get("name"), fc.get("args"))
    for mc in event.get("mcp_calls") or []:
        if isinstance(mc, dict) and mc.get("type") in (None, "mcp_call") and mc.get("name"):
            add(mc.get("name"), mc.get("arguments"), kind="mcp",
                server=mc.get("server_label"), output=mc.get("output"))
    return calls


def _grounding_block(name: Any, tool_input: Any, output: Any) -> Optional[str]:
    """One grounding line: 'Tool "name" (input: …) returned:\\n<output>'."""
    out = _traj_preview(output, 1000)
    if not out:
        return None
    head = f'Tool "{name or "tool"}"'
    inp = _traj_preview(tool_input, 400)
    if inp:
        head += f" (input: {inp})"
    return f"{head} returned:\n{out}"


def _extract_grounding_from_events(events: List[dict]) -> List[str]:
    """Tool + MCP results across ALL trajectory spans of a trace-backed example.

    Trace-backed dataset examples pin the whole run (root LLM span plus separate
    tool / MCP child spans). Only the root event is sent to the evaluator, so the
    tool outputs would otherwise be invisible to the judge. This walks every span
    and returns grounding blocks (input + output) the worker feeds as context, so
    tool-backed answers aren't flagged as hallucinations in dataset metric runs.
    """
    blocks: List[str] = []
    for e in events:
        if not isinstance(e, dict):
            continue
        etype = str(e.get("type") or "").lower()

        # Dedicated tool / MCP child spans carry their own input/output.
        if ("tool" in etype or "mcp" in etype) and e.get("output") is not None:
            blk = _grounding_block(e.get("function") or e.get("name"), e.get("input"), e.get("output"))
            if blk:
                blocks.append(blk)

        # MCP calls carrying an inline output.
        for mc in e.get("mcp_calls") or []:
            if isinstance(mc, dict) and mc.get("output") is not None:
                srv = mc.get("server_label") or mc.get("server_name")
                label = f'{mc.get("name") or "mcp"}' + (f" (MCP server: {srv})" if srv else " (MCP)")
                blk = _grounding_block(
                    label,
                    mc.get("arguments") if mc.get("arguments") is not None else mc.get("input"),
                    mc.get("output"),
                )
                if blk:
                    blocks.append(blk)

        # Embedded tool-result messages (OpenAI role:"tool", Anthropic tool_result),
        # correlated to their call's input args by id.
        msgs = e.get("messages")
        if isinstance(msgs, list):
            call_info: Dict[str, tuple] = {}
            for m in msgs:
                if not isinstance(m, dict):
                    continue
                for tc in m.get("tool_calls") or []:
                    if isinstance(tc, dict) and isinstance(tc.get("id"), str):
                        fn = tc.get("function") or {}
                        call_info[tc["id"]] = (
                            fn.get("name") or tc.get("name"),
                            fn.get("arguments") if "arguments" in fn else tc.get("arguments"),
                        )
            for m in msgs:
                if not isinstance(m, dict):
                    continue
                if str(m.get("role") or "").lower() == "tool":
                    cid = m.get("tool_call_id")
                    nm, inp = call_info.get(cid, (m.get("name"), None)) if isinstance(cid, str) else (m.get("name"), None)
                    blk = _grounding_block(nm or m.get("name"), inp, m.get("content"))
                    if blk:
                        blocks.append(blk)
                    continue
                content = m.get("content")
                if isinstance(content, list):
                    for b in content:
                        if isinstance(b, dict) and b.get("type") == "tool_result":
                            cid = b.get("tool_use_id")
                            nm, inp = call_info.get(cid, (None, None)) if isinstance(cid, str) else (None, None)
                            blk = _grounding_block(nm, inp, b.get("content"))
                            if blk:
                                blocks.append(blk)

    # De-duplicate, cap so the eval message stays small.
    seen: set = set()
    uniq: List[str] = []
    for b in blocks:
        if b in seen:
            continue
        seen.add(b)
        uniq.append(b)
    return uniq[:12]


def _traj_media(event: dict) -> List[dict]:
    """Distinct media (kind/mime) referenced anywhere in the event."""
    out: List[dict] = []
    seen: set = set()

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            ref = obj.get("_media_ref")
            if isinstance(ref, dict):
                key = (ref.get("kind"), ref.get("mime"), ref.get("sha256"))
                if key not in seen:
                    seen.add(key)
                    out.append({"kind": ref.get("kind"), "mime": ref.get("mime")})
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(event)
    return out


def _trajectory_summary(events: List[dict]) -> Dict[str, Any]:
    """Build a compact, DFS-ordered step list + rollup stats from pinned spans.

    Steps are ordered by a depth-first walk of the ``parent_id`` tree (the events
    carry no absolute timestamp), so the container/root comes first and children
    read in call order; ``depth`` lets the UI indent the hierarchy.
    """
    events = [e for e in events if isinstance(e, dict)]
    if not events:
        return {"stats": {}, "steps": []}

    by_id: Dict[str, dict] = {}
    children: Dict[Optional[str], List[str]] = {}
    order_seen: List[str] = []
    for e in events:
        tid = str(e.get("trace_id") or "")
        if not tid or tid in by_id:
            # No id (or a duplicate) — give it a synthetic id so it still renders
            # once instead of overwriting the original / rendering twice.
            tid = f"_anon_{len(order_seen)}"
        by_id[tid] = e
        order_seen.append(tid)
    for tid in order_seen:
        e = by_id[tid]
        pid = e.get("parent_id")
        pid = str(pid) if pid else None
        if pid == tid:            # self-parent (e.g. CrewAI crew) → treat as root
            pid = None
        if pid not in by_id:      # phantom/absent parent → root
            pid = None
        children.setdefault(pid, []).append(tid)

    steps: List[dict] = []
    agents: set = set()
    models: set = set()
    type_counts: Dict[str, int] = {}
    n_tools = n_mcp = 0

    def visit(tid: str, depth: int) -> None:
        nonlocal n_tools, n_mcp
        e = by_id[tid]
        etype = str(e.get("type") or "llm")
        type_counts[etype] = type_counts.get(etype, 0) + 1
        name = e.get("function") or e.get("name") or e.get("api")
        agent = _traj_agent(e)
        # An explicit agent span (CrewAI/ADK) names the sub-agent in its
        # function/name, not a role field — surface it as the agent too.
        if not agent and etype == "agent" and isinstance(name, str) and name.strip():
            agent = name
        if agent:
            agents.add(agent)
        model = e.get("model")
        if isinstance(model, str) and model:
            models.add(model)
        tcalls = _traj_tool_calls(e)
        for c in tcalls:
            if c["kind"] == "mcp":
                n_mcp += 1
            else:
                n_tools += 1
        steps.append({
            "trace_id":    tid,
            "depth":       depth,
            "type":        etype,
            "name":        name,
            "agent":       agent,
            "model":       model if isinstance(model, str) else None,
            "integration": e.get("integration"),
            "latency":     e.get("latency") if isinstance(e.get("latency"), (int, float)) else None,
            "input":       _traj_preview(
                e.get("messages") or e.get("input") or e.get("contents") or e.get("prompts")),
            "output":      _traj_preview(e.get("output") if e.get("output") is not None else e.get("response")),
            "tool_calls":  tcalls,
            "media":       _traj_media(e),
            "success":     e.get("success"),
            "error":       _traj_preview(e.get("error") or e.get("error_traceback"), 300),
        })
        for child in children.get(tid, []):
            visit(child, depth + 1)

    for tid in children.get(None, []):
        visit(tid, 0)
    # Safety net: any event not reached via the tree (cycle) still gets listed.
    reached = {s["trace_id"] for s in steps}
    for tid in order_seen:
        if tid not in reached:
            visit(tid, 0)

    stats = {
        "spans":  len(steps),
        "tools":  n_tools,
        "mcp":    n_mcp,
        "agents": sorted(agents),
        "models": sorted(models),
        "types":  type_counts,
    }
    return {"stats": stats, "steps": steps}


async def _snapshot_run(org_id: uuid.UUID, root_trace_id: uuid.UUID) -> List[dict]:
    """Pin a run's full trajectory into the no-TTL store, media offloaded to S3.

    Copies every span of the run into ``fluiq.dataset_trajectory_spans`` (which
    has no TTL) after uploading any media to S3, so a dataset example stays fully
    evaluable — trajectory, tools, MCP calls, media — even after the source trace
    hits its retention TTL. Returns the (media-offloaded) span events, or [].
    """
    rows = await clickhouse_client.fetch_traces(
        org_id, root_trace_id=root_trace_id, limit=500,
    )
    events = [r["event"] for r in rows if isinstance(r.get("event"), dict)]
    if not events:
        return []
    events = await store_trajectory_media(org_id, events)
    await clickhouse_client.insert_dataset_trajectory(org_id, root_trace_id, events)
    return events


async def _import_agent_runs(
    org_id: uuid.UUID, dataset_id: uuid.UUID, agent_key: str, agent_kind: str,
) -> int:
    """Import every past root run of an agent as a dataset example (deduped by
    source trace id, capped at ``_IMPORT_CAP``). Each example keeps its
    ``source_trace_id`` so a dataset run re-reads the FULL trajectory (all spans)
    for agentic eval — the example IO is a representative summary, not the whole
    run, which lives in ClickHouse under that trace."""
    seen = await existing_source_trace_ids(dataset_id, org_id)
    to_add: List[tuple] = []
    offset = 0
    while len(to_add) < _IMPORT_CAP:
        rows = await clickhouse_client.fetch_traces(
            org_id, agent_key=agent_key, agent_kind=agent_kind,
            roots_only=True, limit=_IMPORT_PAGE, offset=offset,
        )
        if not rows:
            break
        for r in rows:
            event = r.get("event")
            if not isinstance(event, dict):
                continue
            tid = event.get("trace_id")
            if isinstance(tid, str):
                if tid in seen:
                    continue
                seen.add(tid)
            inp, expected, meta = _event_to_example(event, r.get("cost"))
            # Pin the full trajectory (media → S3) into the no-TTL store so the
            # example is self-contained for agentic eval regardless of the source
            # trace's retention. Also derive a representative IO summary from the
            # whole run for agentic wrapper roots (e.g. CrewAI's crew) that carry
            # no direct input of their own.
            snap_events: List[dict] = []
            if isinstance(tid, str):
                try:
                    snap_events = await _snapshot_run(org_id, uuid.UUID(tid))
                except Exception:
                    snap_events = []
            if not inp.strip() and snap_events:
                d_inp, d_out = _trajectory_input_output(snap_events)
                if d_inp.strip():
                    inp = d_inp
                if not expected and d_out:
                    expected = d_out
            # Keep the run if it has any representable content (input or output);
            # the pinned trajectory preserves the full run for eval either way.
            if not inp.strip() and not (expected and str(expected).strip()):
                continue
            to_add.append((inp, expected, meta))
            if len(to_add) >= _IMPORT_CAP:
                break
        if len(rows) < _IMPORT_PAGE:
            break
        offset += _IMPORT_PAGE
    return await add_examples_bulk(dataset_id, org_id, to_add)


@datasets_router.post("/datasets/{dataset_id}/agents", status_code=status.HTTP_201_CREATED)
async def link_dataset_agent(
    dataset_id: uuid.UUID,
    payload: LinkAgentRequest,
    session: dict = Depends(get_current_session),
):
    """Connect an agent to a dataset: import ALL of its runs to date as examples
    now, and link it so future runs auto-append (the auto-append hook lives in
    the tracer worker). Re-connecting is safe — already-imported runs are skipped
    by their source trace id."""
    org_id = uuid.UUID(session["org_id"])
    ok = await link_agent(dataset_id, org_id, payload.agent_key, payload.agent_kind)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    imported = await _import_agent_runs(org_id, dataset_id, payload.agent_key, payload.agent_kind)
    return {"ok": True, "imported": imported}


@datasets_router.get("/datasets/{dataset_id}/agents")
async def get_dataset_agents(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    links = await list_agent_links(dataset_id, org_id)
    return {
        "agents": [
            {
                "agent_key":  l["agent_key"],
                "agent_kind": l["agent_kind"],
                "created_at": l["created_at"].isoformat() if l.get("created_at") else None,
            }
            for l in links
        ]
    }


@datasets_router.delete(
    "/datasets/{dataset_id}/agents", status_code=status.HTTP_204_NO_CONTENT,
)
async def unlink_dataset_agent(
    dataset_id: uuid.UUID,
    agent_key: str = Query(...),
    agent_kind: str = Query(...),
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    removed = await unlink_agent(dataset_id, org_id, agent_key, agent_kind)
    if not removed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")


# ── Custom scorers (dataset-scoped client judges) ─────────────────────────────

class SaveScorerRequest(BaseModel):
    name:      str
    template:  str
    threshold: float = 0.5
    slug:      Optional[str] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        if len(v) > 120:
            raise ValueError("name must be 120 characters or fewer")
        return v

    @field_validator("template")
    @classmethod
    def _validate_template(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("prompt is required")
        # Mirrors the Prompts page: a judge prompt must reference the answer it
        # grades, else it can't see the example's output.
        if not ANSWER_PLACEHOLDER_RE.search(v):
            raise ValueError(
                "A scorer prompt must reference the {{answer}} placeholder "
                "(the example's output being graded)."
            )
        return v

    @field_validator("threshold")
    @classmethod
    def _validate_threshold(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("threshold must be between 0 and 1")
        return v


_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def _slugify(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return s[:63]


def _serialize_scorer(row: dict) -> dict:
    return {
        "slug":       row["slug"],
        "name":       row.get("name") or row["slug"],
        "template":   row.get("template"),
        "threshold":  float(row["threshold"]) if row.get("threshold") is not None else 0.5,
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
    }


class SuggestScorersRequest(BaseModel):
    count: int = 4
    judge: Optional[str] = None  # "provider:model", else server default


@datasets_router.post("/datasets/{dataset_id}/scorers/suggest")
async def suggest_dataset_scorers(
    dataset_id: uuid.UUID,
    payload: SuggestScorersRequest,
    session: dict = Depends(get_current_session),
):
    """Draft custom scorers for this dataset from a sample of its examples.

    Runs an LLM 'eval engineering' pass in the evaluator worker (which owns the
    judge + BYOK), and returns proposals the user reviews and saves. Nothing is
    persisted here — the user accepts a proposal via the normal save route.
    """
    org_id = uuid.UUID(session["org_id"])
    if not await dataset_owned(dataset_id, org_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    examples = await list_examples(dataset_id, org_id, limit=12)
    if not examples:
        return {"proposals": []}

    existing = [s.get("name") for s in await list_dataset_scorers(dataset_id, org_id)]
    correlation_id = str(uuid.uuid4())
    job: Dict[str, Any] = {
        "operation":       "propose_scorers",
        "organization_id": str(org_id),
        "correlation_id":  correlation_id,
        "count":           max(1, min(6, payload.count)),
        "existing":        [n for n in existing if n],
        "examples": [
            {"input": e.get("input"), "expected_output": e.get("expected_output")}
            for e in examples
        ],
    }
    if payload.judge:
        job["judge"] = payload.judge

    await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
    result = await wait_for_playground_reply(correlation_id, timeout=45.0)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Scorer suggestion timed out. Check that a judge model key is configured.",
        )
    return {"proposals": result.get("proposals", []), "error": result.get("error")}


@datasets_router.get("/datasets/{dataset_id}/scorers")
async def get_dataset_scorers(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    org_id = uuid.UUID(session["org_id"])
    rows = await list_dataset_scorers(dataset_id, org_id)
    return {"scorers": [_serialize_scorer(r) for r in rows]}


@datasets_router.post("/datasets/{dataset_id}/scorers", status_code=status.HTTP_201_CREATED)
async def save_dataset_scorer(
    dataset_id: uuid.UUID,
    payload: SaveScorerRequest,
    session: dict = Depends(get_current_session),
):
    """Create (or update) a custom scorer and save it to the dataset.

    The judge prompt lives in the org-wide prompt library (``kind='judge'``) so
    it's reusable across datasets; this also links it to the dataset (with its
    threshold) so the dataset remembers it for future metrics runs.
    """
    org_id = uuid.UUID(session["org_id"])
    if not await dataset_owned(dataset_id, org_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    slug = (payload.slug or _slugify(payload.name)).strip().lower()
    if not _SLUG_RE.match(slug):
        raise HTTPException(
            status_code=422,
            detail="Could not derive a valid slug from the name; use letters, numbers and hyphens.",
        )

    existing = await get_prompt_by_slug(org_id, slug)
    if existing is not None:
        if (existing.get("kind") or "completion") != "judge":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A non-scorer prompt with slug '{slug}' already exists.",
            )
        await update_prompt(
            existing["prompt_id"], org_id, name=payload.name, template=payload.template
        )
    else:
        await create_prompt(
            org_id, payload.name, slug, payload.template,
            model=None, variables=[], kind="judge",
        )

    await link_dataset_scorer(dataset_id, org_id, slug, payload.threshold)
    return _serialize_scorer({
        "slug": slug, "name": payload.name, "template": payload.template,
        "threshold": payload.threshold, "created_at": None,
    })


@datasets_router.delete(
    "/datasets/{dataset_id}/scorers/{slug}", status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_dataset_scorer(
    dataset_id: uuid.UUID,
    slug: str,
    session: dict = Depends(get_current_session),
):
    """Unlink a scorer from the dataset. The reusable judge prompt is kept."""
    org_id = uuid.UUID(session["org_id"])
    removed = await unlink_dataset_scorer(dataset_id, org_id, slug)
    if not removed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Scorer not found")


# ── Per-dataset judge prompts (metric prompts scoped to one dataset) ──────────

class SaveDatasetJudgePromptRequest(BaseModel):
    template: str


@datasets_router.get("/datasets/{dataset_id}/judge-prompts")
async def get_dataset_judge_prompts(
    dataset_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """Prompt names this dataset overrides, with their templates.

    The UI merges these over the org-level list: a name present here is graded
    with the dataset's own prompt; anything absent inherits org → platform.
    """
    org_id = uuid.UUID(session["org_id"])
    return {"overrides": await list_dataset_judge_prompts(dataset_id, org_id)}


@datasets_router.put("/datasets/{dataset_id}/judge-prompts/{name}")
async def save_dataset_judge_prompt(
    dataset_id: uuid.UUID,
    name: str,
    payload: SaveDatasetJudgePromptRequest,
    session: dict = Depends(get_current_session),
):
    """Fork a metric prompt for this dataset only."""
    org_id = uuid.UUID(session["org_id"])
    if not await dataset_owned(dataset_id, org_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    # Validate against the same catalogue the org editor uses, so an override
    # that drops a required placeholder is rejected here rather than silently
    # falling back to the org prompt inside the worker.
    row = await get_org_judge_prompt(org_id, name)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prompt not found")
    _validate_judge_template(row, payload.template)
    await upsert_dataset_judge_prompt(dataset_id, org_id, name, payload.template)
    return {"name": name, "template": payload.template, "is_dataset_override": True}


@datasets_router.delete(
    "/datasets/{dataset_id}/judge-prompts/{name}", status_code=status.HTTP_204_NO_CONTENT,
)
async def reset_dataset_judge_prompt(
    dataset_id: uuid.UUID,
    name: str,
    session: dict = Depends(get_current_session),
):
    """Drop this dataset's fork so the metric inherits org → platform again."""
    org_id = uuid.UUID(session["org_id"])
    removed = await delete_dataset_judge_prompt(dataset_id, org_id, name)
    if not removed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Override not found")


# ── Example enrichment (eval / security / cost by source trace) ───────────────

async def _attach_enrichment(org_id: uuid.UUID, examples: List[dict]) -> None:
    """Merge each example's run signals — eval scores, security verdict, cost —
    onto its serialized dict under ``enrichment``, looked up by source_trace_id."""
    trace_ids: List[str] = []
    for ex in examples:
        tid = (ex.get("metadata") or {}).get("source_trace_id")
        if isinstance(tid, str) and tid:
            trace_ids.append(tid)
    trace_ids = list(dict.fromkeys(trace_ids))  # dedupe, preserve order
    if not trace_ids:
        return

    eval_rows = await clickhouse_client.fetch_dataset_eval_results(org_id, trace_ids)
    sec_rows  = await clickhouse_client.fetch_dataset_security_results(org_id, trace_ids)
    cost_map  = await clickhouse_client.fetch_costs_for_traces(org_id, trace_ids)

    evals_by_trace: Dict[str, Dict[str, float]] = {}
    for r in eval_rows:
        if r["score"] is None:
            continue
        key = r["layer"] or r["metric"]          # agentic layer, else metric name
        if key:
            evals_by_trace.setdefault(r["trace_id"], {})[key] = round(r["score"], 4)
    sec_by_trace = {r["trace_id"]: r for r in sec_rows}

    for ex in examples:
        meta = ex.get("metadata") or {}
        tid = meta.get("source_trace_id")
        if not isinstance(tid, str) or not tid:
            continue
        enrichment: Dict[str, Any] = {}

        evals = evals_by_trace.get(tid)
        if evals:
            enrichment["eval"] = evals

        s = sec_by_trace.get(tid)
        if s:
            enrichment["security"] = {
                "risk_level": s.get("risk_level") or "clean",
                "blocked":    bool(s.get("should_block")),
                "threats":    [k for k in _THREAT_KEYS if s.get(k)],
            }

        cost_entry = cost_map.get(tid)
        if cost_entry and cost_entry.get("cost") is not None:
            enrichment["cost"] = cost_entry["cost"]
        elif isinstance(meta.get("cost"), (int, float)):
            enrichment["cost"] = meta["cost"]

        if enrichment:
            ex["enrichment"] = enrichment


# ── Serializers ───────────────────────────────────────────────────────────────

def _serialize_run(row: dict) -> dict:
    summary = row.get("summary") or {}
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except ValueError:
            summary = {}
    return {
        "run_id":      str(row["run_id"]),
        "dataset_id":  str(row["dataset_id"]),
        "kind":        row["kind"],
        "depth":       row.get("depth"),
        "model":       row.get("model"),
        "batch_id":    str(row["batch_id"]) if row.get("batch_id") else None,
        "status":      row["status"],
        "total":       row.get("total", 0),
        "item_count":  row.get("item_count", row.get("total", 0)),
        "summary":     summary,
        "created_at":  row["created_at"].isoformat() if row.get("created_at") else None,
        "finished_at": row["finished_at"].isoformat() if row.get("finished_at") else None,
    }


def _serialize_dataset(row: dict) -> dict:
    return {
        "dataset_id":    str(row["dataset_id"]),
        "org_id":        str(row["org_id"]),
        "name":          row["name"],
        "description":   row.get("description"),
        "kind":          row.get("kind") or "agentic",
        "example_count": row.get("example_count", 0),
        "created_at":    row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at":    row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def _serialize_example(row: dict) -> dict:
    meta = row.get("metadata") or {}
    if isinstance(meta, str):
        import json as _json
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    return {
        "example_id":      str(row["example_id"]),
        "dataset_id":      str(row["dataset_id"]),
        "org_id":          str(row["org_id"]),
        "input":           row["input"],
        "expected_output": row.get("expected_output"),
        "metadata":        meta,
        "created_at":      row["created_at"].isoformat() if row.get("created_at") else None,
    }


__all__ = ["datasets_router"]
