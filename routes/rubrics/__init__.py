"""fluiq-api — Review rubrics: what a human reviewer is asked.

  GET    /api/v1/rubric              the org's rubric (session)
  POST   /api/v1/rubric/fields       add a field (session, admin)
  PATCH  /api/v1/rubric/fields/{id}  edit one (session, admin)
  DELETE /api/v1/rubric/fields/{id}  remove one (session, admin)
  POST   /api/v1/traces/{id}/review  submit a rubric answer (session, any role)

Annotation used to be one number and one comment, which is what you build when
the reviewer is the person who wrote the code. It stops working the moment the
reviewer is a subject-matter expert: a clinician grading a summary is not
thinking "0.7", they are answering "is the dosage right — yes / no / unclear"
across four separate questions. One number cannot hold four answers, and the one
it holds is an average nobody chose.

Each field's answer becomes its own ``human.annotation`` row, keyed by the
field's ``key``, so a rubric answer sits next to the judge score it may disagree
with — which is what the Review 2×2 reads.
"""
from __future__ import annotations

import re
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql import postgres_client
from routes.auth.helper import get_current_session
from shared.permissions import require_role

rubrics_router = APIRouter()

_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
VALID_KINDS = ("choice", "boolean", "slider", "text")
MAX_FIELDS = 20
MAX_OPTIONS = 10


class FieldPayload(BaseModel):
    key:      str
    label:    str
    help:     Optional[str] = None
    kind:     str = "choice"
    options:  List[Dict[str, Any]] = Field(default_factory=list)
    required: bool = False
    position: int = 0

    @field_validator("key")
    @classmethod
    def _key(cls, v: str) -> str:
        v = (v or "").strip().lower()
        if not _KEY_RE.match(v):
            raise ValueError(
                "key must start with a letter and contain only lowercase "
                "letters, digits, and underscores"
            )
        return v

    @field_validator("kind")
    @classmethod
    def _kind(cls, v: str) -> str:
        v = (v or "choice").strip().lower()
        if v not in VALID_KINDS:
            raise ValueError(f"kind must be one of: {', '.join(VALID_KINDS)}")
        return v

    @field_validator("label")
    @classmethod
    def _label(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("label is required")
        return v


def validate_options(kind: str, options: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """A choice field's options, or empty for the kinds that have none."""
    if kind != "choice":
        return []
    if len(options) < 2:
        raise ValueError("a choice field needs at least two options to choose between")
    if len(options) > MAX_OPTIONS:
        raise ValueError(f"at most {MAX_OPTIONS} options")
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for option in options:
        label = str(option.get("label") or "").strip()
        if not label:
            raise ValueError("every option needs a label")
        if label.lower() in seen:
            raise ValueError(f"duplicate option: {label}")
        seen.add(label.lower())
        try:
            score = float(option.get("score"))
        except (TypeError, ValueError):
            raise ValueError(f"option {label!r} needs a numeric score") from None
        if not 0.0 <= score <= 1.0:
            raise ValueError(f"option {label!r}: score must be between 0 and 1")
        out.append({"label": label, "score": score})
    return out


def _serialize(row: Any) -> Dict[str, Any]:
    import json as _json

    options = row["options"]
    if isinstance(options, str):
        try:
            options = _json.loads(options)
        except ValueError:
            options = []
    return {
        "field_id": str(row["field_id"]),
        "key":      row["key"],
        "label":    row["label"],
        "help":     row.get("help"),
        "kind":     row["kind"],
        "options":  options or [],
        "required": bool(row["required"]),
        "position": row["position"],
    }


@rubrics_router.get("/rubric")
async def get_rubric(session: dict = Depends(get_current_session)):
    """The org's rubric. Readable by every role, including annotators — a
    reviewer who cannot see the questions cannot answer them."""
    org_id = uuid.UUID(session["org_id"])
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM review_rubric_fields WHERE org_id = $1 "
            "ORDER BY position, created_at",
            org_id,
        )
    return {"fields": [_serialize(r) for r in rows]}


@rubrics_router.post("/rubric/fields", status_code=status.HTTP_201_CREATED)
async def add_field(
    payload: FieldPayload,
    session: dict = Depends(get_current_session),
):
    """Add a rubric question. Admin-only: the rubric defines what the org's
    review history *means*, so an annotator changing it mid-review would
    silently redefine every score already collected."""
    org_id = await require_role(session, {"owner", "admin"})
    try:
        options = validate_options(payload.kind, payload.options)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    import json as _json

    async with postgres_client.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM review_rubric_fields WHERE org_id = $1", org_id,
        )
        if count >= MAX_FIELDS:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"A rubric can hold {MAX_FIELDS} questions. More than that and "
                f"reviewers start skipping them.",
            )
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO review_rubric_fields
                    (org_id, key, label, help, kind, options, required, position)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
                RETURNING *
                """,
                org_id, payload.key, payload.label, payload.help, payload.kind,
                _json.dumps(options), payload.required, payload.position,
            )
        except Exception as exc:  # noqa: BLE001
            if "unique" in str(exc).lower():
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    f"A rubric question with key '{payload.key}' already exists.",
                ) from exc
            raise
    return _serialize(row)


class FieldPatch(BaseModel):
    label:    Optional[str] = None
    help:     Optional[str] = None
    options:  Optional[List[Dict[str, Any]]] = None
    required: Optional[bool] = None
    position: Optional[int] = None


@rubrics_router.patch("/rubric/fields/{field_id}")
async def edit_field(
    field_id: uuid.UUID,
    payload: FieldPatch,
    session: dict = Depends(get_current_session),
):
    """Edit a question.

    ``key`` and ``kind`` are deliberately not editable. The key is what every
    recorded answer is filed under, and the kind is what those answers *mean* —
    changing either would leave a history of scores that no longer matches the
    question they answered.
    """
    org_id = await require_role(session, {"owner", "admin"})
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        raise HTTPException(422, "Nothing to update.")

    import json as _json

    async with postgres_client.acquire() as conn:
        current = await conn.fetchrow(
            "SELECT * FROM review_rubric_fields WHERE field_id = $1 AND org_id = $2",
            field_id, org_id,
        )
        if current is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Question not found")

        sets, args, idx = [], [], 1
        for column in ("label", "help", "required", "position"):
            if column in fields:
                sets.append(f"{column} = ${idx}")
                args.append(fields[column])
                idx += 1
        if "options" in fields:
            try:
                options = validate_options(current["kind"], fields["options"] or [])
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc
            sets.append(f"options = ${idx}::jsonb")
            args.append(_json.dumps(options))
            idx += 1
        if not sets:
            raise HTTPException(422, "Nothing to update.")

        args += [field_id, org_id]
        row = await conn.fetchrow(
            f"UPDATE review_rubric_fields SET {', '.join(sets)} "
            f"WHERE field_id = ${idx} AND org_id = ${idx + 1} RETURNING *",
            *args,
        )
    return _serialize(row)


@rubrics_router.delete(
    "/rubric/fields/{field_id}", status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_field(
    field_id: uuid.UUID,
    session: dict = Depends(get_current_session),
):
    """Remove a question. Answers already recorded under its key are kept —
    deleting the question does not unmake the reviews that answered it."""
    org_id = await require_role(session, {"owner", "admin"})
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM review_rubric_fields WHERE field_id = $1 AND org_id = $2",
            field_id, org_id,
        )
    if result != "DELETE 1":
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Question not found")


# ── Submitting a review ───────────────────────────────────────────────────────

class ReviewSubmission(BaseModel):
    #: ``{field key: answer}``. A choice answers with its label, a boolean with
    #: true/false, a slider with 0–1, a text field with a string.
    answers:       Dict[str, Any]
    comment:       Optional[str] = None
    root_trace_id: Optional[str] = None


def score_answer(field: Dict[str, Any], answer: Any) -> Optional[float]:
    """Turn one rubric answer into a 0–1 score, or None when it isn't scored.

    Raises ValueError on an answer the field cannot accept — an unrecognised
    choice scored as 0 would be indistinguishable from a reviewer marking it
    wrong, which is the same failure the LLM choice-scorer avoids.
    """
    kind = field["kind"]
    if kind == "text":
        return None
    if kind == "boolean":
        if not isinstance(answer, bool):
            raise ValueError(f"{field['key']}: expected true or false")
        return 1.0 if answer else 0.0
    if kind == "slider":
        try:
            value = float(answer)
        except (TypeError, ValueError):
            raise ValueError(f"{field['key']}: expected a number between 0 and 1") from None
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{field['key']}: must be between 0 and 1")
        return value
    # choice
    wanted = str(answer or "").strip().lower()
    for option in field.get("options") or []:
        if str(option["label"]).strip().lower() == wanted:
            return float(option["score"])
    labels = ", ".join(str(o["label"]) for o in (field.get("options") or []))
    raise ValueError(f"{field['key']}: {answer!r} is not one of: {labels}")


@rubrics_router.post("/traces/{trace_id}/review", status_code=status.HTTP_201_CREATED)
async def submit_review(
    trace_id: uuid.UUID,
    payload: ReviewSubmission,
    session: dict = Depends(get_current_session),
):
    """Record a reviewer's rubric answers against a trace.

    Every role may do this, annotators included — it is the one thing they are
    here for.
    """
    org_id = uuid.UUID(session["org_id"])
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM review_rubric_fields WHERE org_id = $1", org_id,
        )
    rubric = {r["key"]: _serialize(r) for r in rows}
    if not rubric:
        raise HTTPException(
            422,
            "This organization has no review rubric yet. An admin can add "
            "questions under Review settings.",
        )

    missing = [
        key for key, field in rubric.items()
        if field["required"] and key not in payload.answers
    ]
    if missing:
        raise HTTPException(422, f"Required questions unanswered: {', '.join(missing)}")

    unknown = [key for key in payload.answers if key not in rubric]
    if unknown:
        # A typo'd key would otherwise be recorded under a metric nobody reads.
        raise HTTPException(422, f"Unknown rubric questions: {', '.join(unknown)}")

    reviewer = str(session.get("sub") or "")
    root_id = payload.root_trace_id or str(trace_id)
    written: Dict[str, Optional[float]] = {}

    for key, answer in payload.answers.items():
        field = rubric[key]
        try:
            score = score_answer(field, answer)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

        details: Dict[str, Any] = {
            "source":       "review",
            "annotated_by": reviewer,
            "field":        key,
            "label":        field["label"],
            "kind":         field["kind"],
            "answer":       answer,
        }
        if payload.comment:
            details["comment"] = payload.comment[:2000]

        # A text answer is recorded at 0 with the words in details, and read back
        # via `kind` — scoring prose would invent a number nobody gave.
        await clickhouse_client.insert_human_score(
            organization_id=org_id,
            trace_id=str(trace_id),
            root_trace_id=root_id,
            evaluator="human.annotation",
            metric=key,
            score=score if score is not None else 0.0,
            details=details,
        )
        written[key] = score

    scored = [v for v in written.values() if v is not None]
    return {
        "recorded": list(written),
        # The reviewer's overall verdict, for the caller to show back. Averaged
        # over the scored fields only: a text field has no number to contribute.
        "score": sum(scored) / len(scored) if scored else None,
    }


__all__ = ["rubrics_router"]
