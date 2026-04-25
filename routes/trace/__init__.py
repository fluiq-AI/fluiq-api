import json
from pathlib import Path

from fastapi import APIRouter
from .model import IngestPayload

router = APIRouter()

OUTPUT_FILE = Path("output.json")

@router.post("/v1/ingest")
async def ingestion(payload: IngestPayload):
    #Store data to database in future now output.json
    try:
        existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing, list):
            existing = [existing]
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    existing.append(payload.model_dump(mode="json"))
    OUTPUT_FILE.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    return {"ok": True}