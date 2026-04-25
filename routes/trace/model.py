from pydantic import BaseModel
from typing import Any

class IngestPayload(BaseModel):
    api_key: str | None = None
    event: dict[str, Any]