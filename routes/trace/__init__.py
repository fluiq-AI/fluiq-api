from fastapi import APIRouter
from db_queues.kafka import kafka_queue
from .model import IngestPayload
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

@router.post("/ingest")
async def ingestion(payload: IngestPayload):

    job = payload.model_dump(mode="json")
    await kafka_queue.add_job(
        job, 
        topic=os.getenv("KAFKA_TRACE_TOPIC"), 
        key=payload.api_key
    )
    
    return {"ok": True}