from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, security_reply_consumer, playground_reply_consumer
from db_queues.postgresql import postgres_client
from middleware.audit import AuditMiddleware
from realtime import trace_consumer
from routes import trace, auth
from routes.admin import admin_router
from routes.agents import router as agents_router
from routes.api_keys import api_keys_router
from routes.audit import audit_router
from routes.guardrails import guardrails_router
from routes.evaluate import evaluate_router
from routes.optimize import optimize_router
from routes.datasets import datasets_router
from routes.prompts import prompts_router
from routes.quota import quota_router
from routes.secure import router as secure_router
from routes.contact import router as contact_router
import config

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_queue.start()
    await postgres_client.start()
    await clickhouse_client.start()
    await trace_consumer.start()
    await security_reply_consumer.start()
    await playground_reply_consumer.start()
    try:
        yield
    finally:
        await playground_reply_consumer.stop()
        await security_reply_consumer.stop()
        await trace_consumer.stop()
        await clickhouse_client.stop()
        await postgres_client.stop()
        await kafka_queue.stop()


app = FastAPI(lifespan=lifespan)

_allowed_origins: list[str] = []
if config.FRONTEND_BASE_URL:
    _allowed_origins.append(config.FRONTEND_BASE_URL)
    if config.FRONTEND_BASE_URL.startswith("https://") and not config.FRONTEND_BASE_URL.startswith("https://www."):
        _allowed_origins.append("https://www." + config.FRONTEND_BASE_URL.removeprefix("https://"))

app.add_middleware(AuditMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

app.include_router(admin_router, prefix="/admin")
app.include_router(audit_router, prefix="/api/v1")
app.include_router(guardrails_router, prefix="/api/v1")
app.include_router(trace.router, prefix="/api/v1")
app.include_router(agents_router, prefix="/api/v1")
app.include_router(quota_router, prefix="/api/v1")
app.include_router(evaluate_router, prefix="/api/v1")
app.include_router(prompts_router, prefix="/api/v1")
app.include_router(datasets_router, prefix="/api/v1")
app.include_router(optimize_router, prefix="/api/v1/optimize")
app.include_router(secure_router, prefix="/api/v1")
app.include_router(contact_router, prefix="/api/v1")
app.include_router(auth.auth_router, prefix="/auth")
app.include_router(api_keys_router, prefix="/api-keys")

@app.get("/")
async def root():
    return "Hello from Fluiq API"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app",port=8000,reload=True)