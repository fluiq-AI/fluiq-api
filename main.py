import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, security_reply_consumer, playground_reply_consumer
from db_queues.postgresql import postgres_client
from middleware.audit import AuditMiddleware
from realtime import trace_consumer, alert_consumer
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
from routes.blog import blog_router
from routes.alerts import alerts_router
import config

@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_queue.start()
    await postgres_client.start()
    await clickhouse_client.start()
    await trace_consumer.start()
    await alert_consumer.start()
    await security_reply_consumer.start()
    await playground_reply_consumer.start()
    try:
        yield
    finally:
        await playground_reply_consumer.stop()
        await security_reply_consumer.stop()
        await alert_consumer.stop()
        await trace_consumer.stop()
        await clickhouse_client.stop()
        await postgres_client.stop()
        await kafka_queue.stop()


logger = logging.getLogger(__name__)

app = FastAPI(lifespan=lifespan)

_allowed_origins: list[str] = []
if config.FRONTEND_BASE_URL:
    _allowed_origins.append(config.FRONTEND_BASE_URL)
    if config.FRONTEND_BASE_URL.startswith("https://") and not config.FRONTEND_BASE_URL.startswith("https://www."):
        _allowed_origins.append("https://www." + config.FRONTEND_BASE_URL.removeprefix("https://"))

# Localhost origins are allowed so (a) the frontend prerender step — which runs
# in a headless browser on 127.0.0.1:<random-port> during the build and fetches
# published blog posts from this API — passes CORS, and (b) local dev works
# against a deployed API. Real browsers can't spoof a localhost Origin, so this
# doesn't widen the production surface.
_LOCALHOST_ORIGIN_RE = re.compile(r"https?://(localhost|127\.0\.0\.1)(:\d+)?")


def _cors_headers_for(request: Request) -> dict[str, str]:
    """CORS headers for an *error* response.

    Starlette's ``CORSMiddleware`` only decorates responses that flow back
    through it; a 500 produced by the outermost error layer bypasses it and
    arrives at the browser without ``Access-Control-Allow-Origin``, which the
    browser then reports as a (misleading) CORS failure that masks the real
    error. We re-apply the same allow rules here so backend errors surface as
    the actual status/body instead of a phantom CORS error.
    """
    origin = request.headers.get("origin")
    if not origin:
        return {}
    if origin not in _allowed_origins and not _LOCALHOST_ORIGIN_RE.fullmatch(origin):
        return {}
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Credentials": "true",
        "Vary": "Origin",
    }


app.add_middleware(AuditMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins or ["*"],
    allow_origin_regex=_LOCALHOST_ORIGIN_RE.pattern,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    # Log the real cause (previously swallowed) so cold-start / DB timeouts on
    # first load are diagnosable instead of surfacing only as browser CORS noise.
    logger.exception(
        "Unhandled error on %s %s", request.method, request.url.path,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
        headers=_cors_headers_for(request),
    )

app.include_router(admin_router, prefix="/admin")
app.include_router(audit_router, prefix="/api/v1")
app.include_router(guardrails_router, prefix="/api/v1")
app.include_router(alerts_router, prefix="/api/v1")
app.include_router(trace.router, prefix="/api/v1")
app.include_router(agents_router, prefix="/api/v1")
app.include_router(quota_router, prefix="/api/v1")
app.include_router(evaluate_router, prefix="/api/v1")
app.include_router(prompts_router, prefix="/api/v1")
app.include_router(datasets_router, prefix="/api/v1")
app.include_router(optimize_router, prefix="/api/v1/optimize")
app.include_router(secure_router, prefix="/api/v1")
app.include_router(contact_router, prefix="/api/v1")
app.include_router(blog_router, prefix="/api/v1")
app.include_router(auth.auth_router, prefix="/auth")
app.include_router(api_keys_router, prefix="/api-keys")

@app.get("/")
async def root():
    return "Hello from Fluiq API"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app",port=8000,reload=True)