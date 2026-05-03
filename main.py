from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue
from db_queues.postgresql import postgres_client
from realtime import trace_consumer
from routes import trace, auth
from routes.agents import router as agents_router
from routes.api_keys import api_keys_router
from routes.optimize import optimize_router
from routes.quota import quota_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_queue.start()
    await postgres_client.start()
    await clickhouse_client.start()
    await trace_consumer.start()
    try:
        yield
    finally:
        await trace_consumer.stop()
        await clickhouse_client.stop()
        await postgres_client.stop()
        await kafka_queue.stop()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trace.router, prefix="/api/v1")
app.include_router(agents_router, prefix="/api/v1")
app.include_router(quota_router, prefix="/api/v1")
app.include_router(optimize_router, prefix="/api/v1")
app.include_router(auth.auth_router, prefix="/auth")
app.include_router(api_keys_router, prefix="/api-keys")

@app.get("/")
async def root():
    return "Hello from Fluiq API"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app",port=8000,reload=True)