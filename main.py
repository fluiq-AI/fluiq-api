from contextlib import asynccontextmanager

from fastapi import FastAPI
from db_queues.kafka import kafka_queue
from db_queues.postgresql import postgres_client
from routes import trace, auth


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_queue.start()
    await postgres_client.start()
    try:
        yield
    finally:
        await postgres_client.stop()
        await kafka_queue.stop()


app = FastAPI(lifespan=lifespan)

app.include_router(trace.router, prefix="/api/v1")
app.include_router(auth.auth_router, prefix="/auth")

@app.get("/")
async def root():
    return "Hello from Fluiq API"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app",port=8000,reload=True)