from contextlib import asynccontextmanager

from fastapi import FastAPI
from db_queues.kafka import kafka_queue
from routes import trace


@asynccontextmanager
async def lifespan(app: FastAPI):
    await kafka_queue.start()
    try:
        yield
    finally:
        await kafka_queue.stop()


app = FastAPI(lifespan=lifespan)

app.include_router(trace.router, prefix="/api/v1")

@app.get("/")
async def root():
    return "Hello from Fluiq API"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app",port=8000,reload=True)