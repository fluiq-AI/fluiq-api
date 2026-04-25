from fastapi import FastAPI
from routes import trace

app = FastAPI()

app.include_router(trace.router, prefix="/api")

@app.get("/")
async def root():
    return "Hello from Fluiq API"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app",port=8000,reload=True)