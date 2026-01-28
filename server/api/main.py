# uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import apiStatus

app = FastAPI(title="Cold Case API")
    
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(apiStatus.router, prefix="/api/v1")
