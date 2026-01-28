from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.schemas import HealthResponse, ReadyResponse
from api.deps import get_db

router = APIRouter(tags=["health"])

@router.get("/health", response_model=HealthResponse)
def health():
    return {"status" : "ok"}

@router.get("/ready", response_model=ReadyResponse)
def ready(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return{"status" : "ok", "db" : "ok"}