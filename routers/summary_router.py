from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.database import get_db
from app.schemas import CsvSummaryMasterDaily
from app.models import CsvSummaryMasterDaily as models_CsvSummaryMasterDaily # Import models as schemas for now

router = APIRouter(prefix="/v1/summary", tags=["CSV Summary"])

@router.get("/daily", response_model=List[CsvSummaryMasterDaily])
def get_all_daily_summaries(db: Session = Depends(get_db)):
    summaries = db.query(models_CsvSummaryMasterDaily).all()
    return summaries

@router.get("/daily/{summary_id}", response_model=CsvSummaryMasterDaily)
def get_daily_summary(summary_id: int, db: Session = Depends(get_db)):
    summary = db.query(models_CsvSummaryMasterDaily).filter(models_CsvSummaryMasterDaily.summary_id == summary_id).first()
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found")
    return summary