from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime

from app.database import get_db
from app.schemas import CsvSummaryMasterDaily

router = APIRouter(prefix="/v1/summary", tags=["CSV Summary"])

@router.get("/daily", response_model=List[CsvSummaryMasterDaily])
def get_all_csv_summaries(db: Session = Depends(get_db)):
    """
    Mengambil semua entri dari tabel csv_summary_master_daily.
    """
    summaries = db.query(schemas.CsvSummaryMasterDaily).all()
    return summaries

@router.get("/daily/{summary_id}", response_model=CsvSummaryMasterDaily)
def get_csv_summary_by_id(summary_id: int, db: Session = Depends(get_db)):
    """
    Mengambil entri csv_summary_master_daily berdasarkan summary_id.
    """
    summary = db.query(schemas.CsvSummaryMasterDaily).filter(schemas.CsvSummaryMasterDaily.summary_id == summary_id).first()
    if not summary:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Summary not found")
    return summary