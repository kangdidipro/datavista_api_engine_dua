import os
import sys
from sqlalchemy.orm import Session
import logging

# Add /app/api_engine to sys.path to allow imports from app.database and app.models
if '/app/api_engine' not in sys.path:
    sys.path.insert(0, '/app/api_engine')

from app.database import SessionLocal
from app.models import CsvImportLog, CsvSummaryMasterDaily, AnomalyResult, AnomalyExecutionBatch, AnomalyExecution

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_data(db: Session):
    logger.info("Cleaning up existing transaction and summary data...")

    # Delete from AnomalyResult first due to foreign key constraints
    deleted_anomaly_results = db.query(AnomalyResult).delete()
    logger.info(f"Deleted {deleted_anomaly_results} records from AnomalyResult.")

    # Delete from AnomalyExecutionBatch
    deleted_anomaly_execution_batches = db.query(AnomalyExecutionBatch).delete()
    logger.info(f"Deleted {deleted_anomaly_execution_batches} records from AnomalyExecutionBatch.")

    # Delete from AnomalyExecution
    deleted_anomaly_executions = db.query(AnomalyExecution).delete()
    logger.info(f"Deleted {deleted_anomaly_executions} records from AnomalyExecution.")

    # Delete from CsvImportLog
    deleted_logs = db.query(CsvImportLog).delete()
    logger.info(f"Deleted {deleted_logs} records from CsvImportLog.")

    # Delete from CsvSummaryMasterDaily
    deleted_summaries = db.query(CsvSummaryMasterDaily).delete()
    logger.info(f"Deleted {deleted_summaries} records from CsvSummaryMasterDaily.")

    db.commit()
    logger.info("Data cleanup completed.")

if __name__ == "__main__":
    db = SessionLocal()
    try:
        clean_data(db)
    finally:
        db.close()