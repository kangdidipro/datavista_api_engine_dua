from sqlalchemy.orm import Session
from typing import List, Optional
from app import models as app_models
from datetime import datetime
import uuid

def create_anomaly_execution(
    db: Session,
    executed_by: str,
    rules_applied: List[str],
    rules_config: Optional[dict] = None,
    status: str = "PENDING",
    total_batches_processed: int = 0
) -> app_models.AnomalyExecution:
    """
    Creates a new AnomalyExecution record.
    """
    execution_id = str(uuid.uuid4())
    db_execution = app_models.AnomalyExecution(
        execution_id=execution_id,
        execution_timestamp=datetime.now(),
        executed_by=executed_by,
        status=status,
        rules_applied=rules_applied,
        rules_config=rules_config,
        total_batches_processed=total_batches_processed
    )
    db.add(db_execution)
    db.commit()
    db.refresh(db_execution)
    return db_execution

def get_anomaly_executions(db: Session, skip: int = 0, limit: int = 100) -> List[app_models.AnomalyExecution]:
    """
    Retrieves a list of AnomalyExecution records.
    """
    return db.query(app_models.AnomalyExecution).offset(skip).limit(limit).all()

def get_anomaly_execution_by_id(db: Session, execution_id: str) -> Optional[app_models.AnomalyExecution]:
    """
    Retrieves a single AnomalyExecution record by its ID.
    """
    return db.query(app_models.AnomalyExecution).filter(app_models.AnomalyExecution.execution_id == execution_id).first()

def create_anomaly_execution_batch(
    db: Session,
    execution_id: str,
    summary_id: int,
    batch_status: str = "PENDING",
    anomalies_found: int = 0
) -> app_models.AnomalyExecutionBatch:
    """
    Creates a new AnomalyExecutionBatch record.
    """
    db_batch = app_models.AnomalyExecutionBatch(
        execution_id=execution_id,
        summary_id=summary_id,
        batch_status=batch_status,
        anomalies_found=anomalies_found
    )
    db.add(db_batch)
    db.commit()
    db.refresh(db_batch)
    return db_batch

def get_anomaly_execution_batches_by_execution_id(db: Session, execution_id: str) -> List[app_models.AnomalyExecutionBatch]:
    """
    Retrieves a list of AnomalyExecutionBatch records for a given execution_id.
    """
    return db.query(models.AnomalyExecutionBatch).filter(models.AnomalyExecutionBatch.execution_id == execution_id).all()

def update_anomaly_execution_status(db: Session, execution_id: str, status: str, total_batches_processed: Optional[int] = None) -> Optional[app_models.AnomalyExecution]:
    """
    Updates the status and optionally total_batches_processed of an AnomalyExecution record.
    """
    db_execution = db.query(app_models.AnomalyExecution).filter(app_models.AnomalyExecution.execution_id == execution_id).first()
    if db_execution:
        db_execution.status = status
        if total_batches_processed is not None:
            db_execution.total_batches_processed = total_batches_processed
        db.commit()
        db.refresh(db_execution)
    return db_execution

def update_anomaly_execution_batch_status(db: Session, detail_id: int, batch_status: str, anomalies_found: Optional[int] = None) -> Optional[app_models.AnomalyExecutionBatch]:
    """
    Updates the status and optionally anomalies_found of an AnomalyExecutionBatch record.
    """
    db_batch = db.query(app_models.AnomalyExecutionBatch).filter(app_models.AnomalyExecutionBatch.detail_id == detail_id).first()
    if db_batch:
        db_batch.batch_status = batch_status
        if anomalies_found is not None:
            db_batch.anomalies_found = anomalies_found
        db.commit()
        db.refresh(db_batch)
    return db_batch
