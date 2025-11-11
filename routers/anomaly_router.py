from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Dict
from rq import Queue # Import Queue
from redis import Redis

from app.database import get_db
from app import models as app_models
from app.schemas import AnomalyAnalysisRequest, AnomalyTemplateMaster, AnomalyTemplateMasterCreate, AnomalyExecution, AnomalyExecutionBatch, TransactionAnomalyCriteria, TransactionAnomalyCriteriaCreate, TransactionAnomalyCriteriaUpdate, SpecialAnomalyCriteria, SpecialAnomalyCriteriaCreate, SpecialAnomalyCriteriaUpdate, VideoAiParameter, AccumulatedAnomalyCriteria, CsvSummaryMasterDaily, AccumulatedAnomalyCriteriaCreate, AccumulatedAnomalyCriteriaUpdate
from crud import anomaly_crud, analysis_crud, anomaly_execution_crud

router = APIRouter(
    prefix="/api/anomaly-config",
    tags=["Anomaly Configuration"],
)

# Redis Queue Connection
redis_conn = Redis.from_url('redis://localhost:6379')
q = Queue(connection=redis_conn)

@router.post("/analyze", status_code=202)
def start_analysis(request: AnomalyAnalysisRequest, db: Session = Depends(get_db)):
    template_to_use = None
    if request.template_id:
        template_to_use = anomaly_crud.get_template(db, request.template_id)
        if not template_to_use:
            raise HTTPException(status_code=404, detail=f"Template with id {request.template_id} not found.")
    elif request.transaction_criteria_ids or request.special_criteria_ids or request.accumulated_criteria_ids:
        # Create or get an ad-hoc template
        ad_hoc_template_name = "Ad-Hoc Analysis Template"
        template_to_use = db.query(app_models.AnomalyTemplateMaster).filter_by(role_name=ad_hoc_template_name).first()
        if not template_to_use:
            template_to_use = anomaly_crud.create_template(db, AnomalyTemplateMasterCreate(
                role_name=ad_hoc_template_name,
                description="Template created dynamically for ad-hoc analysis",
                is_default=False,
                created_by=request.executed_by
            ))
        
        # Link the provided criteria to the ad-hoc template
        anomaly_crud.update_template_links(
            db,
            template_to_use.template_id,
            request.transaction_criteria_ids if request.transaction_criteria_ids else [],
            request.special_criteria_ids if request.special_criteria_ids else [],
            [], # No video criteria for now
            request.accumulated_criteria_ids if request.accumulated_criteria_ids else []
        )
        # Refresh the template to load the newly linked criteria
        db.refresh(template_to_use)
    else:
        raise HTTPException(status_code=422, detail="Either template_id or individual criteria IDs must be provided.")

    # Extract rules from the template
    rules_applied = []
    rules_config = {} # This would be more complex to build from criteria, for now keep it simple

    # Collect IDs of linked criteria
    if template_to_use.transaction_criteria:
        rules_applied.extend([f"TC_{c.criteria_id}" for c in template_to_use.transaction_criteria])

    if template_to_use.special_criteria:
        rules_applied.extend([f"SC_{c.special_criteria_id}" for c in template_to_use.special_criteria])

    if template_to_use.accumulated_criteria:
        rules_applied.extend([f"AC_{c.accumulated_criteria_id}" for c in template_to_use.accumulated_criteria])

    # 1. Create AnomalyExecution record
    execution = anomaly_execution_crud.create_anomaly_execution(
        db=db,
        template_id=template_to_use.template_id, # Use the template_id from the resolved template
        executed_by=request.executed_by,
        rules_applied=rules_applied,
        rules_config=rules_config, # For now, this will be empty or manually constructed
        status="QUEUED",
        total_batches_processed=len(request.summary_ids)
    )

    # 2. Create AnomalyExecutionBatch records
    for summary_id in request.summary_ids:
        anomaly_execution_crud.create_anomaly_execution_batch(
            db=db,
            execution_id=execution.execution_id,
            summary_id=summary_id,
            batch_status="QUEUED"
        )

    # 3. Enqueue the analysis job
    # Pass execution_id and summary_ids to the worker
    job = q.enqueue(
        analysis_crud.run_anomaly_analysis,
        execution.execution_id, # Pass execution_id
        request.summary_ids,    # Pass summary_ids
        job_timeout=3600
    )

    # Update execution status to PENDING (or RUNNING) if job enqueued successfully
    anomaly_execution_crud.update_anomaly_execution_status(db, execution.execution_id, "PENDING")

    return {"execution_id": execution.execution_id, "job_id": job.id}





@router.get("/templates", response_model=List[AnomalyTemplateMaster])
def get_templates(db: Session = Depends(get_db)):
    return anomaly_crud.get_templates(db)

@router.get("/templates/{template_id}")
def get_template_details(template_id: int, db: Session = Depends(get_db)):
    template = anomaly_crud.get_template(db, template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    return {
        "template": template,
        "available_volume": db.query(app_models.TransactionAnomalyCriteria).all(),
        "available_special": db.query(app_models.SpecialAnomalyCriteria).all(),
        "available_video": db.query(app_models.VideoAiParameter).all(),
        "available_accumulated": db.query(app_models.AccumulatedAnomalyCriteria).all(),
    }

@router.post("/templates", response_model=AnomalyTemplateMaster, status_code=201)
def create_template(template: AnomalyTemplateMasterCreate, db: Session = Depends(get_db)):
    return anomaly_crud.create_template(db=db, template=template)

@router.put("/templates/{template_id}")
def update_template(template_id: int, links: Dict[str, List[int]], db: Session = Depends(get_db)):
    updated = anomaly_crud.update_template_links(
        db,
        template_id,
        links.get('volume_ids', []),
        links.get('special_ids', []),
        links.get('video_ids', []),
        links.get('accumulated_ids', [])
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"message": "Template updated successfully."}

@router.patch("/templates/{template_id}/set-active")
def set_active(template_id: int, db: Session = Depends(get_db)):
    anomaly_crud.set_active_template(db, template_id)
    return {"message": "Active template switched successfully."}

@router.post("/templates/{template_id}/duplicate", response_model=AnomalyTemplateMaster, status_code=201)
def duplicate_template(template_id: int, db: Session = Depends(get_db)):
    new_template = anomaly_crud.duplicate_template(db, template_id)
    if not new_template:
        raise HTTPException(status_code=404, detail="Template not found")
    return new_template

@router.delete("/templates/{template_id}")
def delete_template(template_id: int, db: Session = Depends(get_db)):
    deleted = anomaly_crud.delete_template(db, template_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"message": "Template deleted successfully."}

# Endpoints for SpecialAnomalyCriteria
@router.post("/special-criteria", response_model=app_models.SpecialAnomalyCriteria, status_code=201)
def create_special_criteria(criteria: SpecialAnomalyCriteriaCreate, db: Session = Depends(get_db)):
    return anomaly_crud.create_special_criteria(db=db, criteria=criteria)

@router.get("/special-criteria", response_model=List[app_models.SpecialAnomalyCriteria])
def get_all_special_criteria(db: Session = Depends(get_db)):
    return anomaly_crud.get_all_special_criteria(db)

@router.get("/special-criteria/{special_criteria_id}", response_model=app_models.SpecialAnomalyCriteria)
def get_special_criteria(special_criteria_id: int, db: Session = Depends(get_db)):
    criteria = anomaly_crud.get_special_criteria(db, special_criteria_id)
    if not criteria:
        raise HTTPException(status_code=404, detail="Special Criteria not found")
    return criteria

@router.put("/special-criteria/{special_criteria_id}", response_model=app_models.SpecialAnomalyCriteria)
def update_special_criteria(special_criteria_id: int, criteria: SpecialAnomalyCriteriaUpdate, db: Session = Depends(get_db)):
    updated_criteria = anomaly_crud.update_special_criteria(db, special_criteria_id, criteria)
    if not updated_criteria:
        raise HTTPException(status_code=404, detail="Special Criteria not found")
    return updated_criteria

@router.delete("/special-criteria/{special_criteria_id}")
def delete_special_criteria(special_criteria_id: int, db: Session = Depends(get_db)):
    deleted = anomaly_crud.delete_special_criteria(db, special_criteria_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Special Criteria not found")
    return {"message": "Special Criteria deleted successfully."}

# Endpoints for TransactionAnomalyCriteria
@router.post("/transaction-criteria", response_model=app_models.TransactionAnomalyCriteria, status_code=201)
def create_transaction_criteria(criteria: TransactionAnomalyCriteriaCreate, db: Session = Depends(get_db)):
    return anomaly_crud.create_transaction_criteria(db=db, criteria=criteria)

@router.get("/transaction-criteria", response_model=List[app_models.TransactionAnomalyCriteria])
def get_all_transaction_criteria(db: Session = Depends(get_db)):
    return anomaly_crud.get_all_transaction_criteria(db)

@router.get("/transaction-criteria/{criteria_id}", response_model=app_models.TransactionAnomalyCriteria)
def get_transaction_criteria(criteria_id: int, db: Session = Depends(get_db)):
    criteria = anomaly_crud.get_transaction_criteria(db, criteria_id)
    if not criteria:
        raise HTTPException(status_code=404, detail="Transaction Criteria not found")
    return criteria

@router.put("/transaction-criteria/{criteria_id}", response_model=app_models.TransactionAnomalyCriteria)
def update_transaction_criteria(criteria_id: int, criteria: TransactionAnomalyCriteriaUpdate, db: Session = Depends(get_db)):
    updated_criteria = anomaly_crud.update_transaction_criteria(db, criteria_id, criteria)
    if not updated_criteria:
        raise HTTPException(status_code=404, detail="Transaction Criteria not found")
    return updated_criteria

@router.delete("/transaction-criteria/{criteria_id}")
def delete_transaction_criteria(criteria_id: int, db: Session = Depends(get_db)):
    deleted = anomaly_crud.delete_transaction_criteria(db, criteria_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Transaction Criteria not found")
    return {"message": "Transaction Criteria deleted successfully."}

# Endpoints for AccumulatedAnomalyCriteria
@router.post("/accumulated-criteria", response_model=app_models.AccumulatedAnomalyCriteria, status_code=201)
def create_accumulated_criteria(criteria: AccumulatedAnomalyCriteriaCreate, db: Session = Depends(get_db)):
    return anomaly_crud.create_accumulated_criteria(db=db, criteria=criteria)

@router.get("/accumulated-criteria", response_model=List[app_models.AccumulatedAnomalyCriteria])
def get_all_accumulated_criteria(db: Session = Depends(get_db)):
    return anomaly_crud.get_all_accumulated_criteria(db)

@router.get("/accumulated-criteria/{accumulated_criteria_id}", response_model=app_models.AccumulatedAnomalyCriteria)
def get_accumulated_criteria(accumulated_criteria_id: int, db: Session = Depends(get_db)):
    criteria = anomaly_crud.get_accumulated_criteria(db, accumulated_criteria_id)
    if not criteria:
        raise HTTPException(status_code=404, detail="Accumulated Criteria not found")
    return criteria

@router.put("/accumulated-criteria/{accumulated_criteria_id}", response_model=app_models.AccumulatedAnomalyCriteria)
def update_accumulated_criteria(accumulated_criteria_id: int, criteria: AccumulatedAnomalyCriteriaUpdate, db: Session = Depends(get_db)):
    updated_criteria = anomaly_crud.update_accumulated_criteria(db, accumulated_criteria_id, criteria)
    if not updated_criteria:
        raise HTTPException(status_code=404, detail="Accumulated Criteria not found")
    return updated_criteria

@router.delete("/accumulated-criteria/{accumulated_criteria_id}")
def delete_accumulated_criteria(accumulated_criteria_id: int, db: Session = Depends(get_db)):
    deleted = anomaly_crud.delete_accumulated_criteria(db, accumulated_criteria_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Accumulated Criteria not found")
    return {"message": "Accumulated Criteria deleted successfully."}
