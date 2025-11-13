import os
import sys
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import logging

# Add /app/api_engine to sys.path to allow imports from app.database and app.models
if '/app/api_engine' not in sys.path:
    sys.path.insert(0, '/app/api_engine')

from app.database import SessionLocal
from app.models import (
    AnomalyTemplateMaster,
    TransactionAnomalyCriteria,
    AccumulatedAnomalyCriteria,
    SpecialAnomalyCriteria,
    TemplateCriteriaVolume,
    TemplateCriteriaAccumulated,
    TemplateCriteriaSpecial
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_default_template_and_link_criteria(db: Session):
    template_role_name = "Default Anomaly Template"
    template_description = "Template default yang mencakup semua kriteria anomali yang tersedia."

    # 1. Delete existing template and linked criteria to ensure a clean state
    logger.info(f"Deleting existing template '{template_role_name}' and its linked criteria...")
    
    # Find the template if it exists using role_name
    existing_template = db.query(AnomalyTemplateMaster).filter_by(role_name=template_role_name).first()
    
    if existing_template:
        template_id_to_delete = existing_template.template_id
        
        # Delete from linker tables first
        db.query(TemplateCriteriaVolume).filter_by(template_id=template_id_to_delete).delete()
        db.query(TemplateCriteriaAccumulated).filter_by(template_id=template_id_to_delete).delete()
        db.query(TemplateCriteriaSpecial).filter_by(template_id=template_id_to_delete).delete()
        
        # Then delete the template itself
        db.delete(existing_template)
        db.commit()
        logger.info(f"Existing template '{template_role_name}' and its linked criteria deleted.")
    else:
        logger.info(f"No existing template '{template_role_name}' found to delete.")

    # 2. Create the default AnomalyTemplateMaster entry
    logger.info(f"Creating new default template: '{template_role_name}'...")
    new_template = AnomalyTemplateMaster(
        role_name=template_role_name,
        description=template_description,
        is_default=True,
        # Removed is_active=True as it's not a column in AnomalyTemplateMaster
    )
    db.add(new_template)
    try:
        db.commit()
        db.refresh(new_template)
        logger.info(f"Default template created with template_id: {new_template.template_id}")
    except IntegrityError:
        db.rollback()
        logger.error(f"Error creating default template '{template_role_name}'. It might already exist.")
        return None
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error creating default template: {e}")
        return None

    # 3. Retrieve all existing criteria
    transaction_criteria = db.query(TransactionAnomalyCriteria).all()
    accumulated_criteria = db.query(AccumulatedAnomalyCriteria).all()
    special_criteria = db.query(SpecialAnomalyCriteria).all()

    # 4. Link criteria to the new template
    logger.info("Linking TransactionAnomalyCriteria to the default template...")
    for criteria in transaction_criteria:
        linker_entry = TemplateCriteriaVolume(
            template_id=new_template.template_id,
            criteria_id=criteria.criteria_id
        )
        db.add(linker_entry)
    
    logger.info("Linking AccumulatedAnomalyCriteria to the default template...")
    for criteria in accumulated_criteria:
        linker_entry = TemplateCriteriaAccumulated(
            template_id=new_template.template_id,
            accumulated_criteria_id=criteria.accumulated_criteria_id
        )
        db.add(linker_entry)

    logger.info("Linking SpecialAnomalyCriteria to the default template...")
    for criteria in special_criteria:
        linker_entry = TemplateCriteriaSpecial(
            template_id=new_template.template_id,
            special_criteria_id=criteria.special_criteria_id
        )
        db.add(linker_entry)

    try:
        db.commit()
        logger.info("All criteria successfully linked to the default template.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error linking criteria to default template: {e}")
        return None

    return new_template.template_id

if __name__ == "__main__":
    db = SessionLocal()
    try:
        template_id = create_default_template_and_link_criteria(db)
        if template_id:
            logger.info(f"Default template setup complete. Use template_id: {template_id}")
        else:
            logger.error("Failed to set up default template.")
    finally:
        db.close()
