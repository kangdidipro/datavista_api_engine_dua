# db_seed.py
# This script ONLY seeds data. It assumes tables already exist.
import logging
from app.database import SessionLocal, initialize_default_anomaly_rules

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = None
try:
    logger.info("--- [DB SEED] Attempting to seed initial data... ---")
    db = SessionLocal()
    initialize_default_anomaly_rules(db)
    db.commit()
    logger.info("--- [DB SEED] Initial data seeded successfully. ---")
except Exception as e:
    logger.error(f"[FATAL] An error occurred during data seeding: {e}", exc_info=True)
    if db:
        db.rollback()
    # Exit with a non-zero code to indicate failure
    exit(1)
finally:
    if db:
        db.close()

exit(0)
