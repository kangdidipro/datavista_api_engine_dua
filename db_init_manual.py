import logging
from app.database import engine, Base, initialize_default_anomaly_rules, SessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    A manual, one-off script to initialize the database within a single session
    to ensure transactional integrity.
    """
    logger.info("--- [MANUAL DB INIT] Starting Manual Database Initialization ---")
    
    db = SessionLocal()
    try:
        logger.info("--- [MANUAL DB INIT] Dropping all tables (if they exist)... ---")
        # Use the session's bound engine for DDL operations
        Base.metadata.drop_all(bind=db.bind)
        logger.info("--- [MANUAL DB INIT] All tables dropped.")

        logger.info("--- [MANUAL DB INIT] Creating all tables... ---")
        Base.metadata.create_all(bind=db.bind)
        logger.info("--- [MANUAL DB INIT] All tables created successfully.")

        logger.info("--- [MANUAL DB INIT] Seeding initial data... ---")
        initialize_default_anomaly_rules(db)
        logger.info("--- [MANUAL DB INIT] Initial data seeded successfully. ---")

        # Commit all changes made in this session
        db.commit()
        logger.info("--- [MANUAL DB INIT] All changes committed. ---")
        
        logger.info("--- [MANUAL DB INIT] Manual Database Initialization Complete. ---")

    except Exception as e:
        logger.error(f"[FATAL] An error occurred during manual DB initialization: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()