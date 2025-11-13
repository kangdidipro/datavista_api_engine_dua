# db_create.py
# This script ONLY creates tables, with an explicit transaction.
import logging
from app.database import engine, Base
from app.models import * # Import all models to ensure Base.metadata is populated

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.info("--- [DB CREATE] Attempting to create all tables within an explicit transaction... ---")
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            Base.metadata.create_all(bind=connection)
            trans.commit()
            logger.info("--- [DB CREATE] All tables created and transaction committed. ---")
        except Exception:
            logger.error("--- [DB CREATE] Error during create_all, rolling back transaction. ---")
            trans.rollback()
            raise
except Exception as e:
    logger.error(f"[FATAL] An error occurred during table creation: {e}", exc_info=True)
    exit(1)

exit(0)