import os
from redis import Redis
from rq import Worker, Queue
import logging
import sys

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure /app/api_engine is in sys.path
if '/app/api_engine' not in sys.path:
    sys.path.insert(0, '/app/api_engine')

logger.debug(f"sys.path: {sys.path}")
logger.debug(f"Current working directory: {os.getcwd()}")

try:
    from crud.analysis_crud import run_anomaly_analysis
    logger.debug("Successfully imported run_anomaly_analysis from crud.analysis_crud")
except ImportError as e:
    logger.error(f"Failed to import run_anomaly_analysis from crud.analysis_crud: {e}", exc_info=True)
    sys.exit(1)

try:
    from app.database import SessionLocal
    logger.debug("Successfully imported SessionLocal from app.database")
except ImportError as e:
    logger.error(f"Failed to import SessionLocal from app.database: {e}", exc_info=True)
    sys.exit(1)


listen = ['default']

redis_host = os.getenv('REDIS_HOST', 'redis_broker')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_conn = Redis(host=redis_host, port=redis_port)
logger.debug(f"Connecting to Redis at {redis_host}:{redis_port}")


# Define a wrapper function that RQ can execute when its string name is passed
# The name in the queue will be 'rq_worker_entrypoint.execute_anomaly_analysis_job'
def execute_anomaly_analysis_job(execution_id: str, summary_ids: list, template_id: int):
    logger.info(f"Wrapper function received job for execution_id: {execution_id}, summary_ids: {summary_ids}, template_id: {template_id}")
    db = SessionLocal() # Create a new session for this job
    try:
        result = run_anomaly_analysis(execution_id=execution_id, summary_ids=summary_ids, template_id=template_id, db=db)
        logger.info(f"Anomaly analysis job completed with result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in execute_anomaly_analysis_job for execution_id {execution_id}: {e}", exc_info=True)
        # Re-raise the exception so RQ marks the job as failed
        raise
    finally:
        db.close() # Close the session


if __name__ == '__main__':
    logger.info("RQ Worker Entrypoint starting...")
    queues = [Queue(name, connection=redis_conn) for name in listen]
    worker = Worker(queues, connection=redis_conn, default_result_ttl=5000)
    logger.info("RQ Worker initialized. Starting work...")
    worker.work()
