import os
import logging
import json
import time
import requests # Import the requests library
from redis import Redis
from app.database import SessionLocal
from app.anomaly_analyzer import analyze_anomaly_job

# Configure logging for the worker
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = os.getenv('REDIS_PORT', '6379')
redis_conn = Redis(host=redis_host, port=int(redis_port))

# Laravel API configuration
LARAVEL_API_URL = os.getenv('LARAVEL_API_URL')
LARAVEL_API_KEY = os.getenv('LARAVEL_API_KEY')

if not LARAVEL_API_URL or not LARAVEL_API_KEY:
    logger.error("LARAVEL_API_URL or LARAVEL_API_KEY environment variables are not set.")
    # Exit or handle this critical error appropriately
    # For now, we'll just log and continue, but status updates will fail.

# Name of the Redis list where Laravel will push JSON jobs
JOB_QUEUE_KEY = 'anomaly_analysis_queue_json'

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def send_status_update(execution_id: str, status: str, message: str = None):
    """
    Sends a status update to the Laravel API.
    """
    if not LARAVEL_API_URL or not LARAVEL_API_KEY:
        logger.error("Cannot send status update: Laravel API URL or Key is not configured.")
        return

    try:
        headers = {
            "X-API-KEY": LARAVEL_API_KEY,
            "Content-Type": "application/json"
        }
        payload = {
            "execution_id": execution_id,
            "status": status,
            "message": message
        }
        response = requests.post(f"{LARAVEL_API_URL}/api/anomaly-execution/update-status", json=payload, headers=headers)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        logger.info(f"Status update sent for {execution_id}: {status}. Response: {response.json()}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send status update for {execution_id} to Laravel API: {e}", exc_info=True)

def process_job(job_payload: dict):
    """
    Processes a single job payload received from Redis.
    """
    execution_id = job_payload.get('execution_id')
    rules = job_payload.get('rules')

    if execution_id is None or rules is None:
        logger.error(f"Invalid job payload received: {job_payload}")
        return

    logger.info(f"Processing job for execution_id: {execution_id} with rules: {rules}")
    db_generator = get_db()
    db = next(db_generator)
    try:
        analyze_anomaly_job(execution_id, rules, db)
        send_status_update(execution_id, "SUCCESS", "Anomaly analysis completed successfully.")
    except Exception as e:
        error_message = f"Error in processing job for execution_id {execution_id}: {e}"
        logger.error(error_message, exc_info=True)
        send_status_update(execution_id, "FAILED", error_message)
    finally:
        try:
            db_generator.throw(None)
        except StopIteration:
            pass

if __name__ == '__main__':
    logger.info(f"Python worker started, polling Redis list: {JOB_QUEUE_KEY}")
    logger.info(f"Redis connection details: Host={redis_host}, Port={redis_port}, DB=0")
    logger.info(f"Redis connection object: {redis_conn}")
    while True:
        # Blocking pop from the list, waits for a job
        # timeout=1 means it will check every second, allowing for graceful shutdown
        popped_item = redis_conn.brpop(JOB_QUEUE_KEY, timeout=1)
        logger.info(f"brpop returned: {popped_item}")
        
        if popped_item: # Check if something was actually popped
            _, job_json = popped_item # Now it's safe to unpack
            logger.info(f"Received job_json: {job_json}")
            try:
                job_payload = json.loads(job_json)
                process_job(job_payload)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON job from Redis: {job_json}. Error: {e}", exc_info=True)
                # If we can't even decode the JSON, we don't have an execution_id to update status
            except Exception as e:
                logger.error(f"Unhandled error while processing job: {e}", exc_info=True)
                # If an unhandled error occurs before execution_id is extracted, we can't update status
        
        # Small delay to prevent busy-waiting if brpop returns None quickly
        # (though brpop with timeout already handles this to some extent)
        time.sleep(0.1)
