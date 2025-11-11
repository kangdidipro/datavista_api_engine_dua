import os
from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
import json
import redis
import requests
import logging
from datetime import datetime
from urllib.parse import urljoin

# --- Konfigurasi ---
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_broker')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
EXPORT_QUEUE = 'export-queue'
# URL ini adalah bagaimana FastAPI (di dalam Docker venv) akan berkomunikasi dengan Laravel (di dalam Docker Sail)
# 'host.docker.internal' adalah DNS khusus yang merujuk ke mesin host dari dalam container.
LARAVEL_API_BASE = 'http://host.docker.internal:8080/api/'

router = APIRouter(prefix="/v1/export", tags=["Export Jobs"])

# --- Model Pydantic untuk Validasi ---
class ExportRequest(BaseModel):
    summary_id: int
    format: str
    source: str
    filters: Optional[List[Dict[str, Any]]] = None
    log_title: str # Diperlukan untuk nama file

# --- Koneksi Redis ---
try:
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_conn.ping()
    logging.info("Berhasil terhubung ke Redis untuk router ekspor.")
except redis.exceptions.ConnectionError as e:
    logging.error(f"Gagal terhubung ke Redis untuk router ekspor: {e}")
    redis_conn = None

@router.post("/start")
async def start_export_job(request: ExportRequest):
    """
    API untuk memulai pekerjaan ekspor asinkron.
    1. Membuat entri log di Laravel.
    2. Mendorong pekerjaan ke antrian Redis untuk diproses oleh worker.
    """
    if not redis_conn:
        raise HTTPException(status_code=503, detail="Layanan Redis tidak tersedia.")

    job_id = str(uuid.uuid4())
    now = datetime.now().strftime('%Y%m%d_%H%M%S')
    title_slug = "".join(c if c.isalnum() else '_' for c in request.log_title)
    
    file_extension = 'zip' if request.format == 'csv' else request.format
    file_name = f"{title_slug}_{now}.{file_extension}"

    # 1. Panggil API Laravel untuk membuat catatan log awal
    create_log_url = urljoin(LARAVEL_API_BASE, 'export/create-log')
    laravel_payload = {
        "summary_id": request.summary_id,
        "job_id": job_id,
        "format": request.format,
        "file_name": file_name,
        "status": "QUEUED"
    }
    
    try:
        # NOTE: Kita perlu cara untuk meneruskan otentikasi pengguna,
        # untuk saat ini kita asumsikan Laravel API terbuka atau menggunakan token internal.
        # Untuk pengembangan, kita akan mengandalkan cookie sesi yang mungkin diteruskan.
        # Dalam produksi, ini akan memerlukan token API.
        # Untuk sekarang, kita akan coba tanpa header otentikasi eksplisit.
        response = requests.post(create_log_url, json=laravel_payload, timeout=10)
        response.raise_for_status()
        export_log = response.json()
        export_log_id = export_log.get('id')
        if not export_log_id:
            raise HTTPException(status_code=500, detail="Gagal mendapatkan ID log ekspor dari Laravel.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Gagal berkomunikasi dengan Laravel API: {e}")
        raise HTTPException(status_code=502, detail=f"Gagal membuat log di server utama: {e}")

    # 2. Siapkan dan dorong pekerjaan ke Redis
    callback_url = urljoin(LARAVEL_API_BASE, f'export/complete/{export_log_id}')
    
    job_data = {
        'job_id': job_id,
        'summary_id': request.summary_id,
        'format': request.format,
        'source': request.source,
        'filters': request.filters,
        'file_name': file_name,
        'log_title': request.log_title,
        'callback_url': callback_url,
    }

    try:
        redis_conn.lpush(EXPORT_QUEUE, json.dumps(job_data))
        logging.info(f"Pekerjaan ekspor {job_id} berhasil didorong ke antrian.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal mendorong pekerjaan ke Redis: {e}")

    return export_log

@router.get("/status/{job_id}")
async def get_export_status(job_id: str):
    """
    API untuk memeriksa status pekerjaan ekspor dari database.
    """
    query = "SELECT id, user_id, summary_id, job_id, status, file_path, file_name, format, error_message, created_at, updated_at FROM export_logs WHERE job_id = %s"
    
    try:
        # Menggunakan koneksi psycopg2 langsung karena ini adalah query sederhana
        from database import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=requests.packages.urllib3.util.selectors.DictCursor) as cursor:
                cursor.execute(query, (job_id,))
                record = cursor.fetchone()
                if not record:
                    raise HTTPException(status_code=404, detail="Pekerjaan ekspor tidak ditemukan.")
                return dict(record)
    except Exception as e:
        logging.error(f"Gagal mengambil status pekerjaan {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Gagal mengambil status pekerjaan: {e}")
