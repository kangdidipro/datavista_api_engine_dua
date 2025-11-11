from fastapi import APIRouter, UploadFile, File, HTTPException, status, Header
from fastapi.responses import JSONResponse
from typing import Optional
import os
import uuid
from datetime import datetime

# Import Absolut yang sudah dikoreksi:
from db_config import REDIS_CONFIG
import redis

router = APIRouter(prefix="/v1/process/video", tags=["Video Analytics Queue"])

# --- FUNGSI UTILITY: KIRIM JOB KE REDIS ---

def push_to_redis_queue(job_id: str, video_path: str, task_type: str):
    """
    Simulasi pengiriman job ke Redis Queue (RQ).
    """
    try:
        # REDIS_CONFIG sudah diimpor di tingkat global
        r = redis.Redis(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'])
        job_payload = {
            "job_id": job_id,
            "path": video_path,
            "task": task_type,
            "created_at": str(datetime.now())
        }
        # Gunakan nama queue 'datavista_queue'
        r.lpush('datavista_queue', str(job_payload))
        return True
    except Exception as e:
        print(f"Failed to push job to Redis: {e}")
        return False


# --- ENDPOINT VIDEO PROCESSING ---

@router.post("/{task_type}")
async def process_video_queue(
    task_type: str, # datetime atau license-plate
    api_key: Optional[str] = Header(None, description="API Key Otorisasi"),
    video_file: UploadFile = File(..., description="File video untuk analisis")
):
    """
    API 2 & 3: Menerima file video dan mengirim job ke GPU Worker secara asinkron.
    """
    # 1. Standar Otorisasi (Contoh)
    if not api_key or api_key != "YOUR_SECURE_API_KEY":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Otorisasi gagal.")
        
    # 2. Validasi Tipe Tugas
    if task_type not in ["datetime", "license-plate"]:
        raise HTTPException(status_code=400, detail="Tipe tugas tidak valid. Gunakan 'datetime' atau 'license-plate'.")
        
    # 3. Penyimpanan File Sementara
    # uuid dan os sudah diimpor di tingkat global
    job_id = str(uuid.uuid4())
    temp_dir = "/tmp/datavista_videos" # Harus di-mount ke Worker Docker
    
    # Pastikan direktori sementara ada
    os.makedirs(temp_dir, exist_ok=True)
    
    file_extension = os.path.splitext(video_file.filename)[1]
    file_location = os.path.join(temp_dir, f"{job_id}{file_extension}")

    try:
        # Tulis file ke storage
        with open(file_location, "wb") as f:
            while chunk := await video_file.read(8192): # Baca per chunk
                f.write(chunk)
        
        # 4. Kirim Job ke Redis
        if not push_to_redis_queue(job_id, file_location, task_type):
            # Hapus file lokal jika pengiriman job gagal
            os.remove(file_location)
            raise HTTPException(status_code=503, detail="Gagal terhubung ke Message Broker Redis.")
            
        # 5. Respons Asinkron
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "message": "Pekerjaan analisis video diterima dan diantrikan.",
                "job_id": job_id,
                "status_url": f"/v1/status/{job_id}"
            }
        )
    except Exception as e:
        # Hapus file jika ada kegagalan
        if os.path.exists(file_location):
            os.remove(file_location)
        raise HTTPException(status_code=500, detail=f"Gagal memproses upload: {e}")
